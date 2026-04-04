#!/usr/bin/env nextflow

// UniProtKB JSON → JSONL.zst + sorted Parquet data lake
//
// Produces (inside <outdir>/<release>/):
//   sorted.jsonl.zst                  (reviewed+taxid-sorted JSONL archive)
//   lake/entries/entries_*.parquet     (one row per protein)
//   lake/features/features_*.parquet  (one row per feature)
//   lake/xrefs/xrefs_*.parquet        (one row per cross-reference)
//   lake/comments/comments_*.parquet  (one row per comment annotation)
//   lake/references/refs_*.parquet    (one row per citation)
//   lake/manifest.json                (file list, schemas, row counts, sort orders)
//   validation_report.txt             (7-check production validation)
//   provenance.json                   (checksums, git commit, row counts)
//
// Architecture:
//   1. Stream JSON(.gz) → single zstd-compressed JSONL
//   2. Schema drift detection — fast sample (10K rows, ~60s)
//      (catches new/removed/changed fields vs committed schema.json)
//   3. Schema check — full-scan JSONL validation against schema.json
//      (reads every row with declared types; ~30-60 min on 180GB)
//   4. Sort JSONL by reviewed DESC, taxid ASC, acc ASC (Swiss-Prot first)
//   5. Transform sorted JSONL → sorted Parquet tables via DuckDB + PyArrow
//      (pre-sorted input makes DuckDB ORDER BY nearly free for entries;
//       child tables benefit from input locality after LATERAL unnest;
//       features additionally sorted by start_pos for positional queries)
//   6. Production validation: completeness, referential integrity,
//      sort order, round-trip spot checks, Parquet integrity
//   7. Generate provenance manifest (only if validation passes)
//
// Schema management: schema.json is the single source of truth (committed to repo).
// Run `schema_bootstrap.py` once against the full dataset to generate it.
// It is human-readable and manually editable.
// All tables sorted by reviewed DESC, taxid ASC, acc ASC (Swiss-Prot first)
// for query locality via Parquet row group statistics.

nextflow.enable.dsl = 2

/* ── PARAMS ────────────────────────────────────────────────────────── */
params.inputfile      = "${projectDir}/entries_test.json"
params.outdir         = "${projectDir}/results/uniprot_lake"
params.release        = "2026_01"
params.process_memory = '96 GB'   // Total memory for heavy processes (Nextflow directive)
params.duckdb_pct     = 75        // % of process_memory allocated to DuckDB buffer pool
params.schema         = "${projectDir}/schema.json"
params.notify_email   = null      // Email for SLURM failure notifications (null = disabled)
params.duckdb_temp    = null      // DuckDB spill directory (null = use $TMPDIR or /tmp)
params.expected_count = null      // Expected entry count (from UniProt release stats, null = skip)

// Final output directory: <outdir>/<release>/
def release_dir = "${params.outdir}/${params.release}"

// Compute DuckDB memory limit as a fraction of process memory, leaving headroom
// for the Python interpreter, PyArrow heap, and JSON parsing overhead.
import nextflow.util.MemoryUnit
def proc_bytes    = MemoryUnit.of(params.process_memory).toBytes()
def duckdb_bytes  = (long)(proc_bytes * params.duckdb_pct / 100)
def duckdb_memory = "${(int)(duckdb_bytes / (1024*1024*1024))}GB"

// Scripts live in bin/ which Nextflow adds to $PATH automatically.
// This allows the pipeline to run from remote URLs (e.g. GitHub).


/* ── PROCESS: Stream input to single zstd-compressed JSONL ────────── */
// Emits a sidecar entry_count.txt for downstream processes.
// Post-hoc verification: decompresses the output and counts lines
// to catch pipe-level data loss between stream_jsonl and zstd.
// Optionally checks against params.expected_count (from release metadata).
process STREAM_JSONL {
    tag 'stream'
    cpus 4
    memory '4 GB'
    time '48h'

    input:
    path inputfile

    output:
    path "uniprot.jsonl.zst", emit: jsonl
    path "entry_count.txt",   emit: count_file

    script:
    def cpus = task.cpus
    def expected_flag = params.expected_count ? "--expected-count ${params.expected_count}" : ''
    """
    set -euo pipefail
    echo " .-- STREAM_JSONL BEGUN \$(date)"

    pigz -dc ${inputfile} \
        | stream_jsonl.py --count-file entry_count.txt ${expected_flag} \
        | zstd -3 -T${cpus} -o uniprot.jsonl.zst

    # Post-hoc verification: line count of compressed output must match
    # the count stream_jsonl.py wrote.  Catches pipe-level data loss.
    EXPECTED=\$(cat entry_count.txt)
    ACTUAL=\$(zstd -dc uniprot.jsonl.zst | wc -l)
    if [ "\$EXPECTED" != "\$ACTUAL" ]; then
        echo "FATAL: JSONL line count mismatch after compression" >&2
        echo "  stream_jsonl wrote: \$EXPECTED entries" >&2
        echo "  zstd output has:    \$ACTUAL lines" >&2
        exit 1
    fi
    echo "  Verified: \$ACTUAL lines in compressed output match stream_jsonl count"

    echo " '-- STREAM_JSONL ENDED \$(date)"
    """
}


/* ── PROCESS: Detect schema drift before committing to full validation ── */
// Samples 10K rows (fast — under 60s) and compares against schema.json.
// Fails if the data has new fields, removed fields, or type changes.
// This catches the case where UniProt adds a field that read_json(columns={...})
// would silently drop.  Run schema_bootstrap.py to update schema.json.
process SCHEMA_DIFF {
    tag 'schema_diff'
    cpus 2
    memory '4 GB'
    time '10m'

    publishDir "${release_dir}", mode: 'copy', pattern: 'schema_diff.json'

    input:
    path jsonl
    path duckdb_schema

    output:
    path "schema_diff.json", emit: report
    val true,                emit: checked

    script:
    """
    set -euo pipefail
    echo " .-- SCHEMA_DIFF BEGUN \$(date)"

    schema_diff.py ${jsonl} \
        --committed-schema ${duckdb_schema} \
        --sample-size 10000 \
        --strict \
        -o schema_diff.json

    echo " '-- SCHEMA_DIFF ENDED \$(date)"
    """
}


/* ── PROCESS: Pre-flight JSONL validation against schema.json ────── */
process SCHEMA_CHECK {
    tag 'schema_check'
    cpus 4
    memory params.process_memory
    time '4h'
    disk '500 GB'        // DuckDB spill space for full-scan validation

    input:
    path jsonl
    path duckdb_schema

    output:
    val true, emit: validated

    script:
    """
    set -euo pipefail
    echo " .-- SCHEMA_CHECK BEGUN \$(date)"

    schema_check.py ${jsonl} \
        --duckdb-schema ${duckdb_schema} \
        --memory-limit ${duckdb_memory} \
        --threads ${task.cpus} \
        ${params.duckdb_temp ? "--temp-dir ${params.duckdb_temp}" : ''}

    echo " '-- SCHEMA_CHECK ENDED \$(date)"
    """
}


/* ── PROCESS: Sort JSONL by reviewed DESC, taxid ASC, acc ASC ──────────────── */
// Sorts the raw JSONL before the Parquet transform.  Pre-sorted input
// makes DuckDB's ORDER BY nearly free for the entries table and improves
// locality for child table sorts after LATERAL unnest.
// Also the published deliverable for end users.
process SORT_JSONL {
    tag 'sort_jsonl'
    cpus 4
    memory params.process_memory
    time '24h'
    disk '1 TB'          // DuckDB spill space for ORDER BY

    publishDir "${release_dir}", mode: 'copy'

    input:
    path jsonl
    path duckdb_schema
    val schema_ok
    val schema_drift_ok

    output:
    path "sorted.jsonl.zst", emit: sorted_jsonl

    script:
    """
    set -euo pipefail
    echo " .-- SORT_JSONL BEGUN \$(date)"

    sort_jsonl.py ${jsonl} \
        -o sorted.jsonl.zst \
        --schema ${duckdb_schema} \
        --memory-limit ${duckdb_memory} \
        --threads ${task.cpus} \
        ${params.duckdb_temp ? "--temp-dir ${params.duckdb_temp}" : ''}

    echo " '-- SORT_JSONL ENDED \$(date)"
    """
}


/* ── PROCESS: Transform sorted JSONL → Parquet tables ───────────── */
// Stages the sorted JSONL to Parquet once (single JSON parse), then all
// five table queries read from the staged Parquet — roughly 2× faster than
// re-parsing JSON for each table.  DuckDB's ORDER BY on entries is nearly
// a no-op since the input is already in (reviewed DESC, taxid ASC, acc ASC) order.
// Child tables (features, xrefs, comments, references) still need their own sort
// after LATERAL unnest, but benefit from the input locality.
process PARQUET_TRANSFORM {
    tag 'transform'
    cpus 4
    memory params.process_memory
    time '48h'
    disk '2 TB'          // DuckDB ORDER BY spill + Parquet output
    errorStrategy 'retry'
    maxRetries 1

    publishDir "${release_dir}", mode: 'copy', pattern: 'lake'

    input:
    path sorted_jsonl
    path duckdb_schema

    output:
    path "lake", emit: lake

    script:
    """
    set -euo pipefail
    echo " .-- PARQUET_TRANSFORM BEGUN \$(date)"

    parquet_transform.py ${sorted_jsonl} \
        --schema ${duckdb_schema} \
        --outdir ./lake \
        --memory-limit ${duckdb_memory} \
        --threads ${task.cpus} \
        --release ${params.release} \
        --skip-existing \
        ${params.duckdb_temp ? "--temp-dir ${params.duckdb_temp}" : ''}

    echo " '-- PARQUET_TRANSFORM ENDED \$(date)"
    """
}


/* ── PROCESS: Production validation of the Parquet data lake ─────── */
// Validates completeness, uniqueness, referential integrity, sort order,
// round-trip spot checks against the source JSONL, Parquet file integrity,
// and manifest consistency.  Exits 1 on ANY failure.
// Uses the sorted JSONL as ground truth (same entries, same count).
process VALIDATE {
    tag 'validate'
    cpus 2
    memory params.process_memory
    time '4h'

    publishDir "${release_dir}", mode: 'move'

    input:
    path lake
    path sorted_jsonl

    output:
    path "validation_report.txt", emit: report
    val true,                     emit: validated

    script:
    """
    set -euo pipefail
    echo " .-- VALIDATE BEGUN \$(date)"

    validate_lake.py \
        --lake ${lake} \
        --jsonl ${sorted_jsonl} \
        --spot-check-n 1000 \
        -o validation_report.txt

    echo " '-- VALIDATE ENDED \$(date)"
    """
}


/* ── PROCESS: Generate provenance manifest ───────────────────────── */
// PROVENANCE depends on VALIDATE (via validation_ok gate) so that it
// is only published if the data lake passes validation.
process PROVENANCE {
    tag 'provenance'
    cpus 1
    memory '1 GB'

    publishDir "${release_dir}", mode: 'move'

    input:
    path lake
    path sorted_jsonl
    path duckdb_schema
    val validation_ok

    output:
    path "provenance.json", emit: provenance

    script:
    """
    set -euo pipefail

    release_manifest.py \
        --lake ${lake} \
        --input-jsonl ${sorted_jsonl} \
        --schema ${duckdb_schema} \
        --release ${params.release} \
        -o provenance.json
    """
}


/* ── WORKFLOW ─────────────────────────────────────────────────────── */
workflow {
    def W = 57
    def bar = '═' * W
    def line = { String s -> "║  ${s.padRight(W - 2)}║" }
    log.info """
    ╔${bar}╗
    ${line('UniProtKB → Parquet Data Lake')}
    ${line("Release: ${params.release}")}
    ╚${bar}╝
    Input:      ${params.inputfile}
    Schema:     ${params.schema}
    Output:     ${release_dir}
    DuckDB mem: ${duckdb_memory} (${params.duckdb_pct}% of ${params.process_memory})
    DuckDB tmp: ${params.duckdb_temp ?: '(default: \$TMPDIR or /tmp)'}
    Expected:   ${params.expected_count ?: '(not set — post-hoc verification only)'}
    ${'─' * (W + 2)}
    """.stripIndent()

    // Validate inputs
    if (!file(params.inputfile).exists()) {
        log.error "Input file not found: ${params.inputfile}"
        System.exit(2)
    }

    // Value channel so it can be consumed by multiple processes without exhaustion
    schema_ch = Channel.value(file(params.schema))

    // 1. Stream input → single zstd-compressed JSONL
    input_ch = Channel.fromPath(params.inputfile)
    STREAM_JSONL(input_ch)

    // 2. Schema drift detection — fast sample (10K rows, ~60s)
    //    Fails if the data has new/removed/changed fields vs schema.json.
    //    Catches silent field drops before the expensive full-scan.
    SCHEMA_DIFF(STREAM_JSONL.out.jsonl, schema_ch)

    // 3. Schema check — full-scan JSONL validation against schema.json
    //    (gate: sort + transform only run if both checks pass)
    SCHEMA_CHECK(STREAM_JSONL.out.jsonl, schema_ch)

    // 4. Sort JSONL by reviewed DESC, taxid ASC, acc ASC
    //    Pre-sorting means DuckDB's ORDER BY in the transform is nearly free.
    //    Gated on both schema checks passing.
    SORT_JSONL(
        STREAM_JSONL.out.jsonl,
        schema_ch,
        SCHEMA_CHECK.out.validated,
        SCHEMA_DIFF.out.checked,
    )

    // 5. Transform sorted JSONL → Parquet tables
    //    Reads pre-sorted input; entries ORDER BY is a no-op,
    //    child tables benefit from input locality after unnest.
    PARQUET_TRANSFORM(
        SORT_JSONL.out.sorted_jsonl,
        schema_ch,
    )

    // 6. Validate the Parquet lake (uses sorted JSONL as ground truth —
    //    same entries, same count, just reordered)
    VALIDATE(
        PARQUET_TRANSFORM.out.lake,
        SORT_JSONL.out.sorted_jsonl,
    )

    // 7. Generate provenance record (only after validation passes)
    PROVENANCE(
        PARQUET_TRANSFORM.out.lake,
        SORT_JSONL.out.sorted_jsonl,
        schema_ch,
        VALIDATE.out.validated,
    )
}
