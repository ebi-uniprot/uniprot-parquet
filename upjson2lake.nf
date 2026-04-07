#!/usr/bin/env nextflow

// UniProtKB JSON → JSONL.zst + sorted Parquet data lake
//
// Produces (inside <outdir>/<release>/):
//   sorted.jsonl.zst                  (reviewed+taxid-sorted JSONL archive)
//   lake/entries/entries_*.parquet     (one row per protein)
//   lake/features/features_*.parquet  (one row per feature)
//   lake/xrefs/xrefs_*.parquet        (one row per cross-reference)
//   lake/comments/comments_*.parquet  (one row per comment annotation)
//   lake/publications/publications_*.parquet  (one row per citation)
//   lake/manifest.json                (file list, schemas, row counts, sort orders)
//   validation_report.txt             (12-check production validation)
//   provenance.json                   (checksums, git commit, row counts)
//
// Architecture:
//   1. Stream JSON(.gz) → single zstd-compressed JSONL
//   2. Sort JSONL by reviewed DESC, taxid ASC, acc ASC (Swiss-Prot first)
//   3. Transform sorted JSONL → sorted Parquet tables via DuckDB + PyArrow
//      (DuckDB infers the schema from the data via read_json_auto;
//       pre-sorted input makes DuckDB ORDER BY nearly free for entries;
//       child tables benefit from input locality after LATERAL unnest;
//       features additionally sorted by start_pos for positional queries)
//   4. Production validation: completeness, referential integrity,
//      sort order, round-trip spot checks, Parquet integrity
//   5. Generate provenance manifest (only if validation passes)
//
// Schema management: DuckDB infers types directly from the data via
// read_json_auto.  The data is a JSON dump from production — whatever
// schema it has is what we use.  Each release is a full rebuild.
// All tables sorted by reviewed DESC, taxid ASC, acc ASC (Swiss-Prot first)
// for query locality via Parquet row group statistics.

nextflow.enable.dsl = 2

/* ── PARAMS ────────────────────────────────────────────────────────── */
params.inputfile      = "${projectDir}/tests/fixtures/small.json.gz"
params.outdir         = "${projectDir}/results/uniprot_lake"
params.release        = "2026_01"
params.process_memory = '96 GB'   // Total memory for heavy processes (Nextflow directive)
params.duckdb_pct     = 75        // % of process_memory allocated to DuckDB buffer pool
params.notify_email   = null      // Email for SLURM failure notifications (null = disabled)
params.duckdb_temp    = null      // DuckDB spill directory (null = use $TMPDIR or /tmp)
params.expected_count = null      // Expected entry count (from UniProt release stats, null = skip)

// Final output directory: <outdir>/<release>/
def release_dir = "${params.outdir}/${params.release}"

// DuckDB memory is computed inside each process script block (not here) so that
// it reacts to task.memory on retry — when Nextflow doubles the allocation after
// an OOM, DuckDB's buffer pool scales up with it.

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

    output:
    path "sorted.jsonl.zst", emit: sorted_jsonl

    script:
    """
    set -euo pipefail
    echo " .-- SORT_JSONL BEGUN \$(date)"

    DUCKDB_MEM="\$(python3 -c "print(str(int(${task.memory.toBytes()} * ${params.duckdb_pct} / 100 / 1073741824)) + 'GB')")"

    sort_jsonl.py ${jsonl} \
        -o sorted.jsonl.zst \
        --memory-limit \${DUCKDB_MEM} \
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
// Child tables (features, xrefs, comments, publications) still need their own sort
// after LATERAL unnest, but benefit from the input locality.
//
// NOTE on --skip-existing and retries:
//   --skip-existing is useful for manual re-runs (e.g. fix root cause, then
//   `nextflow run ... -resume`).  It does NOT help with automatic retries
//   because Nextflow provisions a fresh work directory per attempt — partial
//   outputs from a failed attempt are not visible to the retry.  This is a
//   known limitation; splitting into per-table processes would fix it but
//   adds significant complexity for a rare failure mode.
process PARQUET_TRANSFORM {
    tag 'transform'
    cpus 4
    memory params.process_memory
    time '48h'
    disk '2 TB'          // DuckDB ORDER BY spill + Parquet output

    publishDir "${release_dir}", mode: 'copy', pattern: 'lake'

    input:
    path sorted_jsonl

    output:
    path "lake", emit: lake

    script:
    """
    set -euo pipefail
    echo " .-- PARQUET_TRANSFORM BEGUN \$(date)"

    DUCKDB_MEM="\$(python3 -c "print(str(int(${task.memory.toBytes()} * ${params.duckdb_pct} / 100 / 1073741824)) + 'GB')")"

    parquet_transform.py ${sorted_jsonl} \
        --outdir ./lake \
        --memory-limit \${DUCKDB_MEM} \
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
// manifest consistency, and optionally schema evolution against a baseline.
// Exits 1 on ANY failure.  Uses the sorted JSONL as ground truth (same entries, same count).
//
// SCHEMA EVOLUTION GUARD (intentionally disabled):
//   A schema_baseline.json exists in the repo and validate_lake.py supports
//   --schema-baseline, but the check is deliberately not wired in.
//   Reason: if UniProt changes their JSON schema, it's already in production —
//   failing the pipeline doesn't help; we need to adapt our SQL templates.
//   Renamed fields break the SQL directly (DuckDB BinderError), dropped fields
//   are handled by the optional-fields mechanism (NULLed out), and new fields
//   are picked up automatically by read_json_auto.  The one case it could
//   catch (silent type changes) isn't detected by the baseline anyway, since
//   it only compares column names, not types.
//   For cross-release change tracking, compare manifest.json files instead.
process VALIDATE {
    tag 'validate'
    cpus 2
    memory params.process_memory
    time '4h'

    publishDir "${release_dir}", mode: 'copy'

    input:
    path lake
    path sorted_jsonl

    output:
    path "validation_report.txt", emit: report
    val true,                     emit: validated

    script:
    // Schema evolution check deliberately disabled — see comment above VALIDATE process.
    def schema_baseline_flag = ''
    """
    set -euo pipefail
    echo " .-- VALIDATE BEGUN \$(date)"

    validate_lake.py \
        --lake ${lake} \
        --jsonl ${sorted_jsonl} \
        --spot-check-n 1000 \
        ${schema_baseline_flag} \
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

    publishDir "${release_dir}", mode: 'copy'

    input:
    path lake
    path sorted_jsonl
    val validation_ok

    output:
    path "provenance.json", emit: provenance

    script:
    """
    set -euo pipefail

    release_manifest.py \
        --lake ${lake} \
        --input-jsonl ${sorted_jsonl} \
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
    Output:     ${release_dir}
    DuckDB mem: ${params.duckdb_pct}% of task.memory (scales on retry)
    DuckDB tmp: ${params.duckdb_temp ?: '(default: \$TMPDIR or /tmp)'}
    Expected:   ${params.expected_count ?: '(not set — post-hoc verification only)'}
    ${'─' * (W + 2)}
    """.stripIndent()

    // Validate inputs
    if (!file(params.inputfile).exists()) {
        error "Input file not found: ${params.inputfile}"
    }

    // 1. Stream input → single zstd-compressed JSONL
    input_ch = Channel.fromPath(params.inputfile)
    STREAM_JSONL(input_ch)

    // 2. Sort JSONL by reviewed DESC, taxid ASC, acc ASC
    //    Pre-sorting means DuckDB's ORDER BY in the transform is nearly free.
    SORT_JSONL(STREAM_JSONL.out.jsonl)

    // 3. Transform sorted JSONL → Parquet tables
    //    DuckDB infers schema from the data via read_json_auto.
    //    Reads pre-sorted input; entries ORDER BY is a no-op,
    //    child tables benefit from input locality after unnest.
    PARQUET_TRANSFORM(SORT_JSONL.out.sorted_jsonl)

    // 4. Validate the Parquet lake (uses sorted JSONL as ground truth —
    //    same entries, same count, just reordered)
    VALIDATE(
        PARQUET_TRANSFORM.out.lake,
        SORT_JSONL.out.sorted_jsonl,
    )

    // 5. Generate provenance record (only after validation passes)
    PROVENANCE(
        PARQUET_TRANSFORM.out.lake,
        SORT_JSONL.out.sorted_jsonl,
        VALIDATE.out.validated,
    )
}
