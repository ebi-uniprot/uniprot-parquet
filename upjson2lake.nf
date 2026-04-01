#!/usr/bin/env nextflow

// UniProtKB JSON → JSONL.zst + Iceberg tables (entries, features, xrefs, comments)
//
// Produces (inside <outdir>/<release>/):
//   sorted.jsonl.zst               (reviewed+taxid-sorted JSONL archive)
//   warehouse/uniprotkb/entries/   (Iceberg table: one row per protein)
//   warehouse/uniprotkb/features/  (Iceberg table: one row per feature)
//   warehouse/uniprotkb/xrefs/     (Iceberg table: one row per xref)
//   warehouse/uniprotkb/comments/  (Iceberg table: one row per comment)
//   catalog.db                     (SQLite Iceberg catalog)
//   validation_report.txt          (row counts, snapshot IDs)
//   manifest.json                  (provenance: checksums, git, row counts)
//
// Architecture:
//   1. Stream JSON(.gz) → single zstd-compressed JSONL
//   2. Schema check — full-scan JSONL validation against schema.json
//      (reads every row with declared types; ~30-60 min on 180GB)
//   3. Sort JSONL by reviewed DESC, taxid ASC (Swiss-Prot first)
//   4. Transform sorted JSONL → Iceberg tables via DuckDB + PyIceberg
//      (pre-sorted input makes DuckDB ORDER BY nearly free for entries;
//       child tables benefit from input locality after LATERAL unnest)
//   5. Production validation: completeness, referential integrity,
//      sort order, round-trip spot checks, Parquet integrity
//   6. Generate provenance manifest (only if validation passes)
//
// Schema management: schema.json is the single source of truth (committed to repo).
// Run `schema_bootstrap.py` once against the full dataset to generate it.
// It is human-readable and manually editable.
// No partitioning — Iceberg handles data skipping via per-file column statistics.
// All tables sorted by reviewed DESC, taxid ASC (Swiss-Prot first) for query locality.

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
process STREAM_JSONL {
    tag 'stream'
    cpus 4
    memory '4 GB'
    time '48h'

    input:
    path inputfile

    output:
    path "uniprot.jsonl.zst", emit: jsonl

    script:
    def cpus = task.cpus
    """
    set -euo pipefail
    echo " .-- STREAM_JSONL BEGUN \$(date)"

    pigz -dc ${inputfile} \
        | stream_jsonl.py \
        | zstd -3 -T${cpus} -o uniprot.jsonl.zst

    echo " '-- STREAM_JSONL ENDED \$(date)"
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


/* ── PROCESS: Sort JSONL by reviewed DESC, taxid ASC ──────────────── */
// Sorts the raw JSONL before the Iceberg transform.  Pre-sorted input
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


/* ── PROCESS: Transform sorted JSONL → Iceberg tables ────────────── */
// Reads the pre-sorted JSONL.  DuckDB's ORDER BY on entries is nearly
// a no-op since the input is already in (reviewed DESC, taxid ASC) order.
// Child tables (features, xrefs, comments) still need their own sort
// after LATERAL unnest, but benefit from the input locality.
process ICEBERG_TRANSFORM {
    tag 'transform'
    cpus 4
    memory params.process_memory
    time '48h'
    disk '2 TB'          // DuckDB ORDER BY spill + Parquet output
    errorStrategy 'retry'
    maxRetries 1

    publishDir "${release_dir}", mode: 'copy', pattern: 'warehouse'
    publishDir "${release_dir}", mode: 'copy', pattern: 'catalog.db'

    input:
    path sorted_jsonl
    path duckdb_schema

    output:
    path "warehouse",    emit: warehouse
    path "catalog.db",   emit: catalog

    script:
    """
    set -euo pipefail
    echo " .-- ICEBERG_TRANSFORM BEGUN \$(date)"

    iceberg_transform.py ${sorted_jsonl} \
        --schema ${duckdb_schema} \
        --catalog-uri sqlite:///catalog.db \
        --warehouse ./warehouse \
        --namespace uniprotkb \
        --memory-limit ${duckdb_memory} \
        --threads ${task.cpus} \
        --release ${params.release} \
        --skip-existing \
        ${params.duckdb_temp ? "--temp-dir ${params.duckdb_temp}" : ''}

    echo " '-- ICEBERG_TRANSFORM ENDED \$(date)"
    """
}


/* ── PROCESS: Production validation of the Iceberg data lake ─────── */
// Validates completeness, uniqueness, referential integrity, sort order,
// round-trip spot checks against the source JSONL, Parquet file integrity,
// and snapshot sanity.  Exits 1 on ANY failure.
// Uses the sorted JSONL as ground truth (same entries, same count).
process VALIDATE {
    tag 'validate'
    cpus 2
    memory params.process_memory
    time '4h'

    publishDir "${release_dir}", mode: 'move'

    input:
    path warehouse
    path catalog
    path sorted_jsonl

    output:
    path "validation_report.txt", emit: report
    val true,                     emit: validated

    script:
    """
    set -euo pipefail
    echo " .-- VALIDATE BEGUN \$(date)"

    validate_iceberg.py \
        --catalog-uri sqlite:///${catalog} \
        --warehouse ${warehouse} \
        --jsonl ${sorted_jsonl} \
        --namespace uniprotkb \
        --spot-check-n 1000 \
        -o validation_report.txt

    echo " '-- VALIDATE ENDED \$(date)"
    """
}


/* ── PROCESS: Generate provenance manifest ───────────────────────── */
// MANIFEST depends on VALIDATE (via validation_ok gate) so that a manifest
// is only published if the data lake passes validation.
process MANIFEST {
    tag 'manifest'
    cpus 1
    memory '1 GB'

    publishDir "${release_dir}", mode: 'move'

    input:
    path warehouse
    path catalog
    path sorted_jsonl
    path duckdb_schema
    val validation_ok

    output:
    path "manifest.json", emit: manifest

    script:
    """
    set -euo pipefail

    release_manifest.py \
        --catalog-uri sqlite:///${catalog} \
        --warehouse ${warehouse} \
        --namespace uniprotkb \
        --input-jsonl ${sorted_jsonl} \
        --schema ${duckdb_schema} \
        --release ${params.release} \
        -o manifest.json
    """
}


/* ── WORKFLOW ─────────────────────────────────────────────────────── */
workflow {
    log.info """
    ╔═══════════════════════════════════════════════════════════╗
    ║  UniProtKB → Iceberg Data Lake                            ║
    ║  Release: ${params.release}                               ║
    ╚═══════════════════════════════════════════════════════════╝
    Input:      ${params.inputfile}
    Schema:     ${params.schema}
    Output:     ${release_dir}
    DuckDB mem: ${duckdb_memory} (${params.duckdb_pct}% of ${params.process_memory})
    DuckDB tmp: ${params.duckdb_temp ?: '(default: \$TMPDIR or /tmp)'}
    ─────────────────────────────────────────────────────────────
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

    // 2. Schema check — full-scan JSONL validation against schema.json
    //    (gate: sort + transform only run if validation passes)
    SCHEMA_CHECK(STREAM_JSONL.out.jsonl, schema_ch)

    // 3. Sort JSONL by reviewed DESC, taxid ASC
    //    Pre-sorting means DuckDB's ORDER BY in the transform is nearly free.
    SORT_JSONL(
        STREAM_JSONL.out.jsonl,
        schema_ch,
        SCHEMA_CHECK.out.validated,
    )

    // 4. Transform sorted JSONL → Iceberg tables
    //    Reads pre-sorted input; entries ORDER BY is a no-op,
    //    child tables benefit from input locality after unnest.
    ICEBERG_TRANSFORM(
        SORT_JSONL.out.sorted_jsonl,
        schema_ch,
    )

    // 5. Validate the Iceberg lake (uses sorted JSONL as ground truth —
    //    same entries, same count, just reordered)
    VALIDATE(
        ICEBERG_TRANSFORM.out.warehouse,
        ICEBERG_TRANSFORM.out.catalog,
        SORT_JSONL.out.sorted_jsonl,
    )

    // 6. Generate provenance manifest (only after validation passes)
    MANIFEST(
        ICEBERG_TRANSFORM.out.warehouse,
        ICEBERG_TRANSFORM.out.catalog,
        SORT_JSONL.out.sorted_jsonl,
        schema_ch,
        VALIDATE.out.validated,
    )
}
