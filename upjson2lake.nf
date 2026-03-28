#!/usr/bin/env nextflow

// UniProtKB JSON → JSONL.zst + Iceberg tables (entries, features, xrefs, comments)
//
// Produces (inside <outdir>/<release>/):
//   uniprot.jsonl.zst              (taxid-sorted JSONL archive)
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
//   3. Transform JSONL → Iceberg tables via DuckDB + PyIceberg
//      (Iceberg schemas inferred cheaply via LIMIT 1 at startup)
//   4. Validate row counts + snapshot consistency
//   5. Generate provenance manifest
//
// Schema management: schema.json is the single source of truth (committed to repo).
// Run `schema_bootstrap.py` once against the full dataset to generate it.
// It is human-readable and manually editable.
// No partitioning — Iceberg handles data skipping via per-file column statistics.
// All tables sorted by reviewed, taxid for query locality.

nextflow.enable.dsl = 2

/* ── PARAMS ────────────────────────────────────────────────────────── */
params.inputfile      = "${projectDir}/entries_test.json"
params.outdir         = "${projectDir}/results/uniprot_lake"
params.release        = "2026_01"
params.memory_limit   = '96GB'    // DuckDB memory limit (passed via --memory_limit)
params.schema         = "${projectDir}/schema.json"

// Final output directory: <outdir>/<release>/
def release_dir = "${params.outdir}/${params.release}"

/* ── SCRIPTS ──────────────────────────────────────────────────────── */
stream_script        = "${projectDir}/bin/stream_jsonl.py"
schema_check_script  = "${projectDir}/bin/schema_check.py"
transform_script     = "${projectDir}/bin/iceberg_transform.py"
validate_script      = "${projectDir}/bin/validate_iceberg.py"
manifest_script      = "${projectDir}/bin/release_manifest.py"


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
        | python3 ${stream_script} \
        | zstd -3 -T${cpus} -o uniprot.jsonl.zst

    echo " '-- STREAM_JSONL ENDED \$(date)"
    """
}


/* ── PROCESS: Pre-flight JSONL validation against schema.json ────── */
process SCHEMA_CHECK {
    tag 'schema_check'
    cpus 4
    memory "${params.memory_limit.replace('GB', ' GB')}"
    time '4h'

    input:
    path jsonl
    path duckdb_schema

    output:
    val true, emit: validated

    script:
    """
    set -euo pipefail
    echo " .-- SCHEMA_CHECK BEGUN \$(date)"

    python3 ${schema_check_script} ${jsonl} \
        --duckdb-schema ${duckdb_schema} \
        --memory-limit ${params.memory_limit}

    echo " '-- SCHEMA_CHECK ENDED \$(date)"
    """
}


/* ── PROCESS: Transform JSONL → Iceberg tables ────────────────────── */
process ICEBERG_TRANSFORM {
    tag 'transform'
    cpus 4
    memory "${params.memory_limit.replace('GB', ' GB')}"
    time '48h'

    publishDir "${release_dir}", mode: 'copy', pattern: 'warehouse'
    publishDir "${release_dir}", mode: 'copy', pattern: 'catalog.db'
    publishDir "${release_dir}", mode: 'copy', pattern: 'uniprot.jsonl.zst'

    input:
    path jsonl
    path duckdb_schema
    val schema_ok

    output:
    path "warehouse",         emit: warehouse
    path "catalog.db",        emit: catalog
    path "uniprot.jsonl.zst", emit: jsonl

    script:
    """
    set -euo pipefail
    echo " .-- ICEBERG_TRANSFORM BEGUN \$(date)"

    python3 ${transform_script} ${jsonl} \
        --schema ${duckdb_schema} \
        --catalog-uri sqlite:///catalog.db \
        --warehouse ./warehouse \
        --namespace uniprotkb \
        --memory-limit ${params.memory_limit} \
        --release ${params.release} \
        --sorted-jsonl uniprot.jsonl.zst

    echo " '-- ICEBERG_TRANSFORM ENDED \$(date)"
    """
}


/* ── PROCESS: Validate the Iceberg data lake ──────────────────────── */
process VALIDATE {
    tag 'validate'
    cpus 1
    memory '4 GB'

    publishDir "${release_dir}", mode: 'copy'

    input:
    path warehouse
    path catalog

    output:
    path "validation_report.txt", emit: report

    script:
    """
    set -euo pipefail

    python3 ${validate_script} \
        --catalog-uri sqlite:///${catalog} \
        --warehouse ${warehouse} \
        --namespace uniprotkb \
        -o validation_report.txt
    """
}


/* ── PROCESS: Generate provenance manifest ───────────────────────── */
process MANIFEST {
    tag 'manifest'
    cpus 1
    memory '1 GB'

    publishDir "${release_dir}", mode: 'copy'

    input:
    path warehouse
    path catalog
    path jsonl
    path duckdb_schema

    output:
    path "manifest.json", emit: manifest

    script:
    """
    set -euo pipefail

    python3 ${manifest_script} \
        --catalog-uri sqlite:///${catalog} \
        --warehouse ${warehouse} \
        --namespace uniprotkb \
        --input-jsonl ${jsonl} \
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
    DuckDB mem: ${params.memory_limit}
    ─────────────────────────────────────────────────────────────
    """.stripIndent()

    // Validate inputs
    if (!file(params.inputfile).exists()) {
        log.error "Input file not found: ${params.inputfile}"
        System.exit(2)
    }

    schema_ch = Channel.fromPath(params.schema)

    // 1. Stream input → single zstd-compressed JSONL
    input_ch = Channel.fromPath(params.inputfile)
    STREAM_JSONL(input_ch)

    // 2. Schema check — full-scan JSONL validation against schema.json
    //    (gate: transform only runs if validation passes)
    SCHEMA_CHECK(STREAM_JSONL.out.jsonl, schema_ch)

    // 3. Transform → Iceberg tables (DuckDB reads + PyIceberg writes)
    //    Iceberg schemas are inferred cheaply (LIMIT 1) at startup
    ICEBERG_TRANSFORM(
        STREAM_JSONL.out.jsonl,
        schema_ch,
        SCHEMA_CHECK.out.validated,
    )

    // 4. Validate the Iceberg lake
    VALIDATE(
        ICEBERG_TRANSFORM.out.warehouse,
        ICEBERG_TRANSFORM.out.catalog,
    )

    // 5. Generate provenance manifest
    MANIFEST(
        ICEBERG_TRANSFORM.out.warehouse,
        ICEBERG_TRANSFORM.out.catalog,
        ICEBERG_TRANSFORM.out.jsonl,
        schema_ch,
    )
}
