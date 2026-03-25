#!/usr/bin/env nextflow

// Tue 17 Mar 2026 - v4.0: Apache Iceberg data lake
//
// Pipeline: UniProtKB JSON → JSONL.zst + Iceberg tables (entries + features)
//
// Produces:
//   <outdir>/uniprot.jsonl.zst              (taxid-sorted JSONL archive)
//   <outdir>/warehouse/uniprot/entries/      (Iceberg table: one row per protein)
//   <outdir>/warehouse/uniprot/features/     (Iceberg table: one row per feature)
//   <outdir>/catalog.db                      (SQLite Iceberg catalog)
//   <outdir>/validation_report.txt           (row counts, snapshot IDs)
//
// Architecture:
//   1. Stream JSON(.gz) → single zstd-compressed JSONL
//   2. Transform JSONL → Iceberg tables via DuckDB + PyIceberg
//   3. Validate row counts + snapshot consistency
//
// Schema: defined in bin/iceberg_schema.py (PyIceberg types)
// No partitioning — Iceberg handles data skipping via per-file column statistics.
// Both tables sorted by taxid for organism-level query locality.

nextflow.enable.dsl = 2

/* ── PARAMS ────────────────────────────────────────────────────────── */
params.inputfile      = "${projectDir}/entries_test.json"
params.outdir         = "${projectDir}/results/uniprot_lake"
params.release        = "2026_01"
params.memory_limit   = '16GB'    // DuckDB memory limit
params.schema         = "${projectDir}/schema.json"

/* ── SCRIPTS ──────────────────────────────────────────────────────── */
stream_script        = "${projectDir}/bin/stream_jsonl.py"
transform_script     = "${projectDir}/bin/iceberg_transform.py"
validate_script      = "${projectDir}/bin/validate_iceberg.py"
iceberg_schema_mod   = "${projectDir}/bin/iceberg_schema.py"


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


/* ── PROCESS: Transform JSONL → Iceberg tables ────────────────────── */
process ICEBERG_TRANSFORM {
    tag 'transform'
    cpus 4
    memory '20 GB'
    time '48h'

    publishDir "${params.outdir}", mode: 'copy', pattern: 'warehouse/**'
    publishDir "${params.outdir}", mode: 'copy', pattern: 'catalog.db'
    publishDir "${params.outdir}", mode: 'copy', pattern: 'uniprot.jsonl.zst'

    input:
    path jsonl
    path schema
    path iceberg_schema

    output:
    path "warehouse/**",     emit: warehouse
    path "catalog.db",       emit: catalog
    path "uniprot.jsonl.zst", emit: jsonl

    script:
    """
    set -euo pipefail
    echo " .-- ICEBERG_TRANSFORM BEGUN \$(date)"

    # Make iceberg_schema importable
    export PYTHONPATH=\$(dirname ${iceberg_schema}):\$PYTHONPATH

    python3 ${transform_script} ${jsonl} \
        --schema ${schema} \
        --catalog-uri sqlite:///catalog.db \
        --warehouse ./warehouse \
        --namespace uniprot \
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

    publishDir "${params.outdir}", mode: 'copy'

    input:
    path warehouse
    path catalog
    path iceberg_schema

    output:
    path "validation_report.txt", emit: report

    script:
    """
    set -euo pipefail
    export PYTHONPATH=\$(dirname ${iceberg_schema}):\$PYTHONPATH

    python3 ${validate_script} \
        --catalog-uri sqlite:///${catalog} \
        --namespace uniprot \
        -o validation_report.txt
    """
}


/* ── WORKFLOW ─────────────────────────────────────────────────────── */
workflow {
    log.info """
    ╔═══════════════════════════════════════════════════════════╗
    ║  UniProtKB → Iceberg Data Lake  v4.0                      ║
    ║  Release: ${params.release}                               ║
    ╚═══════════════════════════════════════════════════════════╝
    Input:      ${params.inputfile}
    Schema:     ${params.schema}
    Output:     ${params.outdir}
    DuckDB mem: ${params.memory_limit}
    ─────────────────────────────────────────────────────────────
    """.stripIndent()

    // Validate inputs
    if (!file(params.inputfile).exists()) {
        log.error "Input file not found: ${params.inputfile}"
        System.exit(2)
    }

    schema_ch         = Channel.fromPath(params.schema)
    iceberg_schema_ch = Channel.fromPath(iceberg_schema_mod)

    // 1. Stream input → single zstd-compressed JSONL
    input_ch = Channel.fromPath(params.inputfile)
    STREAM_JSONL(input_ch)

    // 2. Transform → Iceberg tables (DuckDB reads + PyIceberg writes)
    ICEBERG_TRANSFORM(
        STREAM_JSONL.out.jsonl,
        schema_ch,
        iceberg_schema_ch,
    )

    // 3. Validate the Iceberg lake
    VALIDATE(
        ICEBERG_TRANSFORM.out.warehouse,
        ICEBERG_TRANSFORM.out.catalog,
        iceberg_schema_ch,
    )
}
