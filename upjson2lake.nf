#!/usr/bin/env nextflow

//Thu 12 Mar 2026 - v2.0: DuckDB-native star schema pipeline
//
// Pipeline: UniProtKB JSON → Parquet Datalake (Star Schema)
//
// Produces three Hive-partitioned Parquet tables:
//   core/review_status=X/tax_division=Y/*.parquet     (metadata + nested JSON)
//   seqs/review_status=X/tax_division=Y/*.parquet      (acc + sequence)
//   features/review_status=X/tax_division=Y/*.parquet   (unnested positional features)
//
// Architecture:
//   1. Stream JSON(.gz) → zstd-compressed chunked JSONL
//   2. Infer schema per chunk (full scan, no sampling)
//   3. Merge all chunk schemas → unified schema
//   4. Transform each chunk → 3 Hive-partitioned Parquet tables via DuckDB
//   5. Collect chunk outputs → final consolidated lake
//   6. Validate row counts + file sizes

nextflow.enable.dsl = 2

/* ── PARAMS ────────────────────────────────────────────────────────── */
params.inputfile      = "${projectDir}/entries_test.json"
params.outdir         = "${projectDir}/results/uniprot_lake"
params.release        = "2026_01"
params.batchsize      = 500000   // records per JSONL chunk
params.maxforks       = 50
params.memory_limit   = '8GB'    // DuckDB memory limit per process
params.max_parquet_mb = 512      // target max parquet file size

/* ── SCRIPTS ──────────────────────────────────────────────────────── */
stream_script        = "${projectDir}/bin/stream_jsonl.py"
infer_schema_script  = "${projectDir}/bin/infer_schema.py"
merge_schemas_script = "${projectDir}/bin/merge_schemas.py"
transform_script     = "${projectDir}/bin/duckdb_transform.py"
collect_script       = "${projectDir}/bin/collect_lake.py"
validate_script      = "${projectDir}/bin/validate_lake.py"


/* ── PROCESS: Stream input to zstd-compressed chunked JSONL ───────── */
process STREAM_JSONL {
    tag 'stream'
    
    cpus 4
    memory '4 GB'
    time '12h' 

    input:
    path inputfile

    output:
    path "chunks/*.jsonl.zst", emit: chunks

    script:
    def cpus = task.cpus
    """
    set -euo pipefail
    echo " .-- STREAM_JSONL BEGUN \$(date)"

    mkdir -p chunks

    pigz -dc ${inputfile} \\
        | python3 ${stream_script} \\
        | split -l ${params.batchsize} \\
            --filter "zstd -3 -T${cpus} > chunks/\\\$FILE.jsonl.zst" \\
            - chunk_

    echo " '-- STREAM_JSONL ENDED \$(date)"
    """
}

/* ── PROCESS: Infer schema from one chunk (full scan) ─────────────── */
process INFER_SCHEMA {
    tag { "${chunk.baseName}" }
    cpus 1
    memory '4 GB'
    maxForks Math.min(params.maxforks, 20)

    input:
    path chunk

    output:
    path "schema.json", emit: schema

    script:
    """
    set -euo pipefail
    python3 ${infer_schema_script} ${chunk} -o schema.json \
        --memory-limit ${params.memory_limit}
    """
}


/* ── PROCESS: Merge per-chunk schemas into unified schema ─────────── */
process MERGE_SCHEMAS {
    tag 'merge_schemas'
    cpus 1
    memory '2 GB'

    input:
    path schemas, stageAs: "schema_*.json"

    output:
    path "unified_schema.json", emit: schema

    script:
    """
    set -euo pipefail
    python3 ${merge_schemas_script} schema_*.json -o unified_schema.json
    """
}


/* ── PROCESS: Transform JSONL chunk → 3 Parquet tables ────────────── */
process DUCKDB_TRANSFORM {
    tag { "${chunk.baseName}" }
    cpus 2
    memory '8 GB'
    maxForks Math.min(params.maxforks, 50)
    errorStrategy 'retry'
    maxRetries 2

    input:
    path chunk
    path schema

    output:
    path "parquet_out", emit: outdir

    script:
    """
    set -euo pipefail
    echo " .-- DUCKDB_TRANSFORM ${chunk.baseName} attempt ${task.attempt} \$(date)"
    python3 ${transform_script} ${chunk} --schema ${schema} -o parquet_out \
        --memory-limit ${params.memory_limit}

    # Rename data_0.parquet → {chunk_name}.parquet to avoid collisions in collection
    find parquet_out -name 'data_0.parquet' | while read f; do
        dir=\$(dirname "\$f")
        mv "\$f" "\$dir/${chunk.simpleName}.parquet"
    done

    echo " '-- DUCKDB_TRANSFORM ${chunk.baseName} ENDED \$(date)"
    """
}


/* ── PROCESS: Collect chunk outputs into final lake table ─────────── */
process COLLECT_LAKE {
    tag { "${table}" }
    cpus 2
    memory '16 GB'
    maxForks 3

    publishDir "${params.outdir}", mode: 'copy'

    input:
    tuple val(table), path(chunk_dirs, stageAs: "chunk_*")

    output:
    path "${table}", emit: table_dir

    script:
    """
    set -euo pipefail
    echo " .-- COLLECT_LAKE ${table} \$(date)"
    python3 ${collect_script} ${table} chunk_* -o . \
        --memory-limit ${params.memory_limit} \
        --max-parquet-mb ${params.max_parquet_mb}
    echo " '-- COLLECT_LAKE ${table} ENDED \$(date)"
    """
}


/* ── PROCESS: Validate the final datalake ─────────────────────────── */
process VALIDATE {
    tag 'validate'
    cpus 1
    memory '4 GB'

    publishDir "${params.outdir}", mode: 'copy'

    input:
    path table_dirs

    output:
    path "validation_report.txt", emit: report

    script:
    """
    set -euo pipefail
    python3 ${validate_script} ${table_dirs} \
        --max-parquet-mb ${params.max_parquet_mb} \
        -o validation_report.txt
    """
}


/* ── WORKFLOW ─────────────────────────────────────────────────────── */
workflow {
    log.info """
    ╔═══════════════════════════════════════════════════════════╗
    ║  UniProtKB → Parquet Datalake (Star Schema)               ║
    ║  Release: ${params.release}                               ║
    ╚═══════════════════════════════════════════════════════════╝
    Input:      ${params.inputfile}
    Output:     ${params.outdir}
    Batch size: ${params.batchsize}
    Max forks:  ${params.maxforks}
    DuckDB mem: ${params.memory_limit}
    ─────────────────────────────────────────────────────────────
    """.stripIndent()

    // 0. Validate input
    if (!file(params.inputfile).exists()) {
        log.error "Input file not found: ${params.inputfile}"
        System.exit(2)
    }

    // 1. Stream input → zstd-compressed chunked JSONL
    input_ch = Channel.fromPath(params.inputfile)
    STREAM_JSONL(input_ch)
    chunks_ch = STREAM_JSONL.out.chunks.flatten()
    chunks_ch.count().view { n -> "Total JSONL chunks: ${n}" }

    // 2. Infer schema per chunk (full scan, no sampling)
    INFER_SCHEMA(chunks_ch)

    // 3. Merge all chunk schemas → unified schema
    MERGE_SCHEMAS(INFER_SCHEMA.out.schema.collect())

    // 4. Transform each chunk → 3 Parquet tables (DuckDB)
    DUCKDB_TRANSFORM(chunks_ch, MERGE_SCHEMAS.out.schema)

    // 5. Collect all chunk outputs per table → final consolidated lake
    all_outdirs_ch = DUCKDB_TRANSFORM.out.outdir.collect().map { dirs -> [dirs] }
    tables_ch = Channel.of('core', 'seqs', 'features')
    collect_input = tables_ch.combine(all_outdirs_ch)
    COLLECT_LAKE(collect_input)

    // 6. Validate the final lake
    //    Collect the 3 table directories (core/, seqs/, features/) into a single list
    VALIDATE(COLLECT_LAKE.out.table_dir.collect())
}
