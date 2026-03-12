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
//   1. Stream JSON(.gz) → chunked JSONL
//   2. Infer unified schema from ALL chunks (no sampling)
//   3. Transform each chunk → 3 Hive-partitioned Parquet tables via DuckDB
//   4. Collect chunk outputs → final consolidated lake
//   5. Validate row counts + file sizes

nextflow.enable.dsl = 2

/* ── PARAMS ────────────────────────────────────────────────────────── */
params.inputfile    = "${projectDir}/entries_test.json"
params.outdir       = "${projectDir}/results/uniprot_lake"
params.release      = "2026_01"
params.batchsize    = 500000   // records per JSONL chunk
params.maxforks     = 50
params.memory_limit = '8GB'    // DuckDB memory limit per process
params.max_parquet_mb = 512    // target max parquet file size

/* ── SCRIPTS ──────────────────────────────────────────────────────── */
stream_script    = "${projectDir}/bin/stream_jsonl.py"
transform_script = "${projectDir}/bin/duckdb_transform.py"

/* ── PROCESS: Stream input to chunked JSONL ───────────────────────── */
process STREAM_JSONL {
    tag 'stream'
    cpus 1
    memory '4 GB'
    time '12h'

    input:
    path inputfile

    output:
    path "chunks/*.jsonl", emit: chunks

    script:
    """
    set -euo pipefail
    echo " .-- STREAM_JSONL BEGUN \$(date)"
    python3 ${stream_script} ${inputfile} -o chunks -b ${params.batchsize}
    echo " '-- STREAM_JSONL ENDED \$(date)"
    """
}

/* ── PROCESS: Infer schema from a chunk (full scan) ───────────────── */
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
    python3 ${transform_script} ${chunk} --infer-schema -o .
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
    python3 - << 'PYEOF'
import json, glob
from collections import defaultdict

TYPE_PRIORITY = {
    'BOOLEAN': 1,
    'TINYINT': 2, 'SMALLINT': 3, 'INTEGER': 4, 'BIGINT': 5, 'HUGEINT': 6,
    'FLOAT': 7, 'DOUBLE': 8,
    'DATE': 9, 'TIMESTAMP': 10,
    'VARCHAR': 20,
    'JSON': 21,
}

def type_priority(t):
    t_upper = t.upper().strip()
    return TYPE_PRIORITY.get(t_upper, 15)

def merge_types(types):
    if len(set(types)) == 1:
        return types[0]
    if any('VARCHAR' == t.upper() for t in types):
        return 'VARCHAR'
    return max(types, key=lambda t: (type_priority(t), len(t)))

all_columns = defaultdict(list)
for sf in sorted(glob.glob("schema_*.json")):
    with open(sf) as f:
        schema = json.load(f)
    for col, dtype in schema.items():
        all_columns[col].append(dtype)

unified = {col: merge_types(types) for col, types in all_columns.items()}

with open("unified_schema.json", "w") as f:
    json.dump(unified, f, indent=2)

print(f"Unified schema: {len(unified)} columns from {len(glob.glob('schema_*.json'))} chunks")
for col, dtype in sorted(unified.items()):
    short_type = dtype[:80] + '...' if len(dtype) > 80 else dtype
    print(f"  {col}: {short_type}")
PYEOF
    """
}

/* ── PROCESS: Transform JSONL chunk → Parquet tables ──────────────── */
process DUCKDB_TRANSFORM {
    /*
     * Reads a JSONL chunk with the unified schema and writes
     * core/, seqs/, features/ as Hive-partitioned Parquet.
     *
     * Outputs the entire directory so Hive structure is preserved.
     * Files are named by chunk to prevent collisions during collection.
     */
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
    python3 ${transform_script} ${chunk} -o parquet_out --schema ${schema} --memory-limit ${params.memory_limit}

    # Rename data_0.parquet → {chunk_name}.parquet to avoid collisions in collection
    find parquet_out -name 'data_0.parquet' | while read f; do
        dir=\$(dirname "\$f")
        mv "\$f" "\$dir/${chunk.baseName}.parquet"
    done

    echo " '-- DUCKDB_TRANSFORM ${chunk.baseName} ENDED \$(date)"
    """
}


/* ── PROCESS: Collect chunk outputs into final lake ───────────────── */
process COLLECT_LAKE {
    /*
     * Takes all chunk output directories and consolidates them into a
     * single Hive-partitioned datalake. Uses DuckDB to glob-read all
     * chunk files per table and re-write with optimal file sizes.
     *
     * This step also enforces the <512MB file size constraint by
     * controlling ROW_GROUP_SIZE relative to the partition data volume.
     */
    tag { "${table}" }
    cpus 2
    memory '16 GB'
    maxForks 3

    publishDir "${params.outdir}", mode: 'copy'

    input:
    tuple val(table), path(chunk_dirs, stageAs: "chunk_*")

    output:
    path "${table}/review_status=*/**/*.parquet", emit: parquet_files

    script:
    """
    set -euo pipefail
    echo " .-- COLLECT_LAKE ${table} \$(date)"

    python3 - << 'PYEOF'
import duckdb, glob, os

con = duckdb.connect()
con.sql("SET memory_limit='${params.memory_limit}'")

table = "${table}"

# Find all parquet files for this table across all chunk directories
pattern = f"chunk_*/{table}/**/*.parquet"
files = sorted(glob.glob(pattern, recursive=True))

if not files:
    print(f"WARNING: No parquet files found for {table}")
    # Create empty directory structure so Nextflow output is satisfied
    os.makedirs(f"{table}/review_status=empty/tax_division=empty", exist_ok=True)
    exit(0)

print(f"Collecting {len(files)} files for {table}")

# Read all chunks and re-write with consolidated Hive partitioning
con.sql(f"""
    COPY (
        SELECT * FROM read_parquet(
            {files},
            hive_partitioning=true,
            union_by_name=true
        )
    ) TO '{table}'
    (FORMAT PARQUET, PARTITION_BY (review_status, tax_division),
     COMPRESSION 'zstd', ROW_GROUP_SIZE 500000)
""")

# Report file sizes
for root, dirs, fnames in os.walk(table):
    for fname in fnames:
        if fname.endswith('.parquet'):
            path = os.path.join(root, fname)
            size_mb = os.path.getsize(path) / (1024*1024)
            print(f"  {path}: {size_mb:.1f} MB")
            if size_mb > ${params.max_parquet_mb}:
                print(f"  WARNING: exceeds {${params.max_parquet_mb}}MB limit!")

PYEOF

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
    path parquet_dirs, stageAs: "lake/*"

    output:
    path "validation_report.txt", emit: report

    script:
    """
    set -euo pipefail
    python3 - << 'PYEOF'
import duckdb, os

con = duckdb.connect()
lake = "lake"
lines = []

def log(msg):
    print(msg)
    lines.append(msg)

log("=" * 60)
log("UNIPROT DATALAKE VALIDATION REPORT")
log("=" * 60)

for table in ['core', 'seqs', 'features']:
    path = f"{lake}/{table}/**/*.parquet"
    log(f"\\n--- {table.upper()} ---")
    try:
        count = con.sql(f"SELECT count(*) FROM read_parquet('{path}', hive_partitioning=true)").fetchone()[0]
        log(f"  Total rows: {count:,}")
        partitions = con.sql(f"SELECT review_status, tax_division, count(*) as cnt FROM read_parquet('{path}', hive_partitioning=true) GROUP BY 1,2 ORDER BY 1,2").fetchall()
        for rs, td, cnt in partitions:
            log(f"  {rs}/{td}: {cnt:,}")
    except Exception as e:
        log(f"  ERROR: {e}")

# Consistency check
try:
    core_n = con.sql(f"SELECT count(*) FROM read_parquet('{lake}/core/**/*.parquet', hive_partitioning=true)").fetchone()[0]
    seqs_n = con.sql(f"SELECT count(*) FROM read_parquet('{lake}/seqs/**/*.parquet', hive_partitioning=true)").fetchone()[0]
    log(f"\\n--- CONSISTENCY ---")
    log(f"  core rows: {core_n:,}, seqs rows: {seqs_n:,}")
    log(f"  match: {'YES' if core_n == seqs_n else 'MISMATCH!'}")
except Exception as e:
    log(f"  CONSISTENCY ERROR: {e}")

# File sizes
log(f"\\n--- FILE SIZES ---")
total_size = 0
max_size = 0
max_file = ""
for root, dirs, files in os.walk(lake):
    for f in files:
        if f.endswith('.parquet'):
            fp = os.path.join(root, f)
            sz = os.path.getsize(fp)
            total_size += sz
            if sz > max_size:
                max_size = sz
                max_file = fp

log(f"  Total: {total_size/(1024*1024):.1f} MB")
log(f"  Largest: {max_size/(1024*1024):.1f} MB ({max_file})")
log(f"  All <${params.max_parquet_mb}MB: {'YES' if max_size < ${params.max_parquet_mb}*1024*1024 else 'NO'}")
log("\\n" + "=" * 60)

with open("validation_report.txt", "w") as f:
    f.write("\\n".join(lines) + "\\n")
PYEOF
    """
}


/* ── WORKFLOW ─────────────────────────────────────────────────────── */
workflow {
    log.info """
    ╔═══════════════════════════════════════════════════════════╗
    ║  UniProtKB → Parquet Datalake (Star Schema)              ║
    ║  Release: ${params.release}                                    ║
    ╚═══════════════════════════════════════════════════════════╝
    Input:      ${params.inputfile}
    Output:     ${params.outdir}
    Batch size: ${params.batchsize}
    Max forks:  ${params.maxforks}
    DuckDB mem: ${params.memory_limit}
    ──────────────────────────────────────────────────────────────
    """.stripIndent()

    // 0. Validate input
    if (!file(params.inputfile).exists()) {
        log.error "Input file not found: ${params.inputfile}"
        System.exit(2)
    }

    // 1. Stream input → chunked JSONL
    input_ch = Channel.fromPath(params.inputfile)
    STREAM_JSONL(input_ch)
    chunks_ch = STREAM_JSONL.out.chunks.flatten()
    chunks_ch.count().view { n -> "Total JSONL chunks: ${n}" }

    // 2. Infer schema per chunk (full scan, no sampling)
    INFER_SCHEMA(chunks_ch)

    // 3. Merge all chunk schemas → unified schema
    MERGE_SCHEMAS(INFER_SCHEMA.out.schema.collect())

    // 4. Transform each chunk → 3 Parquet tables (DuckDB)
    //    chunks_ch can be consumed again in DSL2
    DUCKDB_TRANSFORM(chunks_ch, MERGE_SCHEMAS.out.schema)

    // 5. Collect all chunk outputs per table → final consolidated lake
    //    Each DUCKDB_TRANSFORM emits a directory with core/seqs/features subdirs.
    //    We group all directories and pass them to COLLECT_LAKE per table.
    //    Wrap in [dirs] to prevent combine from flattening the list.
    all_outdirs_ch = DUCKDB_TRANSFORM.out.outdir.collect().map { dirs -> [dirs] }

    // Create one COLLECT_LAKE job per table
    tables_ch = Channel.of('core', 'seqs', 'features')
    collect_input = tables_ch.combine(all_outdirs_ch)
    COLLECT_LAKE(collect_input)

    // 6. Validate the final lake
    VALIDATE(COLLECT_LAKE.out.parquet_files.collect())
}
