nextflow.enable.dsl = 2

params.outdir = "test_results"

process DUCKDB_SMOKE_TEST {
    publishDir "${params.outdir}", mode: 'copy'

    output:
    path "test.parquet"

    script:
    """
    # 1. Create a dummy JSONL file on the fly
    echo '{"acc": "P12345", "status": "reviewed", "division": "HUM"}' > dummy.jsonl

    # 2. Run a trivial DuckDB command
    duckdb -c "COPY (SELECT * FROM read_json_auto('dummy.jsonl')) TO 'test.parquet' (FORMAT PARQUET);"
    """
}

workflow {
    DUCKDB_SMOKE_TEST()
}