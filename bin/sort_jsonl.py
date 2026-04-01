#!/usr/bin/env python3
"""
Sort a UniProtKB JSONL(.zst) file by reviewed status (Swiss-Prot first)
then organism taxon ID.

Uses DuckDB's out-of-core sorting — spills to disk when the dataset exceeds
memory, so it handles the full 160GB+ UniProtKB in bounded memory.

Output is a zstd-compressed JSONL file with the same content, reordered.
DuckDB handles JSON serialisation and zstd compression natively via COPY.

Usage:
    sort_jsonl.py <input.jsonl.zst> -o <sorted.jsonl.zst> \
        [--schema schema.json] \
        [--memory-limit 16GB] \
        [--threads 4] \
        [--temp-dir /path/to/scratch]
"""

import os
import sys
import argparse
import time

# Allow importing from the same bin/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from iceberg_transform import build_read_clause, init_duckdb, _sql_escape


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


SORT_SQL = """
SELECT *
FROM {read_clause} e
ORDER BY
    CASE WHEN e.entryType LIKE '%Swiss-Prot%'
         THEN true ELSE false END DESC,
    e.organism.taxonId
"""


def main():
    parser = argparse.ArgumentParser(
        description="Sort UniProtKB JSONL by reviewed status then taxon ID"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "-o", "--output", required=True,
        help="Output sorted JSONL(.zst) file",
    )
    parser.add_argument(
        "--schema", default=None,
        help="Committed DuckDB schema JSON (from schema_bootstrap.py). "
             "If omitted, DuckDB auto-detects.",
    )
    parser.add_argument("--memory-limit", default="16GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    parser.add_argument(
        "--temp-dir", default=None,
        help="DuckDB spill directory. Defaults to $TMPDIR or /tmp/duckdb_temp.",
    )
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    output_path = os.path.abspath(args.output)

    eprint("=" * 60)
    eprint("UniProtKB JSONL Sort")
    eprint("=" * 60)
    eprint(f"  Input:  {jsonl_path}")
    eprint(f"  Output: {output_path}")
    eprint(f"  Memory: {args.memory_limit}")
    eprint()

    con = init_duckdb(args.memory_limit, args.threads, args.temp_dir)
    read_clause = build_read_clause(jsonl_path, args.schema)

    sql = SORT_SQL.format(read_clause=read_clause)
    safe_output = _sql_escape(output_path)

    eprint("Sorting (reviewed DESC, taxid ASC) and writing...")
    eprint("  DuckDB will spill to disk if the dataset exceeds memory.")
    t0 = time.time()

    con.sql(f"""
        COPY ({sql}) TO '{safe_output}'
        (FORMAT JSON, COMPRESSION ZSTD)
    """)

    elapsed = time.time() - t0
    eprint(f"  Done in {elapsed:.1f}s")

    # Quick sanity: report output size
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    eprint(f"  Output: {size_mb:.1f} MB")
    eprint("=" * 60)


if __name__ == "__main__":
    main()
