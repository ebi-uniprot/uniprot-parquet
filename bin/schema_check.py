#!/usr/bin/env python3
"""
Pre-flight JSONL validation for UniProtKB Iceberg pipeline.

Validates the new release's JSONL data against the committed schema.json
(DuckDB column types).  If every row conforms, exit 0.  If any row has a
type mismatch, DuckDB throws and we exit 1.

This is the "belt and suspenders" that catches type drift even in the very
last of 250M entries — cheap insurance (~30-60 min) vs the 18-30 hour
Iceberg transform.

The Iceberg schema is deterministically derived from schema.json + the SQL
transforms, so validating the JSONL against schema.json is sufficient to
guarantee downstream correctness.

Usage:
    schema_check.py <input.jsonl.zst> \
        --duckdb-schema schema.json \
        [--memory-limit 16GB]
"""

import os
import sys
import argparse
import time

import duckdb

# Allow importing from the same bin/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from iceberg_transform import build_read_clause, init_duckdb


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main():
    parser = argparse.ArgumentParser(
        description="Pre-flight JSONL validation for UniProtKB Iceberg pipeline"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "--duckdb-schema", required=True,
        help="Committed DuckDB schema.json (column name → DuckDB type).",
    )
    parser.add_argument("--memory-limit", default="16GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    # Kept for Nextflow backward compat but unused:
    parser.add_argument("--release", default=None, help="(ignored, kept for CLI compat)")
    parser.add_argument("--schema-dir", default=None, help="(ignored, kept for CLI compat)")
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    schema_path = os.path.abspath(args.duckdb_schema)

    if not os.path.exists(schema_path):
        eprint(f"ERROR: schema file not found: {schema_path}")
        eprint("Run schema_bootstrap.py to generate schema.json first.")
        sys.exit(2)

    eprint("=" * 60)
    eprint("UniProtKB JSONL Validation")
    eprint("=" * 60)
    eprint(f"  Input:  {jsonl_path}")
    eprint(f"  Schema: {schema_path}")

    # ── Init DuckDB ──
    con = init_duckdb(args.memory_limit, args.threads)
    read_clause = build_read_clause(jsonl_path, schema_path)

    # ── Full-scan validation ──
    # read_clause uses explicit column types from schema.json via
    # DuckDB's read_json(columns={...}).  If any entry doesn't conform,
    # DuckDB throws a type error.
    eprint(f"\n--- FULL-SCAN JSONL VALIDATION ---")
    eprint(f"  Reading every row with declared types from {schema_path}...")
    eprint(f"  (This may take 30-60 minutes on the full dataset)")
    t0 = time.time()
    try:
        row_count = con.sql(
            f"SELECT COUNT(*) FROM {read_clause}"
        ).fetchone()[0]
        elapsed = time.time() - t0
        eprint(f"  All {row_count:,} rows conform to schema.json ({elapsed:.1f}s)")
        eprint("\n" + "=" * 60)
        sys.exit(0)
    except Exception as e:
        elapsed = time.time() - t0
        eprint(f"\n  !! JSONL validation FAILED after {elapsed:.1f}s")
        eprint(f"  Error: {e}")
        eprint(f"\n  The data contains entries that don't match schema.json.")
        eprint(f"  Re-run schema_bootstrap.py on the new data to update schema.json.")
        eprint("\n" + "=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
