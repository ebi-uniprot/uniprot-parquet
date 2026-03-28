#!/usr/bin/env python3
"""
One-time DuckDB schema bootstrap for UniProtKB Iceberg pipeline.

Run this ONCE against the full UniProtKB JSONL to generate the authoritative
schema.json — the single source of truth for all downstream processing.

schema.json maps each top-level JSON field to its DuckDB type.  It is used by
DuckDB's read_json(columns={...}) for deterministic parsing, and the Iceberg
schemas are derived from it at transform time (cheap LIMIT 1 inference).

By default, scans EVERY row (sample_size=-1) so that type unification accounts
for all 250M+ entries.  This is the slow step (~30-60 min on 180GB) but only
needs to run once per schema change.

schema.json is human-readable and manually editable.  If you know what fields
will change in a new release, you can update it by hand — adding new fields is
safe (they'll be NULL until the data has them).

Produces:
  - schema.json    DuckDB column types (full-scan inferred)

After running, commit:
    git add schema.json
    git commit -m "chore: bootstrap schema.json from <release>"

Usage:
    schema_bootstrap.py <input.jsonl.zst> \
        [--schema-json-out schema.json] \
        [--release 2026_01] \
        [--existing-duckdb-schema schema.json] \
        [--memory-limit 96GB]

For the full UniProtKB dataset, use at least 96GB memory and run on a
machine with fast I/O (NVMe scratch).
"""

import os
import sys
import json
import argparse
import time

import duckdb

# Allow importing from the same bin/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from iceberg_transform import init_duckdb


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def infer_duckdb_schema(con, jsonl_path, sample_all=True):
    """Infer the DuckDB JSON schema from the JSONL file.

    If sample_all=True, uses sample_size=-1 to scan every row (slow but
    comprehensive).  Otherwise uses DuckDB's default sampling.

    Returns a dict of {field_name: duckdb_type_string}.
    """
    sample_clause = "sample_size=-1" if sample_all else ""
    extra = f", {sample_clause}" if sample_clause else ""

    sql = f"""
        DESCRIBE SELECT * FROM read_json_auto(
            '{jsonl_path}',
            format='newline_delimited',
            maximum_object_size=536870912
            {extra}
        )
    """
    result = con.sql(sql).fetchall()
    # Each row is (column_name, column_type, null, key, default, extra)
    schema = {}
    for row in result:
        col_name = row[0]
        col_type = row[1]
        schema[col_name] = col_type
    return schema


def main():
    parser = argparse.ArgumentParser(
        description="One-time DuckDB schema bootstrap for UniProtKB Iceberg pipeline"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file (use the full dataset)")
    parser.add_argument(
        "--schema-json-out", default=None,
        help="Output path for schema.json. Defaults to schema.json in cwd.",
    )
    parser.add_argument(
        "--release", default=None,
        help="Release label (for informational logging only).",
    )
    parser.add_argument(
        "--existing-duckdb-schema", default=None,
        help="Existing schema.json to use (skips inference if provided).",
    )
    parser.add_argument(
        "--skip-full-scan", action="store_true",
        help="Skip full-scan inference (use default sampling). "
             "Faster but may miss type variations in rare entries.",
    )
    parser.add_argument("--memory-limit", default="96GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    # Kept for backward compat but unused:
    parser.add_argument("--schema-dir", default=None, help="(ignored, kept for CLI compat)")
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    schema_json_out = args.schema_json_out or "schema.json"

    eprint("=" * 60)
    eprint("UniProtKB Schema Bootstrap")
    eprint("=" * 60)
    eprint(f"  Input:       {jsonl_path}")
    eprint(f"  Output:      {schema_json_out}")
    if args.release:
        eprint(f"  Release:     {args.release}")
    eprint(f"  Full scan:   {'no (sampling only)' if args.skip_full_scan else 'yes (every row)'}")
    eprint()

    # ── Init DuckDB ──
    con = init_duckdb(args.memory_limit, args.threads)

    if args.existing_duckdb_schema and os.path.exists(args.existing_duckdb_schema):
        eprint("--- USING EXISTING SCHEMA ---")
        eprint(f"  Loading: {args.existing_duckdb_schema}")
        with open(args.existing_duckdb_schema) as f:
            duckdb_schema = json.load(f)
        eprint(f"  {len(duckdb_schema)} top-level fields")
    else:
        sample_all = not args.skip_full_scan
        mode = "FULL SCAN (every row)" if sample_all else "SAMPLED"
        eprint(f"--- INFERRING DuckDB JSON SCHEMA ({mode}) ---")
        eprint(f"  This {'may take 30-60 minutes on the full dataset' if sample_all else 'should be quick'}...")
        t0 = time.time()

        duckdb_schema = infer_duckdb_schema(con, jsonl_path, sample_all=sample_all)

        elapsed = time.time() - t0
        eprint(f"  {len(duckdb_schema)} top-level fields inferred in {elapsed:.1f}s")

    # Save schema.json
    with open(schema_json_out, "w") as f:
        json.dump(duckdb_schema, f, indent=4)
    eprint(f"  Saved: {schema_json_out}")

    # ── Summary ──
    eprint("\n" + "=" * 60)
    eprint("DONE. Commit this file to the repo:")
    eprint(f"  git add {schema_json_out}")
    eprint(f"  git commit -m 'chore: bootstrap schema.json"
           f"{' for ' + args.release if args.release else ''}'")
    eprint()
    eprint("On future releases, the pipeline will:")
    eprint("  1. Validate every JSONL row against schema.json (full scan, ~30-60 min)")
    eprint("  2. Infer Iceberg schemas cheaply from schema.json + SQL (LIMIT 1, sub-second)")
    eprint("  3. Transform and write Iceberg tables")
    eprint("=" * 60)


if __name__ == "__main__":
    main()
