#!/usr/bin/env python3
"""
Infer DuckDB schema from a JSONL(.zst) file.

By default does a full scan (sample_size=-1).  Use --sample-size to limit.
Use --where to filter rows before inference, e.g. to scan only Swiss-Prot:

    infer_schema.py uniprot.jsonl.zst -o schema.json \
        --where "entryType LIKE '%Swiss-Prot%'"

Strategy for UniProtKB: reviewed (Swiss-Prot) entries are manually curated
and tend to populate every optional field, so scanning all ~570K of them
gives near-complete schema coverage in under a minute.  Optionally combine
with a TrEMBL sample to catch any unreviewed-only fields:

    infer_schema.py uniprot.jsonl.zst -o schema.json \
        --where "entryType LIKE '%Swiss-Prot%'" \
        --supplement-sample 1000000

Usage:
    infer_schema.py <input.jsonl[.zst]> -o <schema.json>
        [--sample-size N] [--where EXPR] [--supplement-sample N]
"""

import os
import sys
import json
import argparse
import time
import duckdb

# Allow importing from the same bin/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parquet_transform import _sql_escape


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


TYPE_PRIORITY = {
    'BOOLEAN': 0, 'TINYINT': 1, 'SMALLINT': 2, 'INTEGER': 3, 'BIGINT': 4,
    'FLOAT': 5, 'DOUBLE': 6, 'VARCHAR': 7, 'JSON': 8,
}


def broader_type(a, b):
    """Return the broader of two DuckDB types.  Prefers longer STRUCT defs."""
    if a == b:
        return a
    pa = TYPE_PRIORITY.get(a)
    pb = TYPE_PRIORITY.get(b)
    if pa is not None and pb is not None:
        return a if pa >= pb else b
    # For STRUCT/LIST types, pick the longer definition (more fields)
    if len(a) >= len(b):
        return a
    return b


def read_json_expr(path, sample_size=-1):
    return (
        f"read_json('{_sql_escape(path)}', "
        f"maximum_object_size=536870912, "
        f"format='newline_delimited', "
        f"sample_size={sample_size})"
    )


def infer_from(con, path, sample_size=-1, where=None):
    """Run DESCRIBE on the JSONL file, optionally with a WHERE filter."""
    rj = read_json_expr(path, sample_size)
    if where:
        # Wrap in a subquery so the WHERE applies during inference
        query = f"DESCRIBE SELECT * FROM (SELECT * FROM {rj} WHERE {where})"
    else:
        query = f"DESCRIBE SELECT * FROM {rj}"
    eprint(f"  Running: {query[:120]}...")
    rows = con.sql(query).fetchall()
    return {row[0]: row[1] for row in rows}


def merge_schemas(primary, supplement):
    """Merge supplement schema into primary, taking the broader type per column."""
    merged = dict(primary)
    for col, dtype in supplement.items():
        if col in merged:
            merged[col] = broader_type(merged[col], dtype)
        else:
            merged[col] = dtype
    return merged


def main():
    parser = argparse.ArgumentParser(
        description='Infer DuckDB schema from JSONL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('input', help='Input JSONL(.zst) file')
    parser.add_argument('-o', '--output', default='schema.json', help='Output schema JSON')
    parser.add_argument('--memory-limit', default='8GB', help='DuckDB memory limit')
    parser.add_argument('--sample-size', type=int, default=-1,
                        help='Number of records to sample (-1 = full scan, default)')
    parser.add_argument('--where', default=None,
                        help="SQL WHERE clause to filter rows, e.g. \"entryType LIKE '%%Swiss-Prot%%'\"")
    parser.add_argument('--supplement-sample', type=int, default=0,
                        help='Additional sample of records NOT matching --where (catches remaining fields)')
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)

    con = duckdb.connect()
    con.sql(f"SET memory_limit='{args.memory_limit}'")

    eprint(f"Inferring schema from {jsonl_path}")
    t0 = time.time()

    # Primary inference (optionally filtered + sampled)
    schema = infer_from(con, jsonl_path,
                        sample_size=args.sample_size,
                        where=args.where)
    eprint(f"  Primary: {len(schema)} columns in {time.time()-t0:.1f}s")

    # Supplement: sample records that DON'T match the primary filter
    if args.supplement_sample > 0 and args.where:
        t1 = time.time()
        neg_where = f"NOT ({args.where})"
        supplement = infer_from(con, jsonl_path,
                                sample_size=args.supplement_sample,
                                where=neg_where)
        eprint(f"  Supplement: {len(supplement)} columns in {time.time()-t1:.1f}s")
        new_cols = set(supplement) - set(schema)
        if new_cols:
            eprint(f"  New columns from supplement: {', '.join(sorted(new_cols))}")
        schema = merge_schemas(schema, supplement)
        eprint(f"  Merged: {len(schema)} columns")

    with open(args.output, 'w') as f:
        json.dump(schema, f, indent=2)

    eprint(f"\n{len(schema)} columns inferred in {time.time()-t0:.1f}s")
    eprint(f"Written to {args.output}")


if __name__ == '__main__':
    main()
