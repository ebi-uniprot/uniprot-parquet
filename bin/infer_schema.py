#!/usr/bin/env python3
"""
Infer DuckDB schema from a JSONL(.gz) file using sample_size=-1 (full scan).

Outputs a JSON file mapping column_name → DuckDB_type.
DuckDB reads .jsonl.gz natively.

Usage:
    infer_schema.py <input.jsonl[.gz]> -o <schema.json>
"""

import os
import sys
import json
import argparse
import time
import duckdb


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main():
    parser = argparse.ArgumentParser(description='Infer DuckDB schema from JSONL')
    parser.add_argument('input', help='Input JSONL(.gz) file')
    parser.add_argument('-o', '--output', default='schema.json', help='Output schema JSON')
    parser.add_argument('--memory-limit', default='8GB', help='DuckDB memory limit')
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)

    con = duckdb.connect()
    con.sql(f"SET memory_limit='{args.memory_limit}'")

    eprint(f"Inferring schema from {jsonl_path} (full scan, sample_size=-1)...")
    t0 = time.time()

    result = con.sql(f"""
        DESCRIBE SELECT * FROM read_json('{jsonl_path}',
            maximum_object_size = 568435456,
            format='newline_delimited',
            sample_size=-1
        )
    """).fetchall()

    schema = {row[0]: row[1] for row in result}

    with open(args.output, 'w') as f:
        json.dump(schema, f, indent=2)

    eprint(f"  {len(schema)} columns inferred in {time.time()-t0:.1f}s")
    eprint(f"  Written to {args.output}")


if __name__ == '__main__':
    main()
