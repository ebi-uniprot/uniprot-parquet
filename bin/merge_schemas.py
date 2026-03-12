#!/usr/bin/env python3
"""
Merge multiple per-chunk DuckDB schemas into a single unified schema.

For each column, keeps the broadest type found across all chunks.
This ensures no data loss from schema differences in highly dimensional data.

Usage:
    merge_schemas.py schema_1.json schema_2.json ... -o unified_schema.json
    merge_schemas.py schema_*.json -o unified_schema.json
"""

import sys
import json
import argparse
from collections import defaultdict


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# DuckDB type hierarchy - broader types win when merging
TYPE_PRIORITY = {
    'BOOLEAN': 1,
    'TINYINT': 2, 'SMALLINT': 3, 'INTEGER': 4, 'BIGINT': 5, 'HUGEINT': 6,
    'FLOAT': 7, 'DOUBLE': 8,
    'DATE': 9, 'TIMESTAMP': 10,
    'VARCHAR': 20,
    'JSON': 21,
}


def type_priority(t):
    """Get priority for a type; complex types (STRUCT, LIST) get mid priority."""
    return TYPE_PRIORITY.get(t.upper().strip(), 15)


def merge_types(types):
    """Given a list of types for the same column, pick the broadest."""
    unique = set(types)
    if len(unique) == 1:
        return types[0]
    # VARCHAR is the safest fallback for scalar conflicts
    if any(t.upper() == 'VARCHAR' for t in types):
        return 'VARCHAR'
    # For STRUCT/LIST types, keep the longest (most complete) definition
    return max(types, key=lambda t: (type_priority(t), len(t)))


def main():
    parser = argparse.ArgumentParser(description='Merge per-chunk DuckDB schemas')
    parser.add_argument('schemas', nargs='+', help='Input schema JSON files')
    parser.add_argument('-o', '--output', default='unified_schema.json', help='Output schema')
    args = parser.parse_args()

    all_columns = defaultdict(list)
    for sf in args.schemas:
        with open(sf) as f:
            schema = json.load(f)
        for col, dtype in schema.items():
            all_columns[col].append(dtype)

    unified = {col: merge_types(types) for col, types in all_columns.items()}

    with open(args.output, 'w') as f:
        json.dump(unified, f, indent=2)

    eprint(f"Unified schema: {len(unified)} columns from {len(args.schemas)} chunks")
    for col, dtype in sorted(unified.items()):
        short = dtype[:80] + '...' if len(dtype) > 80 else dtype
        eprint(f"  {col}: {short}")


if __name__ == '__main__':
    main()
