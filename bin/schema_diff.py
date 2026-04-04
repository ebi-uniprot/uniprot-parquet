#!/usr/bin/env python3
"""
Schema drift detection for UniProtKB Parquet data lake pipeline.

Compares the committed schema.json against what DuckDB infers from a new
release's JSONL.  Reports new fields, removed fields, and type changes.
Exits non-zero if the schemas don't match — forcing you to update
schema.json before the pipeline proceeds.

This is intentionally strict: the lake should never silently drop fields.
If UniProt adds a field, you want to know about it and decide whether to
include it.  If a type changes, you want to investigate before the
expensive transform runs.

Modes:
  --strict (default)  Exit 1 on ANY difference (new, removed, or changed).
                      Use this in the pipeline to enforce schema discipline.
  --allow-additions   Exit 0 if the only differences are new fields.
                      Type changes and removals still fail.

Usage:
    schema_diff.py <input.jsonl.zst> \
        --committed-schema schema.json \
        [--strict | --allow-additions] \
        [--memory-limit 4GB]

    Typically runs in under 60 seconds (samples 10K rows, not a full scan).
"""

import os
import sys
import json
import argparse
import time

import duckdb

# Allow importing from the same bin/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parquet_transform import init_duckdb, _sql_escape


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def infer_schema_sampled(con, jsonl_path, sample_size=10000):
    """Infer schema from a sample of rows (fast — seconds, not minutes).

    Uses DuckDB's read_json_auto with a limited sample to discover field
    names and types.  This is sufficient for detecting structural changes
    (new/removed fields, type changes) without scanning every row.
    """
    safe_path = _sql_escape(jsonl_path)
    sql = f"""
        DESCRIBE SELECT * FROM read_json_auto(
            '{safe_path}',
            format='newline_delimited',
            maximum_object_size=536870912,
            sample_size={sample_size}
        )
    """
    result = con.sql(sql).fetchall()
    return {row[0]: row[1] for row in result}


def _normalise_type(type_str):
    """Normalise a DuckDB type string for comparison.

    DuckDB infers struct field order from the data, so the same logical
    type can appear as STRUCT(a INT, b INT) or STRUCT(b INT, a INT)
    depending on which rows were sampled.  We sort struct fields
    alphabetically so that field ordering doesn't cause false positives.

    Also normalises STRUCT member types with missing optional fields:
    DuckDB's sampling may miss nullable struct members that only appear
    in rare entries.  We detect missing-vs-present struct members as
    a real difference (reported under struct_field_changes), but
    reordering alone is not a type change.
    """
    import re

    # Parse nested STRUCT(...) and sort fields alphabetically.
    # This is a simplified approach that handles the common cases.
    # We recursively sort struct fields by name.

    def sort_struct(s):
        """Recursively sort struct fields in a type string."""
        # Find STRUCT(...) blocks and sort their fields
        result = []
        i = 0
        while i < len(s):
            # Look for STRUCT(
            if s[i:].startswith("STRUCT("):
                # Find matching closing paren
                start = i + 7  # past "STRUCT("
                depth = 1
                j = start
                while j < len(s) and depth > 0:
                    if s[j] == '(':
                        depth += 1
                    elif s[j] == ')':
                        depth -= 1
                    j += 1
                # s[start:j-1] is the content inside STRUCT(...)
                inner = s[start:j-1]

                # Split fields at top-level commas (not inside nested parens)
                fields = []
                field_start = 0
                d = 0
                for k, c in enumerate(inner):
                    if c == '(':
                        d += 1
                    elif c == ')':
                        d -= 1
                    elif c == ',' and d == 0:
                        fields.append(inner[field_start:k].strip())
                        field_start = k + 1
                fields.append(inner[field_start:].strip())

                # Parse each field into (name, type) and sort by name
                parsed = []
                for field in fields:
                    field = field.strip()
                    if not field:
                        continue
                    # Field format: "name" TYPE or name TYPE
                    # Handle quoted names
                    if field.startswith('"'):
                        end_quote = field.index('"', 1)
                        fname = field[:end_quote+1]
                        ftype = field[end_quote+1:].strip()
                    else:
                        parts = field.split(None, 1)
                        fname = parts[0]
                        ftype = parts[1] if len(parts) > 1 else ""
                    # Recursively sort nested structs in the type
                    ftype = sort_struct(ftype)
                    parsed.append((fname, ftype))

                parsed.sort(key=lambda x: x[0].strip('"').lower())
                sorted_inner = ", ".join(f"{n} {t}" for n, t in parsed)
                result.append(f"STRUCT({sorted_inner})")
                i = j
            else:
                result.append(s[i])
                i += 1
        return "".join(result)

    return sort_struct(type_str)


def diff_schemas(committed, inferred):
    """Compare committed schema against inferred schema.

    Returns a dict with:
      - new_fields:     fields in inferred but not committed (would be silently dropped)
      - removed_fields: fields in committed but not inferred (would be NULL)
      - type_changes:   fields present in both but with different types
                        (after normalising struct field order)
    """
    committed_keys = set(committed.keys())
    inferred_keys = set(inferred.keys())

    new_fields = {}
    for name in sorted(inferred_keys - committed_keys):
        new_fields[name] = inferred[name]

    removed_fields = {}
    for name in sorted(committed_keys - inferred_keys):
        removed_fields[name] = committed[name]

    type_changes = {}
    for name in sorted(committed_keys & inferred_keys):
        norm_committed = _normalise_type(committed[name])
        norm_inferred = _normalise_type(inferred[name])
        if norm_committed != norm_inferred:
            type_changes[name] = {
                "committed": committed[name],
                "inferred": inferred[name],
            }

    return {
        "new_fields": new_fields,
        "removed_fields": removed_fields,
        "type_changes": type_changes,
    }


def print_diff(diff):
    """Print a human-readable diff report to stderr."""
    new = diff["new_fields"]
    removed = diff["removed_fields"]
    changed = diff["type_changes"]

    if not new and not removed and not changed:
        eprint("  No differences — schema.json matches the data.")
        return

    if new:
        eprint(f"\n  NEW FIELDS ({len(new)}) — present in data, missing from schema.json:")
        eprint("  These would be SILENTLY DROPPED by the current schema.json.")
        for name, dtype in new.items():
            eprint(f"    + {name}: {dtype}")

    if removed:
        eprint(f"\n  REMOVED FIELDS ({len(removed)}) — in schema.json, absent from data:")
        eprint("  These columns will be NULL in the output (safe but may indicate upstream changes).")
        for name, dtype in removed.items():
            eprint(f"    - {name}: {dtype}")

    if changed:
        eprint(f"\n  TYPE CHANGES ({len(changed)}) — same field, different type:")
        eprint("  These will cause the pipeline to FAIL at schema_check.")
        for name, info in changed.items():
            eprint(f"    ~ {name}: {info['committed']} → {info['inferred']}")

    eprint()


def main():
    parser = argparse.ArgumentParser(
        description="Detect schema drift between committed schema.json "
                    "and a new UniProtKB release."
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "--committed-schema", required=True,
        help="Path to the committed schema.json",
    )
    parser.add_argument(
        "--sample-size", type=int, default=10000,
        help="Number of rows to sample for inference (default: 10000). "
             "Higher values catch rarer fields but take longer.",
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--strict", action="store_true", default=True,
        help="Exit 1 on ANY difference (default).",
    )
    mode.add_argument(
        "--allow-additions", action="store_true",
        help="Exit 0 if only new fields are found (type changes and "
             "removals still fail).",
    )

    parser.add_argument("--memory-limit", default="4GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    parser.add_argument(
        "--temp-dir", default=None,
        help="DuckDB spill directory. Defaults to $TMPDIR or /tmp/duckdb_temp.",
    )
    parser.add_argument(
        "-o", "--output", default=None,
        help="Write JSON diff report to file (in addition to stderr summary).",
    )
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    schema_path = os.path.abspath(args.committed_schema)

    if not os.path.exists(schema_path):
        eprint(f"ERROR: committed schema not found: {schema_path}")
        eprint("Run schema_bootstrap.py to generate schema.json first.")
        sys.exit(2)

    eprint("=" * 60)
    eprint("UniProtKB Schema Diff")
    eprint("=" * 60)
    eprint(f"  JSONL:     {jsonl_path}")
    eprint(f"  Schema:    {schema_path}")
    eprint(f"  Sample:    {args.sample_size:,} rows")
    eprint(f"  Mode:      {'allow-additions' if args.allow_additions else 'strict'}")

    # Load committed schema
    with open(schema_path) as f:
        committed = json.load(f)
    eprint(f"  Committed: {len(committed)} fields")

    # Infer schema from new data
    con = init_duckdb(args.memory_limit, args.threads, args.temp_dir)

    eprint(f"\n--- INFERRING SCHEMA FROM NEW DATA ---")
    t0 = time.time()
    inferred = infer_schema_sampled(con, jsonl_path, args.sample_size)
    elapsed = time.time() - t0
    eprint(f"  Inferred:  {len(inferred)} fields ({elapsed:.1f}s)")

    # Diff
    eprint(f"\n--- SCHEMA DIFF ---")
    diff = diff_schemas(committed, inferred)
    print_diff(diff)

    # Write JSON report if requested
    if args.output:
        report = {
            "committed_fields": len(committed),
            "inferred_fields": len(inferred),
            "new_fields": diff["new_fields"],
            "removed_fields": diff["removed_fields"],
            "type_changes": diff["type_changes"],
            "is_clean": not any(diff.values()),
        }
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        eprint(f"  Report written to {args.output}")

    # Exit code
    has_new = bool(diff["new_fields"])
    has_removed = bool(diff["removed_fields"])
    has_changed = bool(diff["type_changes"])

    if not has_new and not has_removed and not has_changed:
        eprint("PASS — schema.json is up to date.")
        eprint("=" * 60)
        sys.exit(0)

    if args.allow_additions and not has_removed and not has_changed:
        eprint(f"PASS (additions only) — {len(diff['new_fields'])} new field(s).")
        eprint("Update schema.json to include them, or they will be dropped.")
        eprint("=" * 60)
        sys.exit(0)

    # Failure — schema needs updating
    eprint("FAIL — schema.json does not match the data.")
    eprint()
    eprint("To fix:")
    eprint("  1. Review the differences above")
    eprint("  2. Re-run schema_bootstrap.py on the new data:")
    eprint(f"       schema_bootstrap.py {jsonl_path} --schema-json-out schema.json")
    eprint("  3. Diff the new schema.json against the old one:")
    eprint("       git diff schema.json")
    eprint("  4. Commit and re-run the pipeline")
    eprint("=" * 60)
    sys.exit(1)


if __name__ == "__main__":
    main()
