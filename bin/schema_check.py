#!/usr/bin/env python3
"""
Pre-flight schema check for UniProtKB → Iceberg pipeline.

Runs the SQL transforms against the JSONL with LIMIT 0 to infer the
post-transform Arrow schema without materialising any data, then compares
it against the previous release's saved schema.

Produces:
  - A schema snapshot (JSON) for the new release
  - A human-readable diff if a previous schema exists
  - Exit code 0 if schemas match, 1 if they differ

Workflow:
    1. schema_check.py <input.jsonl.zst> --release 2026_02 --schema-dir schemas/
       → Infers schema, diffs against schemas/latest.json, saves schemas/2026_02.json
       → You review the diff
    2. iceberg_transform.py <input.jsonl.zst> ...
       → Proceeds with the build

Usage:
    schema_check.py <input.jsonl.zst> \
        --release 2026_02 \
        --schema-dir schemas/ \
        [--duckdb-schema schema.json] \
        [--memory-limit 16GB]
"""

import os
import sys
import json
import argparse
import time

import duckdb
import pyarrow as pa


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# ─── Import SQL definitions from transform module ───────────────────────

# Allow importing from the same bin/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from iceberg_transform import (
    ENTRIES_SQL,
    FEATURES_SQL,
    build_read_clause,
    init_duckdb,
)


# ─── Schema serialisation ───────────────────────────────────────────────

def _arrow_type_to_str(t: pa.DataType) -> str:
    """Convert an Arrow type to a readable string, recursing into structs/lists."""
    if pa.types.is_struct(t):
        fields = []
        for i in range(t.num_fields):
            f = t.field(i)
            nullable = "" if f.nullable else " NOT NULL"
            fields.append(f"{f.name}: {_arrow_type_to_str(f.type)}{nullable}")
        return "struct<" + ", ".join(fields) + ">"
    elif pa.types.is_list(t) or pa.types.is_large_list(t):
        return f"list<{_arrow_type_to_str(t.value_type)}>"
    elif pa.types.is_map(t):
        return f"map<{_arrow_type_to_str(t.key_field.type)}, {_arrow_type_to_str(t.item_field.type)}>"
    else:
        return str(t)


def arrow_schema_to_dict(schema: pa.Schema) -> dict:
    """Serialise an Arrow schema to a JSON-friendly dict.

    Returns {"columns": [{"name": ..., "type": ..., "nullable": ...}, ...]}.
    """
    cols = []
    for f in schema:
        cols.append({
            "name": f.name,
            "type": _arrow_type_to_str(f.type),
            "nullable": f.nullable,
        })
    return {"columns": cols}


def schema_dict_to_lookup(d: dict) -> dict:
    """Convert a schema dict to {name: {type, nullable}} for diffing."""
    return {c["name"]: {"type": c["type"], "nullable": c["nullable"]} for c in d["columns"]}


# ─── Schema diffing ─────────────────────────────────────────────────────

def diff_schemas(old: dict, new: dict) -> dict:
    """Diff two schema dicts.  Returns a structured diff.

    Categories:
      - added:   columns in new but not old (nullable = safe, required = warning)
      - removed: columns in old but not new (breaking)
      - type_changed: same column, different type (breaking)
      - nullability_changed: same column, nullable→required or vice versa
      - unchanged: identical columns
    """
    old_cols = schema_dict_to_lookup(old)
    new_cols = schema_dict_to_lookup(new)

    old_names = set(old_cols)
    new_names = set(new_cols)

    added = []
    for name in sorted(new_names - old_names):
        c = new_cols[name]
        added.append({"name": name, "type": c["type"], "nullable": c["nullable"]})

    removed = []
    for name in sorted(old_names - new_names):
        c = old_cols[name]
        removed.append({"name": name, "type": c["type"], "nullable": c["nullable"]})

    type_changed = []
    nullability_changed = []
    unchanged = []
    for name in sorted(old_names & new_names):
        oc, nc = old_cols[name], new_cols[name]
        if oc["type"] != nc["type"]:
            type_changed.append({
                "name": name,
                "old_type": oc["type"],
                "new_type": nc["type"],
            })
        elif oc["nullable"] != nc["nullable"]:
            nullability_changed.append({
                "name": name,
                "old_nullable": oc["nullable"],
                "new_nullable": nc["nullable"],
            })
        else:
            unchanged.append(name)

    return {
        "added": added,
        "removed": removed,
        "type_changed": type_changed,
        "nullability_changed": nullability_changed,
        "unchanged": unchanged,
    }


def format_diff(table_name: str, d: dict) -> str:
    """Format a schema diff as a human-readable report."""
    lines = [f"  {table_name}:"]

    if not d["added"] and not d["removed"] and not d["type_changed"] and not d["nullability_changed"]:
        lines.append(f"    No changes ({len(d['unchanged'])} columns unchanged)")
        return "\n".join(lines)

    if d["added"]:
        lines.append(f"    ADDED ({len(d['added'])}):")
        for c in d["added"]:
            null_str = "nullable" if c["nullable"] else "NOT NULL"
            lines.append(f"      + {c['name']}: {c['type']} ({null_str})")

    if d["removed"]:
        lines.append(f"    REMOVED ({len(d['removed'])}):")
        for c in d["removed"]:
            lines.append(f"      - {c['name']}: {c['type']}")

    if d["type_changed"]:
        lines.append(f"    TYPE CHANGED ({len(d['type_changed'])}):")
        for c in d["type_changed"]:
            lines.append(f"      ~ {c['name']}: {c['old_type']} → {c['new_type']}")

    if d["nullability_changed"]:
        lines.append(f"    NULLABILITY CHANGED ({len(d['nullability_changed'])}):")
        for c in d["nullability_changed"]:
            old_n = "nullable" if c["old_nullable"] else "NOT NULL"
            new_n = "nullable" if c["new_nullable"] else "NOT NULL"
            lines.append(f"      ~ {c['name']}: {old_n} → {new_n}")

    lines.append(f"    ({len(d['unchanged'])} columns unchanged)")
    return "\n".join(lines)


# ─── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pre-flight schema check for UniProtKB → Iceberg pipeline"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "--release", required=True,
        help="Release label (e.g. 2026_02). Used as the schema filename.",
    )
    parser.add_argument(
        "--schema-dir", default="schemas",
        help="Directory for schema snapshots (default: schemas/)",
    )
    parser.add_argument(
        "--duckdb-schema", default=None,
        help="Committed DuckDB schema JSON (from infer_schema.py). "
             "If omitted, DuckDB auto-detects.",
    )
    parser.add_argument("--memory-limit", default="16GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    schema_dir = os.path.abspath(args.schema_dir)
    os.makedirs(schema_dir, exist_ok=True)

    eprint("=" * 60)
    eprint("UniProtKB Schema Check")
    eprint("=" * 60)
    eprint(f"  Input:   {jsonl_path}")
    eprint(f"  Release: {args.release}")
    eprint(f"  Schemas: {schema_dir}")
    eprint()

    # ── Init DuckDB ──
    con = init_duckdb(args.memory_limit, args.threads)
    read_clause = build_read_clause(jsonl_path, args.duckdb_schema)

    # ── Infer schemas ──
    # Run each SQL and read a single batch to capture the Arrow schema.
    # We can't use LIMIT 0 because LATERAL UNNEST requires at least one row.
    eprint("--- INFERRING POST-TRANSFORM SCHEMAS ---")
    t0 = time.time()

    entries_sql = ENTRIES_SQL.format(read_clause=read_clause)
    reader = con.sql(entries_sql).to_arrow_reader(batch_size=1)
    entries_batch = next(reader)
    del reader
    entries_schema = arrow_schema_to_dict(entries_batch.schema)
    eprint(f"  entries: {len(entries_schema['columns'])} columns")

    features_sql = FEATURES_SQL.format(read_clause=read_clause)
    reader = con.sql(features_sql).to_arrow_reader(batch_size=1)
    features_batch = next(reader)
    del reader
    features_schema = arrow_schema_to_dict(features_batch.schema)
    eprint(f"  features: {len(features_schema['columns'])} columns")

    elapsed = time.time() - t0
    eprint(f"  Inferred in {elapsed:.1f}s")

    # ── Save new schema ──
    new_schema = {
        "release": args.release,
        "entries": entries_schema,
        "features": features_schema,
    }

    new_path = os.path.join(schema_dir, f"{args.release}.json")
    with open(new_path, "w") as f:
        json.dump(new_schema, f, indent=2)
    eprint(f"\n  Saved: {new_path}")

    # ── Diff against previous (latest.json) ──
    latest_path = os.path.join(schema_dir, "latest.json")
    has_diff = False

    if os.path.exists(latest_path):
        with open(latest_path) as f:
            old_schema = json.load(f)

        old_release = old_schema.get("release", "unknown")
        eprint(f"\n--- SCHEMA DIFF: {old_release} → {args.release} ---")

        entries_diff = diff_schemas(old_schema["entries"], entries_schema)
        features_diff = diff_schemas(old_schema["features"], features_schema)

        report = format_diff("entries", entries_diff)
        eprint(report)
        report = format_diff("features", features_diff)
        eprint(report)

        has_changes = (
            entries_diff["added"] or entries_diff["removed"]
            or entries_diff["type_changed"] or entries_diff["nullability_changed"]
            or features_diff["added"] or features_diff["removed"]
            or features_diff["type_changed"] or features_diff["nullability_changed"]
        )

        if has_changes:
            has_diff = True
            # Save the diff as well
            diff_path = os.path.join(
                schema_dir, f"diff_{old_release}_vs_{args.release}.json"
            )
            diff_report = {
                "old_release": old_release,
                "new_release": args.release,
                "entries": entries_diff,
                "features": features_diff,
            }
            with open(diff_path, "w") as f:
                json.dump(diff_report, f, indent=2)
            eprint(f"\n  Diff saved: {diff_path}")

            eprint("\n  ⚠  Schema changes detected. Review the diff above.")
            eprint("  If acceptable, update latest.json and proceed with the transform:")
            eprint(f"    cp {new_path} {latest_path}")
        else:
            eprint("\n  ✓  No schema changes. Safe to proceed with the transform.")
    else:
        eprint(f"\n  No previous schema found ({latest_path}).")
        eprint(f"  This is the first run. Saving as baseline.")

    # ── Update latest.json ──
    # On first run or no changes, auto-update latest.json.
    # On schema changes, require manual review (user copies new → latest).
    if not has_diff:
        with open(latest_path, "w") as f:
            json.dump(new_schema, f, indent=2)
        eprint(f"  Updated: {latest_path}")

    eprint("\n" + "=" * 60)
    sys.exit(1 if has_diff else 0)


if __name__ == "__main__":
    main()
