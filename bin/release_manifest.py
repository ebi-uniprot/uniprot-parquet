#!/usr/bin/env python3
"""
Generate a release manifest for provenance tracking.

Records everything needed to reproduce and audit a pipeline run:
  - Input file checksums (MD5)
  - schema.json checksum
  - Pipeline version (git commit)
  - Row counts per Iceberg table
  - Snapshot IDs
  - Timestamps

Produces: manifest.json

Usage:
    release_manifest.py \
        --catalog-uri sqlite:///catalog.db \
        --warehouse /path/to/warehouse \
        --input-jsonl uniprot.jsonl.zst \
        --schema schema.json \
        --release 2026_01 \
        [-o manifest.json]
"""

import os
import sys
import json
import hashlib
import argparse
import subprocess
from datetime import datetime, timezone

from pyiceberg.catalog.sql import SqlCatalog


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def md5_file(path, chunk_size=8 * 1024 * 1024):
    """Compute MD5 of a file without loading it into memory."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def git_info():
    """Get current git commit and dirty status, or None if not in a repo."""
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain"], stderr=subprocess.DEVNULL
        ).decode().strip()
        return {
            "commit": commit,
            "dirty": len(dirty) > 0,
        }
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Generate a release manifest for provenance tracking"
    )
    parser.add_argument(
        "--catalog-uri", required=True,
        help="SQLite catalog URI",
    )
    parser.add_argument(
        "--warehouse", required=True,
        help="Iceberg warehouse directory",
    )
    parser.add_argument("--namespace", default="uniprotkb")
    parser.add_argument(
        "--input-jsonl", required=True,
        help="Path to the input JSONL(.zst) file",
    )
    parser.add_argument(
        "--schema", required=True,
        help="Path to schema.json",
    )
    parser.add_argument("--release", required=True, help="Release label")
    parser.add_argument("-o", "--output", default="manifest.json")
    args = parser.parse_args()

    eprint("Generating release manifest...")

    manifest = {
        "release": args.release,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── Input checksums ──
    eprint("  Computing input checksums...")
    manifest["inputs"] = {}
    for label, path in [("jsonl", args.input_jsonl), ("schema", args.schema)]:
        abspath = os.path.abspath(path)
        manifest["inputs"][label] = {
            "path": os.path.basename(abspath),
            "md5": md5_file(abspath),
            "size_bytes": os.path.getsize(abspath),
        }

    # ── Git info ──
    git = git_info()
    if git:
        manifest["pipeline"] = git
    else:
        manifest["pipeline"] = {"commit": "unknown", "dirty": None}

    # ── Iceberg table stats ──
    eprint("  Reading Iceberg metadata...")
    catalog = SqlCatalog(
        "uniprot",
        **{"uri": args.catalog_uri, "warehouse": args.warehouse},
    )

    tables_info = {}
    for table_name in ["entries", "features", "xrefs", "comments"]:
        full_name = f"{args.namespace}.{table_name}"
        try:
            table = catalog.load_table(full_name)
        except Exception as e:
            eprint(f"    WARNING: could not load {full_name}: {e}")
            continue

        info = {"columns": len(table.schema().fields)}

        snapshot = table.current_snapshot()
        if snapshot:
            info["snapshot_id"] = snapshot.snapshot_id
            info["snapshot_ts"] = datetime.fromtimestamp(
                snapshot.timestamp_ms / 1000, tz=timezone.utc
            ).isoformat()
            if snapshot.summary:
                summary = snapshot.summary.ser_model()
                total = summary.get("total-records")
                if total is not None:
                    info["row_count"] = int(total)

        # Data file sizes
        try:
            data_files = list(table.inspect.data_files())
            if data_files:
                info["data_files"] = len(data_files)
                info["total_size_bytes"] = sum(
                    f["file_size_in_bytes"] for f in data_files
                )
        except Exception:
            pass

        tables_info[table_name] = info

    manifest["tables"] = tables_info

    # ── Summary row counts ──
    manifest["total_rows"] = sum(
        t.get("row_count", 0) for t in tables_info.values()
    )

    # ── Write manifest ──
    with open(args.output, "w") as f:
        json.dump(manifest, f, indent=2)

    eprint(f"  Saved: {args.output}")
    eprint(f"  Release {args.release}: {manifest['total_rows']:,} total rows "
           f"across {len(tables_info)} tables")


if __name__ == "__main__":
    main()
