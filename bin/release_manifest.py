#!/usr/bin/env python3
"""
Generate a release manifest for provenance tracking.

Records everything needed to reproduce and audit a pipeline run:
  - Input file checksums (MD5)
  - Pipeline version (git commit)
  - Row counts per table (from lake manifest.json)
  - Timestamps

Produces: provenance.json

Usage:
    release_manifest.py \
        --lake /path/to/lake \
        --input-jsonl uniprot.jsonl.zst \
        --release 2026_01 \
        [-o provenance.json]
"""

import os
import sys
import json
import hashlib
import argparse
import subprocess
from datetime import datetime, timezone


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
        "--lake", required=True,
        help="Lake directory (contains entries/, features/, manifest.json)",
    )
    parser.add_argument(
        "--input-jsonl", required=True,
        help="Path to the input JSONL(.zst) file",
    )
    parser.add_argument("--release", required=True, help="Release label")
    parser.add_argument("-o", "--output", default="provenance.json")
    args = parser.parse_args()

    eprint("Generating release manifest...")

    manifest = {
        "release": args.release,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── Input checksums ──
    eprint("  Computing input checksums...")
    manifest["inputs"] = {}
    for label, path in [("jsonl", args.input_jsonl)]:
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

    # ── Lake table stats (from manifest.json) ──
    eprint("  Reading lake manifest...")
    lake_manifest_path = os.path.join(args.lake, "manifest.json")
    if os.path.exists(lake_manifest_path):
        with open(lake_manifest_path) as f:
            lake_manifest = json.load(f)

        tables_info = {}
        for table_name in ["entries", "features", "xrefs", "comments", "publications"]:
            table_data = lake_manifest.get("tables", {}).get(table_name, {})
            info = {
                "row_count": table_data.get("row_count", 0),
                "data_files": len(table_data.get("files", [])),
                "columns": len(table_data.get("columns", [])),
                "sort_order": table_data.get("sort_order", []),
            }
            # Compute total file sizes on disk
            table_dir = os.path.join(args.lake, table_name)
            if os.path.isdir(table_dir):
                total_size = sum(
                    os.path.getsize(os.path.join(table_dir, f))
                    for f in os.listdir(table_dir) if f.endswith(".parquet")
                )
                info["total_size_bytes"] = total_size
            tables_info[table_name] = info

        manifest["tables"] = tables_info
        manifest["total_rows"] = sum(
            t.get("row_count", 0) for t in tables_info.values()
        )
    else:
        eprint("    WARNING: manifest.json not found in lake directory")
        manifest["tables"] = {}
        manifest["total_rows"] = 0

    # ── Write manifest ──
    with open(args.output, "w") as f:
        json.dump(manifest, f, indent=2)

    eprint(f"  Saved: {args.output}")
    eprint(f"  Release {args.release}: {manifest['total_rows']:,} total rows "
           f"across {len(manifest.get('tables', {}))} tables")


if __name__ == "__main__":
    main()
