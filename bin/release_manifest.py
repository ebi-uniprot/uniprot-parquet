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


def hash_file(path, chunk_size=8 * 1024 * 1024):
    """(sha256, md5) of a file in one streamed pass.

    Return order matches _hash_file in parquet_transform.py — keep the two
    in sync."""
    md5, sha = hashlib.md5(), hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5.update(chunk)
            sha.update(chunk)
    return sha.hexdigest(), md5.hexdigest()


def git_info():
    """Git commit and dirty status of the pipeline checkout this script lives
    in, or None if it is not a git repo.  Resolved from __file__, not the
    working directory: Nextflow runs this in a task work dir that may be
    outside the repo (or inside an unrelated one)."""
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, cwd=repo_dir
        ).decode().strip()
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain"], stderr=subprocess.DEVNULL, cwd=repo_dir
        ).decode().strip()
        return {
            "commit": commit,
            "dirty": len(dirty) > 0,
        }
    except (subprocess.CalledProcessError, OSError):
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
    parser.add_argument(
        "--complete-marker", default=None,
        help="Path of the RELEASE_COMPLETE marker (default: RELEASE_COMPLETE next to -o). "
             "Written last; mirrors test for it before reading anything else (plan H.5).",
    )
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
        sha256, md5 = hash_file(abspath)
        manifest["inputs"][label] = {
            "path": os.path.basename(abspath),
            "md5": md5,
            "sha256": sha256,          # sorted.jsonl.zst is published beside the lake (plan F.7)
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
        for table_name, table_data in lake_manifest.get("tables", {}).items():
            info = {
                "row_count": table_data.get("row_count", 0),
                "data_files": len(table_data.get("files", [])),
                "columns": len(table_data.get("columns", [])),
                "sort_order": table_data.get("sort_order", []),
                "total_size_bytes": table_data.get("size_bytes"),
            }
            # Cross-check the manifest's size_bytes against the files on disk
            # (recursive: tables are Hive-partitioned, <table>/review_status=<side>/).
            table_dir = os.path.join(args.lake, table_name)
            if os.path.isdir(table_dir):
                on_disk = sum(
                    os.path.getsize(os.path.join(root, f))
                    for root, _, files in os.walk(table_dir)
                    for f in files if f.endswith(".parquet")
                )
                if info["total_size_bytes"] is None:
                    info["total_size_bytes"] = on_disk
                elif on_disk != info["total_size_bytes"]:
                    eprint(f"  WARNING: {table_name}: manifest size_bytes {info['total_size_bytes']:,} "
                           f"!= on-disk {on_disk:,}")
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

    # ── RELEASE_COMPLETE: the last action (plan H.5) ──
    write_complete_marker(args, lake_manifest if os.path.exists(lake_manifest_path) else {})


def write_complete_marker(args, lake_manifest):
    """Write RELEASE_COMPLETE after provenance.json.  Content: release,
    schema_version, sha256 of lake/SHA256SUMS.txt, UTC timestamp — one key
    per line.  This runs only after validation passed (the PROVENANCE
    process depends on VALIDATE), so the marker never appears for a failed
    release.

    The marker is what mirrors trust, so it refuses to describe a lake whose
    sidecars are missing (transform aborted after the Parquet files, wrong
    --lake path): no manifest.json / schema_version / SHA256SUMS.txt → exit 1
    with nothing written."""
    marker = args.complete_marker or os.path.join(
        os.path.dirname(os.path.abspath(args.output)), "RELEASE_COMPLETE")
    sums = os.path.join(args.lake, "SHA256SUMS.txt")
    missing = []
    if not lake_manifest:
        missing.append("manifest.json")
    elif not lake_manifest.get("schema_version"):
        missing.append("manifest.json schema_version")
    if not os.path.exists(sums):
        missing.append("SHA256SUMS.txt")
    if missing:
        sys.exit(f"FATAL: not writing {marker}: {args.lake} lacks {', '.join(missing)}")
    lines = [
        f"release: {args.release}",
        f"schema_version: {lake_manifest['schema_version']}",
        f"sha256sums_sha256: {hash_file(sums)[0]}",
        f"completed_at: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
    ]
    with open(marker, "w") as f:
        f.write("\n".join(lines) + "\n")
    eprint(f"  Wrote {marker}")


if __name__ == "__main__":
    main()
