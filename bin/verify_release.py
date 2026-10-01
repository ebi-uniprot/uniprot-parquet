#!/usr/bin/env python3
"""
Verify a published release directory against its own metadata.

The workflow runs this on ``<outdir>/<release>/`` before it writes
RELEASE_COMPLETE.  VALIDATE checked the lake in the work directory; this
checks the copy that publishDir made, which a killed head job or an
interrupted copy can leave incomplete.  Mirrors can run it too (``--full``
re-hashes every file).

Checks:
  - lake/SHA256SUMS.txt exists, and every file it lists exists;
  - every Parquet file has the size lake/manifest.json records for it;
  - no Parquet file under lake/ is missing from SHA256SUMS.txt;
  - lake/RELEASE.metalink exists, and the lake's small metadata files
    (manifest.json, datapackage.json, LICENSE) match their SHA-256;
  - provenance.json exists, and every sidecar it records under "published"
    (sorted.jsonl.zst, SCHEMA.md, validation_report.txt / .json) exists with
    the recorded size;
  - with --marker, the marker's sha256sums_sha256 and release match;
  - with --full, every file's SHA-256 (Parquet against SHA256SUMS.txt,
    sidecars against provenance.json).

Standard library only, so it runs wherever Python does.

Usage:
    verify_release.py <outdir>/<release> [--full] [--marker PATH]

Exit codes:
    0  the release directory matches its metadata
    1  it does not (each problem is printed)
"""

import argparse
import hashlib
import json
import os
import sys

# Files under this size are always hashed: the lake's metadata, never Parquet data.
SMALL_FILE_BYTES = 16 << 20


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_sums(path: str) -> dict[str, str]:
    """{relative path: sha256} from a sha256sum-format file."""
    sums = {}
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if line:
                digest, name = line.split(None, 1)
                sums[name.lstrip("*")] = digest
    return sums


def verify(release_dir: str, full: bool = False, marker: str | None = None) -> list[str]:
    """Every way ``release_dir`` differs from its metadata; [] if none."""
    problems = []
    lake = os.path.join(release_dir, "lake")
    sums_path = os.path.join(lake, "SHA256SUMS.txt")
    if not os.path.isfile(sums_path):
        return [f"missing {sums_path}"]
    sums = read_sums(sums_path)

    sizes = {}
    try:
        with open(os.path.join(lake, "manifest.json")) as f:
            manifest = json.load(f)
        for table, info in manifest.get("tables", {}).items():
            for rel, details in info.get("file_details", {}).items():
                sizes[f"{table}/{rel}"] = details.get("size_bytes")
    except (OSError, ValueError) as e:
        problems.append(f"lake/manifest.json unreadable: {e}")

    for name, digest in sums.items():
        path = os.path.join(lake, name)
        if not os.path.isfile(path):
            problems.append(f"lake/{name}: missing")
            continue
        size = os.path.getsize(path)
        if name.endswith(".parquet"):
            if name not in sizes:
                problems.append(f"lake/{name}: not in manifest.json file_details")
            elif size != sizes[name]:
                problems.append(f"lake/{name}: {size:,} bytes, manifest.json says {sizes[name]:,}")
        if (full or size <= SMALL_FILE_BYTES) and sha256(path) != digest:
            problems.append(f"lake/{name}: SHA-256 differs from SHA256SUMS.txt")

    for root, _, files in os.walk(lake):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), lake)
            if f.endswith(".parquet") and rel not in sums:
                problems.append(f"lake/{rel}: not listed in SHA256SUMS.txt")
    if not os.path.isfile(os.path.join(lake, "RELEASE.metalink")):
        problems.append("lake/RELEASE.metalink: missing")

    provenance_path = os.path.join(release_dir, "provenance.json")
    try:
        with open(provenance_path) as f:
            provenance = json.load(f)
    except (OSError, ValueError) as e:
        return problems + [f"provenance.json unreadable: {e}"]
    published = provenance.get("published") or {}
    if not published:
        problems.append("provenance.json records no published files")
    for name, info in published.items():
        path = os.path.join(release_dir, name)
        if not os.path.isfile(path):
            problems.append(f"{name}: missing")
        elif os.path.getsize(path) != info.get("size_bytes"):
            problems.append(f"{name}: {os.path.getsize(path):,} bytes, provenance.json says "
                            f"{info.get('size_bytes'):,}")
        elif (full or info.get("size_bytes", 0) <= SMALL_FILE_BYTES) and sha256(path) != info.get("sha256"):
            problems.append(f"{name}: SHA-256 differs from provenance.json")

    if marker is not None:
        try:
            with open(marker) as f:
                fields = dict(line.split(": ", 1) for line in f.read().splitlines() if ": " in line)
        except OSError as e:
            return problems + [f"marker unreadable: {e}"]
        if fields.get("sha256sums_sha256") != sha256(sums_path):
            problems.append("lake/SHA256SUMS.txt: SHA-256 differs from the marker's sha256sums_sha256")
        if fields.get("release") != provenance.get("release"):
            problems.append(f"marker release {fields.get('release')!r} != provenance.json "
                            f"release {provenance.get('release')!r}")
    return problems


def main():
    parser = argparse.ArgumentParser(description="Verify a published release directory")
    parser.add_argument("release_dir", help="<outdir>/<release>: holds lake/ and provenance.json")
    parser.add_argument("--full", action="store_true",
                        help="Re-hash every file (default: sizes, and hashes of small files only)")
    parser.add_argument("--marker", default=None,
                        help="RELEASE_COMPLETE (or the workflow's .pending copy) to check against")
    args = parser.parse_args()
    problems = verify(args.release_dir, args.full, args.marker)
    for p in problems:
        print(f"FAIL: {p}")
    if problems:
        print(f"{args.release_dir}: {len(problems)} problem(s); this is not a complete release")
        sys.exit(1)
    print(f"{args.release_dir}: OK ({'full SHA-256' if args.full else 'sizes'})")


if __name__ == "__main__":
    main()
