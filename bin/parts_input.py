#!/usr/bin/env python3
"""
A finished dat2jsonl output as the pipeline's input (FROM_PARTS, --input_parts).

dat2jsonl (dat2jsonl/, UniProt's own code over the release's flat files)
writes OUTDIR/parts/part-NNNNN.jsonl.zst, one per byte range of each .dat in
OUTDIR/plan.tsv, then checks them (OUTDIR/CHECK.txt, OUTDIR/COMPLETE).  The
parts in plan order are the release's entries in .dat order, one JSON line
each; zstd frames concatenate, so joined they are the JSONL STREAM_JSONL
would have made from a REST dump.

  parts_input.py parts OUTDIR
      Refuse an output whose check has not passed (no COMPLETE, CHECK.txt
      not ending "CHECK PASSED", a plan row without its part or stats); print
      the parts' paths in plan order, one a line.

  parts_input.py manifest OUTDIR --jsonl JOINED --entry-count N
                 [--expected-count E] -o input_manifest.json
      N is the joined JSONL's line count; it must equal the parts' own counts
      (their stats) and E if given.  Writes what the release was built from,
      with JOINED's SHA-256, which provenance.json records under inputs.source.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any


class PartsError(Exception):
    pass


def plan_rows(outdir: Path) -> list[dict[str, str]]:
    plan = outdir / "plan.tsv"
    if not plan.is_file():
        raise PartsError(f"{outdir} is not a dat2jsonl output: no plan.tsv")
    lines = plan.read_text().splitlines()
    header = lines[0].split("\t")
    if header != ["task", "dat", "start", "end", "part"]:
        raise PartsError(f"{plan}: unexpected header {header}")
    return [dict(zip(header, line.split("\t"), strict=True)) for line in lines[1:] if line]


def checked_parts(outdir: Path) -> list[tuple[dict[str, str], Path, Path]]:
    """(plan row, part, stats) for every task, if the output's check passed."""
    if not (outdir / "COMPLETE").exists():
        raise PartsError(f"{outdir}/COMPLETE is missing: dat2jsonl's check has not passed")
    check = outdir / "CHECK.txt"
    if not check.is_file() or check.read_text().rstrip().splitlines()[-1:] != ["CHECK PASSED"]:
        raise PartsError(f"{check} does not end with CHECK PASSED")
    rows = plan_rows(outdir)
    if not rows:
        raise PartsError(f"{outdir}/plan.tsv lists no parts")
    out = []
    for row in rows:
        part = outdir / "parts" / f"{row['part']}.jsonl.zst"
        stats = outdir / "parts" / f"{row['part']}.stats.tsv"
        if not part.is_file() or not stats.is_file():
            raise PartsError(f"{row['part']}: {part.name} or {stats.name} missing")
        out.append((row, part, stats))
    return out


def stats_row(stats: Path) -> dict[str, str]:
    header, row = stats.read_text().splitlines()[:2]
    return dict(zip(header.split("\t"), row.split("\t"), strict=True))


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def manifest(outdir: Path, jsonl: Path, entry_count: int, expected_count: int | None) -> dict[str, Any]:
    parts = checked_parts(outdir)
    per_dat: dict[str, dict[str, Any]] = {}
    total = 0
    for row, _, stats in parts:
        s = stats_row(stats)
        n = int(s["entries"])
        total += n
        d = per_dat.setdefault(row["dat"], {"path": row["dat"], "entries": 0, "parts": 0})
        d["entries"] += n
        d["parts"] += 1
        # the last range ends at the file's size (the check verified it)
        d["size_bytes"] = max(d.get("size_bytes", 0), int(s["end_offset"]))
    if entry_count != total:
        raise PartsError(f"the joined parts hold {entry_count:,} lines, their stats say {total:,}")
    if expected_count is not None and entry_count != expected_count:
        raise PartsError(f"{entry_count:,} entries, expected {expected_count:,}")

    def lines(name: str) -> list[str]:
        p = outdir / name
        return p.read_text().splitlines() if p.is_file() else []

    return {
        "kind": "dat2jsonl",
        "directory": str(outdir.resolve()),
        "entry_count": entry_count,
        "expected_count": expected_count,
        "jsonl_sha256": sha256_of(jsonl),   # the parts joined in plan order, as SORT_JSONL reads them
        "parts": len(parts),
        "dat": list(per_dat.values()),
        "inputs": lines("inputs.txt"),  # dat2jsonl's record: jar and uniprot-core version, files
        "check": lines("CHECK.txt"),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("parts", help="the checked parts, in plan order")
    p.add_argument("outdir", type=Path)
    m = sub.add_parser("manifest", help="the input manifest, after the joined parts are counted")
    m.add_argument("outdir", type=Path)
    m.add_argument("--jsonl", type=Path, required=True, help="the parts joined")
    m.add_argument("--entry-count", type=int, required=True)
    m.add_argument("--expected-count", type=int, default=None)
    m.add_argument("-o", "--output", required=True)
    a = ap.parse_args()
    try:
        if a.cmd == "parts":
            for _, part, _ in checked_parts(a.outdir):
                print(os.fspath(part))
        else:
            doc = manifest(a.outdir, a.jsonl, a.entry_count, a.expected_count)
            with open(a.output, "w") as f:
                json.dump(doc, f, indent=2)
                f.write("\n")
            print(f"  parts_input: {doc['parts']} parts, {doc['entry_count']:,} entries → {a.output}",
                  file=sys.stderr)
    except PartsError as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
