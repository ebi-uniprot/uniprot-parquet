#!/usr/bin/env python3
"""
A dat2jsonl output as the pipeline's input: a finished one (FROM_PARTS,
--input_parts), or the one the pipeline makes itself (FROM_DAT, --input_dat).

dat2jsonl (dat2jsonl/, UniProt's own code over the release's flat files)
writes OUTDIR/parts/part-NNNNN.jsonl.zst, one per byte range of each .dat in
OUTDIR/plan.tsv, then checks them (OUTDIR/CHECK.txt, OUTDIR/COMPLETE).  The
parts in plan order are the release's entries in .dat order, one JSON line
each; zstd frames concatenate, so joined they are the JSONL STREAM_JSONL
would have made from a REST dump.

  parts_input.py check OUTDIR [--expected-count E]
      dat2jsonl/slurm/check.sh's check, for the parts the pipeline makes: from
      the stats alone, every .dat in plan.tsv is covered once (in plan order,
      its first entry starts at byte 0, each part's last entry ends where the
      next part's first starts, the last ends at the file's size) and the
      entries add up to E if given.  Writes OUTDIR/CHECK.txt, and
      OUTDIR/COMPLETE if it passes (exit 0), else exit 1.

  parts_input.py parts OUTDIR
      Refuse an output whose check has not passed (no COMPLETE, CHECK.txt
      not ending "CHECK PASSED", a plan row without its part or stats); print
      the parts' paths in plan order, one a line.

  parts_input.py manifest OUTDIR --jsonl JOINED --entry-count N
                 [--expected-count E] [--kind K] [--directory D] -o input_manifest.json
      N is the joined JSONL's line count; it must equal the parts' own counts
      (their stats) and E if given.  Writes what the release was built from,
      with JOINED's SHA-256, which provenance.json records under inputs.source
      (kind: dat2jsonl, or K; directory: OUTDIR's name, or D's).  The record is
      published with the release, so it names files, never their absolute paths
      or dates: the build machine's layout stays out of it.

  parts_input.py public PROVENANCE_JSON -o OUT
      The same for a provenance.json written before records were public:
      inputs.source with names in place of paths, and no file dates.  Every
      other field is kept as it is.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import re
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


def check(outdir: Path, expected_count: int | None) -> tuple[bool, list[str]]:
    """Whether the parts cover every .dat once, and the report's lines."""
    rows = plan_rows(outdir)
    report = [f"dat2jsonl check: {outdir}"]
    missing: list[str] = []
    by_dat: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        stats_file = outdir / "parts" / f"{row['part']}.stats.tsv"
        part = outdir / "parts" / f"{row['part']}.jsonl.zst"
        if not stats_file.is_file() or not part.is_file():
            missing.append(row["part"])
            continue
        by_dat.setdefault(row["dat"], []).append(stats_row(stats_file))
    ok = not missing and bool(rows)
    if missing:
        report.append(f"MISSING {len(missing)} parts: {' '.join(missing)}")
    if not rows:
        report.append("MISSING: plan.tsv lists no parts")
    total = with_go = with_uniparc = 0
    for dat in dict.fromkeys(r["dat"] for r in rows):   # plan order
        dat_stats = by_dat.get(dat, [])
        if not dat_stats:
            continue
        size = os.stat(dat).st_size
        gaps = []
        if int(dat_stats[0]["first_offset"]) != 0:
            gaps.append(f"GAP {dat}: first entry at {dat_stats[0]['first_offset']}, not 0")
        for a, b in itertools.pairwise(dat_stats):
            if a["end_offset"] != b["first_offset"]:
                gaps.append(f"GAP {dat}: a part ends at {a['end_offset']}, the next starts at {b['first_offset']}")
        if int(dat_stats[-1]["end_offset"]) != size:
            gaps.append(f"GAP {dat}: last entry ends at {dat_stats[-1]['end_offset']}, file is {size} bytes")
        n = sum(int(s["entries"]) for s in dat_stats)
        total += n
        with_go += sum(int(s["with_go"]) for s in dat_stats)
        with_uniparc += sum(int(s["with_uniparc"]) for s in dat_stats)
        report += gaps
        report.append(f"{dat}: {n} entries{'' if gaps else ', every byte covered once'}")
        ok = ok and not gaps
    report.append(f"total: {total} entries; GO evidences on {with_go}; UniParc id on {with_uniparc} "
                  f"({total - with_uniparc} without)")
    if expected_count is not None:
        if total == expected_count:
            report.append(f"expected {expected_count} entries: matches")
        else:
            report.append(f"EXPECTED {expected_count} entries, got {total} ({total - expected_count:+d})")
            ok = False
    report.append("CHECK PASSED" if ok else "CHECK FAILED")
    return ok, report


def stats_row(stats: Path) -> dict[str, str]:
    header, row = stats.read_text().splitlines()[:2]
    return dict(zip(header.split("\t"), row.split("\t"), strict=True))


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


# An absolute path (a word starting with "/"), and a file's date: "<size> bytes, modified <date>"
_ABSOLUTE_PATH = re.compile(r"(?<![\w.~])/(?:[^\s/:]+/)*([^\s/:]+)/?")
_MODIFIED = re.compile(r",? modified .*$")


def public_line(line: str) -> str | None:
    """A line of inputs.txt or CHECK.txt as published: paths cut to their last
    part, dates dropped, the source directory (SRC) left out."""
    if line.startswith("SRC:"):
        return None
    return _MODIFIED.sub("", _ABSOLUTE_PATH.sub(r"\1", line))


def public_source(source: dict[str, Any]) -> dict[str, Any]:
    """inputs.source as published: names, not the build machine's paths."""
    out = dict(source)
    if out.get("directory"):
        out["directory"] = os.path.basename(str(out["directory"]).rstrip("/"))
    out["dat"] = [{**d, "path": os.path.basename(d["path"])} for d in out.get("dat", [])]
    for key in ("inputs", "check"):
        out[key] = [kept for line in out.get(key, []) if (kept := public_line(line)) is not None]
    return out


def manifest(outdir: Path, jsonl: Path, entry_count: int, expected_count: int | None,
             kind: str = "dat2jsonl", directory: str | None = None) -> dict[str, Any]:
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

    return public_source({
        "kind": kind,
        "directory": directory or str(outdir.resolve()),
        "entry_count": entry_count,
        "expected_count": expected_count,
        "jsonl_sha256": sha256_of(jsonl),   # the parts joined in plan order, as SORT_JSONL reads them
        "parts": len(parts),
        "dat": list(per_dat.values()),
        "inputs": lines("inputs.txt"),  # dat2jsonl's record: jar and uniprot-core version, files
        "check": lines("CHECK.txt"),
    })


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="do the parts cover every .dat once? CHECK.txt, COMPLETE")
    c.add_argument("outdir", type=Path)
    c.add_argument("--expected-count", type=int, default=None)
    p = sub.add_parser("parts", help="the checked parts, in plan order")
    p.add_argument("outdir", type=Path)
    m = sub.add_parser("manifest", help="the input manifest, after the joined parts are counted")
    m.add_argument("outdir", type=Path)
    m.add_argument("--jsonl", type=Path, required=True, help="the parts joined")
    m.add_argument("--entry-count", type=int, required=True)
    m.add_argument("--expected-count", type=int, default=None)
    m.add_argument("--kind", default="dat2jsonl")
    m.add_argument("--directory", default=None, help="recorded instead of OUTDIR's absolute path")
    m.add_argument("-o", "--output", required=True)
    pp = sub.add_parser("public", help="a provenance.json's inputs.source without paths or dates")
    pp.add_argument("provenance", type=Path)
    pp.add_argument("-o", "--output", type=Path, required=True)
    a = ap.parse_args()
    try:
        if a.cmd == "check":
            (a.outdir / "COMPLETE").unlink(missing_ok=True)
            ok, report = check(a.outdir, a.expected_count)
            (a.outdir / "CHECK.txt").write_text("\n".join(report) + "\n")
            print("\n".join(report), file=sys.stderr)
            if not ok:
                return 1
            (a.outdir / "COMPLETE").touch()
        elif a.cmd == "public":
            prov = json.loads(a.provenance.read_text())
            source = prov.get("inputs", {}).get("source")
            if not source or not str(source.get("kind", "")).startswith("dat2jsonl"):
                raise PartsError(f"{a.provenance}: inputs.source is not a dat2jsonl record")
            prov["inputs"]["source"] = public_source(source)
            a.output.write_text(json.dumps(prov, indent=2) + "\n")
            print(f"  parts_input: inputs.source without paths or dates → {a.output}", file=sys.stderr)
        elif a.cmd == "parts":
            for _, part, _ in checked_parts(a.outdir):
                print(os.fspath(part))
        else:
            doc = manifest(a.outdir, a.jsonl, a.entry_count, a.expected_count, a.kind, a.directory)
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
