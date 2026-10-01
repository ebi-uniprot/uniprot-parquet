#!/usr/bin/env python3
"""
bench_zstd.py — choose the zstd level by measurement.

For each level in (1, 3, 9, 15): build `entries` and `xrefs` from a JSONL with
--zstd-level, record compressed bytes and the Parquet write wall time, and
re-run the organism query of bench_file_size.py locally for read time.
Prints a Markdown table and writes JSON + text under --out.

Meant for the measurement slice; the test fixtures only show the mechanics.

Usage:
    python benchmarks/bench_zstd.py --jsonl slice.sorted.jsonl.zst --out benchmarks/results/
"""

import os
import sys
import argparse
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _bench_common import (  # noqa: E402
    eprint, human_size, build_lake, table_files, table_bytes, time_query, write_results,
)

LEVELS = (1, 3, 9, 15)
QUERY = ("SELECT accession, id, reviewed, organism_id, organism_scientific_name, gene_primary, protein_name_primary, length "
         "FROM read_parquet('{lake}/entries/**/*.parquet', hive_partitioning = true) WHERE organism_id = {organism_id}")


def measure(jsonl, level, organism_id, memory, tmp_root):
    import duckdb
    outdir = os.path.join(tmp_root, f"zstd_{level}")
    wall = build_lake(jsonl, outdir, only="entries,xrefs", memory=memory,
                      extra=["--zstd-level", str(level)])
    entries = table_bytes(table_files(outdir, "entries"))
    xrefs = table_bytes(table_files(outdir, "xrefs"))
    con = duckdb.connect()
    timing = time_query(con, QUERY.format(lake=outdir, organism_id=organism_id))
    return {"level": level, "entries_bytes": entries, "xrefs_bytes": xrefs,
            "write_s": round(wall, 1), "read_ms_median": timing["median_ms"]}


def main():
    ap = argparse.ArgumentParser(description="zstd level sweep (entries + xrefs)")
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "results"))
    ap.add_argument("--organism_id", type=int, default=9606)
    ap.add_argument("--memory", default="4GB")
    ap.add_argument("--levels", default=",".join(str(l) for l in LEVELS))
    ap.add_argument("--label", default="zstd")
    args = ap.parse_args()

    results = {"jsonl": os.path.abspath(args.jsonl), "rows": []}
    with tempfile.TemporaryDirectory(prefix="bench_zstd_") as tmp:
        for level in (int(l) for l in args.levels.split(",")):
            eprint(f"[level {level}] building entries + xrefs…")
            results["rows"].append(measure(args.jsonl, level, args.organism_id, args.memory, tmp))

    base = results["rows"][0]
    lines = ["zstd level sweep (entries + xrefs; write_s is the whole transform incl. staging)", ""]
    lines.append("| Level | entries bytes | xrefs bytes | Write time | Read time (organism query) |")
    lines.append("| --- | --- | --- | --- | --- |")
    for r in results["rows"]:
        lines.append(f"| {r['level']} | {human_size(r['entries_bytes'])} ({r['entries_bytes']/base['entries_bytes']:.2f}×) "
                     f"| {human_size(r['xrefs_bytes'])} ({r['xrefs_bytes']/base['xrefs_bytes']:.2f}×) "
                     f"| {r['write_s']} s ({r['write_s']/base['write_s']:.2f}×) | {r['read_ms_median']} ms |")
    lines.append("")
    lines.append("Decision rule: highest level whose write-time cost stays within the SLURM budget "
                 "(a 2× slowdown of the Parquet write alone is acceptable; 2× of the whole stage is not).")
    text = "\n".join(lines) + "\n"
    print(text)
    path = write_results(args.label, results, args.out, text)
    eprint(f"wrote {path}")


if __name__ == "__main__":
    main()
