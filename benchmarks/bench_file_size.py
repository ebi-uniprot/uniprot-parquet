#!/usr/bin/env python3
"""
bench_file_size.py — plan F.4: footer cost vs file-size target for `entries`.

For each target in (256, 512, 1024) MB: build `entries` only from a JSONL
into a temp dir, serve it over HTTP, run the default-column organism query
(`WHERE taxid = 9606`) through DuckDB httpfs, and record files opened, footer
bytes of those files, data bytes fetched (bytes served by the HTTP server),
request count and wall time; plus the footer-size delta with and without the
page index.  Prints the plan's F.4 table and writes JSON + text under --out.

Meant for the F.1 measurement slice.  On the test fixtures every build is one
file per side, so the table only shows the mechanics.

Usage:
    python benchmarks/bench_file_size.py --jsonl slice.sorted.jsonl.zst --out benchmarks/results/
"""

import os
import sys
import argparse
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _bench_common import (  # noqa: E402
    eprint, human_size, build_lake, table_files, footer_bytes, table_bytes,
    serve_directory, duckdb_http_connection, time_query, write_results, read_parquet_source,
)

QUERY = ("SELECT acc, id, reviewed, taxid, organism_name, gene_names, protein_name, seq_length "
         "FROM read_parquet({source}, hive_partitioning = true) WHERE taxid = {taxid}")
TARGETS_MB = (256, 512, 1024)
FULL_ENTRIES = 248_000_000


def measure(jsonl, target_mb, taxid, memory, tmp_root):
    outdir = os.path.join(tmp_root, f"target_{target_mb}")
    build_s = build_lake(jsonl, outdir, only="entries", memory=memory,
                         extra=["--target-file-bytes", str(target_mb * 1024 * 1024)])
    files = table_files(outdir, "entries")
    row = {"target_mb": target_mb, "build_s": round(build_s, 1), "files": len(files),
           "entries_bytes": table_bytes(files), "footer_bytes_all": footer_bytes(files)}
    import pyarrow.parquet as pq
    n_rows = sum(pq.read_metadata(f).num_rows for f in files)
    row["rows"] = n_rows
    row["files_extrapolated"] = round(len(files) * FULL_ENTRIES / max(n_rows, 1))
    with serve_directory(outdir) as (base, stats):
        con = duckdb_http_connection()
        sql = QUERY.format(source=read_parquet_source(outdir, "entries", base), taxid=taxid)
        con.sql(sql).fetchall()                      # cold run: what a first query costs
        cold = dict(stats)
        stats["requests"] = 0
        stats["bytes"] = 0
        timing = time_query(con, sql, warmup=0, runs=3)
        row.update({"cold_requests": cold["requests"], "cold_bytes": cold["bytes"],
                    "warm_requests_per_run": round(stats["requests"] / 3),
                    "warm_bytes_per_run": round(stats["bytes"] / 3),
                    "wall_ms_median": timing["median_ms"], "result_rows": timing["row_count"]})
        # Files opened: DuckDB reads footers of every candidate file; with
        # hive pruning only files whose taxid range overlaps are read fully.
        txt = con.sql("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
        import re
        m = re.search(r"Total Files Read: (\d+)", txt)
        row["files_opened"] = int(m.group(1)) if m else None
    row["footer_bytes_opened"] = (footer_bytes(files) if row["files_opened"] is None
                                  else round(footer_bytes(files) * row["files_opened"] / len(files)))
    row["footer_over_data"] = (round(row["footer_bytes_opened"] / row["cold_bytes"], 4)
                               if row["cold_bytes"] else None)
    return row


def page_index_delta(jsonl, memory, tmp_root):
    """Footer bytes with (default) and without write_page_index, same build."""
    import pyarrow.parquet as pq
    outdir = os.path.join(tmp_root, "pageindex")
    build_lake(jsonl, outdir, only="entries", memory=memory)
    files = table_files(outdir, "entries")
    with_idx = footer_bytes(files)
    without = 0
    for f in files:
        t = pq.read_table(f)
        p = f + ".noidx"
        pq.write_table(t, p, compression="zstd", write_page_index=False)
        without += pq.read_metadata(p).serialized_size
        os.remove(p)
    return {"with_page_index": with_idx, "without_page_index": without, "delta": with_idx - without}


def main():
    ap = argparse.ArgumentParser(description="F.4 file-size target benchmark (entries only)")
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "results"))
    ap.add_argument("--taxid", type=int, default=9606)
    ap.add_argument("--memory", default="4GB")
    ap.add_argument("--targets", default=",".join(str(t) for t in TARGETS_MB), help="MB, comma-separated")
    ap.add_argument("--label", default="file_size")
    args = ap.parse_args()

    results = {"jsonl": os.path.abspath(args.jsonl), "taxid": args.taxid, "rows": []}
    with tempfile.TemporaryDirectory(prefix="bench_file_size_") as tmp:
        for target in (int(t) for t in args.targets.split(",")):
            eprint(f"[target {target} MB] building entries…")
            results["rows"].append(measure(args.jsonl, target, args.taxid, args.memory, tmp))
        eprint("[page index] measuring footer delta…")
        results["page_index"] = page_index_delta(args.jsonl, args.memory, tmp)

    lines = ["F.4 file-size target (entries only, organism query over httpfs)", ""]
    lines.append("| Target | Files (this input) | Files (extrapolated to 248M) | Footer bytes (opened files) | Data bytes (cold) | Footer / data | Requests (cold) | Wall ms (median, warm) |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in results["rows"]:
        lines.append(f"| {r['target_mb']} MB | {r['files']} | {r['files_extrapolated']:,} | "
                     f"{human_size(r['footer_bytes_opened'])} | {human_size(r['cold_bytes'])} | "
                     f"{r['footer_over_data']} | {r['cold_requests']} | {r['wall_ms_median']} |")
    pi = results["page_index"]
    lines.append("")
    lines.append(f"Page index footer delta: {human_size(pi['delta'])} "
                 f"({human_size(pi['with_page_index'])} with, {human_size(pi['without_page_index'])} without)")
    lines.append("")
    lines.append("Decision rule (plan F.4): smallest target whose extrapolated footer bytes are < 10% of data bytes; else 1 GB.")
    text = "\n".join(lines) + "\n"
    print(text)
    path = write_results(args.label, results, args.out, text)
    eprint(f"wrote {path}")


if __name__ == "__main__":
    main()
