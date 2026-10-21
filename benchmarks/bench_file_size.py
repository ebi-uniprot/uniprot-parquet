#!/usr/bin/env python3
"""
bench_file_size.py — footer cost vs file-size target for `entries`.

For each target in (256, 512, 1024) MB: build `entries` only from a JSONL
into a temp dir, serve it over HTTP, run the default-column organism query
(`WHERE organism_id = 9606`) through DuckDB httpfs, and record files opened, footer
bytes of those files, data bytes fetched (bytes served by the HTTP server),
request count and wall time; plus the file bytes the page index adds.  Prints
a Markdown table and writes JSON + text under --out.

Meant for the measurement slice.  On the test fixtures every build is one
file per side, so the table only shows the mechanics.

Usage:
    python benchmarks/bench_file_size.py --jsonl slice.sorted.jsonl.zst --out benchmarks/results/
"""

import argparse
import math
import os
import sys
import tempfile
from collections.abc import Sequence
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _bench_common import (
    BIN_DIR,
    build_lake,
    duckdb_http_connection,
    eprint,
    footer_bytes,
    human_size,
    read_parquet_source,
    serve_directory,
    table_bytes,
    table_files,
    time_query,
    write_results,
)

sys.path.insert(0, BIN_DIR)
from parquet_transform import ROW_GROUP_SIZE

QUERY = ("SELECT accession, id, reviewed, organism_id, organism_scientific_name, "
         "gene_primary, protein_name_primary, length "
         "FROM read_parquet({source}, hive_partitioning = true) WHERE organism_id = {organism_id}")
TARGETS_MB = (256, 512, 1024)
FULL_ENTRIES = 248_000_000


def extrapolate_files(files: Sequence[str], target_bytes: int, full_rows: int = FULL_ENTRIES) -> int:
    """Files a full release would have at ``target_bytes``: each partition's
    bytes per row scaled to its share of ``full_rows``, in files of the
    target size.  Not the sample's own file count scaled by rows: a sample
    smaller than the target holds one partial file per partition."""
    import pyarrow.parquet as pq
    by_part: dict[str, tuple[int, int]] = {}
    for f in files:
        size, rows = by_part.get(os.path.dirname(f), (0, 0))
        by_part[os.path.dirname(f)] = (size + os.path.getsize(f), rows + pq.read_metadata(f).num_rows)
    total_rows = sum(rows for _, rows in by_part.values())
    if not total_rows:
        return 0
    return sum(math.ceil(size * full_rows / total_rows / target_bytes)
               for size, _ in by_part.values())


def measure(jsonl: str, target_mb: int, organism_id: int, memory: str, tmp_root: str) -> dict[str, Any]:
    outdir = os.path.join(tmp_root, f"target_{target_mb}")
    build_s = build_lake(jsonl, outdir, only="entries", memory=memory,
                         extra=["--target-file-bytes", str(target_mb * 1024 * 1024)])
    files = table_files(outdir, "entries")
    row: dict[str, Any] = {"target_mb": target_mb, "build_s": round(build_s, 1), "files": len(files),
                           "entries_bytes": table_bytes(files), "footer_bytes_all": footer_bytes(files)}
    import pyarrow.parquet as pq
    n_rows = sum(pq.read_metadata(f).num_rows for f in files)
    row["rows"] = n_rows
    row["files_extrapolated"] = extrapolate_files(files, target_mb * 1024 * 1024)
    with serve_directory(outdir) as (base, stats):
        con = duckdb_http_connection()
        sql = QUERY.format(source=read_parquet_source(outdir, "entries", base), organism_id=organism_id)
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
        # hive pruning only files whose organism_id range overlaps are read fully.
        txt = con.sql("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
        import re
        m = re.search(r"Total Files Read: (\d+)", txt)
        row["files_opened"] = int(m.group(1)) if m else None
    row["footer_bytes_opened"] = (footer_bytes(files) if row["files_opened"] is None
                                  else round(footer_bytes(files) * row["files_opened"] / len(files)))
    # cold_bytes counts every byte served, the opened files' footers included.
    data_bytes = row["cold_bytes"] - row["footer_bytes_opened"]
    row["footer_over_data"] = (round(row["footer_bytes_opened"] / data_bytes, 4)
                               if data_bytes > 0 else None)
    return row


def page_index_delta(jsonl: str, memory: str, tmp_root: str) -> dict[str, int]:
    """File bytes with and without write_page_index: each built file
    rewritten both ways with the transform's row-group size, so only the
    page index differs.  File size, not the footer's serialized_size: the
    column and offset indexes sit between the data and the Thrift footer,
    which holds only their offsets."""
    import pyarrow.parquet as pq
    outdir = os.path.join(tmp_root, "pageindex")
    build_lake(jsonl, outdir, only="entries", memory=memory)
    sizes = {True: 0, False: 0}
    for f in table_files(outdir, "entries"):
        t = pq.read_table(f, partitioning=None)  # stored columns only, no Hive review_status
        for with_index in sizes:
            p = f + ".rewrite"
            pq.write_table(t, p, compression="zstd", row_group_size=ROW_GROUP_SIZE,
                           write_page_index=with_index)
            sizes[with_index] += os.path.getsize(p)
            os.remove(p)
    return {"with_page_index": sizes[True], "without_page_index": sizes[False],
            "delta": sizes[True] - sizes[False]}


def main() -> None:
    ap = argparse.ArgumentParser(description="File-size target benchmark (entries only)")
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "results"))
    ap.add_argument("--organism_id", type=int, default=9606)
    ap.add_argument("--memory", default="4GB")
    ap.add_argument("--targets", default=",".join(str(t) for t in TARGETS_MB), help="MB, comma-separated")
    ap.add_argument("--label", default="file_size")
    args = ap.parse_args()

    results: dict[str, Any] = {"jsonl": os.path.abspath(args.jsonl), "organism_id": args.organism_id, "rows": []}
    with tempfile.TemporaryDirectory(prefix="bench_file_size_") as tmp:
        for target in (int(t) for t in args.targets.split(",")):
            eprint(f"[target {target} MB] building entries…")
            results["rows"].append(measure(args.jsonl, target, args.organism_id, args.memory, tmp))
        eprint("[page index] measuring footer delta…")
        results["page_index"] = page_index_delta(args.jsonl, args.memory, tmp)

    lines = ["File-size target (entries only, organism query over httpfs)", ""]
    lines.append("| Target | Files (this input) | Files (extrapolated to 248M) | Footer bytes (opened files) "
                 "| Data bytes (cold) | Footer / data | Requests (cold) | Wall ms (median, warm) |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in results["rows"]:
        lines.append(f"| {r['target_mb']} MB | {r['files']} | {r['files_extrapolated']:,} | "
                     f"{human_size(r['footer_bytes_opened'])} | {human_size(r['cold_bytes'])} | "
                     f"{r['footer_over_data']} | {r['cold_requests']} | {r['wall_ms_median']} |")
    pi = results["page_index"]
    lines.append("")
    lines.append(f"Page index cost (file bytes): {human_size(pi['delta'])} "
                 f"({human_size(pi['with_page_index'])} with, {human_size(pi['without_page_index'])} without)")
    lines.append("")
    lines.append("Decision rule: smallest target whose extrapolated footer bytes are < 10% of data bytes; else 1 GB.")
    text = "\n".join(lines) + "\n"
    print(text)
    path = write_results(args.label, results, args.out, text)
    eprint(f"wrote {path}")


if __name__ == "__main__":
    main()
