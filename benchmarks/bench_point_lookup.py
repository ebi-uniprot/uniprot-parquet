#!/usr/bin/env python3
"""
bench_point_lookup.py — evidence that accession lookup works.

Workloads: 1, 37 and 1,000 accessions, each against entries (the nine hot
columns), features and xrefs, each in the single-step form
(`WHERE accession IN (...)`) and the two-step form (resolve through accession_map,
then `WHERE reviewed = ? AND organism_id = ? AND accession = ?` per accession), locally
and — when --url is given — over httpfs (request count and bytes from a
Range-capable counting HTTP server when --serve is used, or DuckDB's own
counters are unavailable).  Plus one organism workload: the seven default
columns for one organism, request count.

Row groups: DuckDB 1.5.5 does not print row groups scanned in EXPLAIN
ANALYZE, so "candidate row groups" are counted from
parquet_metadata() min/max statistics — the number a reader must at least
look at — and the output says so.  Bloom filters are not written (PyArrow
23 cannot), so the single-step form has no filter to consult.

Wall times over the --serve local Python server carry a ~1 s per-query
connection artefact (DuckDB 1.5.5 httpfs against http.server; the same query
takes 3 ms once DuckDB's metadata cache is warm), so from a --serve run take
the request and byte counts; take remote wall times from --url against a
real HTTP server.

Prints the pass criteria with PASS/FAIL per criterion.
Run on the slice or a full build, never on the fixture (one row group per
table shows nothing); the fixture run only proves the mechanics.

Usage:
    python benchmarks/bench_point_lookup.py --lake slice/lake --label slice
    python benchmarks/bench_point_lookup.py --lake slice/lake --serve --label slice_http
    python benchmarks/bench_point_lookup.py --lake slice/lake --url https://host/release/lake --label remote
"""

import argparse
import os
import random
import sys
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _bench_common import (
    duckdb_http_connection,
    eprint,
    human_size,
    read_parquet_source,
    serve_directory,
    time_query,
    write_results,
)

if TYPE_CHECKING:
    import duckdb

HOT = ("accession, id, reviewed, organism_id, organism_scientific_name, gene_primary, protein_name_primary, length, "
       "sequence")
DEFAULT7 = "accession, id, reviewed, organism_id, organism_scientific_name, gene_primary, protein_name_primary"
SIZES = (1, 37, 1000)
TABLES = ("entries", "features", "xrefs")


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def candidate_row_groups(con: duckdb.DuckDBPyConnection, source: str, col: str, values: Sequence[Any],
                         is_string: bool = True) -> tuple[int, int]:
    """Row groups whose [min_value, max_value] on `col` admit any value (total, candidates).
    stats_min_value / stats_max_value, not stats_min / stats_max: the latter are the legacy
    Parquet fields, which PyArrow omits for strings."""
    rows = con.sql(f"""
        SELECT file_name, row_group_id, stats_min_value, stats_max_value
        FROM parquet_metadata({source}) WHERE path_in_schema = '{col}'
    """).fetchall()
    total = len(rows)
    cand = 0
    for _, _, lo, hi in rows:
        if lo is None or hi is None:
            cand += 1
            continue
        if is_string:
            if any(lo <= v <= hi for v in values):
                cand += 1
        else:
            if any(int(lo) <= v <= int(hi) for v in values):
                cand += 1
    return total, cand


def two_step_candidates(con: duckdb.DuckDBPyConnection, source: str,
                        keys: Sequence[tuple[Any, ...]]) -> tuple[int, int]:
    """Row groups of a data table that admit any (reviewed, organism_id, accession) key."""
    rows = con.sql(f"""
        SELECT r.file_name, r.row_group_id, r.stats_min_value, r.stats_max_value,
               t.stats_min_value, t.stats_max_value, a.stats_min_value, a.stats_max_value
        FROM parquet_metadata({source}) r
        JOIN parquet_metadata({source}) t USING (file_name, row_group_id)
        JOIN parquet_metadata({source}) a USING (file_name, row_group_id)
        WHERE r.path_in_schema = 'reviewed' AND t.path_in_schema = 'organism_id'
          AND a.path_in_schema = 'accession'
    """).fetchall()
    def admits(lo: Any, hi: Any, v: Any) -> bool:
        return lo is None or hi is None or lo <= v <= hi

    cand = 0
    for _, _, rlo, rhi, tlo, thi, alo, ahi in rows:
        for reviewed, organism_id, accession in keys:
            rv = "true" if reviewed else "false"
            if (admits(rlo and rlo.lower(), rhi and rhi.lower(), rv)
                    and admits(tlo and int(tlo), thi and int(thi), organism_id)
                    and admits(alo, ahi, accession)):
                cand += 1
                break
    return len(rows), cand


def run_workloads(con: duckdb.DuckDBPyConnection, lake: str, base: str | None, accessions: Sequence[str], label: str,
                  stats: dict[str, int] | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    src = {t: read_parquet_source(lake, t, base) for t in ("entries", "features", "xrefs", "accession_map")}
    results: list[dict[str, Any]] = []
    for n in SIZES:
        accs = accessions[:n]
        in_list = ",".join(q(a) for a in accs)
        # step 1 of the two-step form
        t0 = time.perf_counter()
        keys = con.sql(f"SELECT reviewed, organism_id, primary_accession "
                       f"FROM read_parquet({src['accession_map']}, hive_partitioning = true) "
                       f"WHERE accession IN ({in_list})").fetchall()
        step1_ms = (time.perf_counter() - t0) * 1000
        amap_total, amap_cand = candidate_row_groups(con, src["accession_map"], "accession", accs)
        for table in TABLES:
            cols = HOT if table == "entries" else "*"
            single = (f"SELECT {cols} FROM read_parquet({src[table]}, hive_partitioning = true) "
                      f"WHERE accession IN ({in_list})")
            preds = " OR ".join(f"(reviewed = {str(r).lower()} AND organism_id = {t} AND accession = {q(a)})"
                                for r, t, a in keys)
            two = (f"SELECT {cols} FROM read_parquet({src[table]}, hive_partitioning = true) WHERE {preds}"
                   if keys else single)
            for form, sql in (("single", single), ("two_step", two)):
                con.sql(sql).fetchall()                  # warm-up, outside the counted window
                if stats is not None:
                    stats["requests"] = 0
                    stats["bytes"] = 0
                timing = time_query(con, sql, warmup=0, runs=5)
                row: dict[str, Any] = {"n": n, "table": table, "form": form, **timing}
                if stats is not None:
                    row["requests_per_run"] = round(stats["requests"] / 5)
                    row["bytes_per_run"] = round(stats["bytes"] / 5)
                if form == "single":
                    row["row_groups_total"], row["candidate_row_groups"] = candidate_row_groups(
                        con, src[table], "accession", accs)
                else:
                    total, cand = two_step_candidates(con, src[table], keys)
                    row["row_groups_total"], row["candidate_row_groups"] = total, cand
                    row["accession_map_row_groups"] = amap_cand
                    row["accession_map_total"] = amap_total
                    row["step1_ms"] = round(step1_ms, 3)
                results.append(row)
                eprint(f"  [{label}] n={n:5d} {table:8s} {form:8s} {timing['median_ms']:9.2f} ms "
                       f"rg {row['candidate_row_groups']}/{row['row_groups_total']}")
    # Organism workload: default seven columns for one organism
    # fetchone() is None only if accessions[0] is not in entries (then TypeError, as before)
    organism_id = cast(tuple[Any, ...], con.sql(
        f"SELECT organism_id FROM read_parquet({src['entries']}, hive_partitioning = true) "
        f"WHERE accession = {q(accessions[0])}").fetchone())[0]
    sql = (f"SELECT {DEFAULT7} FROM read_parquet({src['entries']}, hive_partitioning = true) "
           f"WHERE organism_id = {organism_id}")
    con.sql(sql).fetchall()                              # warm-up, outside the counted window
    if stats is not None:
        stats["requests"] = 0
        stats["bytes"] = 0
    timing = time_query(con, sql, warmup=0, runs=3)
    c3: dict[str, Any] = {"organism_id": organism_id, **timing}
    if stats is not None:
        c3["requests_per_run"] = round(stats["requests"] / 3)
        c3["bytes_per_run"] = round(stats["bytes"] / 3)
    return results, c3


def pass_criteria(results: Sequence[dict[str, Any]], c3: dict[str, Any]) -> list[tuple[str, bool | None, str]]:
    """Pass criteria, PASS/FAIL each (bloom-filter criterion is N/A: none written)."""
    out: list[tuple[str, bool | None, str]] = []
    two_entries_1 = next((r for r in results if r["n"] == 1 and r["table"] == "entries" and r["form"] == "two_step"),
                         None)
    if two_entries_1:
        total = two_entries_1["candidate_row_groups"] + two_entries_1["accession_map_row_groups"]
        out.append(("two-step entries lookup reads <= 3 row groups (accession_map + entries)", total <= 3, f"{total}"))
    out.append(("single-step entries false positives within 2 x row_groups x fpp", None,
                "N/A: no bloom filters (PyArrow 23 cannot write them)"))
    two_37 = next((r for r in results if r["n"] == 37 and r["table"] == "entries" and r["form"] == "two_step"), None)
    single_37 = next((r for r in results if r["n"] == 37 and r["table"] == "entries" and r["form"] == "single"), None)
    if two_37 and single_37:
        speedup = single_37["median_ms"] / max(two_37["median_ms"] + two_37["step1_ms"], 1e-6)
        out.append(("37-accession batch, two-step >= 10x faster than single-step baseline", speedup >= 10,
                    f"{speedup:.1f}x"))
    if two_entries_1 and "requests_per_run" in two_entries_1:
        out.append(("over httpfs the two-step form issues < 20 range requests", two_entries_1["requests_per_run"] < 20,
                    f"{two_entries_1['requests_per_run']}"))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Point-lookup benchmark")
    ap.add_argument("--lake", required=True, help="Local lake directory (manifest.json is read from here)")
    ap.add_argument("--url", default=None, help="Optional httpfs base URL of the same lake")
    ap.add_argument("--serve", action="store_true", help="Serve --lake over a local counting HTTP server")
    ap.add_argument("--accessions", default=None,
                    help="File with one accession per line (default: 1,000 sampled from accession_map)")
    ap.add_argument("--label", default="point_lookup")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "results"))
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    import duckdb
    con = duckdb.connect()
    if args.accessions:
        with open(args.accessions) as f:
            accessions = [line.strip() for line in f if line.strip()]
    else:
        src = read_parquet_source(args.lake, "accession_map")
        accs = [r[0] for r in
                con.sql(f"SELECT accession FROM read_parquet({src}, hive_partitioning = true)").fetchall()]
        random.Random(args.seed).shuffle(accs)
        accessions = accs[:1000]
    eprint(f"{len(accessions)} accessions")

    results: dict[str, Any] = {"lake": os.path.abspath(args.lake), "label": args.label, "n_accessions": len(accessions),
               "note": "candidate row groups are counted from parquet_metadata min/max statistics; "
                       "DuckDB 1.5.5 does not report row groups scanned, and no bloom filters are written. "
                       "Wall times from --serve (local http.server) include a ~1 s connection artefact; "
                       "use its request/byte counts, and --url against a real server for remote wall times."}
    eprint("[local]")
    results["local"], results["local_c3"] = run_workloads(con, args.lake, None, accessions, "local")
    results["criteria_local"] = pass_criteria(results["local"], results["local_c3"])

    if args.serve:
        with serve_directory(args.lake) as (base, stats):
            hcon = duckdb_http_connection()
            eprint(f"[http {base}]")
            results["http"], results["http_c3"] = run_workloads(hcon, args.lake, base, accessions, "http", stats)
            results["criteria_http"] = pass_criteria(results["http"], results["http_c3"])
    elif args.url:
        hcon = duckdb_http_connection()
        eprint(f"[remote {args.url}]")
        results["remote"], results["remote_c3"] = run_workloads(hcon, args.lake, args.url.rstrip("/"), accessions,
                                                                "remote")
        results["criteria_remote"] = pass_criteria(results["remote"], results["remote_c3"])

    lines = [f"Point-lookup benchmark — {args.label}", "", results["note"], ""]
    for scope in ("local", "http", "remote"):
        if scope not in results:
            continue
        lines.append(f"## {scope}")
        lines.append("| n | table | form | median ms | candidate row groups / total | requests/run | bytes/run |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for r in results[scope]:
            lines.append(f"| {r['n']} | {r['table']} | {r['form']} | {r['median_ms']} | "
                         f"{r['candidate_row_groups']}/{r['row_groups_total']} | {r.get('requests_per_run', '')} | "
                         f"{human_size(r['bytes_per_run']) if 'bytes_per_run' in r else ''} |")
        c3 = results[f"{scope}_c3"]
        lines.append(f"Default-7-column organism query (organism_id {c3['organism_id']}): {c3['median_ms']} ms"
                     + (f", {c3['requests_per_run']} requests/run, {human_size(c3['bytes_per_run'])}/run"
                        if "requests_per_run" in c3 else ""))
        lines.append("")
        lines.append("Pass criteria:")
        for name, ok, detail in results[f"criteria_{scope}"]:
            verdict = "N/A" if ok is None else ("PASS" if ok else "FAIL")
            lines.append(f"  [{verdict}] {name} — {detail}")
        lines.append("")
    text = "\n".join(lines) + "\n"
    print(text)
    path = write_results(args.label, results, args.out, text)
    eprint(f"wrote {path}")


if __name__ == "__main__":
    main()
