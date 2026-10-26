#!/usr/bin/env python3
"""
bench_queries.py — wall times of common user queries against a lake.

The queries are what users ask of UniProtKB, written the way the README
shows them, through the client's views and macros (uniprot_parquet.connect):

  lookup     one protein, a batch of 100 and a secondary accession, each
             single-step (WHERE accession = ...) and two-step (resolve in
             accession_map, then a read pruned on reviewed, organism_id)
  organism   one organism's proteome, sequences, features, xrefs, comments
             and joins (the helper macros)
  search     whole-lake filters: keyword, GO term, EC number, gene name,
             PDB id (xrefs), PubMed id
  aggregate  counts and distributions over whole tables

Each run opens a fresh connection (outside the timed window), so DuckDB
carries nothing between runs; the operating system's page cache does.  So
"first" is the coldest run this process can measure (cold only if no other
process read those files recently), and "warm" the median of the others.
Every result is read in full, as Arrow, inside the timed window.  "read" is
the bytes the process read during the first run (Linux /proc/self/io rchar:
page-cache hits included, nothing for a remote lake).

Values come from the lake itself (--accession and --organism must exist in
it, or another is chosen), so the same command runs on the test fixtures, a
subset and a full release.  --threads defaults to the CPUs this process may
use (SLURM's allocation); --memory-limit to 75% of SLURM's --mem when set.

Usage:
    python benchmarks/bench_queries.py --lake $OUT/lake --label 2026_03
    python benchmarks/bench_queries.py --lake $OUT/lake --group lookup --runs 5
    python benchmarks/bench_queries.py --lake https://host/release/lake --label remote --timeout 600
    python benchmarks/bench_queries.py --list
"""

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _bench_common import REPO_ROOT, eprint, human_size, write_results

sys.path.insert(0, REPO_ROOT)

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

HOT = ("accession, id, reviewed, organism_id, organism_scientific_name, gene_primary, protein_name_primary, length, "
       "sequence")
DEFAULT7 = "accession, id, reviewed, organism_id, organism_scientific_name, gene_primary, protein_name_primary"
GROUPS = ("lookup", "organism", "search", "aggregate")


def q(s: object) -> str:
    return "'" + str(s).replace("'", "''") + "'"


@dataclass
class Params:
    """The values the queries ask about: defaults, checked against the lake."""
    organism: int = 9606
    accession: str = "P04637"
    trembl_accession: str | None = None
    secondary_accession: str | None = None
    batch: list[str] = field(default_factory=list)
    gene: str | None = None
    pdb_id: str | None = None
    pubmed_id: str | None = None
    keyword: str = "Kinase"
    go_id: str = "GO:0005524"
    ec: str = "2.7.11.1"


def discover(con: duckdb.DuckDBPyConnection, p: Params, batch_size: int, seed: int) -> Params:
    """Fill in p from the lake: the accession and organism if absent, a TrEMBL
    entry of the organism, a secondary accession, a random batch, and the
    accession's own gene, PDB id and PubMed id (so those searches find rows)."""
    if not con.sql(f"SELECT 1 FROM entries WHERE organism_id = {p.organism} LIMIT 1").fetchall():
        row = con.sql("SELECT organism_id FROM entries GROUP BY organism_id ORDER BY count(*) DESC LIMIT 1").fetchall()
        eprint(f"  organism {p.organism} is not in the lake; using {row[0][0]}")
        p.organism = row[0][0]
    if not con.sql(f"SELECT 1 FROM accession_map WHERE accession = {q(p.accession)}").fetchall():
        row = con.sql(f"SELECT accession FROM entries WHERE organism_id = {p.organism} "
                      f"ORDER BY reviewed DESC, accession LIMIT 1").fetchall()
        eprint(f"  accession {p.accession} is not in the lake; using {row[0][0]}")
        p.accession = row[0][0]
    row = con.sql(f"SELECT accession FROM entries WHERE NOT reviewed AND organism_id = {p.organism} "
                  f"LIMIT 1").fetchall()
    p.trembl_accession = row[0][0] if row else None
    row = con.sql("SELECT accession FROM accession_map WHERE NOT is_primary LIMIT 1").fetchall()
    p.secondary_accession = row[0][0] if row else None
    # SAMPLE applies before WHERE, so the filter goes in a subquery
    p.batch = [r[0] for r in con.sql(
        f"SELECT accession FROM (SELECT accession FROM accession_map WHERE is_primary) "
        f"USING SAMPLE reservoir({batch_size} ROWS) REPEATABLE ({seed})").fetchall()]
    row = con.sql(f"SELECT gene_primary[1], lit_pubmed_id[1] FROM entries "
                  f"WHERE accession = {q(p.accession)}").fetchall()
    p.gene, p.pubmed_id = row[0] if row else (None, None)
    row = con.sql(f"SELECT min(id) FROM xrefs WHERE accession = {q(p.accession)} AND database = 'PDB'").fetchall()
    p.pdb_id = row[0][0]
    return p


# ── Two-step lookup (README "Looking up by accession") ──────────────────────

def resolve(con: duckdb.DuckDBPyConnection, accessions: Sequence[str]) -> list[tuple[Any, ...]]:
    """Step 1: (reviewed, organism_id, primary_accession) of each accession."""
    in_list = ", ".join(q(a) for a in accessions)
    return con.sql(f"SELECT DISTINCT reviewed, organism_id, primary_accession FROM accession_map "
                   f"WHERE accession IN ({in_list})").fetchall()


def keyed_sql(table: str, columns: str, keys: Sequence[tuple[Any, ...]], order: str = "") -> str:
    """Step 2: the keys as literals, so DuckDB prunes row groups on them."""
    preds = " OR ".join(f"(reviewed = {str(r).lower()} AND organism_id = {o} AND accession = {q(a)})"
                        for r, o, a in keys)
    return f"SELECT {columns} FROM {table} WHERE {preds or 'false'}{order}"


# ── The queries ─────────────────────────────────────────────────────────────

Run = Callable[["duckdb.DuckDBPyConnection", Params], "pa.Table"]


@dataclass
class Query:
    name: str
    group: str
    description: str
    run: Run
    needs: str | None = None        # a Params field that must be set, or the query is skipped


def fetch(con: duckdb.DuckDBPyConnection, sql: str) -> pa.Table:
    return con.sql(sql).arrow().read_all()


def sql(template: str) -> Run:
    """A query whose SQL is `template` formatted with the Params, strings as SQL literals."""
    return lambda con, p: fetch(con, template.format(
        **{k: q(v) if isinstance(v, str) else v for k, v in asdict(p).items()}))


def two_step(table: str, columns: str, which: str = "accession", order: str = "") -> Run:
    def run(con: duckdb.DuckDBPyConnection, p: Params) -> pa.Table:
        value = getattr(p, which)
        keys = resolve(con, value if isinstance(value, list) else [value])
        return fetch(con, keyed_sql(table, columns, keys, order))
    return run


QUERIES: list[Query] = [
    # ── lookup ──
    Query("entry_single_step", "lookup", "One entry's first nine columns, WHERE accession = (scans the column)",
          sql(f"SELECT {HOT} FROM entries WHERE accession = {{accession}}")),
    Query("entry_two_step", "lookup", "The same through accession_map, then a pruned read",
          two_step("entries", HOT)),
    Query("protein_card", "lookup", "protein_card() macro",
          sql("SELECT * FROM protein_card({accession})")),
    Query("features_single_step", "lookup", "One entry's features in UniProt's order, WHERE accession = ",
          sql("SELECT feature_index, type, start_pos, end_pos, description FROM features "
              "WHERE accession = {accession} ORDER BY feature_index")),
    Query("features_two_step", "lookup", "The same, two-step",
          two_step("features", "feature_index, type, start_pos, end_pos, description",
                   order=" ORDER BY feature_index")),
    Query("xrefs_two_step", "lookup", "One entry's cross-references, two-step",
          two_step("xrefs", "database, id, properties", order=" ORDER BY xref_index")),
    Query("trembl_entry_two_step", "lookup", "One TrEMBL entry, two-step",
          two_step("entries", HOT, "trembl_accession"), needs="trembl_accession"),
    Query("secondary_accession", "lookup", "A secondary accession to its current entry, two-step",
          two_step("entries", DEFAULT7, "secondary_accession"), needs="secondary_accession"),
    Query("batch_single_step", "lookup", "A random batch of entries, WHERE accession IN (...)",
          lambda con, p: fetch(con, f"SELECT {HOT} FROM entries WHERE accession IN "
                                    f"({', '.join(q(a) for a in p.batch)})"), needs="batch"),
    Query("batch_two_step", "lookup", "The same batch, two-step",
          two_step("entries", HOT, "batch"), needs="batch"),
    Query("isoforms", "lookup", "unnest_isoforms() macro",
          sql("SELECT * FROM unnest_isoforms({accession})")),
    # ── organism ──
    Query("proteome", "organism", "Every entry of the organism, default columns",
          sql(f"SELECT {DEFAULT7} FROM entries WHERE organism_id = {{organism}}")),
    Query("swissprot_sequences", "organism", "The organism's Swiss-Prot sequences (a FASTA export)",
          sql("SELECT accession, id, protein_name_primary, sequence.value FROM entries "
              "WHERE reviewed AND organism_id = {organism}")),
    Query("organism_features", "organism", "organism_features(organism, 'Transmembrane')",
          sql("SELECT * FROM organism_features({organism}, 'Transmembrane')")),
    Query("organism_xrefs", "organism", "organism_xrefs(organism, ['PDB', 'AlphaFoldDB'])",
          sql("SELECT * FROM organism_xrefs({organism}, ['PDB', 'AlphaFoldDB'])")),
    Query("organism_comments", "organism", "organism_comments(organism, 'FUNCTION')",
          sql("SELECT * FROM organism_comments({organism}, 'FUNCTION')")),
    Query("entries_with_features", "organism", "entries_with_features(organism), signal peptides",
          sql("SELECT * FROM entries_with_features({organism}) WHERE type = 'Signal'")),
    Query("entries_with_xrefs", "organism", "entries_with_xrefs(organism, ['Ensembl'])",
          sql("SELECT * FROM entries_with_xrefs({organism}, ['Ensembl'])")),
    # ── search ──
    Query("keyword_swissprot", "search", "Swiss-Prot entries with a keyword (default Kinase)",
          sql("SELECT accession, gene_primary, protein_name_primary, organism_scientific_name FROM entries "
              "WHERE reviewed AND list_contains(keyword, {keyword})")),
    Query("go_term", "search", "Every entry with a GO term (default GO:0005524, ATP binding)",
          sql("SELECT accession, organism_id FROM entries WHERE list_contains(go_id, {go_id})")),
    Query("ec_number", "search", "Every entry with an EC number (default 2.7.11.1)",
          sql("SELECT accession, organism_id, protein_name_primary FROM entries "
              "WHERE list_contains(ec, {ec})")),
    Query("gene_name", "search", "A gene name in every organism",
          sql("SELECT accession, reviewed, organism_id, organism_scientific_name FROM entries "
              "WHERE list_contains(gene_primary, {gene})"), needs="gene"),
    Query("pdb_id_to_accession", "search", "Entries cross-referencing a PDB id (ID mapping)",
          sql("SELECT accession, reviewed, organism_id FROM xrefs WHERE database = 'PDB' AND id = {pdb_id}"),
          needs="pdb_id"),
    Query("pubmed_id", "search", "Entries citing a PubMed id",
          sql("SELECT accession, organism_id FROM entries WHERE list_contains(lit_pubmed_id, {pubmed_id})"),
          needs="pubmed_id"),
    # ── aggregate ──
    Query("count_entries", "aggregate", "count(*) of entries (Parquet metadata only)",
          sql("SELECT count(*) FROM entries")),
    Query("entries_by_division", "aggregate", "Entries per taxonomic division and review status",
          sql("SELECT division, reviewed, count(*) AS n FROM entries GROUP BY ALL ORDER BY ALL")),
    Query("top_organisms_swissprot", "aggregate", "The 20 organisms with most Swiss-Prot entries",
          sql("SELECT organism_id, organism_scientific_name, count(*) AS n FROM entries WHERE reviewed "
              "GROUP BY ALL ORDER BY n DESC LIMIT 20")),
    Query("length_by_status", "aggregate", "Sequence length statistics per review status",
          sql("SELECT reviewed, count(*), avg(length), median(length), max(length) FROM entries GROUP BY ALL")),
    Query("feature_types_swissprot", "aggregate", "Swiss-Prot features per type",
          sql("SELECT type, count(*) AS n FROM features WHERE reviewed GROUP BY ALL ORDER BY n DESC")),
    Query("xref_databases", "aggregate", "Cross-references per database (every xrefs row)",
          sql("SELECT database, count(*) AS n FROM xrefs GROUP BY ALL ORDER BY n DESC")),
]


# ── Measuring ───────────────────────────────────────────────────────────────

def rchar() -> int | None:
    """Bytes this process has read (Linux), or None."""
    try:
        with open("/proc/self/io") as f:
            for line in f:
                if line.startswith("rchar:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


def timed_run(con: duckdb.DuckDBPyConnection, query: Query, p: Params,
              timeout: float | None) -> tuple[float, pa.Table, int | None]:
    """(seconds, result, bytes read) of one run; DuckDB's InterruptException after `timeout`."""
    timer = threading.Timer(timeout, con.interrupt) if timeout else None
    before = rchar()
    if timer:
        timer.start()
    t0 = time.perf_counter()
    try:
        result = query.run(con, p)
    finally:
        elapsed = time.perf_counter() - t0
        if timer:
            timer.cancel()
    after = rchar()
    return elapsed, result, (after - before if before is not None and after is not None else None)


def bench(open_con: Callable[[], duckdb.DuckDBPyConnection], query: Query, p: Params, runs: int,
          timeout: float | None) -> dict[str, Any]:
    import duckdb
    row: dict[str, Any] = {"name": query.name, "group": query.group, "description": query.description}
    if query.needs and not getattr(p, query.needs):
        row["skipped"] = f"the lake has no {query.needs.replace('_', ' ')}"
        return row
    times: list[float] = []
    for i in range(runs):
        con = open_con()
        try:
            elapsed, result, read = timed_run(con, query, p, timeout)
        except duckdb.InterruptException:
            row["timeout"] = f"over {timeout:g} s"
            break
        except duckdb.Error as e:
            row["error"] = f"{type(e).__name__}: {e}".splitlines()[0]
            break
        finally:
            con.close()
        times.append(elapsed)
        if i == 0:
            row.update(first_s=round(elapsed, 4), rows=result.num_rows, result_bytes=result.nbytes)
            if read is not None:
                row["read_bytes"] = read
    if len(times) > 1:
        rest = times[1:]
        row.update(warm_median_s=round(statistics.median(rest), 4), warm_min_s=round(min(rest), 4))
    row["runs"] = len(times)
    return row


def secs(s: float | None) -> str:
    if s is None:
        return ""
    return f"{s * 1000:.1f} ms" if s < 1 else f"{s:.2f} s"


def report(meta: dict[str, Any], rows: Sequence[dict[str, Any]]) -> str:
    lines = [f"Query benchmark — {meta['label']}", "",
             f"Lake {meta['lake']} (release {meta['release']}), DuckDB {meta['duckdb']}, "
             f"{meta['threads']} threads, memory limit {meta['memory_limit'] or 'DuckDB default'}, "
             f"{meta['runs']} runs, host {meta['host']}", "",
             "Values: " + ", ".join(f"{k}={v}" for k, v in meta["params"].items() if k != "batch")
             + f", batch of {len(meta['params']['batch'])}", "",
             "first: the first run; warm: the median of the others (fresh connection each run, OS cache warm).",
             "read: bytes read by the process during the first run (Linux rchar, page-cache hits included).", ""]
    header = f"{'group':<10} {'query':<26} {'first':>10} {'warm':>10} {'rows':>12} {'result':>11} {'read':>11}"
    lines += [header, "-" * len(header)]
    for r in rows:
        status = r.get("skipped") or r.get("timeout") or r.get("error")
        if status:
            lines.append(f"{r['group']:<10} {r['name']:<26} {status}")
            continue
        lines.append(f"{r['group']:<10} {r['name']:<26} {secs(r['first_s']):>10} {secs(r.get('warm_median_s')):>10} "
                     f"{r['rows']:>12,} {human_size(r['result_bytes']):>11} "
                     f"{human_size(r['read_bytes']) if 'read_bytes' in r else '':>11}")
    return "\n".join(lines) + "\n"


def default_threads() -> int:
    """The CPUs this process may run on (SLURM's allocation), not the node's."""
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 1


def default_memory_limit() -> str | None:
    """75% of SLURM's --mem (SLURM_MEM_PER_NODE, in MB), or None outside SLURM."""
    mb = os.environ.get("SLURM_MEM_PER_NODE")
    return f"{int(mb) * 3 // 4}MB" if mb and mb.isdigit() else None


def git_commit() -> str | None:
    try:
        return subprocess.run(["git", "-C", REPO_ROOT, "rev-parse", "HEAD"], capture_output=True, text=True,
                              check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def select(names: Sequence[str] | None, groups: Sequence[str] | None) -> list[Query]:
    chosen = [qy for qy in QUERIES if (not groups or qy.group in groups) and (not names or qy.name in names)]
    unknown = set(names or ()) - {qy.name for qy in QUERIES}
    if unknown:
        sys.exit(f"Unknown query: {', '.join(sorted(unknown))} (--list shows them)")
    return chosen


def main(argv: Sequence[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Wall times of common user queries against a lake",
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("--lake", help="Lake directory or URL (what uniprot_parquet.connect() takes)")
    ap.add_argument("--label", default="queries", help="Name of the results files (default: queries)")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
    ap.add_argument("--runs", type=int, default=3, help="Runs per query, the first included (default: 3)")
    ap.add_argument("--timeout", type=float, default=None, help="Seconds before a run is interrupted")
    ap.add_argument("--group", action="append", choices=GROUPS, help="Only these groups (repeatable)")
    ap.add_argument("--query", action="append", help="Only these queries (repeatable; --list)")
    ap.add_argument("--list", action="store_true", help="List the queries and exit")
    ap.add_argument("--threads", type=int, default=default_threads())
    ap.add_argument("--memory-limit", default=default_memory_limit(), help="DuckDB memory limit, e.g. 16GB")
    ap.add_argument("--organism", type=int, default=Params.organism)
    ap.add_argument("--accession", default=Params.accession)
    ap.add_argument("--keyword", default=Params.keyword)
    ap.add_argument("--go-id", default=Params.go_id)
    ap.add_argument("--ec", default=Params.ec)
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args(argv)

    queries = select(args.query, args.group)
    if args.list:
        for qy in queries:
            print(f"{qy.group:<10} {qy.name:<26} {qy.description}")
        return
    if not args.lake:
        ap.error("--lake is required")
    if args.runs < 1:
        ap.error("--runs must be at least 1")

    import duckdb
    import pyarrow  # noqa: F401 (imported now, not inside the first timed run)

    from uniprot_parquet import connect, manifest

    def open_con() -> duckdb.DuckDBPyConnection:
        return connect(args.lake, memory_limit=args.memory_limit, threads=args.threads)

    eprint("Choosing values from the lake ...")
    con = open_con()
    p = discover(con, Params(organism=args.organism, accession=args.accession, keyword=args.keyword,
                             go_id=args.go_id, ec=args.ec), args.batch_size, args.seed)
    con.close()
    meta: dict[str, Any] = {
        "label": args.label, "lake": args.lake, "release": manifest(args.lake).get("release"),
        "duckdb": duckdb.__version__, "threads": args.threads, "memory_limit": args.memory_limit,
        "runs": args.runs, "timeout_s": args.timeout, "host": platform.node(), "cpus": os.cpu_count(),
        "python": platform.python_version(), "commit": git_commit(),
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "params": asdict(p)}
    eprint(json.dumps({k: v for k, v in asdict(p).items() if k != "batch"}))

    rows: list[dict[str, Any]] = []
    for qy in queries:
        row = bench(open_con, qy, p, args.runs, args.timeout)
        rows.append(row)
        status = row.get("skipped") or row.get("timeout") or row.get("error")
        eprint(f"  {qy.group:<10} {qy.name:<26} "
               + (status or f"{secs(row['first_s']):>10} {secs(row.get('warm_median_s')):>10} {row['rows']:>12,}"))
    text = report(meta, rows)
    print(text)
    path = write_results(f"queries_{args.label}", {"meta": meta, "queries": rows}, args.out, text)
    eprint(f"wrote {path}")


if __name__ == "__main__":
    main()
