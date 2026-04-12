#!/usr/bin/env python3
"""
Benchmark VARIANT-based Parquet lakes against the star-schema baseline.

Runs the same 16 queries from bench_baseline.py, adapted for VARIANT syntax,
against both Layout A (single table) and Layout B (star schema + VARIANT).

Usage:
    python benchmarks/bench_variant.py [--variant-lake benchmarks/variant_lake]
                                       [--baseline-lake demo/lake/2026_01/lake]

Output:
    benchmarks/results/variant_<timestamp>.json   (machine-readable)
    benchmarks/results/variant_<timestamp>.txt    (human-readable comparison)
"""

import argparse
import json
import os
import sys
import time
import statistics
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Utilities (shared with bench_baseline.py)
# ---------------------------------------------------------------------------

def _human_size(nbytes):
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024 or unit == "TB":
            if unit == "bytes":
                return f"{int(nbytes)} bytes"
            return f"{nbytes:.2f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.2f} TB"


def _time_query(con, sql, warmup=1, runs=3):
    """Run a query multiple times and return timing stats in milliseconds."""
    for _ in range(warmup):
        con.sql(sql).fetchall()

    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        result = con.sql(sql).fetchall()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000)

    return {
        "min_ms": round(min(times), 3),
        "max_ms": round(max(times), 3),
        "median_ms": round(statistics.median(times), 3),
        "mean_ms": round(statistics.mean(times), 3),
        "stdev_ms": round(statistics.stdev(times), 3) if len(times) > 1 else 0,
        "runs": runs,
        "row_count": len(result),
    }


# ---------------------------------------------------------------------------
# Query Suites (same 16 queries, adapted for each layout)
# ---------------------------------------------------------------------------

def _layout_a_queries(base):
    """Layout A: single entries table, everything via VARIANT dot notation."""
    e = f"read_parquet('{base}/entries/*.parquet')"
    return [
        # ── Point lookups ──
        {
            "name": "point_lookup_by_acc",
            "description": "Single protein lookup by accession",
            "sql": f"SELECT * FROM {e} WHERE acc = '{{acc}}'",
            "setup": f"SELECT acc FROM {e} WHERE reviewed LIMIT 1",
        },
        {
            "name": "protein_card_macro",
            "description": "Protein card equivalent — single accession, key fields",
            "sql": f"""
                SELECT acc, taxid, reviewed,
                       data.organism.scientificName AS organism_name,
                       data.proteinDescription.recommendedName.fullName.value AS protein_name,
                       CAST(data.sequence.value AS VARCHAR) AS sequence
                FROM {e} WHERE acc = '{{acc}}'
            """,
            "setup": f"SELECT acc FROM {e} WHERE reviewed LIMIT 1",
        },

        # ── Organism filters ──
        {
            "name": "organism_filter_human",
            "description": "All human entries (taxid=9606)",
            "sql": f"""
                SELECT acc,
                       list_transform(CAST(data.genes AS STRUCT(geneName STRUCT(value VARCHAR))[]),
                                      g -> g.geneName.value) AS gene_names,
                       data.proteinDescription.recommendedName.fullName.value AS protein_name
                FROM {e} WHERE taxid = 9606
            """,
        },
        {
            "name": "organism_features",
            "description": "Human domain features (unnest at query time)",
            "sql": f"""
                SELECT e.acc, f.type,
                       CAST(f.location.start.value AS INTEGER) AS start_pos,
                       CAST(f.location.end.value AS INTEGER) AS end_pos,
                       CAST(f.description AS VARCHAR) AS description
                FROM {e} e,
                LATERAL UNNEST(CAST(e.data.features AS STRUCT(
                    type VARCHAR, description VARCHAR,
                    location STRUCT(start STRUCT(value INTEGER), "end" STRUCT(value INTEGER))
                )[])) AS t(f)
                WHERE e.taxid = 9606 AND f.type = 'Domain'
            """,
        },

        # ── Keyword / GO searches ──
        {
            "name": "keyword_search",
            "description": "Entries with keyword 'Kinase'",
            "sql": f"""
                SELECT acc,
                       data.proteinDescription.recommendedName.fullName.value AS protein_name
                FROM {e}
                WHERE list_contains(
                    list_transform(CAST(data.keywords AS STRUCT(name VARCHAR)[]),
                                   x -> x.name),
                    'Kinase'
                )
            """,
        },
        {
            "name": "go_term_search",
            "description": "Entries with GO:0005524 (ATP binding)",
            "sql": f"""
                SELECT acc,
                       data.proteinDescription.recommendedName.fullName.value AS protein_name
                FROM {e}
                WHERE list_contains(
                    [x.id FOR x IN CAST(data.uniProtKBCrossReferences
                        AS STRUCT(database VARCHAR, id VARCHAR)[])
                     IF x.database = 'GO'],
                    'GO:0005524'
                )
            """,
        },

        # ── Aggregations ──
        {
            "name": "count_by_organism",
            "description": "Entry count per organism (top 10)",
            "sql": f"""
                SELECT taxid,
                       data.organism.scientificName AS organism_name,
                       count(*) AS n
                FROM {e} GROUP BY taxid, organism_name ORDER BY n DESC LIMIT 10
            """,
        },
        {
            "name": "feature_type_distribution",
            "description": "Feature count by type (unnest at query time)",
            "sql": f"""
                SELECT f.type, count(*) AS n
                FROM {e} e,
                LATERAL UNNEST(CAST(e.data.features AS STRUCT(type VARCHAR)[])) AS t(f)
                GROUP BY f.type ORDER BY n DESC
            """,
        },
        {
            "name": "xref_database_distribution",
            "description": "Xref count by database (top 10, unnest at query time)",
            "sql": f"""
                SELECT x.database, count(*) AS n
                FROM {e} e,
                LATERAL UNNEST(CAST(e.data.uniProtKBCrossReferences
                    AS STRUCT(database VARCHAR)[])) AS t(x)
                GROUP BY x.database ORDER BY n DESC LIMIT 10
            """,
        },

        # ── Joins (self-join via unnest for Layout A) ──
        {
            "name": "entries_join_features",
            "description": "Entries + features for human (unnest at query time)",
            "sql": f"""
                SELECT e.acc,
                       e.data.proteinDescription.recommendedName.fullName.value AS protein_name,
                       f.type,
                       CAST(f.location.start.value AS INTEGER) AS start_pos,
                       CAST(f.location.end.value AS INTEGER) AS end_pos,
                       CAST(f.description AS VARCHAR) AS description
                FROM {e} e,
                LATERAL UNNEST(CAST(e.data.features AS STRUCT(
                    type VARCHAR, description VARCHAR,
                    location STRUCT(start STRUCT(value INTEGER), "end" STRUCT(value INTEGER))
                )[])) AS t(f)
                WHERE e.taxid = 9606
            """,
        },

        # ── Wide scan ──
        {
            "name": "full_scan_entries",
            "description": "Full table scan — count all entries",
            "sql": f"SELECT count(*) FROM {e}",
        },
        {
            "name": "full_scan_xrefs",
            "description": "Full scan — count all xrefs (unnest at query time)",
            "sql": f"""
                SELECT count(*)
                FROM {e} e,
                LATERAL UNNEST(CAST(e.data.uniProtKBCrossReferences
                    AS STRUCT(database VARCHAR)[])) AS t(x)
            """,
        },

        # ── Nested column access (VARIANT dot notation) ──
        {
            "name": "nested_organism_access",
            "description": "Access organism via VARIANT dot notation",
            "sql": f"""
                SELECT acc, data.organism.taxonId, data.organism.scientificName
                FROM {e} WHERE taxid = 9606
            """,
        },
        {
            "name": "nested_protein_desc_access",
            "description": "Access proteinDescription via VARIANT",
            "sql": f"""
                SELECT acc, data.proteinDescription.recommendedName.fullName.value
                FROM {e} WHERE data.proteinDescription.recommendedName IS NOT NULL LIMIT 50
            """,
        },

        # ── Comment / publication queries (unnest at query time) ──
        {
            "name": "comment_function_search",
            "description": "FUNCTION comments for human (unnest at query time)",
            "sql": f"""
                SELECT e.acc,
                       list_transform(
                           CAST(c.texts AS STRUCT(value VARCHAR)[]),
                           t -> t.value
                       ) AS text_values
                FROM {e} e,
                LATERAL UNNEST(CAST(e.data.comments AS STRUCT(
                    commentType VARCHAR, texts STRUCT(value VARCHAR)[]
                )[])) AS t(c)
                WHERE e.taxid = 9606 AND c.commentType = 'FUNCTION'
            """,
        },
        {
            "name": "publication_count_per_entry",
            "description": "Top 10 most-cited entries (unnest at query time)",
            "sql": f"""
                SELECT e.acc, len(CAST(e.data."references" AS STRUCT(x VARCHAR)[])) AS n
                FROM {e} e
                ORDER BY n DESC LIMIT 10
            """,
        },
    ]


def _layout_b_queries(base):
    """Layout B: star schema with VARIANT — child tables already unnested."""
    e = f"read_parquet('{base}/entries/*.parquet')"
    f = f"read_parquet('{base}/features/*.parquet')"
    x = f"read_parquet('{base}/xrefs/*.parquet')"
    c = f"read_parquet('{base}/comments/*.parquet')"
    p = f"read_parquet('{base}/publications/*.parquet')"

    return [
        # ── Point lookups ──
        {
            "name": "point_lookup_by_acc",
            "description": "Single protein lookup by accession",
            "sql": f"SELECT * FROM {e} WHERE acc = '{{acc}}'",
            "setup": f"SELECT acc FROM {e} WHERE reviewed LIMIT 1",
        },
        {
            "name": "protein_card_macro",
            "description": "Protein card equivalent — key fields via VARIANT",
            "sql": f"""
                SELECT acc, taxid, reviewed,
                       data.organism.scientificName AS organism_name,
                       data.proteinDescription.recommendedName.fullName.value AS protein_name,
                       CAST(data.sequence.value AS VARCHAR) AS sequence
                FROM {e} WHERE acc = '{{acc}}'
            """,
            "setup": f"SELECT acc FROM {e} WHERE reviewed LIMIT 1",
        },

        # ── Organism filters ──
        {
            "name": "organism_filter_human",
            "description": "All human entries (taxid=9606)",
            "sql": f"""
                SELECT acc,
                       list_transform(CAST(data.genes AS STRUCT(geneName STRUCT(value VARCHAR))[]),
                                      g -> g.geneName.value) AS gene_names,
                       data.proteinDescription.recommendedName.fullName.value AS protein_name
                FROM {e} WHERE taxid = 9606
            """,
        },
        {
            "name": "organism_features",
            "description": "Human domain features (pre-unnested table)",
            "sql": f"""
                SELECT acc,
                       CAST(data.location.start.value AS INTEGER) AS start_pos,
                       CAST(data.location.end.value AS INTEGER) AS end_pos,
                       CAST(data.description AS VARCHAR) AS description
                FROM {f} WHERE taxid = 9606 AND type = 'Domain'
            """,
        },

        # ── Keyword / GO searches ──
        {
            "name": "keyword_search",
            "description": "Entries with keyword 'Kinase'",
            "sql": f"""
                SELECT acc,
                       data.proteinDescription.recommendedName.fullName.value AS protein_name
                FROM {e}
                WHERE list_contains(
                    list_transform(CAST(data.keywords AS STRUCT(name VARCHAR)[]),
                                   x -> x.name),
                    'Kinase'
                )
            """,
        },
        {
            "name": "go_term_search",
            "description": "Entries with GO:0005524 (ATP binding)",
            "sql": f"""
                SELECT acc,
                       data.proteinDescription.recommendedName.fullName.value AS protein_name
                FROM {e}
                WHERE list_contains(
                    [x.id FOR x IN CAST(data.uniProtKBCrossReferences
                        AS STRUCT(database VARCHAR, id VARCHAR)[])
                     IF x.database = 'GO'],
                    'GO:0005524'
                )
            """,
        },

        # ── Aggregations ──
        {
            "name": "count_by_organism",
            "description": "Entry count per organism (top 10)",
            "sql": f"""
                SELECT taxid,
                       data.organism.scientificName AS organism_name,
                       count(*) AS n
                FROM {e} GROUP BY taxid, organism_name ORDER BY n DESC LIMIT 10
            """,
        },
        {
            "name": "feature_type_distribution",
            "description": "Feature count by type (pre-unnested table)",
            "sql": f"SELECT type, count(*) AS n FROM {f} GROUP BY type ORDER BY n DESC",
        },
        {
            "name": "xref_database_distribution",
            "description": "Xref count by database (top 10, pre-unnested table)",
            "sql": f"SELECT database, count(*) AS n FROM {x} GROUP BY database ORDER BY n DESC LIMIT 10",
        },

        # ── Joins ──
        {
            "name": "entries_join_features",
            "description": "Entries + features for human",
            "sql": f"""
                SELECT e.acc,
                       e.data.proteinDescription.recommendedName.fullName.value AS protein_name,
                       ft.type,
                       CAST(ft.data.location.start.value AS INTEGER) AS start_pos,
                       CAST(ft.data.location.end.value AS INTEGER) AS end_pos,
                       CAST(ft.data.description AS VARCHAR) AS description
                FROM {e} e
                JOIN {f} ft ON e.acc = ft.acc
                WHERE e.taxid = 9606
            """,
        },

        # ── Wide scan ──
        {
            "name": "full_scan_entries",
            "description": "Full table scan — count all entries",
            "sql": f"SELECT count(*) FROM {e}",
        },
        {
            "name": "full_scan_xrefs",
            "description": "Full table scan — count all xrefs",
            "sql": f"SELECT count(*) FROM {x}",
        },

        # ── Nested column access ──
        {
            "name": "nested_organism_access",
            "description": "Access organism via VARIANT dot notation",
            "sql": f"""
                SELECT acc, data.organism.taxonId, data.organism.scientificName
                FROM {e} WHERE taxid = 9606
            """,
        },
        {
            "name": "nested_protein_desc_access",
            "description": "Access proteinDescription via VARIANT",
            "sql": f"""
                SELECT acc, data.proteinDescription.recommendedName.fullName.value
                FROM {e} WHERE data.proteinDescription.recommendedName IS NOT NULL LIMIT 50
            """,
        },

        # ── Comment / publication queries ──
        {
            "name": "comment_function_search",
            "description": "FUNCTION comments for human (pre-unnested table)",
            "sql": f"""
                SELECT acc,
                       list_transform(
                           CAST(data.texts AS STRUCT(value VARCHAR)[]),
                           t -> t.value
                       ) AS text_values
                FROM {c}
                WHERE comment_type = 'FUNCTION' AND taxid = 9606
            """,
        },
        {
            "name": "publication_count_per_entry",
            "description": "Top 10 most-cited entries (pre-unnested table)",
            "sql": f"SELECT acc, count(*) AS n FROM {p} GROUP BY acc ORDER BY n DESC LIMIT 10",
        },
    ]


# ---------------------------------------------------------------------------
# Run benchmarks for a layout
# ---------------------------------------------------------------------------

def run_query_suite(con, queries):
    """Run all queries and return timing results."""
    results = {}
    for q in queries:
        sql = q["sql"]

        # Handle parameterised queries
        if "{acc}" in sql:
            if "setup" in q:
                setup_result = con.sql(q["setup"]).fetchone()
                if setup_result:
                    sql = sql.replace("{acc}", setup_result[0])
                else:
                    results[q["name"]] = {"skipped": "no data"}
                    continue

        try:
            timing = _time_query(con, sql)
            timing["description"] = q["description"]
            results[q["name"]] = timing
        except Exception as e:
            results[q["name"]] = {"error": str(e), "description": q["description"]}

    return results


# ---------------------------------------------------------------------------
# Storage Analysis
# ---------------------------------------------------------------------------

def analyze_storage(base_path):
    """Analyze total and per-table Parquet sizes."""
    results = {"tables": {}, "totals": {}}
    total_bytes = 0
    total_rows = 0
    total_columns = 0

    for entry in sorted(os.listdir(base_path)):
        table_dir = os.path.join(base_path, entry)
        if not os.path.isdir(table_dir):
            continue

        table_bytes = 0
        table_rows = 0
        table_cols = 0
        for fname in sorted(os.listdir(table_dir)):
            if not fname.endswith(".parquet"):
                continue
            fpath = os.path.join(table_dir, fname)
            table_bytes += os.path.getsize(fpath)
            meta = pq.read_metadata(fpath)
            table_rows += meta.num_rows
            table_cols = meta.num_columns

        results["tables"][entry] = {
            "total_bytes": table_bytes,
            "total_human": _human_size(table_bytes),
            "total_rows": table_rows,
            "bytes_per_row": round(table_bytes / table_rows, 2) if table_rows > 0 else 0,
            "parquet_columns": table_cols,
        }
        total_bytes += table_bytes
        total_rows += table_rows
        total_columns += table_cols

    results["totals"] = {
        "total_bytes": total_bytes,
        "total_human": _human_size(total_bytes),
        "total_rows": total_rows,
        "total_parquet_columns": total_columns,
    }
    return results


# ---------------------------------------------------------------------------
# Comparison Report
# ---------------------------------------------------------------------------

def format_comparison(all_results):
    """Generate a side-by-side comparison report."""
    lines = []
    lines.append("=" * 90)
    lines.append("  VARIANT vs BASELINE BENCHMARK COMPARISON")
    lines.append(f"  Generated: {all_results['metadata']['timestamp']}")
    lines.append(f"  DuckDB: {all_results['metadata']['duckdb_version']}")
    lines.append("=" * 90)
    lines.append("")
    lines.append("  Layouts tested:")
    lines.append("    Baseline — current star schema with 88 typed convenience columns")
    lines.append("    A — single entries table, full VARIANT, no child tables")
    lines.append("    B — star schema + VARIANT (arrays included in entries VARIANT)")
    lines.append("    C — star schema + VARIANT (arrays STRIPPED from entries VARIANT)")

    baseline = all_results.get("baseline", {})
    layout_a = all_results.get("layout_a", {})
    layout_b = all_results.get("layout_b", {})
    layout_c = all_results.get("layout_c", {})

    bq = baseline.get("query_latency", {})
    aq = layout_a.get("query_latency", {})
    bq2 = layout_b.get("query_latency", {})
    cq = layout_c.get("query_latency", {})

    # ── Query Latency ──
    lines.append("\n┌────────────────────────────────────────────────────────────────────────────────┐")
    lines.append("│  1. QUERY LATENCY (median ms)                                                 │")
    lines.append("└────────────────────────────────────────────────────────────────────────────────┘\n")

    lines.append(f"  {'Query':<28} {'Base':>8} {'A':>8} {'B':>8} {'C':>8} {'C/Base':>8}")
    lines.append(f"  {'─'*28} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*8}")

    for name in bq:
        vals = {}
        for key, src in [("base", bq), ("A", aq), ("B", bq2), ("C", cq)]:
            vals[key] = src.get(name, {}).get("median_ms")

        parts = []
        for key in ["base", "A", "B", "C"]:
            v = vals[key]
            parts.append(f"{v:>7.1f}ms" if v is not None else f"{'ERR':>8}s")

        if vals["base"] and vals["C"]:
            ratio = vals["C"] / vals["base"]
            r_str = f"{ratio:.1f}x"
            if ratio > 5.0:
                r_str += " !!"
            elif ratio <= 1.5:
                r_str += " ✓"
        else:
            r_str = "N/A"

        lines.append(f"  {name:<28} {parts[0]} {parts[1]} {parts[2]} {parts[3]} {r_str:>8}")

    # Row count verification
    lines.append(f"\n  Row count check:")
    mismatches = []
    for name in bq:
        b_rows = bq.get(name, {}).get("row_count")
        c_rows = cq.get(name, {}).get("row_count")
        if b_rows is not None and c_rows is not None and b_rows != c_rows:
            mismatches.append(f"    {name}: base={b_rows} C={c_rows}")
    if mismatches:
        for m in mismatches:
            lines.append(m)
    else:
        lines.append(f"    All queries return identical row counts between Baseline and Layout C ✓")

    # ── Storage ──
    lines.append("\n┌────────────────────────────────────────────────────────────────────────────────┐")
    lines.append("│  2. STORAGE                                                                    │")
    lines.append("└────────────────────────────────────────────────────────────────────────────────┘\n")

    storage_sources = {
        "base": baseline.get("storage", {}).get("totals", {}),
        "A": layout_a.get("storage", {}).get("totals", {}),
        "B": layout_b.get("storage", {}).get("totals", {}),
        "C": layout_c.get("storage", {}).get("totals", {}),
    }

    lines.append(f"  {'':>20} {'Base':>12} {'A':>12} {'B':>12} {'C':>12}")
    lines.append(f"  {'─'*20} {'─'*12} {'─'*12} {'─'*12} {'─'*12}")
    for metric, key in [("Total size", "total_human"), ("Total rows", "total_rows"), ("Parquet columns", "total_parquet_columns")]:
        parts = []
        for src_key in ["base", "A", "B", "C"]:
            v = storage_sources[src_key].get(key)
            if key == "total_rows" and v is not None:
                parts.append(f"{v:>12,}")
            elif v is not None:
                parts.append(f"{v:>12}")
            else:
                parts.append(f"{'—':>12}")
        lines.append(f"  {metric:<20} {parts[0]} {parts[1]} {parts[2]} {parts[3]}")

    # Per-table
    lines.append(f"\n  Per-table:")
    all_tables = set()
    for layout_key in ["baseline", "layout_a", "layout_b", "layout_c"]:
        all_tables.update(all_results.get(layout_key, {}).get("storage", {}).get("tables", {}).keys())

    for table in sorted(all_tables):
        parts = []
        for layout_key in ["baseline", "layout_a", "layout_b", "layout_c"]:
            info = all_results.get(layout_key, {}).get("storage", {}).get("tables", {}).get(table, {})
            parts.append(info.get("total_human", "—"))
        lines.append(f"    {table:<16} {parts[0]:>12} {parts[1]:>12} {parts[2]:>12} {parts[3]:>12}")

    # ── Build Time ──
    lines.append("\n┌────────────────────────────────────────────────────────────────────────────────┐")
    lines.append("│  3. BUILD TIME                                                                 │")
    lines.append("└────────────────────────────────────────────────────────────────────────────────┘\n")

    bp = baseline.get("pipeline", {}).get("summary", {})
    bm = all_results.get("build_meta", {})
    lines.append(f"  Baseline pipeline:  {bp.get('median_s', '?')}s")
    lines.append(f"  Layout A:           {bm.get('layout_a', {}).get('build_time_s', '?')}s")
    lines.append(f"  Layout B:           {bm.get('layout_b', {}).get('build_time_s', '?')}s")
    lines.append(f"  Layout C:           {bm.get('layout_c', {}).get('build_time_s', '?')}s")

    # ── Schema Complexity ──
    lines.append("\n┌────────────────────────────────────────────────────────────────────────────────┐")
    lines.append("│  4. COMPLEXITY                                                                 │")
    lines.append("└────────────────────────────────────────────────────────────────────────────────┘\n")

    sc = baseline.get("schema_complexity", {})
    code = sc.get("code_complexity", {})
    lines.append(f"  Baseline:  {code.get('total_pipeline_loc', '?')} LOC, {code.get('sql_builder_loc', '?')} SQL builder LOC ({code.get('sql_builder_pct', '?')}%)")
    lines.append(f"             {sc.get('summary', {}).get('total_convenience_columns', '?')} convenience cols, {sc.get('summary', {}).get('total_nested_columns', '?')} nested cols")
    lines.append(f"  VARIANT:   ~{bm.get('estimated_variant_loc', '?')} LOC, 0 SQL builder LOC")
    lines.append(f"             0 convenience cols (VARIANT shredding replaces them)")

    # ── Verdict ──
    lines.append("\n┌────────────────────────────────────────────────────────────────────────────────┐")
    lines.append("│  5. VERDICT                                                                    │")
    lines.append("└────────────────────────────────────────────────────────────────────────────────┘\n")

    b_bytes = storage_sources["base"].get("total_parquet_bytes", storage_sources["base"].get("total_bytes", 0))
    for key, label in [("A", "Layout A"), ("B", "Layout B"), ("C", "Layout C")]:
        times_b = [v["median_ms"] for v in bq.values() if "median_ms" in v]
        src = {"A": aq, "B": bq2, "C": cq}[key]
        times_x = [v["median_ms"] for v in src.values() if "median_ms" in v]
        x_bytes = storage_sources[key].get("total_bytes", 0)

        if times_b and times_x:
            ratio = statistics.mean(times_x) / statistics.mean(times_b)
            lines.append(f"  {label}  query: {ratio:>7.1f}x avg    storage: {x_bytes/b_bytes:.2f}x" if b_bytes else f"  {label}  query: {ratio:>7.1f}x avg")

    lines.append("\n" + "=" * 90)
    lines.append("  END OF COMPARISON")
    lines.append("=" * 90)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Benchmark VARIANT layouts")
    parser.add_argument("--variant-lake", default="benchmarks/variant_lake",
                        help="Path to VARIANT lake directory")
    parser.add_argument("--baseline-lake", default="demo/lake/2026_01/lake",
                        help="Path to baseline star-schema lake")
    parser.add_argument("--output-dir", default="benchmarks/results",
                        help="Directory for output files")
    args = parser.parse_args()

    repo_root = str(Path(__file__).resolve().parents[1])
    os.chdir(repo_root)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    os.makedirs(args.output_dir, exist_ok=True)

    con = duckdb.connect()

    all_results = {
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duckdb_version": duckdb.__version__,
            "benchmark_type": "variant_comparison",
        }
    }

    print("=" * 60, file=sys.stderr)
    print("  VARIANT BENCHMARK COMPARISON", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    # ── Run baseline queries (re-run for fair comparison) ──
    print("\n[1/5] Re-running baseline queries...", file=sys.stderr)
    sys.path.insert(0, repo_root)
    from uniprot_parquet import connect
    baseline_con = connect(str(Path(args.baseline_lake).resolve()))

    from bench_baseline import QUERY_SUITE as baseline_queries, analyze_storage as baseline_storage_fn
    baseline_results = {}
    for q in baseline_queries:
        sql = q["sql"]
        if "{acc}" in sql and "setup" in q:
            r = baseline_con.sql(q["setup"]).fetchone()
            if r:
                sql = sql.replace("{acc}", r[0])
            else:
                baseline_results[q["name"]] = {"skipped": "no data"}
                continue
        try:
            baseline_results[q["name"]] = _time_query(baseline_con, sql)
            baseline_results[q["name"]]["description"] = q["description"]
        except Exception as e:
            baseline_results[q["name"]] = {"error": str(e)}

    all_results["baseline"] = {
        "query_latency": baseline_results,
        "storage": baseline_storage_fn(args.baseline_lake),
    }
    # Load pipeline results from latest baseline
    baseline_latest = os.path.join(args.output_dir, "baseline_latest.json")
    if os.path.exists(baseline_latest):
        with open(baseline_latest) as f:
            bl = json.load(f)
        all_results["baseline"]["pipeline"] = bl.get("pipeline", {})
        all_results["baseline"]["schema_complexity"] = bl.get("schema_complexity", {})

    print(f"  {len(baseline_results)} baseline queries", file=sys.stderr)

    # ── Layout A ──
    print("\n[2/5] Running Layout A queries...", file=sys.stderr)
    layout_a_path = os.path.join(args.variant_lake, "layout_a")
    a_queries = _layout_a_queries(layout_a_path)
    a_results = run_query_suite(con, a_queries)
    all_results["layout_a"] = {
        "query_latency": a_results,
        "storage": analyze_storage(layout_a_path),
    }
    errors_a = [k for k, v in a_results.items() if "error" in v]
    print(f"  {len(a_results)} queries ({len(errors_a)} errors)", file=sys.stderr)
    for e in errors_a:
        print(f"    ERROR {e}: {a_results[e]['error'][:100]}", file=sys.stderr)

    # ── Layout B ──
    print("\n[3/5] Running Layout B queries...", file=sys.stderr)
    layout_b_path = os.path.join(args.variant_lake, "layout_b")
    b_queries = _layout_b_queries(layout_b_path)
    b_results = run_query_suite(con, b_queries)
    all_results["layout_b"] = {
        "query_latency": b_results,
        "storage": analyze_storage(layout_b_path),
    }
    errors_b = [k for k, v in b_results.items() if "error" in v]
    print(f"  {len(b_results)} queries ({len(errors_b)} errors)", file=sys.stderr)
    for e in errors_b:
        print(f"    ERROR {e}: {b_results[e]['error'][:100]}", file=sys.stderr)

    # ── Layout C ──
    print("\n[4/7] Running Layout C queries...", file=sys.stderr)
    layout_c_path = os.path.join(args.variant_lake, "layout_c")
    c_queries = _layout_b_queries(layout_c_path)  # same query shape as B
    c_results = run_query_suite(con, c_queries)
    all_results["layout_c"] = {
        "query_latency": c_results,
        "storage": analyze_storage(layout_c_path),
    }
    errors_c = [k for k, v in c_results.items() if "error" in v]
    print(f"  {len(c_results)} queries ({len(errors_c)} errors)", file=sys.stderr)
    for e in errors_c:
        print(f"    ERROR {e}: {c_results[e]['error'][:100]}", file=sys.stderr)

    # ── Build metadata ──
    print("\n[5/7] Loading build metadata...", file=sys.stderr)
    build_meta_path = os.path.join(args.variant_lake, "build_meta.json")
    if os.path.exists(build_meta_path):
        with open(build_meta_path) as f:
            build_meta = json.load(f)
    else:
        build_meta = {}

    # Estimate VARIANT pipeline LOC (build_variant_lake.py)
    builder_path = os.path.join(repo_root, "benchmarks", "build_variant_lake.py")
    if os.path.exists(builder_path):
        with open(builder_path) as f:
            build_meta["estimated_variant_loc"] = len(f.readlines())

    all_results["build_meta"] = build_meta

    # ── Generate comparison ──
    print("\n[6/7] Generating comparison report...", file=sys.stderr)
    report = format_comparison(all_results)

    json_path = os.path.join(args.output_dir, f"variant_{timestamp}.json")
    txt_path = os.path.join(args.output_dir, f"variant_{timestamp}.txt")
    latest_json = os.path.join(args.output_dir, "variant_latest.json")
    latest_txt = os.path.join(args.output_dir, "variant_latest.txt")

    with open(json_path, "w") as f:
        json.dump(all_results, f, indent=2)
    with open(txt_path, "w") as f:
        f.write(report)
    with open(latest_json, "w") as f:
        json.dump(all_results, f, indent=2)
    with open(latest_txt, "w") as f:
        f.write(report)

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  Results: {json_path}", file=sys.stderr)
    print(f"  Report:  {txt_path}", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    print(report)
    con.close()


if __name__ == "__main__":
    main()
