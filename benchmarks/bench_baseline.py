#!/usr/bin/env python3
"""
Benchmark baseline: measure current star-schema Parquet lake performance.

Captures four dimensions:
  1. Query latency   — common analytical queries, cold + warm timings
  2. Storage         — file sizes, compression ratios, column-level breakdown
  3. Pipeline        — end-to-end transform time from JSONL → Parquet
  4. Schema metrics  — column counts, convenience vs nested, LOC complexity

Usage:
    python benchmarks/bench_baseline.py [--lake demo/lake/2026_01/lake] [--input demo/input.json.gz]

Output:
    benchmarks/results/baseline_<timestamp>.json   (machine-readable)
    benchmarks/results/baseline_<timestamp>.txt    (human-readable report)
"""

import argparse
import json
import os
import sys
import time
import statistics
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _human_size(nbytes):
    """Return a human-readable size string."""
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024 or unit == "TB":
            if unit == "bytes":
                return f"{int(nbytes)} bytes"
            return f"{nbytes:.2f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.2f} TB"


def _time_query(con, sql, warmup=1, runs=5):
    """Run a query multiple times and return timing stats in milliseconds."""
    # Warmup
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
# 1. Query Latency
# ---------------------------------------------------------------------------

QUERY_SUITE = [
    # ── Point lookups ──
    {
        "name": "point_lookup_by_acc",
        "description": "Single protein lookup by accession",
        "table": "entries",
        "sql": "SELECT * FROM entries WHERE acc = '{acc}'",
        "setup": "SELECT acc FROM entries WHERE reviewed LIMIT 1",
    },
    {
        "name": "protein_card_macro",
        "description": "protein_card() macro — single accession",
        "table": "entries",
        "sql": "SELECT * FROM protein_card('{acc}')",
        "setup": "SELECT acc FROM entries WHERE reviewed LIMIT 1",
    },

    # ── Organism filters ──
    {
        "name": "organism_filter_human",
        "description": "All human entries (taxid=9606)",
        "table": "entries",
        "sql": "SELECT acc, gene_names, protein_name FROM entries WHERE taxid = 9606",
    },
    {
        "name": "organism_features",
        "description": "organism_features() macro — human domains",
        "table": "features",
        "sql": "SELECT * FROM organism_features(9606, 'Domain')",
    },

    # ── Keyword / GO searches ──
    {
        "name": "keyword_search",
        "description": "Entries with keyword 'Kinase'",
        "table": "entries",
        "sql": "SELECT acc, protein_name FROM entries WHERE list_contains(keyword_names, 'Kinase')",
    },
    {
        "name": "go_term_search",
        "description": "Entries with a specific GO term (GO:0005524 = ATP binding)",
        "table": "entries",
        "sql": "SELECT acc, protein_name FROM entries WHERE list_contains(go_ids, 'GO:0005524')",
    },

    # ── Aggregations ──
    {
        "name": "count_by_organism",
        "description": "Entry count per organism (top 10)",
        "table": "entries",
        "sql": "SELECT taxid, organism_name, count(*) AS n FROM entries GROUP BY taxid, organism_name ORDER BY n DESC LIMIT 10",
    },
    {
        "name": "feature_type_distribution",
        "description": "Feature count by type",
        "table": "features",
        "sql": "SELECT type, count(*) AS n FROM features GROUP BY type ORDER BY n DESC",
    },
    {
        "name": "xref_database_distribution",
        "description": "Xref count by database (top 10)",
        "table": "xrefs",
        "sql": "SELECT database, count(*) AS n FROM xrefs GROUP BY database ORDER BY n DESC LIMIT 10",
    },

    # ── Joins ──
    {
        "name": "entries_join_features",
        "description": "Join entries with features on acc (human only)",
        "table": "entries+features",
        "sql": """
            SELECT e.acc, e.protein_name, f.type, f.start_pos, f.end_pos, f.description
            FROM entries e
            JOIN features f ON e.acc = f.acc
            WHERE e.taxid = 9606
        """,
    },

    # ── Wide scan ──
    {
        "name": "full_scan_entries",
        "description": "Full table scan — count all entries",
        "table": "entries",
        "sql": "SELECT count(*) FROM entries",
    },
    {
        "name": "full_scan_xrefs",
        "description": "Full table scan — count all xrefs",
        "table": "xrefs",
        "sql": "SELECT count(*) FROM xrefs",
    },

    # ── Nested column access (baseline for VARIANT comparison) ──
    {
        "name": "nested_organism_access",
        "description": "Access nested organism struct directly",
        "table": "entries",
        "sql": "SELECT acc, organism.taxonId, organism.scientificName FROM entries WHERE organism.taxonId = 9606",
    },
    {
        "name": "nested_protein_desc_access",
        "description": "Access nested proteinDescription struct",
        "table": "entries",
        "sql": "SELECT acc, protein_desc.recommendedName.fullName.value FROM entries WHERE protein_desc IS NOT NULL LIMIT 50",
    },

    # ── Comment / publication queries ──
    {
        "name": "comment_function_search",
        "description": "FUNCTION comments for a specific organism",
        "table": "comments",
        "sql": "SELECT acc, text_value FROM comments WHERE comment_type = 'FUNCTION' AND taxid = 9606",
    },
    {
        "name": "publication_count_per_entry",
        "description": "Top 10 most-cited entries",
        "table": "publications",
        "sql": "SELECT acc, count(*) AS n FROM publications GROUP BY acc ORDER BY n DESC LIMIT 10",
    },
]


def run_query_benchmarks(con, lake_path):
    """Run all query benchmarks and return results."""
    results = {}

    # Setup views and macros via the client library
    sys.path.insert(0, str(Path(lake_path).resolve().parents[2]))
    # Use connect from the repo root
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    from uniprot_parquet import connect

    con = connect(str(Path(lake_path).resolve()))

    for query in QUERY_SUITE:
        sql = query["sql"]

        # Handle parameterised queries
        if "{acc}" in sql:
            if "setup" in query:
                setup_result = con.sql(query["setup"]).fetchone()
                if setup_result:
                    acc = setup_result[0]
                    sql = sql.replace("{acc}", acc)
                else:
                    results[query["name"]] = {"skipped": "no data for setup query"}
                    continue
            else:
                results[query["name"]] = {"skipped": "no acc parameter available"}
                continue

        try:
            timing = _time_query(con, sql)
            timing["description"] = query["description"]
            timing["table"] = query["table"]
            timing["sql"] = sql.strip()
            results[query["name"]] = timing
        except Exception as e:
            results[query["name"]] = {
                "error": str(e),
                "description": query["description"],
            }

    return results, con


# ---------------------------------------------------------------------------
# 2. Storage Analysis
# ---------------------------------------------------------------------------

def analyze_storage(lake_path):
    """Analyze Parquet file sizes and column-level storage breakdown."""
    results = {"tables": {}, "totals": {}}
    total_bytes = 0
    total_rows = 0

    tables = ["entries", "features", "xrefs", "comments", "publications"]

    for table in tables:
        table_dir = os.path.join(lake_path, table)
        if not os.path.isdir(table_dir):
            continue

        table_info = {"files": [], "columns": []}
        table_bytes = 0
        table_rows = 0

        for fname in sorted(os.listdir(table_dir)):
            if not fname.endswith(".parquet"):
                continue
            fpath = os.path.join(table_dir, fname)
            fsize = os.path.getsize(fpath)
            table_bytes += fsize

            # Read Parquet metadata
            meta = pq.read_metadata(fpath)
            table_rows += meta.num_rows
            table_info["files"].append({
                "name": fname,
                "size_bytes": fsize,
                "size_human": _human_size(fsize),
                "num_rows": meta.num_rows,
                "num_row_groups": meta.num_row_groups,
                "num_columns": meta.num_columns,
            })

            # Column-level breakdown (from first file only)
            if len(table_info["columns"]) == 0:
                schema = pq.read_schema(fpath)
                for rg_idx in range(meta.num_row_groups):
                    rg = meta.row_group(rg_idx)
                    for col_idx in range(rg.num_columns):
                        col = rg.column(col_idx)
                        col_name = col.path_in_schema
                        # Find physical type from schema
                        table_info["columns"].append({
                            "name": col_name,
                            "total_compressed_bytes": col.total_compressed_size,
                            "total_uncompressed_bytes": col.total_uncompressed_size,
                            "compression_ratio": round(
                                col.total_uncompressed_size / col.total_compressed_size, 2
                            ) if col.total_compressed_size > 0 else 0,
                            "compressed_human": _human_size(col.total_compressed_size),
                        })

        table_info["total_bytes"] = table_bytes
        table_info["total_human"] = _human_size(table_bytes)
        table_info["total_rows"] = table_rows
        table_info["bytes_per_row"] = round(table_bytes / table_rows, 2) if table_rows > 0 else 0

        total_bytes += table_bytes
        total_rows += table_rows
        results["tables"][table] = table_info

    # Input file size for compression ratio
    results["totals"] = {
        "total_parquet_bytes": total_bytes,
        "total_parquet_human": _human_size(total_bytes),
        "total_rows": total_rows,
        "avg_bytes_per_row": round(total_bytes / total_rows, 2) if total_rows > 0 else 0,
    }

    return results


# ---------------------------------------------------------------------------
# 3. Pipeline Throughput
# ---------------------------------------------------------------------------

def benchmark_pipeline(input_path, repo_root):
    """Re-run the pipeline on the test fixture and measure throughput."""
    transform_script = os.path.join(repo_root, "bin", "parquet_transform.py")
    if not os.path.exists(transform_script):
        return {"error": f"Transform script not found: {transform_script}"}

    results = {"runs": []}
    n_runs = 3

    for run_idx in range(n_runs):
        with tempfile.TemporaryDirectory(prefix="bench_pipeline_") as tmpdir:
            outdir = os.path.join(tmpdir, "lake")
            cmd = [
                sys.executable, transform_script,
                input_path,
                "--outdir", outdir,
                "--release", "bench",
                "--memory-limit", "4GB",
                "--batch-size", "50000",
            ]

            t0 = time.perf_counter()
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300,
            )
            t1 = time.perf_counter()
            elapsed_s = t1 - t0

            run_info = {
                "run": run_idx + 1,
                "elapsed_s": round(elapsed_s, 3),
                "returncode": proc.returncode,
            }

            if proc.returncode == 0:
                # Parse timing from stderr
                lines = proc.stderr.strip().split("\n")
                # Count output rows from stderr
                for line in lines:
                    if "Total:" in line and "rows" in line:
                        run_info["summary_line"] = line.strip()

                # Measure output size
                total_output = 0
                for root, dirs, files in os.walk(outdir):
                    for f in files:
                        total_output += os.path.getsize(os.path.join(root, f))
                run_info["output_bytes"] = total_output
                run_info["output_human"] = _human_size(total_output)
                run_info["throughput_mb_s"] = round(
                    (total_output / 1024 / 1024) / elapsed_s, 2
                ) if elapsed_s > 0 else 0
            else:
                run_info["stderr_tail"] = proc.stderr[-500:] if proc.stderr else ""

            results["runs"].append(run_info)

    # Aggregate
    successful = [r for r in results["runs"] if r["returncode"] == 0]
    if successful:
        times = [r["elapsed_s"] for r in successful]
        results["summary"] = {
            "min_s": round(min(times), 3),
            "max_s": round(max(times), 3),
            "median_s": round(statistics.median(times), 3),
            "mean_s": round(statistics.mean(times), 3),
        }

    return results


# ---------------------------------------------------------------------------
# 4. Schema Complexity Metrics
# ---------------------------------------------------------------------------

def analyze_schema_complexity(lake_path, repo_root):
    """Measure schema complexity: column counts, convenience vs nested, LOC."""
    results = {}

    # Column classification
    convenience_columns = {
        "entries": [
            "acc", "id", "reviewed", "secondary_accs", "taxid", "organism_name",
            "organism_common", "lineage", "gene_names", "gene_synonyms",
            "protein_name", "alt_protein_names", "protein_flag", "ec_numbers",
            "protein_existence", "annotation_score", "sequence", "seq_length",
            "seq_mass", "seq_md5", "seq_crc64", "go_ids", "xref_dbs",
            "keyword_ids", "keyword_names", "first_public", "last_modified",
            "last_seq_modified", "entry_version", "seq_version", "feature_count",
            "xref_count", "comment_count", "reference_count", "uniparc_id",
            "entry_type", "extra_attributes",
        ],
        "features": [
            "acc", "from_reviewed", "taxid", "organism_name", "seq_length",
            "type", "start_pos", "end_pos", "start_modifier", "end_modifier",
            "description", "feature_id", "evidence_codes", "original_sequence",
            "alternative_sequences", "ligand_name", "ligand_id", "ligand_label",
            "ligand_note",
        ],
        "xrefs": [
            "acc", "from_reviewed", "taxid", "database", "id", "properties",
            "isoform_id", "evidences",
        ],
        "comments": [
            "acc", "from_reviewed", "taxid", "comment_type", "text_value",
        ],
        "publications": [
            "acc", "from_reviewed", "taxid", "reference_number", "citation_type",
            "citation_id", "title", "authors", "authoring_group", "publication_date",
            "journal", "volume", "first_page", "last_page", "submission_database",
            "citation_xrefs", "reference_positions", "reference_comments", "evidences",
        ],
    }

    nested_columns = {
        "entries": ["organism", "protein_desc", "genes", "keywords", "organism_hosts", "gene_locations"],
        "features": ["feature"],
        "xrefs": [],
        "comments": ["comment"],
        "publications": ["reference"],
    }

    table_metrics = {}
    for table in ["entries", "features", "xrefs", "comments", "publications"]:
        conv = convenience_columns.get(table, [])
        nest = nested_columns.get(table, [])
        table_metrics[table] = {
            "total_columns": len(conv) + len(nest),
            "convenience_columns": len(conv),
            "nested_columns": len(nest),
            "convenience_ratio": round(len(conv) / (len(conv) + len(nest)), 2) if (len(conv) + len(nest)) > 0 else 0,
            "convenience_list": conv,
            "nested_list": nest,
        }

    results["tables"] = table_metrics

    # SQL builder complexity (LOC)
    transform_path = os.path.join(repo_root, "bin", "parquet_transform.py")
    if os.path.exists(transform_path):
        with open(transform_path) as f:
            lines = f.readlines()

        total_lines = len(lines)

        # Count lines in SQL builder functions
        builder_funcs = [
            "_build_entries_sql", "_build_features_sql", "_build_xrefs_sql",
            "_build_comments_sql", "_build_publications_sql",
        ]
        builder_lines = 0
        in_builder = False
        current_indent = 0
        for i, line in enumerate(lines):
            stripped = line.rstrip()
            if any(f"def {fn}(" in stripped for fn in builder_funcs):
                in_builder = True
                current_indent = len(line) - len(line.lstrip())
                builder_lines += 1
            elif in_builder:
                if stripped == "":
                    builder_lines += 1
                elif len(line) - len(line.lstrip()) <= current_indent and stripped and not stripped.startswith("#"):
                    # Check if this is a new top-level def or class
                    if stripped.startswith("def ") or stripped.startswith("class "):
                        in_builder = False
                    else:
                        builder_lines += 1
                else:
                    builder_lines += 1

        results["code_complexity"] = {
            "total_pipeline_loc": total_lines,
            "sql_builder_loc": builder_lines,
            "sql_builder_pct": round(builder_lines / total_lines * 100, 1) if total_lines > 0 else 0,
            "builder_functions": len(builder_funcs),
            "description": (
                f"SQL builders account for {builder_lines} of {total_lines} lines "
                f"({round(builder_lines/total_lines*100, 1)}%). "
                f"VARIANT could eliminate most of this complexity."
            ),
        }

    # Summary
    total_conv = sum(m["convenience_columns"] for m in table_metrics.values())
    total_nest = sum(m["nested_columns"] for m in table_metrics.values())
    results["summary"] = {
        "total_convenience_columns": total_conv,
        "total_nested_columns": total_nest,
        "total_columns": total_conv + total_nest,
        "overall_convenience_ratio": round(total_conv / (total_conv + total_nest), 2),
        "description": (
            f"{total_conv} convenience columns (manually extracted) vs "
            f"{total_nest} nested columns across 5 tables. "
            f"VARIANT shredding would auto-extract the {total_conv} convenience columns."
        ),
    }

    return results


# ---------------------------------------------------------------------------
# Report Generation
# ---------------------------------------------------------------------------

def format_report(all_results):
    """Generate a human-readable text report."""
    lines = []
    lines.append("=" * 72)
    lines.append("  UNIPROT-PARQUET BASELINE BENCHMARK")
    lines.append(f"  Generated: {all_results['metadata']['timestamp']}")
    lines.append(f"  DuckDB: {all_results['metadata']['duckdb_version']}")
    lines.append("=" * 72)

    # ── Query Latency ──
    lines.append("\n┌─────────────────────────────────────────────────────┐")
    lines.append("│  1. QUERY LATENCY (median, 5 runs after 1 warmup)  │")
    lines.append("└─────────────────────────────────────────────────────┘\n")
    lines.append(f"  {'Query':<35} {'Median':>8} {'Min':>8} {'Max':>8} {'Rows':>8}")
    lines.append(f"  {'─'*35} {'─'*8} {'─'*8} {'─'*8} {'─'*8}")

    for name, data in all_results.get("query_latency", {}).items():
        if "error" in data or "skipped" in data:
            status = data.get("error", data.get("skipped", "?"))
            lines.append(f"  {name:<35} SKIP: {status}")
            continue
        lines.append(
            f"  {name:<35} {data['median_ms']:>7.2f}ms "
            f"{data['min_ms']:>7.2f}ms "
            f"{data['max_ms']:>7.2f}ms "
            f"{data['row_count']:>7}"
        )

    # ── Storage ──
    lines.append("\n┌─────────────────────────────────────────────────────┐")
    lines.append("│  2. STORAGE ANALYSIS                                │")
    lines.append("└─────────────────────────────────────────────────────┘\n")

    storage = all_results.get("storage", {})
    totals = storage.get("totals", {})
    lines.append(f"  Total Parquet: {totals.get('total_parquet_human', '?')}")
    lines.append(f"  Total rows:    {totals.get('total_rows', 0):,}")
    lines.append(f"  Avg bytes/row: {totals.get('avg_bytes_per_row', 0):.1f}")

    if "input_size_human" in totals:
        lines.append(f"  Input JSON.gz: {totals['input_size_human']}")
    if "compression_vs_input" in totals:
        lines.append(f"  Parquet/Input:  {totals['compression_vs_input']}x")

    lines.append(f"\n  {'Table':<15} {'Size':>10} {'Rows':>10} {'B/row':>8} {'Cols':>6}")
    lines.append(f"  {'─'*15} {'─'*10} {'─'*10} {'─'*8} {'─'*6}")

    for table, info in storage.get("tables", {}).items():
        lines.append(
            f"  {table:<15} {info['total_human']:>10} "
            f"{info['total_rows']:>10,} "
            f"{info['bytes_per_row']:>7.1f} "
            f"{len(info.get('columns', [])):>6}"
        )

    # Top 5 largest columns per table
    for table, info in storage.get("tables", {}).items():
        cols = sorted(info.get("columns", []), key=lambda c: c["total_compressed_bytes"], reverse=True)[:5]
        if cols:
            lines.append(f"\n  {table} — top 5 columns by compressed size:")
            for c in cols:
                lines.append(
                    f"    {c['name']:<40} {c['compressed_human']:>10} "
                    f"(ratio {c['compression_ratio']:.1f}x)"
                )

    # ── Pipeline Throughput ──
    lines.append("\n┌─────────────────────────────────────────────────────┐")
    lines.append("│  3. PIPELINE THROUGHPUT                             │")
    lines.append("└─────────────────────────────────────────────────────┘\n")

    pipeline = all_results.get("pipeline", {})
    summary = pipeline.get("summary", {})
    if summary:
        lines.append(f"  Median time:  {summary.get('median_s', '?')}s")
        lines.append(f"  Min/Max:      {summary.get('min_s', '?')}s / {summary.get('max_s', '?')}s")

    for run in pipeline.get("runs", []):
        status = "OK" if run.get("returncode") == 0 else "FAIL"
        lines.append(
            f"  Run {run.get('run', '?')}: {run.get('elapsed_s', '?')}s [{status}] "
            f"{run.get('output_human', '')}"
        )

    # ── Schema Complexity ──
    lines.append("\n┌─────────────────────────────────────────────────────┐")
    lines.append("│  4. SCHEMA COMPLEXITY                               │")
    lines.append("└─────────────────────────────────────────────────────┘\n")

    schema = all_results.get("schema_complexity", {})
    code = schema.get("code_complexity", {})
    if code:
        lines.append(f"  Pipeline LOC:        {code.get('total_pipeline_loc', '?')}")
        lines.append(f"  SQL builder LOC:     {code.get('sql_builder_loc', '?')} ({code.get('sql_builder_pct', '?')}%)")
        lines.append(f"  Builder functions:   {code.get('builder_functions', '?')}")

    summary_s = schema.get("summary", {})
    if summary_s:
        lines.append(f"  Convenience columns: {summary_s.get('total_convenience_columns', '?')}")
        lines.append(f"  Nested columns:      {summary_s.get('total_nested_columns', '?')}")
        lines.append(f"  Convenience ratio:   {summary_s.get('overall_convenience_ratio', '?')}")

    lines.append(f"\n  {'Table':<15} {'Conv':>6} {'Nested':>8} {'Total':>7} {'Ratio':>7}")
    lines.append(f"  {'─'*15} {'─'*6} {'─'*8} {'─'*7} {'─'*7}")

    for table, info in schema.get("tables", {}).items():
        lines.append(
            f"  {table:<15} {info['convenience_columns']:>6} "
            f"{info['nested_columns']:>8} "
            f"{info['total_columns']:>7} "
            f"{info['convenience_ratio']:>6.0%}"
        )

    lines.append("\n" + "=" * 72)
    lines.append("  END OF BASELINE BENCHMARK")
    lines.append("=" * 72)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Benchmark uniprot-parquet baseline")
    parser.add_argument("--lake", default="demo/lake/2026_01/lake",
                        help="Path to existing lake directory")
    parser.add_argument("--input", default="demo/lake/2026_01/sorted.jsonl.zst",
                        help="Path to input JSONL(.zst) for pipeline throughput")
    parser.add_argument("--skip-pipeline", action="store_true",
                        help="Skip pipeline throughput benchmark (slow)")
    parser.add_argument("--output-dir", default="benchmarks/results",
                        help="Directory for output files")
    args = parser.parse_args()

    repo_root = str(Path(__file__).resolve().parents[1])
    os.chdir(repo_root)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    os.makedirs(args.output_dir, exist_ok=True)

    all_results = {
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duckdb_version": duckdb.__version__,
            "lake_path": str(Path(args.lake).resolve()),
            "input_path": str(Path(args.input).resolve()) if os.path.exists(args.input) else None,
            "benchmark_type": "baseline_star_schema",
        }
    }

    print("=" * 60, file=sys.stderr)
    print("  UNIPROT-PARQUET BASELINE BENCHMARK", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    # 1. Query Latency
    print("\n[1/4] Query latency benchmarks...", file=sys.stderr)
    con = duckdb.connect()
    query_results, con = run_query_benchmarks(con, args.lake)
    all_results["query_latency"] = query_results
    print(f"  {len(query_results)} queries benchmarked", file=sys.stderr)

    # 2. Storage Analysis
    print("\n[2/4] Storage analysis...", file=sys.stderr)
    storage_results = analyze_storage(args.lake)

    # Add input size for compression comparison
    if os.path.exists(args.input):
        input_size = os.path.getsize(args.input)
        storage_results["totals"]["input_size_bytes"] = input_size
        storage_results["totals"]["input_size_human"] = _human_size(input_size)
        parquet_bytes = storage_results["totals"]["total_parquet_bytes"]
        if input_size > 0:
            storage_results["totals"]["compression_vs_input"] = round(
                parquet_bytes / input_size, 2
            )

    all_results["storage"] = storage_results
    print(f"  {len(storage_results['tables'])} tables analyzed", file=sys.stderr)

    # 3. Pipeline Throughput
    if not args.skip_pipeline and os.path.exists(args.input):
        print("\n[3/4] Pipeline throughput (3 runs)...", file=sys.stderr)
        pipeline_results = benchmark_pipeline(
            str(Path(args.input).resolve()), repo_root
        )
        all_results["pipeline"] = pipeline_results
        summary = pipeline_results.get("summary", {})
        if summary:
            print(f"  Median: {summary.get('median_s', '?')}s", file=sys.stderr)
    else:
        print("\n[3/4] Pipeline throughput (skipped)", file=sys.stderr)
        all_results["pipeline"] = {"skipped": True}

    # 4. Schema Complexity
    print("\n[4/4] Schema complexity analysis...", file=sys.stderr)
    schema_results = analyze_schema_complexity(args.lake, repo_root)
    all_results["schema_complexity"] = schema_results
    print(f"  {schema_results['summary']['total_columns']} total columns analyzed", file=sys.stderr)

    # Write outputs
    json_path = os.path.join(args.output_dir, f"baseline_{timestamp}.json")
    txt_path = os.path.join(args.output_dir, f"baseline_{timestamp}.txt")
    # Also write a "latest" symlink-style copy
    latest_json = os.path.join(args.output_dir, "baseline_latest.json")
    latest_txt = os.path.join(args.output_dir, "baseline_latest.txt")

    report = format_report(all_results)

    with open(json_path, "w") as f:
        json.dump(all_results, f, indent=2)

    with open(txt_path, "w") as f:
        f.write(report)

    # Update latest
    with open(latest_json, "w") as f:
        json.dump(all_results, f, indent=2)
    with open(latest_txt, "w") as f:
        f.write(report)

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  Results: {json_path}", file=sys.stderr)
    print(f"  Report:  {txt_path}", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    # Print report to stdout
    print(report)

    con.close()


if __name__ == "__main__":
    main()
