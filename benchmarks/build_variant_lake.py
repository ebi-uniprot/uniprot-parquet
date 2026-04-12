#!/usr/bin/env python3
"""
Build VARIANT-based Parquet lakes for benchmarking against the star-schema baseline.

Creates two layouts from the same sorted JSONL:

  Layout A (single table):
    entries/ with acc, taxid, reviewed (typed) + data VARIANT (full entry).
    No child tables — features, xrefs, etc. accessed via dot notation at query time.

  Layout B (star schema + VARIANT):
    Same 5 tables as baseline, but convenience columns replaced by data VARIANT.
    entries:      acc, taxid, reviewed, data VARIANT
    features:     acc, taxid, type, data VARIANT (one row per feature)
    xrefs:        acc, taxid, database, data VARIANT (one row per xref)
    comments:     acc, taxid, comment_type, data VARIANT (one row per comment)
    publications: acc, taxid, data VARIANT (one row per publication)

Usage:
    python benchmarks/build_variant_lake.py [--input demo/lake/2026_01/sorted.jsonl.zst]
                                            [--outdir benchmarks/variant_lake]
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import duckdb


def _human_size(nbytes):
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024 or unit == "TB":
            if unit == "bytes":
                return f"{int(nbytes)} bytes"
            return f"{nbytes:.2f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.2f} TB"


def build_layout_a(con, read_clause, outdir):
    """Layout A: single entries table with full VARIANT."""
    os.makedirs(os.path.join(outdir, "entries"), exist_ok=True)
    out = os.path.join(outdir, "entries", "entries_00001.parquet")

    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS reviewed,
                e::VARIANT AS data
            FROM {read_clause} e
            ORDER BY reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0

    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout A entries: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)
    return elapsed


def build_layout_b(con, read_clause, outdir):
    """Layout B: star schema with VARIANT instead of convenience columns."""
    total_elapsed = 0.0

    # ── entries ──
    os.makedirs(os.path.join(outdir, "entries"), exist_ok=True)
    out = os.path.join(outdir, "entries", "entries_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS reviewed,
                e::VARIANT AS data
            FROM {read_clause} e
            ORDER BY reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout B entries: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    # ── features ──
    os.makedirs(os.path.join(outdir, "features"), exist_ok=True)
    out = os.path.join(outdir, "features", "features_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS from_reviewed,
                CAST(unnest.type AS VARCHAR) AS type,
                unnest::VARIANT AS data
            FROM {read_clause} e,
            LATERAL UNNEST(COALESCE(e.features, [])) AS t(unnest)
            ORDER BY from_reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout B features: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    # ── xrefs ──
    os.makedirs(os.path.join(outdir, "xrefs"), exist_ok=True)
    out = os.path.join(outdir, "xrefs", "xrefs_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS from_reviewed,
                CAST(unnest.database AS VARCHAR) AS database,
                unnest::VARIANT AS data
            FROM {read_clause} e,
            LATERAL UNNEST(COALESCE(e.uniProtKBCrossReferences, [])) AS t(unnest)
            ORDER BY from_reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout B xrefs: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    # ── comments ──
    os.makedirs(os.path.join(outdir, "comments"), exist_ok=True)
    out = os.path.join(outdir, "comments", "comments_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS from_reviewed,
                CAST(unnest.commentType AS VARCHAR) AS comment_type,
                unnest::VARIANT AS data
            FROM {read_clause} e,
            LATERAL UNNEST(COALESCE(e.comments, [])) AS t(unnest)
            ORDER BY from_reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout B comments: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    # ── publications ──
    os.makedirs(os.path.join(outdir, "publications"), exist_ok=True)
    out = os.path.join(outdir, "publications", "publications_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS from_reviewed,
                unnest::VARIANT AS data
            FROM {read_clause} e,
            LATERAL UNNEST(COALESCE(e."references", [])) AS t(unnest)
            ORDER BY from_reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout B publications: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    return total_elapsed


def build_layout_c(con, read_clause, outdir):
    """Layout C: star schema + VARIANT, but big arrays STRIPPED from entries.

    The entries VARIANT only contains scalar/small-nested fields (organism,
    proteinDescription, genes, keywords, sequence, entryAudit, etc.).
    Features, xrefs, comments, and references are excluded — they live
    in their own pre-unnested child tables.

    Also promotes the most-queried fields to typed columns for fast filtering.
    """
    total_elapsed = 0.0

    # ── entries (stripped VARIANT — no features/xrefs/comments/references) ──
    os.makedirs(os.path.join(outdir, "entries"), exist_ok=True)
    out = os.path.join(outdir, "entries", "entries_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS reviewed,
                e.sequence.value AS sequence,
                CAST(e.sequence.length AS INTEGER) AS seq_length,
                struct_pack(
                    entryType            := e.entryType,
                    uniProtkbId          := e.uniProtkbId,
                    entryAudit           := e.entryAudit,
                    annotationScore      := e.annotationScore,
                    organism             := e.organism,
                    proteinExistence     := e.proteinExistence,
                    proteinDescription   := e.proteinDescription,
                    genes                := e.genes,
                    keywords             := e.keywords,
                    sequence             := e.sequence,
                    extraAttributes      := e.extraAttributes,
                    secondaryAccessions  := e.secondaryAccessions,
                    geneLocations        := e.geneLocations
                )::VARIANT AS data
            FROM {read_clause} e
            ORDER BY reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout C entries: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    # ── Child tables identical to Layout B ──
    # features
    os.makedirs(os.path.join(outdir, "features"), exist_ok=True)
    out = os.path.join(outdir, "features", "features_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS from_reviewed,
                CAST(unnest.type AS VARCHAR) AS type,
                unnest::VARIANT AS data
            FROM {read_clause} e,
            LATERAL UNNEST(COALESCE(e.features, [])) AS t(unnest)
            ORDER BY from_reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout C features: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    # xrefs
    os.makedirs(os.path.join(outdir, "xrefs"), exist_ok=True)
    out = os.path.join(outdir, "xrefs", "xrefs_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS from_reviewed,
                CAST(unnest.database AS VARCHAR) AS database,
                unnest::VARIANT AS data
            FROM {read_clause} e,
            LATERAL UNNEST(COALESCE(e.uniProtKBCrossReferences, [])) AS t(unnest)
            ORDER BY from_reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout C xrefs: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    # comments
    os.makedirs(os.path.join(outdir, "comments"), exist_ok=True)
    out = os.path.join(outdir, "comments", "comments_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS from_reviewed,
                CAST(unnest.commentType AS VARCHAR) AS comment_type,
                unnest::VARIANT AS data
            FROM {read_clause} e,
            LATERAL UNNEST(COALESCE(e.comments, [])) AS t(unnest)
            ORDER BY from_reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout C comments: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    # publications
    os.makedirs(os.path.join(outdir, "publications"), exist_ok=True)
    out = os.path.join(outdir, "publications", "publications_00001.parquet")
    t0 = time.perf_counter()
    con.sql(f"""
        COPY (
            SELECT
                e.primaryAccession AS acc,
                e.organism.taxonId AS taxid,
                CASE WHEN e.entryType LIKE '%Swiss-Prot%'
                     THEN true ELSE false END AS from_reviewed,
                unnest::VARIANT AS data
            FROM {read_clause} e,
            LATERAL UNNEST(COALESCE(e."references", [])) AS t(unnest)
            ORDER BY from_reviewed DESC, taxid, acc
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    elapsed = time.perf_counter() - t0
    total_elapsed += elapsed
    size = os.path.getsize(out)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  Layout C publications: {rows:,} rows, {_human_size(size)}, {elapsed:.2f}s",
          file=sys.stderr)

    return total_elapsed


def main():
    parser = argparse.ArgumentParser(description="Build VARIANT Parquet lakes")
    parser.add_argument("--input", default="demo/lake/2026_01/sorted.jsonl.zst",
                        help="Path to sorted JSONL(.zst) input")
    parser.add_argument("--outdir", default="benchmarks/variant_lake",
                        help="Output base directory")
    parser.add_argument("--memory-limit", default="3GB",
                        help="DuckDB memory limit")
    args = parser.parse_args()

    repo_root = str(Path(__file__).resolve().parents[1])
    os.chdir(repo_root)

    con = duckdb.connect()
    con.sql(f"SET memory_limit='{args.memory_limit}'")

    read_clause = (
        f"read_json_auto('{args.input}', format='newline_delimited', "
        f"sample_size=-1, maximum_object_size=536870912)"
    )

    print("=" * 60, file=sys.stderr)
    print("  BUILDING VARIANT LAKES", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    # Layout A
    outdir_a = os.path.join(args.outdir, "layout_a")
    os.makedirs(outdir_a, exist_ok=True)
    print("\n--- Layout A (single table, full VARIANT) ---", file=sys.stderr)
    time_a = build_layout_a(con, read_clause, outdir_a)

    # Layout B
    outdir_b = os.path.join(args.outdir, "layout_b")
    os.makedirs(outdir_b, exist_ok=True)
    print("\n--- Layout B (star schema + VARIANT) ---", file=sys.stderr)
    time_b = build_layout_b(con, read_clause, outdir_b)

    # Layout C
    outdir_c = os.path.join(args.outdir, "layout_c")
    os.makedirs(outdir_c, exist_ok=True)
    print("\n--- Layout C (star schema + VARIANT, arrays stripped from entries) ---",
          file=sys.stderr)
    time_c = build_layout_c(con, read_clause, outdir_c)

    # Summary
    total_a = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, files in os.walk(outdir_a) for f in files if f.endswith(".parquet")
    )
    total_b = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, files in os.walk(outdir_b) for f in files if f.endswith(".parquet")
    )
    total_c = sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, files in os.walk(outdir_c) for f in files if f.endswith(".parquet")
    )

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  Layout A: {_human_size(total_a)} total, {time_a:.2f}s", file=sys.stderr)
    print(f"  Layout B: {_human_size(total_b)} total, {time_b:.2f}s", file=sys.stderr)
    print(f"  Layout C: {_human_size(total_c)} total, {time_c:.2f}s", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    # Write metadata for benchmark script
    meta = {
        "input": args.input,
        "layout_a": {"path": outdir_a, "total_bytes": total_a, "build_time_s": round(time_a, 3)},
        "layout_b": {"path": outdir_b, "total_bytes": total_b, "build_time_s": round(time_b, 3)},
        "layout_c": {"path": outdir_c, "total_bytes": total_c, "build_time_s": round(time_c, 3)},
        "duckdb_version": duckdb.__version__,
    }
    meta_path = os.path.join(args.outdir, "build_meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    con.close()


if __name__ == "__main__":
    main()
