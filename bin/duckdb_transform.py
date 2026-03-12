#!/usr/bin/env python3
"""
DuckDB-native transformation: JSONL chunk → 3 Hive-partitioned Parquet tables.

Produces:
  outdir/core/review_status=X/tax_division=Y/data_0.parquet
  outdir/seqs/review_status=X/tax_division=Y/data_0.parquet
  outdir/features/review_status=X/tax_division=Y/data_0.parquet

Key design decisions:
  - sample_size=-1: full schema scan, no sampling (handles highly dimensional data)
  - union_by_name=true: when merging schemas across chunks
  - Tax division classification is done in pure SQL (no Python enrichment)
  - ZSTD compression throughout
  - ROW_GROUP_SIZE tuned for <512MB per file target

Usage:
    duckdb_transform.py <input.jsonl> -o <outdir> [--schema <schema.json>]
    duckdb_transform.py --infer-schema <input.jsonl> [-o <schema.json>]
"""

import os
import sys
import json
import argparse
import time
import duckdb
from pathlib import Path


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# ─── SQL Building Blocks ───────────────────────────────────────────────

TAX_DIV_SQL = """
    CASE
        WHEN organism.lineage[1] = 'Bacteria' THEN 'bacteria'
        WHEN organism.lineage[1] = 'Archaea' THEN 'archaea'
        WHEN organism.lineage[1] = 'Viruses' THEN 'viruses'
        WHEN list_contains(organism.lineage, 'Homo') THEN 'human'
        WHEN organism.lineage[1] = 'Eukaryota' AND organism.lineage[2] = 'Fungi' THEN 'fungi'
        WHEN list_contains(organism.lineage, 'Rodentia') THEN 'rodents'
        WHEN list_contains(organism.lineage, 'Mammalia') THEN 'mammals'
        WHEN organism.lineage[1] = 'Eukaryota'
            AND len(organism.lineage) >= 4
            AND organism.lineage[3] = 'Chordata'
            AND organism.lineage[4] = 'Craniata' THEN 'vertebrates'
        WHEN list_contains(organism.lineage, 'Viridiplantae')
            OR list_contains(organism.lineage, 'Rhodophyta')
            OR list_contains(organism.lineage, 'Stramenopiles') THEN 'plants'
        WHEN organism.lineage[1] = 'Eukaryota' THEN 'invertebrates'
        ELSE 'unclassified'
    END
""".strip()


def read_json_clause(jsonl_path, schema_path=None):
    """Build the read_json SQL clause, optionally with explicit schema."""
    if schema_path and os.path.exists(schema_path):
        with open(schema_path) as f:
            schema = json.load(f)
        # Build columns={name: type} map for read_json
        cols = ', '.join(f"'{name}': '{dtype}'" for name, dtype in schema.items())
        return f"read_json('{jsonl_path}', format='newline_delimited', columns={{{cols}}})"
    else:
        return f"read_json('{jsonl_path}', format='newline_delimited', sample_size=-1)"


def infer_schema(con, jsonl_path):
    """Infer full schema from a JSONL file using sample_size=-1."""
    result = con.sql(f"""
        DESCRIBE SELECT * FROM read_json('{jsonl_path}',
            format='newline_delimited',
            sample_size=-1
        )
    """).fetchall()
    return {row[0]: row[1] for row in result}


def write_core(con, jsonl_path, outdir, schema_path=None):
    """Write core.parquet with Hive partitioning."""
    read_clause = read_json_clause(jsonl_path, schema_path)
    out = f"{outdir}/core"
    os.makedirs(out, exist_ok=True)

    con.sql(f"""
        COPY (
            SELECT
                -- Analytical aliases (flattened)
                primaryAccession AS acc,
                uniProtkbId AS id,
                organism.taxonId AS taxid,
                organism.scientificName AS organism_name,
                genes[1].geneName.value AS gene_name,
                CASE WHEN entryType LIKE '%Swiss-Prot%' THEN 'reviewed'
                     ELSE 'unreviewed' END AS review_status,
                {TAX_DIV_SQL} AS tax_division,

                -- Sequence meta-metrics
                sequence.length AS seq_len,
                sequence.molWeight AS seq_mass,
                sequence.md5 AS seq_md5,

                -- Preserved nested data (everything except heavy columns + extracted aliases)
                * EXCLUDE (
                    primaryAccession,
                    uniProtkbId,
                    entryType,
                    sequence,
                    features
                )
            FROM {read_clause}
        ) TO '{out}'
        (FORMAT PARQUET, PARTITION_BY (review_status, tax_division),
         COMPRESSION 'zstd', ROW_GROUP_SIZE 500000)
    """)
    return out


def write_seqs(con, jsonl_path, outdir, schema_path=None):
    """Write seqs.parquet with Hive partitioning."""
    read_clause = read_json_clause(jsonl_path, schema_path)
    out = f"{outdir}/seqs"
    os.makedirs(out, exist_ok=True)

    con.sql(f"""
        COPY (
            SELECT
                primaryAccession AS acc,
                sequence.value AS sequence,
                CASE WHEN entryType LIKE '%Swiss-Prot%' THEN 'reviewed'
                     ELSE 'unreviewed' END AS review_status,
                {TAX_DIV_SQL} AS tax_division
            FROM {read_clause}
        ) TO '{out}'
        (FORMAT PARQUET, PARTITION_BY (review_status, tax_division),
         COMPRESSION 'zstd', ROW_GROUP_SIZE 500000)
    """)
    return out


def write_features(con, jsonl_path, outdir, schema_path=None):
    """Write features.parquet with Hive partitioning (unnested)."""
    read_clause = read_json_clause(jsonl_path, schema_path)
    out = f"{outdir}/features"
    os.makedirs(out, exist_ok=True)

    con.sql(f"""
        COPY (
            SELECT
                acc,
                f.type AS type,
                f.location.start.value AS start_pos,
                f.location.end.value AS end_pos,
                f.description AS description,
                review_status,
                tax_division
            FROM (
                SELECT
                    primaryAccession AS acc,
                    CASE WHEN entryType LIKE '%Swiss-Prot%' THEN 'reviewed'
                         ELSE 'unreviewed' END AS review_status,
                    {TAX_DIV_SQL} AS tax_division,
                    unnest(features) AS f
                FROM {read_clause}
                WHERE features IS NOT NULL AND len(features) > 0
            )
        ) TO '{out}'
        (FORMAT PARQUET, PARTITION_BY (review_status, tax_division),
         COMPRESSION 'zstd', ROW_GROUP_SIZE 500000)
    """)
    return out


def main():
    parser = argparse.ArgumentParser(description='DuckDB JSONL → Parquet transformer')
    parser.add_argument('input', help='Input JSONL file')
    parser.add_argument('-o', '--outdir', default='parquet_out', help='Output directory')
    parser.add_argument('--schema', default=None, help='Explicit schema JSON (from infer step)')
    parser.add_argument('--infer-schema', action='store_true',
                        help='Only infer schema, write to --outdir as schema.json')
    parser.add_argument('--memory-limit', default='8GB', help='DuckDB memory limit')
    parser.add_argument('--threads', type=int, default=None, help='DuckDB threads')
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.sql(f"SET memory_limit='{args.memory_limit}'")
    if args.threads:
        con.sql(f"SET threads={args.threads}")

    # ─── Schema-only mode ─────────────────────────────────────────
    if args.infer_schema:
        eprint(f"Inferring schema from {jsonl_path} (full scan)...")
        t0 = time.time()
        schema = infer_schema(con, jsonl_path)
        schema_file = outdir / 'schema.json'
        with open(schema_file, 'w') as f:
            json.dump(schema, f, indent=2)
        eprint(f"  {len(schema)} columns inferred in {time.time()-t0:.1f}s")
        eprint(f"  Written to {schema_file}")
        # Also print to stdout for Nextflow to capture
        print(str(schema_file))
        return

    # ─── Transform mode ───────────────────────────────────────────
    eprint(f"Transforming {jsonl_path} → {outdir}/")

    t0 = time.time()
    eprint("  Writing core...")
    write_core(con, jsonl_path, str(outdir), args.schema)
    eprint(f"    done ({time.time()-t0:.1f}s)")

    t1 = time.time()
    eprint("  Writing seqs...")
    write_seqs(con, jsonl_path, str(outdir), args.schema)
    eprint(f"    done ({time.time()-t1:.1f}s)")

    t2 = time.time()
    eprint("  Writing features...")
    write_features(con, jsonl_path, str(outdir), args.schema)
    eprint(f"    done ({time.time()-t2:.1f}s)")

    total = time.time() - t0
    eprint(f"\nAll tables written in {total:.1f}s")

    # Write a summary for Nextflow
    summary = {
        'input': jsonl_path,
        'tables': ['core', 'seqs', 'features'],
        'elapsed_seconds': round(total, 1)
    }
    with open(outdir / 'transform_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)


if __name__ == '__main__':
    main()
