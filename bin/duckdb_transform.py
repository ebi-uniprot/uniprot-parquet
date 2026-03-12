#!/usr/bin/env python3
"""
DuckDB-native transformation: JSONL(.zst) chunk → 3 Hive-partitioned Parquet tables.

Produces:
  outdir/core/review_status=X/tax_division=Y/data_0.parquet
  outdir/seqs/review_status=X/tax_division=Y/data_0.parquet
  outdir/features/review_status=X/tax_division=Y/data_0.parquet

Requires a unified schema JSON (from merge_schemas.py).
DuckDB reads .jsonl.zst natively.

Usage:
    duckdb_transform.py <input.jsonl.zst> --schema <schema.json> -o <outdir>
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


def read_json_clause(jsonl_path, schema_path):
    """Build the read_json SQL clause with explicit schema."""
    with open(schema_path) as f:
        schema = json.load(f)
    cols = ', '.join(f"'{name}': '{dtype}'" for name, dtype in schema.items())
    return f"read_json('{jsonl_path}', format='newline_delimited', columns={{{cols}}})"


def write_core(con, read_clause, outdir):
    """Write core.parquet with Hive partitioning."""
    out = f"{outdir}/core"
    os.makedirs(out, exist_ok=True)
    con.sql(f"""
        COPY (
            SELECT
                primaryAccession AS acc,
                uniProtkbId AS id,
                organism.taxonId AS taxid,
                organism.scientificName AS organism_name,
                genes[1].geneName.value AS gene_name,
                CASE WHEN entryType LIKE '%Swiss-Prot%' THEN 'reviewed'
                     ELSE 'unreviewed' END AS review_status,
                {TAX_DIV_SQL} AS tax_division,
                sequence.length AS seq_len,
                sequence.molWeight AS seq_mass,
                sequence.md5 AS seq_md5,
                * EXCLUDE (primaryAccession, uniProtkbId, entryType, sequence, features)
            FROM {read_clause}
        ) TO '{out}'
        (FORMAT PARQUET, PARTITION_BY (review_status, tax_division),
         COMPRESSION 'zstd', ROW_GROUP_SIZE 500000)
    """)


def write_seqs(con, read_clause, outdir):
    """Write seqs.parquet with Hive partitioning."""
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


def write_features(con, read_clause, outdir):
    """Write features.parquet with Hive partitioning (unnested)."""
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


def main():
    parser = argparse.ArgumentParser(description='DuckDB JSONL → Parquet transformer')
    parser.add_argument('input', help='Input JSONL(.zst) file')
    parser.add_argument('--schema', required=True, help='Unified schema JSON')
    parser.add_argument('-o', '--outdir', default='parquet_out', help='Output directory')
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

    read_clause = read_json_clause(jsonl_path, args.schema)

    eprint(f"Transforming {jsonl_path} → {outdir}/")
    t0 = time.time()

    eprint("  Writing core...")
    write_core(con, read_clause, str(outdir))
    eprint(f"    done ({time.time()-t0:.1f}s)")

    t1 = time.time()
    eprint("  Writing seqs...")
    write_seqs(con, read_clause, str(outdir))
    eprint(f"    done ({time.time()-t1:.1f}s)")

    t2 = time.time()
    eprint("  Writing features...")
    write_features(con, read_clause, str(outdir))
    eprint(f"    done ({time.time()-t2:.1f}s)")

    eprint(f"\nAll tables written in {time.time()-t0:.1f}s")


if __name__ == '__main__':
    main()
