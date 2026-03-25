#!/usr/bin/env python3
"""
Transform JSONL(.zst) → Apache Iceberg tables (entries + features).

Architecture:
  - DuckDB reads the JSONL and performs all SQL transformations (flattening,
    unnesting, sorting) — it's purpose-built for this.
  - DuckDB streams Arrow record batches (bounded memory, never materialises
    the full dataset).
  - Batches are accumulated to a target size, then flushed to Iceberg via
    PyIceberg's table.append().  Each flush creates a new data file in a
    single Iceberg snapshot (committed at the end).

Memory model:
  At any moment we hold at most --batch-size rows in Arrow memory.  DuckDB
  may spill to disk during the ORDER BY, but the Arrow → Iceberg path stays
  bounded regardless of dataset size.

Catalog: SQLite (local filesystem, no cloud needed).

Usage:
    iceberg_transform.py <input.jsonl.zst> \
        --catalog-uri sqlite:///path/to/catalog.db \
        --warehouse /path/to/warehouse \
        [--namespace uniprot] \
        [--memory-limit 16GB] \
        [--batch-size 1000000] \
        [--sorted-jsonl /path/to/sorted.jsonl.zst]

Requires: duckdb, pyiceberg[pyarrow], pyarrow
"""

import os
import sys
import argparse
import time
import json

import duckdb
import pyarrow as pa

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
)

# Our schema definitions
from iceberg_schema import (
    ENTRIES_SCHEMA,
    FEATURES_SCHEMA,
    ENTRIES_SORT_ORDER,
    FEATURES_SORT_ORDER,
)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# ─── DuckDB helpers ──────────────────────────────────────────────────────

def build_read_clause(jsonl_path: str, schema_path: str | None) -> str:
    """Build the DuckDB read_json SQL fragment.

    If a committed schema.json is provided, use explicit column types
    for deterministic parsing.  Otherwise fall back to auto-detect.
    """
    if schema_path and os.path.exists(schema_path):
        with open(schema_path) as f:
            schema = json.load(f)
        cols = ", ".join(f"'{name}': '{dtype}'" for name, dtype in schema.items())
        return (
            f"read_json('{jsonl_path}', format='newline_delimited', "
            f"maximum_object_size=536870912, columns={{{cols}}})"
        )
    else:
        return (
            f"read_json_auto('{jsonl_path}', format='newline_delimited', "
            f"maximum_object_size=536870912)"
        )


def init_duckdb(memory_limit: str, threads: int | None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql(f"SET memory_limit='{memory_limit}'")
    if threads:
        con.sql(f"SET threads={threads}")
    # Ensure temp directory uses local storage for spilling
    con.sql("SET temp_directory='/tmp/duckdb_temp'")
    return con


# ─── SQL for each table ─────────────────────────────────────────────────

ENTRIES_SQL = """
SELECT
    -- Identity
    e.primaryAccession                              AS acc,
    e.uniProtkbId                                   AS id,
    CASE WHEN e.entryType LIKE '%Swiss-Prot%'
         THEN true ELSE false END                   AS reviewed,
    e.secondaryAccessions                           AS secondary_accs,

    -- Organism (flattened)
    e.organism.taxonId                              AS taxid,
    e.organism.scientificName                       AS organism_name,
    e.organism.commonName                           AS organism_common,
    e.organism.lineage                              AS lineage,

    -- Gene & protein (flattened)
    e.genes[1].geneName.value                       AS gene_name,
    e.proteinDescription.recommendedName.fullName.value AS protein_name,
    -- EC numbers: extract from recommendedName.ecNumbers[]
    list_transform(
        COALESCE(e.proteinDescription.recommendedName.ecNumbers, []),
        x -> x.value
    )                                               AS ec_numbers,
    e.proteinExistence                              AS protein_existence,
    e.annotationScore                               AS annotation_score,

    -- Sequence (flattened)
    e.sequence.value                                AS sequence,
    CAST(e.sequence.length AS INTEGER)              AS seq_length,
    CAST(e.sequence.molWeight AS INTEGER)           AS seq_mass,
    e.sequence.md5                                  AS seq_md5,
    e.sequence.crc64                                AS seq_crc64,

    -- Cross-reference shortcuts
    list_distinct([
        x.id
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
        IF x.database = 'GO'
    ])                                              AS go_ids,
    list_distinct([
        x.database
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
    ])                                              AS xref_dbs,
    list_transform(
        COALESCE(e.keywords, []),
        x -> x.id
    )                                               AS keyword_ids,
    list_transform(
        COALESCE(e.keywords, []),
        x -> x.name
    )                                               AS keyword_names,

    -- Versioning
    CAST(e.entryAudit.firstPublicDate AS DATE)      AS first_public,
    CAST(e.entryAudit.lastAnnotationUpdateDate AS DATE) AS last_modified,
    CAST(e.entryAudit.lastSequenceUpdateDate AS DATE)   AS last_seq_modified,
    CAST(e.entryAudit.entryVersion AS INTEGER)      AS entry_version,
    CAST(e.entryAudit.sequenceVersion AS INTEGER)   AS seq_version,

    -- Counts
    CAST(len(COALESCE(e.features, [])) AS INTEGER)  AS feature_count,
    CAST(len(COALESCE(e.uniProtKBCrossReferences, [])) AS INTEGER) AS xref_count,
    CAST(len(COALESCE(e.comments, [])) AS INTEGER)  AS comment_count,
    e.extraAttributes.uniParcId                     AS uniparc_id,

    -- Full nested structures (preserved for power users)
    e.organism                                      AS organism,
    e.proteinDescription                            AS protein_desc,
    e.genes                                         AS genes,
    e.keywords                                      AS keywords,
    -- comments stored as MAP(VARCHAR, JSON)[] in source — cast to JSON string list
    e.comments                                      AS comments,
    e.uniProtKBCrossReferences                      AS xrefs,
    e.references                                    AS "references",
    e.features                                      AS features,
    e.organismHosts                                 AS organism_hosts,
    e.geneLocations                                 AS gene_locations

FROM {read_clause} e
ORDER BY e.organism.taxonId
"""


FEATURES_SQL = """
SELECT
    sub.acc,
    sub.reviewed,
    sub.taxid,
    sub.organism_name,
    sub.seq_length,

    f.type                                          AS type,
    CAST(f.location.start.value AS INTEGER)         AS start_pos,
    CAST(f.location.end.value AS INTEGER)           AS end_pos,
    f.location.start.modifier                       AS start_modifier,
    f.location.end.modifier                         AS end_modifier,
    f.description                                   AS description,
    f.featureId                                     AS feature_id,

    list_transform(
        COALESCE(f.evidences, []),
        x -> x.evidenceCode
    )                                               AS evidence_codes,

    f.alternativeSequence.originalSequence           AS original_sequence,
    f.alternativeSequence.alternativeSequences       AS alternative_sequences,

    f.ligand.name                                   AS ligand_name,
    f.ligand.id                                     AS ligand_id,
    f.ligand.label                                  AS ligand_label,
    f.ligand.note                                   AS ligand_note

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        CASE WHEN e.entryType LIKE '%Swiss-Prot%'
             THEN true ELSE false END                AS reviewed,
        e.organism.taxonId                           AS taxid,
        e.organism.scientificName                    AS organism_name,
        CAST(e.sequence.length AS INTEGER)           AS seq_length,
        e.features
    FROM {read_clause} e
    WHERE e.features IS NOT NULL AND len(e.features) > 0
) sub, LATERAL unnest(sub.features) AS f
ORDER BY sub.taxid
"""


SORTED_JSONL_SQL = """
SELECT *
FROM {read_clause} e
ORDER BY e.organism.taxonId
"""


# ─── Iceberg catalog / table helpers ────────────────────────────────────

def get_catalog(catalog_uri: str, warehouse: str) -> SqlCatalog:
    """Create or connect to a SQLite-backed Iceberg catalog."""
    return SqlCatalog(
        "uniprot",
        **{
            "uri": catalog_uri,
            "warehouse": warehouse,
        },
    )


def ensure_table(catalog, namespace, table_name, schema, sort_order):
    """Create namespace + table if they don't already exist."""
    try:
        catalog.create_namespace(namespace)
        eprint(f"  Created namespace '{namespace}'")
    except NamespaceAlreadyExistsError:
        pass

    full_name = f"{namespace}.{table_name}"
    try:
        table = catalog.create_table(
            full_name,
            schema=schema,
            sort_order=sort_order,
        )
        eprint(f"  Created table '{full_name}'")
    except TableAlreadyExistsError:
        table = catalog.load_table(full_name)
        eprint(f"  Loaded existing table '{full_name}'")

    return table


# ─── Core pipeline ──────────────────────────────────────────────────────

def stream_to_iceberg(con, sql, table, batch_size, label="table"):
    """Stream DuckDB result → Iceberg table in bounded-memory batches.

    DuckDB executes the query lazily and yields Arrow record batches of
    `batch_size` rows.  We accumulate batches and flush to Iceberg once
    we hit the target size.  This keeps Arrow memory bounded to roughly
    one batch at a time (plus whatever DuckDB needs for the ORDER BY
    spill).

    Returns the total number of rows written.
    """
    eprint(f"  Querying DuckDB for {label} (sorted by taxid)...")
    t0 = time.time()

    result = con.sql(sql)
    reader = result.fetch_record_batch(chunk_size=batch_size)

    total_rows = 0
    batch_num = 0

    for record_batch in reader:
        arrow_tbl = pa.Table.from_batches([record_batch])
        n = arrow_tbl.num_rows
        if n == 0:
            continue

        batch_num += 1
        total_rows += n
        t1 = time.time()
        table.append(arrow_tbl)
        eprint(
            f"    batch {batch_num}: {n:,} rows "
            f"(total {total_rows:,}, "
            f"wrote in {time.time()-t1:.1f}s)"
        )

    elapsed = time.time() - t0
    eprint(f"  {label}: {total_rows:,} rows in {batch_num} batches ({elapsed:.1f}s)")
    return total_rows


def write_entries(con, read_clause, table, batch_size):
    """Stream entries from DuckDB → Iceberg entries table."""
    sql = ENTRIES_SQL.format(read_clause=read_clause)
    return stream_to_iceberg(con, sql, table, batch_size, label="entries")


def write_features(con, read_clause, table, batch_size):
    """Stream features from DuckDB → Iceberg features table."""
    sql = FEATURES_SQL.format(read_clause=read_clause)
    return stream_to_iceberg(con, sql, table, batch_size, label="features")


def write_sorted_jsonl(con, read_clause, output_path, batch_size):
    """Write a taxid-sorted JSONL.zst file for downstream consumers.

    Streams from DuckDB in Arrow batches → orjson → zstd pipe.
    Memory-bounded: only one batch of rows in Python at a time.
    """
    import subprocess

    eprint(f"  Writing sorted JSONL to {output_path}...")
    t0 = time.time()

    sql = SORTED_JSONL_SQL.format(read_clause=read_clause)
    result = con.sql(sql)

    try:
        import orjson
    except ImportError:
        eprint("  WARNING: orjson not available, skipping sorted JSONL")
        return

    proc = subprocess.Popen(
        ["zstd", "-3", "-T0", "-o", output_path],
        stdin=subprocess.PIPE,
        bufsize=4 * 1024 * 1024,  # 4 MB write buffer
    )

    reader = result.fetch_record_batch(chunk_size=batch_size)
    count = 0
    batch_num = 0

    for record_batch in reader:
        batch_num += 1
        rows = pa.Table.from_batches([record_batch]).to_pylist()
        buf = b"".join(orjson.dumps(row) + b"\n" for row in rows)
        proc.stdin.write(buf)
        count += len(rows)
        if batch_num % 10 == 0:
            eprint(f"    jsonl batch {batch_num}: {count:,} rows written...")

    proc.stdin.close()
    rc = proc.wait()
    if rc != 0:
        eprint(f"  WARNING: zstd exited with code {rc}")

    eprint(f"  Wrote {count:,} sorted JSONL records in {time.time()-t0:.1f}s")


# ─── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Transform UniProtKB JSONL → Iceberg tables (entries + features)"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "--schema", default=None,
        help="Committed DuckDB schema JSON (from infer_schema.py). "
             "If omitted, DuckDB auto-detects.",
    )
    parser.add_argument(
        "--catalog-uri", default="sqlite:///catalog.db",
        help="SQLite catalog URI (default: sqlite:///catalog.db in cwd)",
    )
    parser.add_argument(
        "--warehouse", default="./warehouse",
        help="Iceberg warehouse directory (where data files live)",
    )
    parser.add_argument(
        "--namespace", default="uniprot",
        help="Iceberg namespace (default: uniprot)",
    )
    parser.add_argument("--memory-limit", default="16GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    parser.add_argument(
        "--batch-size", type=int, default=1_000_000,
        help="Rows per Arrow batch (controls peak memory; default 1M)",
    )
    parser.add_argument(
        "--sorted-jsonl", default=None,
        help="If set, also write a taxid-sorted JSONL.zst file to this path",
    )
    parser.add_argument(
        "--release", default=None,
        help="Release label (e.g. 2026_01). Stored as table property.",
    )
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    warehouse = os.path.abspath(args.warehouse)
    os.makedirs(warehouse, exist_ok=True)

    eprint("=" * 60)
    eprint("UniProtKB → Iceberg Transform")
    eprint("=" * 60)
    eprint(f"  Input:     {jsonl_path}")
    eprint(f"  Catalog:   {args.catalog_uri}")
    eprint(f"  Warehouse: {warehouse}")
    eprint(f"  Namespace: {args.namespace}")
    eprint(f"  Memory:    {args.memory_limit}")
    eprint()

    # ── Init DuckDB ──
    con = init_duckdb(args.memory_limit, args.threads)
    read_clause = build_read_clause(jsonl_path, args.schema)

    # ── Init Iceberg catalog ──
    catalog = get_catalog(args.catalog_uri, warehouse)
    entries_table = ensure_table(
        catalog, args.namespace, "entries",
        ENTRIES_SCHEMA, ENTRIES_SORT_ORDER,
    )
    features_table = ensure_table(
        catalog, args.namespace, "features",
        FEATURES_SCHEMA, FEATURES_SORT_ORDER,
    )

    # Tag release as a table property if provided
    if args.release:
        with entries_table.transaction() as txn:
            txn.set_properties({"uniprot.release": args.release})
        with features_table.transaction() as txn:
            txn.set_properties({"uniprot.release": args.release})
        eprint(f"  Tagged tables with release={args.release}")

    # ── Write entries ──
    eprint("\n--- ENTRIES ---")
    t_total = time.time()
    n_entries = write_entries(con, read_clause, entries_table, args.batch_size)

    # ── Write features ──
    eprint("\n--- FEATURES ---")
    n_features = write_features(con, read_clause, features_table, args.batch_size)

    # ── Sorted JSONL (optional) ──
    if args.sorted_jsonl:
        eprint("\n--- SORTED JSONL ---")
        write_sorted_jsonl(con, read_clause, args.sorted_jsonl, args.batch_size)

    # ── Summary ──
    elapsed = time.time() - t_total
    eprint("\n" + "=" * 60)
    eprint(f"DONE in {elapsed:.1f}s")
    eprint(f"  entries:  {n_entries:,} rows")
    eprint(f"  features: {n_features:,} rows")

    # Print snapshot info
    eprint(f"\n  entries snapshot:  {entries_table.current_snapshot()}")
    eprint(f"  features snapshot: {features_table.current_snapshot()}")
    eprint("=" * 60)


if __name__ == "__main__":
    main()
