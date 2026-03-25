#!/usr/bin/env python3
"""
Transform JSONL(.zst) → Apache Iceberg tables (entries + features).

Architecture:
  - DuckDB reads the JSONL and performs all SQL transformations (flattening,
    unnesting, sorting) — it's purpose-built for this.
  - The Iceberg schema is **inferred** from DuckDB's Arrow output on the first
    batch, then applied to all subsequent batches.  No hand-maintained schema
    file is needed.
  - DuckDB streams Arrow record batches (bounded memory, never materialises
    the full dataset).
  - PyIceberg writes each batch as a Parquet file, then registers all files
    in a single atomic Iceberg snapshot.

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

Requires: duckdb, pyiceberg[pyarrow,sql-sqlite], pyarrow
"""

import os
import sys
import argparse
import time
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
)
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform


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
    # Use SLURM's $TMPDIR (local scratch) if available, else system default
    temp_dir = os.environ.get("TMPDIR", "/tmp/duckdb_temp")
    temp_dir = os.path.join(temp_dir, "duckdb_spill")
    os.makedirs(temp_dir, exist_ok=True)
    con.sql(f"SET temp_directory='{temp_dir}'")
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
    e.comments                                      AS comments,
    e.uniProtKBCrossReferences                      AS xrefs,
    e.references                                    AS "references",
    e.features                                      AS features,
    e.organismHosts                                 AS organism_hosts,
    e.geneLocations                                 AS gene_locations

FROM {read_clause} e
ORDER BY reviewed, e.organism.taxonId
"""


FEATURES_SQL = """
SELECT
    sub.acc,
    sub.reviewed,
    sub.taxid,
    sub.organism_name,
    sub.seq_length,

    unnest.type                                     AS type,
    CAST(unnest.location.start.value AS INTEGER)    AS start_pos,
    CAST(unnest.location.end.value AS INTEGER)      AS end_pos,
    unnest.location.start.modifier                  AS start_modifier,
    unnest.location.end.modifier                    AS end_modifier,
    unnest.description                              AS description,
    unnest.featureId                                AS feature_id,

    list_transform(
        COALESCE(unnest.evidences, []),
        x -> x.evidenceCode
    )                                               AS evidence_codes,

    unnest.alternativeSequence.originalSequence      AS original_sequence,
    unnest.alternativeSequence.alternativeSequences  AS alternative_sequences,

    unnest.ligand.name                              AS ligand_name,
    unnest.ligand.id                                AS ligand_id,
    unnest.ligand.label                             AS ligand_label,
    unnest.ligand.note                              AS ligand_note

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
) sub, LATERAL unnest(sub.features)
ORDER BY sub.reviewed, sub.taxid
"""


SORTED_JSONL_SQL = """
SELECT *
FROM {read_clause} e
ORDER BY e.organism.taxonId
"""


# ─── Schema inference ────────────────────────────────────────────────────

def infer_iceberg_schema(con, sql: str, label: str) -> Schema:
    """Infer the Iceberg schema from a DuckDB SQL query.

    Runs the query, reads the first batch to capture the Arrow schema
    (which DuckDB fixes before yielding any rows), then returns the
    corresponding Iceberg schema with auto-assigned field IDs.
    """
    eprint(f"  Inferring {label} schema from DuckDB...")
    t0 = time.time()

    result = con.sql(sql)
    # Read a single small batch — we only need the schema, not the data.
    # DuckDB resolves the full schema before the first row is yielded.
    reader = result.to_arrow_reader(batch_size=1)
    batch = next(reader)
    # Close the reader to release DuckDB resources
    del reader, result

    raw_schema = _pyarrow_to_schema_without_ids(batch.schema)
    # Assign unique, sequential field IDs (the raw schema has -1 for all)
    iceberg_schema = assign_fresh_schema_ids(raw_schema)

    eprint(f"    {len(iceberg_schema.fields)} columns inferred in {time.time()-t0:.1f}s")
    return iceberg_schema


def _sort_order_for(iceberg_schema: Schema, *column_names: str) -> SortOrder:
    """Build a SortOrder on one or more named columns by looking up their field IDs."""
    return SortOrder(
        *(
            SortField(source_id=iceberg_schema.find_field(name).field_id, transform=IdentityTransform())
            for name in column_names
        ),
    )



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


def _write_version_hint(table):
    """Write a version-hint.text file so DuckDB's iceberg_scan can find the latest metadata.

    PyIceberg metadata files are named like 00002-<uuid>.metadata.json.
    The version-hint.text file just contains the version number (e.g. "2").
    """
    table_location = table.location().replace("file://", "")
    metadata_dir = os.path.join(table_location, "metadata")
    if not os.path.isdir(metadata_dir):
        return
    # Find the highest-numbered metadata file and create DuckDB-compatible symlinks.
    # PyIceberg writes metadata as "00002-<uuid>.metadata.json" but DuckDB's
    # iceberg_scan expects "v2.metadata.json".  We write the version-hint.text
    # AND create a symlink so DuckDB can find it.
    metadata_files = {}
    for fname in os.listdir(metadata_dir):
        if fname.endswith(".metadata.json"):
            try:
                version = int(fname.split("-", 1)[0])
                metadata_files[version] = fname
            except ValueError:
                continue
    if metadata_files:
        latest = max(metadata_files)
        # Write version hint
        hint_path = os.path.join(metadata_dir, "version-hint.text")
        with open(hint_path, "w") as f:
            f.write(str(latest))
        # Create symlink: v2.metadata.json → 00002-<uuid>.metadata.json
        symlink_name = f"v{latest}.metadata.json"
        symlink_path = os.path.join(metadata_dir, symlink_name)
        if os.path.lexists(symlink_path):
            os.remove(symlink_path)
        os.symlink(metadata_files[latest], symlink_path)


# ─── Core pipeline ──────────────────────────────────────────────────────

def stream_to_iceberg(con, sql, table, batch_size, label="table"):
    """Stream DuckDB result → Iceberg table in bounded-memory batches.

    DuckDB executes the query lazily and yields Arrow record batches of
    `batch_size` rows.  Each batch is written as a Parquet file, then all
    files are registered with Iceberg in a single atomic snapshot via
    add_files().

    The Arrow schema is determined once by DuckDB before the first batch
    is yielded — all batches share the same schema.

    Memory model: only one Arrow batch is held at a time (plus whatever
    DuckDB needs for the ORDER BY spill).

    Returns the total number of rows written.
    """
    eprint(f"  Querying DuckDB for {label} (sorted by taxid)...")
    t0 = time.time()

    result = con.sql(sql)
    reader = result.to_arrow_reader(batch_size=batch_size)

    # Resolve the data directory for this Iceberg table
    table_location = table.location()
    data_dir = os.path.join(table_location.replace("file://", ""), "data")
    os.makedirs(data_dir, exist_ok=True)

    total_rows = 0
    file_num = 0
    parquet_files = []

    for record_batch in reader:
        arrow_tbl = pa.Table.from_batches([record_batch])
        n = arrow_tbl.num_rows
        if n == 0:
            continue

        file_num += 1
        total_rows += n

        parquet_path = os.path.join(data_dir, f"{label}_{file_num:05d}.parquet")
        t1 = time.time()
        pq.write_table(arrow_tbl, parquet_path, compression="zstd")
        parquet_files.append(parquet_path)
        eprint(
            f"    batch {file_num}: {n:,} rows "
            f"(total {total_rows:,}, "
            f"wrote in {time.time()-t1:.1f}s)"
        )

    # Register all Parquet files with Iceberg in one atomic snapshot
    if parquet_files:
        t1 = time.time()
        table.add_files(parquet_files)
        eprint(f"    registered {len(parquet_files)} files with Iceberg "
               f"in {time.time()-t1:.1f}s")

    elapsed = time.time() - t0
    eprint(f"  {label}: {total_rows:,} rows in {file_num} files, "
           f"1 snapshot ({elapsed:.1f}s)")
    return total_rows


def write_entries(con, read_clause, table, batch_size):
    """Stream entries from DuckDB → Iceberg entries table."""
    sql = ENTRIES_SQL.format(read_clause=read_clause)
    return stream_to_iceberg(con, sql, table, batch_size, label="entries")


def write_features(con, read_clause, table, batch_size):
    """Stream features from DuckDB → Iceberg features table."""
    sql = FEATURES_SQL.format(read_clause=read_clause)
    return stream_to_iceberg(con, sql, table, batch_size, label="features")


def write_sorted_jsonl(con, read_clause, output_path):
    """Write a taxid-sorted JSONL.zst file using DuckDB's native COPY.

    DuckDB handles the JSON serialisation and zstd compression internally,
    which is faster than streaming through Python and avoids the orjson /
    subprocess dependency for this code path.
    """
    eprint(f"  Writing sorted JSONL to {output_path}...")
    t0 = time.time()

    sql = SORTED_JSONL_SQL.format(read_clause=read_clause)
    con.sql(f"""
        COPY ({sql}) TO '{output_path}'
        (FORMAT JSON, COMPRESSION ZSTD)
    """)

    elapsed = time.time() - t0
    eprint(f"  Wrote sorted JSONL in {elapsed:.1f}s")


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

    # ── Infer Iceberg schemas from DuckDB Arrow output ──
    eprint("--- SCHEMA INFERENCE ---")
    entries_schema = infer_iceberg_schema(
        con, ENTRIES_SQL.format(read_clause=read_clause), "entries"
    )
    features_schema = infer_iceberg_schema(
        con, FEATURES_SQL.format(read_clause=read_clause), "features"
    )

    entries_sort = _sort_order_for(entries_schema, "reviewed", "taxid")
    features_sort = _sort_order_for(features_schema, "reviewed", "taxid")

    # ── Init Iceberg catalog ──
    catalog = get_catalog(args.catalog_uri, warehouse)
    entries_table = ensure_table(
        catalog, args.namespace, "entries",
        entries_schema, entries_sort,
    )
    features_table = ensure_table(
        catalog, args.namespace, "features",
        features_schema, features_sort,
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
        write_sorted_jsonl(con, read_clause, args.sorted_jsonl)

    # ── Write version-hint.text for DuckDB iceberg_scan compatibility ──
    for tbl_name, tbl in [("entries", entries_table), ("features", features_table)]:
        _write_version_hint(tbl)
        eprint(f"  wrote version-hint.text for {tbl_name}")

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
