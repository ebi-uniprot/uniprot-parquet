#!/usr/bin/env python3
"""
Transform JSONL(.zst) → Apache Iceberg tables (entries, features, xrefs, comments).

Architecture:
  - DuckDB reads the JSONL and performs all SQL transformations (flattening,
    unnesting, sorting) — it's purpose-built for this.
  - Iceberg schemas are inferred cheaply at startup (LIMIT 1, no ORDER BY) —
    sub-second even on 180GB files.  The schemas are deterministically derived
    from schema.json + the SQL transforms, so no need to persist them separately.
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
        [--namespace uniprotkb] \
        [--memory-limit 16GB] \
        [--batch-size 1000000]

Requires: duckdb, pyiceberg[pyarrow,sql-sqlite], pyarrow
"""

import os
import re
import sys
import argparse
import time
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq  # used by stream_to_iceberg

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection
from pyiceberg.transforms import IdentityTransform


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# ─── DuckDB helpers ──────────────────────────────────────────────────────

def _sql_escape(path: str) -> str:
    """Escape a file path for use inside a DuckDB SQL string literal."""
    return path.replace("'", "''")


def build_read_clause(jsonl_path: str, schema_path: str | None) -> str:
    """Build the DuckDB read_json SQL fragment.

    If a committed schema.json is provided, use explicit column types
    for deterministic parsing.  Otherwise fall back to auto-detect.
    """
    safe_path = _sql_escape(jsonl_path)
    if schema_path and os.path.exists(schema_path):
        with open(schema_path) as f:
            schema = json.load(f)
        cols = ", ".join(f"'{name}': '{dtype}'" for name, dtype in schema.items())
        return (
            f"read_json('{safe_path}', format='newline_delimited', "
            f"maximum_object_size=536870912, columns={{{cols}}})"
        )
    else:
        return (
            f"read_json_auto('{safe_path}', format='newline_delimited', "
            f"maximum_object_size=536870912)"
        )


def init_duckdb(memory_limit: str, threads: int | None,
                 temp_dir: str | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql(f"SET memory_limit='{memory_limit}'")
    if threads:
        con.sql(f"SET threads={threads}")
    # Priority: explicit --temp-dir > $TMPDIR (SLURM local scratch) > /tmp
    if temp_dir is None:
        temp_dir = os.environ.get("TMPDIR", "/tmp/duckdb_temp")
    spill_dir = os.path.join(temp_dir, "duckdb_spill")
    os.makedirs(spill_dir, exist_ok=True)
    con.sql(f"SET temp_directory='{_sql_escape(spill_dir)}'")
    eprint(f"  DuckDB temp directory: {spill_dir}")
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
    -- features, xrefs, and comments are in their own tables
    e.organism                                      AS organism,
    e.proteinDescription                            AS protein_desc,
    e.genes                                         AS genes,
    e.keywords                                      AS keywords,
    e.references                                    AS "references",
    e.organismHosts                                 AS organism_hosts,
    e.geneLocations                                 AS gene_locations

FROM {read_clause} e
ORDER BY reviewed DESC, e.organism.taxonId
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
ORDER BY sub.reviewed DESC, sub.taxid
"""


XREFS_SQL = """
SELECT
    sub.acc,
    sub.reviewed,
    sub.taxid,

    unnest.database                                 AS database,
    unnest.id                                       AS id,
    unnest.properties                               AS properties

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        CASE WHEN e.entryType LIKE '%Swiss-Prot%'
             THEN true ELSE false END                AS reviewed,
        e.organism.taxonId                           AS taxid,
        e.uniProtKBCrossReferences
    FROM {read_clause} e
    WHERE e.uniProtKBCrossReferences IS NOT NULL
      AND len(e.uniProtKBCrossReferences) > 0
) sub, LATERAL unnest(sub.uniProtKBCrossReferences)
ORDER BY sub.reviewed DESC, sub.taxid, unnest.database
"""


COMMENTS_SQL = """
SELECT
    sub.acc,
    sub.reviewed,
    sub.taxid,

    unnest.commentType                              AS comment_type,
    -- Extract text value from the common texts[] array (most comment types)
    CASE WHEN unnest.texts IS NOT NULL AND len(unnest.texts) > 0
         THEN unnest.texts[1].value
         ELSE NULL END                              AS text_value,
    -- Full comment structure for complex/polymorphic types
    unnest                                          AS comment

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        CASE WHEN e.entryType LIKE '%Swiss-Prot%'
             THEN true ELSE false END                AS reviewed,
        e.organism.taxonId                           AS taxid,
        e.comments
    FROM {read_clause} e
    WHERE e.comments IS NOT NULL AND len(e.comments) > 0
) sub, LATERAL unnest(sub.comments)
ORDER BY sub.reviewed DESC, sub.taxid, unnest.commentType
"""



# ─── Schema inference ────────────────────────────────────────────────────

def _inference_sql(sql: str) -> str:
    """Wrap a transform SQL for cheap schema inference.

    Strips ORDER BY (avoids a full-dataset sort) and adds LIMIT 1.
    DuckDB resolves the full output schema from just one row, so this
    turns an hours-long sort into a sub-second operation on large files.
    """
    # Remove the final ORDER BY clause (all our SQL templates end with one)
    stripped = re.sub(r'\s+ORDER\s+BY\s+[^)]+$', '', sql, flags=re.IGNORECASE)
    return f"SELECT * FROM ({stripped}) _infer LIMIT 1"


def infer_iceberg_schema(con, sql: str, label: str) -> Schema:
    """Infer the Iceberg schema from a DuckDB SQL query.

    Uses _inference_sql() to strip ORDER BY and add LIMIT 1, so the
    query touches only a handful of input rows instead of sorting the
    entire dataset.
    """
    eprint(f"  Inferring {label} schema from DuckDB...")
    t0 = time.time()

    cheap_sql = _inference_sql(sql)
    result = con.sql(cheap_sql)
    reader = result.to_arrow_reader(batch_size=1)
    try:
        batch = next(reader)
    except StopIteration:
        raise RuntimeError(
            f"Schema inference for '{label}' returned no rows. "
            f"The input JSONL may be empty or have no data for this table."
        )
    del reader, result

    raw_schema = _pyarrow_to_schema_without_ids(batch.schema)
    iceberg_schema = assign_fresh_schema_ids(raw_schema)

    eprint(f"    {len(iceberg_schema.fields)} columns inferred in {time.time()-t0:.1f}s")
    return iceberg_schema


def _sort_order_for(iceberg_schema: Schema, *column_specs) -> SortOrder:
    """Build a SortOrder on one or more columns by looking up their field IDs.

    Each spec is either a column name (defaults to ASC) or a tuple of
    (column_name, SortDirection).
    """
    fields = []
    for spec in column_specs:
        if isinstance(spec, tuple):
            name, direction = spec
        else:
            name, direction = spec, SortDirection.ASC
        fields.append(
            SortField(
                source_id=iceberg_schema.find_field(name).field_id,
                transform=IdentityTransform(),
                direction=direction,
            )
        )
    return SortOrder(*fields)



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


def _table_has_data(catalog, namespace, table_name) -> bool:
    """Check if a table exists and has at least one snapshot with rows."""
    full_name = f"{namespace}.{table_name}"
    try:
        table = catalog.load_table(full_name)
        snapshot = table.current_snapshot()
        if snapshot and snapshot.summary:
            count = int(snapshot.summary.get("total-records", 0))
            return count > 0
    except Exception:
        pass
    return False


def ensure_table(catalog, namespace, table_name, schema, sort_order):
    """Create a fresh table, dropping any existing one first.

    Each pipeline run is a full rebuild — appending to a pre-existing table
    would duplicate data.  We unconditionally drop+recreate to guarantee
    idempotency: running twice on the same warehouse produces identical results.
    """
    try:
        catalog.create_namespace(namespace)
        eprint(f"  Created namespace '{namespace}'")
    except NamespaceAlreadyExistsError:
        pass

    full_name = f"{namespace}.{table_name}"

    # Drop existing table to prevent data duplication on re-runs
    try:
        catalog.drop_table(full_name)
        eprint(f"  Dropped existing table '{full_name}'")
    except Exception:
        pass  # Table didn't exist — fine

    table = catalog.create_table(
        full_name,
        schema=schema,
        sort_order=sort_order,
    )
    eprint(f"  Created table '{full_name}'")
    return table


def _write_version_hint(table):
    """Write a version-hint.text file so DuckDB's iceberg_scan can find the latest metadata.

    ⚠️  FRAGILE INTEROP WORKAROUND — PyIceberg ↔ DuckDB
    PyIceberg names metadata files as "00002-<uuid>.metadata.json".
    DuckDB's iceberg_scan expects "v2.metadata.json" (or version-hint.text).
    This function bridges the gap by writing the hint file AND creating a
    symlink.  If either tool changes its naming convention, this will break.

    Do NOT introduce additional Iceberg writers (e.g. Spark) that won't
    maintain these hint files.  If you need multi-writer support, switch to
    a REST catalog which handles metadata resolution centrally.

    Tested against: PyIceberg 0.7-0.11, DuckDB 1.1+.
    """
    table_location = table.location().replace("file://", "")
    metadata_dir = os.path.join(table_location, "metadata")
    if not os.path.isdir(metadata_dir):
        return
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
    eprint(f"  Querying DuckDB for {label} (sorted by reviewed DESC, taxid)...")
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
        pq.write_table(arrow_tbl, parquet_path, compression="zstd",
                       row_group_size=100_000)
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


def write_xrefs(con, read_clause, table, batch_size):
    """Stream xrefs from DuckDB → Iceberg xrefs table."""
    sql = XREFS_SQL.format(read_clause=read_clause)
    return stream_to_iceberg(con, sql, table, batch_size, label="xrefs")


def write_comments(con, read_clause, table, batch_size):
    """Stream comments from DuckDB → Iceberg comments table."""
    sql = COMMENTS_SQL.format(read_clause=read_clause)
    return stream_to_iceberg(con, sql, table, batch_size, label="comments")



# ─── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Transform UniProtKB JSONL → Iceberg tables (entries, features, xrefs, comments)"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "--schema", default=None,
        help="Committed DuckDB schema JSON (from schema_bootstrap.py). "
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
        "--namespace", default="uniprotkb",
        help="Iceberg namespace (default: uniprotkb)",
    )
    parser.add_argument("--memory-limit", default="16GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    parser.add_argument(
        "--batch-size", type=int, default=1_000_000,
        help="Rows per Arrow batch (controls peak memory; default 1M)",
    )
    parser.add_argument(
        "--release", default=None,
        help="Release label (e.g. 2026_01). Stored as table property.",
    )
    parser.add_argument(
        "--temp-dir", default=None,
        help="DuckDB spill directory for ORDER BY temp files. "
             "Defaults to $TMPDIR or /tmp/duckdb_temp.",
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip tables that already have data in the catalog. "
             "Enables resume after OOM: on retry, previously written "
             "tables are skipped instead of dropped+recreated.",
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
    con = init_duckdb(args.memory_limit, args.threads, args.temp_dir)
    read_clause = build_read_clause(jsonl_path, args.schema)

    # ── Infer Iceberg schemas (cheap: LIMIT 1, no ORDER BY) ──
    eprint("--- SCHEMA INFERENCE ---")
    TABLE_DEFS = [
        ("entries",  ENTRIES_SQL,  (("reviewed", SortDirection.DESC), "taxid")),
        ("features", FEATURES_SQL, (("reviewed", SortDirection.DESC), "taxid")),
        ("xrefs",    XREFS_SQL,    (("reviewed", SortDirection.DESC), "taxid", "database")),
        ("comments", COMMENTS_SQL, (("reviewed", SortDirection.DESC), "taxid", "comment_type")),
    ]

    schemas = {}
    sort_orders = {}
    for name, sql_template, sort_cols in TABLE_DEFS:
        schemas[name] = infer_iceberg_schema(
            con, sql_template.format(read_clause=read_clause), name
        )
        sort_orders[name] = _sort_order_for(schemas[name], *sort_cols)

    # ── Init Iceberg catalog ──
    catalog = get_catalog(args.catalog_uri, warehouse)

    # With --skip-existing, check which tables already have data and skip them.
    # This enables resume after OOM: on retry, previously written tables
    # are left intact instead of being dropped+recreated.
    skip_set = set()
    if args.skip_existing:
        for name, _, _ in TABLE_DEFS:
            if _table_has_data(catalog, args.namespace, name):
                skip_set.add(name)
                eprint(f"  SKIP {name} (already has data, --skip-existing)")

    tables = {}
    for name, _, _ in TABLE_DEFS:
        if name in skip_set:
            tables[name] = catalog.load_table(f"{args.namespace}.{name}")
        else:
            tables[name] = ensure_table(
                catalog, args.namespace, name, schemas[name], sort_orders[name]
            )

    # Tag release as a table property if provided
    if args.release:
        for tbl in tables.values():
            with tbl.transaction() as txn:
                txn.set_properties({"uniprot.release": args.release})
        eprint(f"  Tagged tables with release={args.release}")

    # ── Write tables ──
    t_total = time.time()
    counts = {}

    table_writers = [
        ("entries",  write_entries),
        ("features", write_features),
        ("xrefs",    write_xrefs),
        ("comments", write_comments),
    ]

    for name, writer_fn in table_writers:
        eprint(f"\n--- {name.upper()} ---")
        if name in skip_set:
            prev = int(tables[name].current_snapshot().summary["total-records"])
            eprint(f"  Skipped ({prev:,} rows already written)")
            counts[name] = prev
        else:
            counts[name] = writer_fn(con, read_clause, tables[name], args.batch_size)

    # ── Write version-hint.text for DuckDB iceberg_scan compatibility ──
    for tbl_name, tbl in tables.items():
        _write_version_hint(tbl)
        eprint(f"  wrote version-hint.text for {tbl_name}")

    # ── Summary ──
    elapsed = time.time() - t_total
    eprint("\n" + "=" * 60)
    eprint(f"DONE in {elapsed:.1f}s")
    for name in ["entries", "features", "xrefs", "comments"]:
        status = " (skipped)" if name in skip_set else ""
        eprint(f"  {name}: {counts[name]:,} rows{status}")

    for name, tbl in tables.items():
        eprint(f"  {name} snapshot: {tbl.current_snapshot()}")
    eprint("=" * 60)


if __name__ == "__main__":
    main()
