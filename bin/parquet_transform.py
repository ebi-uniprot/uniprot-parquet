#!/usr/bin/env python3
"""
Transform JSONL(.zst) → sorted Parquet tables (entries, features, xrefs, comments, publications).

Architecture:
  - The JSONL is staged to a Parquet file once (single JSON parse), then all
    five table queries read from Parquet instead of re-parsing JSON.  This
    roughly halves the total transform time at production scale.
  - DuckDB performs all SQL transformations (flattening, unnesting, sorting).
  - DuckDB streams Arrow record batches (bounded memory, never materialises
    the full dataset).
  - PyArrow writes each batch as a zstd-compressed Parquet file.
  - A manifest.json records file lists, row counts, schemas, and sort orders.
  - The staging Parquet is cleaned up after all tables are written.

Memory model:
  At any moment we hold at most --batch-size rows in Arrow memory.  DuckDB
  may spill to disk during the ORDER BY, but the Arrow → Parquet path stays
  bounded regardless of dataset size.

Output layout:
  <outdir>/
    entries/entries_00001.parquet, ...
    features/features_00001.parquet, ...
    xrefs/xrefs_00001.parquet, ...
    comments/comments_00001.parquet, ...
    publications/publications_00001.parquet, ...
    manifest.json

Schema is inferred automatically from the data via DuckDB's read_json_auto.

Usage:
    parquet_transform.py <input.jsonl.zst> \
        --outdir /path/to/lake \
        [--memory-limit 16GB] \
        [--batch-size 1000000]

Requires: duckdb, pyarrow
"""

import os
import sys
import argparse
import time
import json
import shutil
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def build_sorting_columns(sort_order, arrow_schema):
    """Convert sort order strings to PyArrow SortingColumn objects.

    Args:
        sort_order: List of strings like ["reviewed DESC", "taxid ASC", "acc ASC"]
        arrow_schema: PyArrow schema to map column names to indices

    Returns:
        List of pq.SortingColumn objects, or None if PyArrow < 16.0 or parse fails.
        Falls back to None (no sorting metadata) if SortingColumn is not available.
    """
    if not sort_order:
        return None

    try:
        # Check if pq.SortingColumn exists (PyArrow 16.0+)
        if not hasattr(pq, 'SortingColumn'):
            eprint("  WARNING: PyArrow SortingColumn not available (requires PyArrow 16.0+). Skipping sort metadata.")
            return None

        # Build a map of column name -> index
        col_name_to_idx = {field.name: i for i, field in enumerate(arrow_schema)}

        sorting_columns = []
        for spec in sort_order:
            parts = spec.strip().split()
            if len(parts) != 2:
                eprint(f"  WARNING: Invalid sort spec '{spec}' (expected 'column ASC/DESC'). Skipping.")
                continue

            col_name, direction = parts
            if col_name not in col_name_to_idx:
                eprint(f"  WARNING: Column '{col_name}' not found in schema. Skipping from sort order.")
                continue

            col_idx = col_name_to_idx[col_name]
            descending = direction.upper() == "DESC"
            sorting_columns.append(pq.SortingColumn(col_idx, descending=descending, nulls_first=False))

        return sorting_columns if sorting_columns else None

    except Exception as e:
        eprint(f"  WARNING: Failed to build sorting columns: {e}. Continuing without sort metadata.")
        return None


# ─── DuckDB helpers ──────────────────────────────────────────────────────

def _sql_escape(path: str) -> str:
    """Escape a file path for use inside a DuckDB SQL string literal."""
    return path.replace("'", "''")


def build_read_clause(jsonl_path: str) -> str:
    """Build the DuckDB read_json_auto SQL fragment.

    DuckDB infers column names and types from the data itself.
    This is the right approach for UniProtKB: the data is a JSON dump
    from production, so whatever schema it has is what we use.

    sample_size=-1 forces DuckDB to scan the entire file for schema
    inference, ensuring rare nested struct fields (e.g. a ligand sub-struct
    that only appears in a handful of entries) are never silently dropped.
    """
    safe_path = _sql_escape(jsonl_path)
    return (
        # sample_size=-1: scan all records to infer schema (no sampling).
        # maximum_object_size=512 MB: some UniProtKB JSON entries exceed
        # DuckDB's 16 MB default (e.g. titin has >34k residues + huge
        # annotation arrays).  512 MB covers the largest known entry.
        f"read_json_auto('{safe_path}', format='newline_delimited', "
        f"sample_size=-1, maximum_object_size=536870912)"
    )


def stage_to_parquet(con, read_clause: str, staging_path: str) -> str:
    """Convert JSONL to a Parquet staging file for faster repeated reads.

    Parses the JSON once and writes a zstd-compressed Parquet file.
    Returns a read_parquet() clause that can replace the original
    read_json() clause in all SQL templates.

    At production scale (~250M entries, 160GB compressed JSON), this
    trades one JSON parse (~2-4h) for five fast Parquet reads instead
    of five JSON parses.  Net saving: ~40-50% of total transform time.
    """
    safe_path = _sql_escape(staging_path)
    eprint(f"  Staging JSONL → Parquet: {staging_path}")
    t0 = time.time()
    # ROW_GROUP_SIZE 100k: keeps per-group memory bounded while giving
    # DuckDB enough rows for efficient predicate pushdown and min/max stats.
    con.sql(f"""
        COPY (SELECT * FROM {read_clause})
        TO '{safe_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)
    size_gb = os.path.getsize(staging_path) / (1024**3)
    eprint(f"  Staged in {time.time()-t0:.1f}s ({size_gb:.1f} GB)")
    return f"read_parquet('{safe_path}')"


def get_available_columns(con, read_clause: str) -> set[str]:
    """Discover which top-level columns exist in the data source.

    Used to handle optional fields that may not appear in all datasets
    (e.g. organismHosts only exists in virus entries).  SQL templates
    use NULL for columns that don't exist in the current dataset.
    """
    result = con.sql(f"DESCRIBE SELECT * FROM {read_clause}").fetchall()
    return {row[0] for row in result}


def get_struct_subfields(con, read_clause: str, column: str) -> set[str]:
    """Discover sub-field names inside a top-level STRUCT column.

    Used to handle optional nested fields that may not appear in all
    datasets (e.g. proteinDescription.submittedNames only exists for
    TrEMBL entries, not Swiss-Prot).

    LIMIT 1 is correct here because this reads from the staged Parquet,
    which has a uniform struct schema (DuckDB infers it with sample_size=-1
    during staging, so the schema is the union of all rows).
    """
    try:
        result = con.sql(
            f"SELECT unnest(struct_keys(t.{column})) AS k "
            f"FROM (SELECT * FROM {read_clause} LIMIT 1) t"
        ).fetchall()
        return {row[0] for row in result}
    except Exception:
        return set()


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

# Optional fields that may not appear in all UniProtKB subsets.
# (e.g. organismHosts only exists in virus/parasite entries,
#  geneLocations is rare in some organisms.)
# The SQL generator substitutes NULL for any that are absent.
_OPTIONAL_ENTRY_FIELDS = {"organismHosts", "geneLocations"}


def _build_entries_sql(available_cols: set[str], pd_subfields: set[str] | None = None) -> str:
    """Build the entries SQL with NULLs for any missing optional columns.

    Parameters
    ----------
    available_cols : set[str]
        Top-level column names present in the data.
    pd_subfields : set[str], optional
        Sub-field names inside proteinDescription (e.g. submittedNames).
        Used to conditionally include nested fields that may not exist
        in all UniProtKB subsets.
    """
    if pd_subfields is None:
        pd_subfields = set()

    organism_hosts = (
        "e.organismHosts" if "organismHosts" in available_cols else "NULL"
    )
    gene_locations = (
        "e.geneLocations" if "geneLocations" in available_cols else "NULL"
    )

    # EC numbers: extract from recommended, alternative, and (if present) submitted names.
    # submittedNames only exists in TrEMBL entries; Swiss-Prot-only datasets lack it.
    ec_parts = [
        "list_transform(COALESCE(e.proteinDescription.recommendedName.ecNumbers, []), x -> x.value)",
        """flatten(list_transform(
            COALESCE(e.proteinDescription.alternativeNames, []),
            n -> list_transform(COALESCE(n.ecNumbers, []), x -> x.value)
        ))""",
    ]
    if "submittedNames" in pd_subfields:
        ec_parts.append("""flatten(list_transform(
            COALESCE(e.proteinDescription.submittedNames, []),
            n -> list_transform(COALESCE(n.ecNumbers, []), x -> x.value)
        ))""")
    ec_numbers_expr = "list_distinct(flatten([\n        " + ",\n        ".join(ec_parts) + "\n    ]))"

    # protein_name: COALESCE across recommendedName → submittedNames[1] → alternativeNames[1].
    # Swiss-Prot entries have recommendedName; TrEMBL entries typically only have submittedNames.
    # Without this fallback, protein_name is NULL for >99% of the lake (TrEMBL dominates).
    protein_name_parts = ["e.proteinDescription.recommendedName.fullName.value"]
    if "submittedNames" in pd_subfields:
        protein_name_parts.append("e.proteinDescription.submittedNames[1].fullName.value")
    protein_name_parts.append("(list_extract(COALESCE(e.proteinDescription.alternativeNames, []), 1)).fullName.value")
    protein_name_expr = "COALESCE(" + ", ".join(protein_name_parts) + ")"

    return f"""
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
    list_transform(
        COALESCE(e.genes, []),
        g -> g.geneName.value
    )                                               AS gene_names,
    -- All gene synonyms across all genes (searchable list)
    flatten(list_transform(
        COALESCE(e.genes, []),
        g -> list_transform(COALESCE(g.synonyms, []), s -> s.value)
    ))                                              AS gene_synonyms,
    {protein_name_expr}                                AS protein_name,
    -- Alternative protein names (searchable list)
    list_transform(
        COALESCE(e.proteinDescription.alternativeNames, []),
        x -> x.fullName.value
    )                                               AS alt_protein_names,
    -- Precursor / Fragment flag (commonly used to filter incomplete sequences)
    e.proteinDescription.flag                       AS protein_flag,
    -- EC numbers: extract from all naming blocks (recommended, alternative, submitted)
    {ec_numbers_expr}                               AS ec_numbers,
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
    CAST(len(COALESCE(e."references", [])) AS INTEGER) AS reference_count,
    e.extraAttributes.uniParcId                     AS uniparc_id,

    -- Entry type (lossless round-trip — the boolean 'reviewed' loses the exact string)
    e.entryType                                     AS entry_type,
    -- Extra attributes (countByCommentType, countByFeatureType, uniParcId)
    e.extraAttributes                               AS extra_attributes,

    -- Full nested structures (preserved for power users)
    -- features, xrefs, comments, and publications are in their own tables
    e.organism                                      AS organism,
    e.proteinDescription                            AS protein_desc,
    e.genes                                         AS genes,
    e.keywords                                      AS keywords,
    {organism_hosts}                                 AS organism_hosts,
    {gene_locations}                                 AS gene_locations

FROM {{read_clause}} e
ORDER BY reviewed DESC, e.organism.taxonId, e.primaryAccession
"""


FEATURES_SQL = """
SELECT
    sub.acc,
    sub.from_reviewed,
    sub.taxid,
    sub.organism_name,
    sub.seq_length,

    -- Flattened convenience columns (fast querying)
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
    unnest.ligand.note                              AS ligand_note,

    -- Full original nested struct (lossless round-trip)
    unnest                                          AS feature

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        CASE WHEN e.entryType LIKE '%Swiss-Prot%'
             THEN true ELSE false END                AS from_reviewed,
        e.organism.taxonId                           AS taxid,
        e.organism.scientificName                    AS organism_name,
        CAST(e.sequence.length AS INTEGER)           AS seq_length,
        e.features
    FROM {read_clause} e
    WHERE e.features IS NOT NULL AND len(e.features) > 0
) sub, LATERAL unnest(sub.features)
ORDER BY sub.from_reviewed DESC, sub.taxid, sub.acc, unnest.location.start.value
"""


XREFS_SQL = """
SELECT
    sub.acc,
    sub.from_reviewed,
    sub.taxid,

    -- Flattened convenience columns (fast querying)
    unnest.database                                 AS database,
    unnest.id                                       AS id,
    unnest.properties                               AS properties,
    unnest.isoformId                                AS isoform_id,
    unnest.evidences                                AS evidences

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        CASE WHEN e.entryType LIKE '%Swiss-Prot%'
             THEN true ELSE false END                AS from_reviewed,
        e.organism.taxonId                           AS taxid,
        e.uniProtKBCrossReferences
    FROM {read_clause} e
    WHERE e.uniProtKBCrossReferences IS NOT NULL
      AND len(e.uniProtKBCrossReferences) > 0
) sub, LATERAL unnest(sub.uniProtKBCrossReferences)
ORDER BY sub.from_reviewed DESC, sub.taxid, sub.acc, unnest.database
"""


COMMENTS_SQL = """
SELECT
    sub.acc,
    sub.from_reviewed,
    sub.taxid,

    -- Strip embedded double quotes from commentType (JSON scalar → VARCHAR)
    -- so users can write comment_type = 'FUNCTION' instead of '"FUNCTION"'
    trim('"' FROM unnest.commentType::VARCHAR)      AS comment_type,
    -- Concatenate all text values (multi-paragraph comments have texts[1..N])
    -- texts is JSON, so extract all values and join with double newline
    array_to_string(
        from_json(unnest.texts->'$[*].value', '["VARCHAR"]'),
        chr(10) || chr(10)
    )                                               AS text_value,
    -- Full comment as JSON (lossless — preserves original structure).
    -- Stored as VARCHAR in Parquet; the comments view casts to JSON
    -- so users get ->> operators without explicit casting.
    unnest::JSON                                    AS comment

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        CASE WHEN e.entryType LIKE '%Swiss-Prot%'
             THEN true ELSE false END                AS from_reviewed,
        e.organism.taxonId                           AS taxid,
        e.comments
    FROM {read_clause} e
    WHERE e.comments IS NOT NULL AND len(e.comments) > 0
) sub, LATERAL unnest(sub.comments)
ORDER BY sub.from_reviewed DESC, sub.taxid, sub.acc,
         trim('"' FROM unnest.commentType::VARCHAR)
"""


REFERENCES_SQL = """
SELECT
    sub.acc,
    sub.from_reviewed,
    sub.taxid,

    -- Flattened convenience columns (fast querying)
    CAST(unnest.referenceNumber AS INTEGER)          AS reference_number,
    unnest.citation.citationType                    AS citation_type,
    unnest.citation.id                              AS citation_id,
    unnest.citation.title                           AS title,
    unnest.citation.authors                         AS authors,
    unnest.citation.authoringGroup                  AS authoring_group,
    unnest.citation.publicationDate                 AS publication_date,
    unnest.citation.journal                         AS journal,
    unnest.citation.volume                          AS volume,
    unnest.citation.firstPage                       AS first_page,
    unnest.citation.lastPage                        AS last_page,
    unnest.citation.submissionDatabase              AS submission_database,
    unnest.citation.citationCrossReferences         AS citation_xrefs,
    unnest.referencePositions                       AS reference_positions,
    unnest.referenceComments                        AS reference_comments,
    unnest.evidences                                AS evidences,

    -- Full original nested struct (lossless round-trip)
    unnest                                          AS reference

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        CASE WHEN e.entryType LIKE '%Swiss-Prot%'
             THEN true ELSE false END                AS from_reviewed,
        e.organism.taxonId                           AS taxid,
        e."references"
    FROM {read_clause} e
    WHERE e."references" IS NOT NULL AND len(e."references") > 0
) sub, LATERAL unnest(sub."references")
ORDER BY sub.from_reviewed DESC, sub.taxid, sub.acc, unnest.citation.citationType
"""


# ─── Table definitions ──────────────────────────────────────────────────

TABLE_DEFS = [
    ("entries",    None,            ["reviewed DESC", "taxid ASC", "acc ASC"]),  # SQL built dynamically
    ("features",   FEATURES_SQL,   ["from_reviewed DESC", "taxid ASC", "acc ASC", "start_pos ASC"]),
    ("xrefs",      XREFS_SQL,      ["from_reviewed DESC", "taxid ASC", "acc ASC", "database ASC"]),
    ("comments",   COMMENTS_SQL,   ["from_reviewed DESC", "taxid ASC", "acc ASC", "comment_type ASC"]),
    ("publications", REFERENCES_SQL, ["from_reviewed DESC", "taxid ASC", "acc ASC", "citation_type ASC"]),
]


# ─── Semantic metadata (embedded in manifest.json for agents/tools) ─────
# This is the metadata that Parquet files *don't* encode: what each table
# means, how they relate, and which columns are the analyst-friendly
# shortcuts vs the full nested structures for lossless round-trip.

TABLE_META = {
    "entries": {
        "description": "One row per protein. Primary table for most queries.",
        "primary_key": ["acc"],
        "foreign_keys": {},
        "columns": {
            "convenience": [
                "acc", "id", "reviewed", "secondary_accs",
                "taxid", "organism_name", "organism_common", "lineage",
                "gene_names", "gene_synonyms", "protein_name", "alt_protein_names",
                "protein_flag", "ec_numbers", "protein_existence", "annotation_score",
                "sequence", "seq_length", "seq_mass", "seq_md5", "seq_crc64",
                "go_ids", "xref_dbs", "keyword_ids", "keyword_names",
                "first_public", "last_modified", "last_seq_modified",
                "entry_version", "seq_version",
                "feature_count", "xref_count", "comment_count", "reference_count",
                "uniparc_id", "entry_type", "extra_attributes",
            ],
            "nested": [
                "organism", "protein_desc", "genes", "keywords",
                "organism_hosts", "gene_locations",
            ],
        },
    },
    "features": {
        "description": "One row per positional annotation (domain, signal, transmembrane, etc). Sorted by start_pos within each protein.",
        "primary_key": [],
        "foreign_keys": {"acc": "entries.acc", "taxid": "entries.taxid"},
        "columns": {
            "convenience": [
                "acc", "from_reviewed", "taxid", "organism_name", "seq_length",
                "type", "start_pos", "end_pos", "start_modifier", "end_modifier",
                "description", "feature_id", "evidence_codes",
                "original_sequence", "alternative_sequences",
                "ligand_name", "ligand_id", "ligand_label", "ligand_note",
            ],
            "nested": ["feature"],
        },
    },
    "xrefs": {
        "description": "One row per cross-reference to an external database (PDB, Ensembl, GO, InterPro, etc).",
        "primary_key": [],
        "foreign_keys": {"acc": "entries.acc", "taxid": "entries.taxid"},
        "columns": {
            "convenience": [
                "acc", "from_reviewed", "taxid",
                "database", "id", "properties", "isoform_id", "evidences",
            ],
            "nested": [],
        },
    },
    "comments": {
        "description": "One row per comment annotation (function, subcellular location, disease, etc).",
        "primary_key": [],
        "foreign_keys": {"acc": "entries.acc", "taxid": "entries.taxid"},
        "columns": {
            "convenience": [
                "acc", "from_reviewed", "taxid",
                "comment_type", "text_value",
            ],
            "nested": ["comment"],  # JSON — use comment->>'$.key' to extract
        },
    },
    "publications": {
        "description": "One row per literature citation or submission (derived from UniProtKB entry.references).",
        "primary_key": [],
        "foreign_keys": {"acc": "entries.acc", "taxid": "entries.taxid"},
        "columns": {
            "convenience": [
                "acc", "from_reviewed", "taxid",
                "reference_number", "citation_type", "citation_id", "title",
                "authors", "authoring_group", "publication_date",
                "journal", "volume", "first_page", "last_page",
                "submission_database", "citation_xrefs",
                "reference_positions", "reference_comments", "evidences",
            ],
            "nested": ["reference"],
        },
    },
}


# ─── Core pipeline ──────────────────────────────────────────────────────

def stream_to_parquet(con, sql, table_dir, batch_size, label="table", sort_order=None):
    """Stream DuckDB result → Parquet files in bounded-memory batches.

    DuckDB executes the query lazily and yields Arrow record batches of
    `batch_size` rows. Multiple batches are accumulated into larger files
    targeting ~256MB per file using ParquetWriter.

    The Arrow schema is determined once by DuckDB before the first batch
    is yielded — all batches share the same schema.

    Memory model: only one Arrow batch is held at a time (plus whatever
    DuckDB needs for the ORDER BY spill).

    Atomicity: Files are written to a temporary .tmp/ directory and moved
    to the final location after all batches are successfully written.

    Args:
        con: DuckDB connection
        sql: SQL query to execute
        table_dir: Output directory for Parquet files
        batch_size: Rows per Arrow batch
        label: Table name (for logging)
        sort_order: List of sort order strings (e.g. ["reviewed DESC", "taxid ASC"])
                   to embed in Parquet footer metadata (PyArrow 16.0+).

    Returns (total_rows, file_list, arrow_schema).
    """
    TARGET_FILE_BYTES = 256 * 1024 * 1024  # ~256 MB per file

    eprint(f"  Querying DuckDB for {label}...")
    t0 = time.time()

    result = con.sql(sql)
    reader = result.to_arrow_reader(batch_size=batch_size)

    # Create main output directory and temporary directory
    os.makedirs(table_dir, exist_ok=True)
    tmp_dir = os.path.join(table_dir, ".tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    total_rows = 0
    file_num = 0
    parquet_files = []
    arrow_schema = None
    writer = None
    current_file_num = 0
    current_file_path = None
    sorting_columns = None

    try:
        for record_batch in reader:
            arrow_tbl = pa.Table.from_batches([record_batch])
            n = arrow_tbl.num_rows
            if n == 0:
                continue

            if arrow_schema is None:
                arrow_schema = arrow_tbl.schema
                # Build sorting columns once schema is available
                if sort_order:
                    sorting_columns = build_sorting_columns(sort_order, arrow_schema)

            total_rows += n

            # Initialize writer for first batch or if we haven't started yet
            if writer is None:
                current_file_num += 1
                filename = f"{label}_{current_file_num:05d}.parquet"
                current_file_path = os.path.join(tmp_dir, filename)
                writer_kwargs = {
                    "compression": "zstd",
                }
                if sorting_columns:
                    writer_kwargs["sorting_columns"] = sorting_columns
                writer = pq.ParquetWriter(
                    current_file_path,
                    arrow_schema,
                    **writer_kwargs
                )

            # Write batch to current file (row_group_size controls Parquet row group boundaries)
            writer.write_table(arrow_tbl, row_group_size=100_000)

            # Check current file size and close if it exceeds target
            current_size = os.path.getsize(current_file_path)
            if current_size >= TARGET_FILE_BYTES:
                writer.close()
                parquet_files.append(os.path.basename(current_file_path))
                writer = None

            elapsed = time.time() - t0
            rate = total_rows / elapsed if elapsed > 0 else 0
            eprint(
                f"    batch {current_file_num}: {n:,} rows "
                f"(total {total_rows:,}, "
                f"{elapsed:.0f}s elapsed, "
                f"{rate:,.0f} rows/s)"
            )

        # Close final writer if it's still open
        if writer is not None:
            writer.close()
            parquet_files.append(os.path.basename(current_file_path))

        # Move all files from .tmp/ to final location atomically
        for filename in parquet_files:
            tmp_path = os.path.join(tmp_dir, filename)
            final_path = os.path.join(table_dir, filename)
            shutil.move(tmp_path, final_path)

        # Clean up empty .tmp/ directory
        if os.path.isdir(tmp_dir) and not os.listdir(tmp_dir):
            os.rmdir(tmp_dir)

    except Exception:
        # On error, close writer and leave .tmp/ for cleanup by caller
        if writer is not None:
            writer.close()
        raise

    elapsed = time.time() - t0
    eprint(f"  {label}: {total_rows:,} rows in {len(parquet_files)} files ({elapsed:.1f}s)")
    return total_rows, parquet_files, arrow_schema


def _schema_to_dict(arrow_schema):
    """Convert Arrow schema to a JSON-serializable dict for the manifest."""
    columns = []
    for field in arrow_schema:
        columns.append({
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        })
    return columns


# ─── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Transform UniProtKB JSONL → sorted Parquet tables "
                    "(entries, features, xrefs, comments, publications)"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "--outdir", default="./lake",
        help="Output directory for Parquet tables and manifest",
    )
    parser.add_argument("--memory-limit", default="16GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    parser.add_argument(
        "--batch-size", type=int, default=1_000_000,
        help="Rows per Arrow batch (controls peak memory; default 1M)",
    )
    parser.add_argument(
        "--release", default=None,
        help="Release label (e.g. 2026_01). Recorded in manifest.",
    )
    parser.add_argument(
        "--temp-dir", default=None,
        help="DuckDB spill directory for ORDER BY temp files. "
             "Defaults to $TMPDIR or /tmp/duckdb_temp.",
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip tables whose output directory already contains "
             "Parquet files. Enables resume after OOM.",
    )
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    if not os.path.exists(jsonl_path):
        eprint(f"ERROR: Input file not found: {jsonl_path}")
        sys.exit(1)
    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    eprint("=" * 60)
    eprint("UniProtKB → Parquet Data Lake")
    eprint("=" * 60)
    eprint(f"  Input:     {jsonl_path}")
    eprint(f"  Output:    {outdir}")
    eprint(f"  Memory:    {args.memory_limit}")
    eprint()

    # ── Init DuckDB ──
    con = init_duckdb(args.memory_limit, args.threads, args.temp_dir)
    staging_dir = os.path.join(outdir, ".staging")
    staging_path = os.path.join(staging_dir, "staged.parquet")

    try:
        json_read_clause = build_read_clause(jsonl_path)

        # ── Determine which tables to write ──
        skip_set = set()
        if args.skip_existing:
            for name, _, _ in TABLE_DEFS:
                table_dir = os.path.join(outdir, name)
                if os.path.isdir(table_dir):
                    existing = [f for f in os.listdir(table_dir) if f.endswith(".parquet")]
                    if existing:
                        skip_set.add(name)
                        eprint(f"  SKIP {name} (already has {len(existing)} Parquet files, --skip-existing)")

        tables_to_write = {name for name, _, _ in TABLE_DEFS} - skip_set

        # ── Stage JSONL → Parquet (parse JSON once, read Parquet 5× faster) ──
        if tables_to_write:
            eprint("\n--- PARQUET STAGING ---")
            os.makedirs(staging_dir, exist_ok=True)
            read_clause = stage_to_parquet(con, json_read_clause, staging_path)
        else:
            eprint("\n--- PARQUET STAGING (skipped — all tables already written) ---")
            read_clause = json_read_clause

        # ── Discover available columns (for optional field handling) ──
        available_cols = get_available_columns(con, read_clause)
        missing_optional = _OPTIONAL_ENTRY_FIELDS - available_cols
        if missing_optional:
            eprint(f"  Optional fields not in data (will be NULL): {', '.join(sorted(missing_optional))}")

        # Discover proteinDescription sub-fields (submittedNames is absent in Swiss-Prot-only datasets)
        pd_subfields = get_struct_subfields(con, read_clause, "proteinDescription") if "proteinDescription" in available_cols else set()
        if "submittedNames" not in pd_subfields:
            eprint("  Note: proteinDescription.submittedNames absent (normal for Swiss-Prot-only data)")

        # ── Write tables ──
        t_total = time.time()
        manifest_tables = {}

        for name, sql_template, sort_order in TABLE_DEFS:
            eprint(f"\n--- {name.upper()} ---")
            table_dir = os.path.join(outdir, name)

            meta = TABLE_META.get(name, {})

            if name in skip_set:
                existing = sorted(f for f in os.listdir(table_dir) if f.endswith(".parquet"))
                # Count rows from existing files
                row_count = 0
                for fname in existing:
                    fmeta = pq.read_metadata(os.path.join(table_dir, fname))
                    row_count += fmeta.num_rows
                eprint(f"  Skipped ({row_count:,} rows in {len(existing)} existing files)")
                # Read schema from first file
                schema = pq.read_schema(os.path.join(table_dir, existing[0]))
                manifest_tables[name] = {
                    "description": meta.get("description", ""),
                    "primary_key": meta.get("primary_key", []),
                    "foreign_keys": meta.get("foreign_keys", {}),
                    "files": existing,
                    "row_count": row_count,
                    "sort_order": sort_order,
                    "columns": _schema_to_dict(schema),
                    "column_categories": meta.get("columns", {}),
                }
            else:
                # Entries SQL is built dynamically to handle optional fields
                if sql_template is None:
                    sql_template = _build_entries_sql(available_cols, pd_subfields)
                sql = sql_template.format(read_clause=read_clause)
                row_count, files, arrow_schema = stream_to_parquet(
                    con, sql, table_dir, args.batch_size, label=name, sort_order=sort_order
                )
                manifest_tables[name] = {
                    "description": meta.get("description", ""),
                    "primary_key": meta.get("primary_key", []),
                    "foreign_keys": meta.get("foreign_keys", {}),
                    "files": files,
                    "row_count": row_count,
                    "sort_order": sort_order,
                    "columns": _schema_to_dict(arrow_schema) if arrow_schema else [],
                    "column_categories": meta.get("columns", {}),
                }

        # ── Write manifest.json ──
        manifest = {
            "format": "uniprot-parquet-lake",
            "version": 1,
            "release": args.release,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "tables": manifest_tables,
            "total_rows": sum(t["row_count"] for t in manifest_tables.values()),
        }
        manifest_path = os.path.join(outdir, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        eprint(f"  Wrote {manifest_path}")

        # ── Summary ──
        elapsed = time.time() - t_total
        eprint("\n" + "=" * 60)
        eprint(f"DONE in {elapsed:.1f}s")
        for name, _, _ in TABLE_DEFS:
            status = " (skipped)" if name in skip_set else ""
            eprint(f"  {name}: {manifest_tables[name]['row_count']:,} rows{status}")
        eprint(f"  Total: {manifest['total_rows']:,} rows")
        eprint("=" * 60)

    finally:
        # Clean up staging file (also on crash, to avoid leaking 160GB+ to disk)
        if os.path.exists(staging_path):
            os.remove(staging_path)
            eprint(f"\n  Removed staging file: {staging_path}")
        if os.path.isdir(staging_dir) and not os.listdir(staging_dir):
            os.rmdir(staging_dir)
        con.close()


if __name__ == "__main__":
    main()
