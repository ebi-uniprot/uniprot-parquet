#!/usr/bin/env python3
"""
Transform JSONL(.zst) → sorted Parquet tables (entries, features, xrefs, comments, references).

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
    references/references_00001.parquet, ...
    manifest.json

Usage:
    parquet_transform.py <input.jsonl.zst> \
        --outdir /path/to/lake \
        [--memory-limit 16GB] \
        [--batch-size 1000000]

Requires: duckdb, pyarrow
"""

import os
import re
import sys
import argparse
import time
import json
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


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
    con.sql(f"""
        COPY (SELECT * FROM {read_clause})
        TO '{safe_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)
    size_gb = os.path.getsize(staging_path) / (1024**3)
    eprint(f"  Staged in {time.time()-t0:.1f}s ({size_gb:.1f} GB)")
    return f"read_parquet('{safe_path}')"


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
    -- All gene synonyms across all genes (searchable list)
    flatten(list_transform(
        COALESCE(e.genes, []),
        g -> list_transform(COALESCE(g.synonyms, []), s -> s.value)
    ))                                              AS gene_synonyms,
    e.proteinDescription.recommendedName.fullName.value AS protein_name,
    -- Alternative protein names (searchable list)
    list_transform(
        COALESCE(e.proteinDescription.alternativeNames, []),
        x -> x.fullName.value
    )                                               AS alt_protein_names,
    -- Precursor / Fragment flag (commonly used to filter incomplete sequences)
    e.proteinDescription.flag                       AS protein_flag,
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
    CAST(len(COALESCE(e."references", [])) AS INTEGER) AS reference_count,
    e.extraAttributes.uniParcId                     AS uniparc_id,

    -- Entry type (lossless round-trip — the boolean 'reviewed' loses the exact string)
    e.entryType                                     AS entry_type,
    -- Extra attributes (countByCommentType, countByFeatureType, uniParcId)
    e.extraAttributes                               AS extra_attributes,

    -- Full nested structures (preserved for power users)
    -- features, xrefs, comments, and references are in their own tables
    e.organism                                      AS organism,
    e.proteinDescription                            AS protein_desc,
    e.genes                                         AS genes,
    e.keywords                                      AS keywords,
    e.organismHosts                                 AS organism_hosts,
    e.geneLocations                                 AS gene_locations

FROM {read_clause} e
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
             THEN true ELSE false END                AS from_reviewed,
        e.organism.taxonId                           AS taxid,
        e.comments
    FROM {read_clause} e
    WHERE e.comments IS NOT NULL AND len(e.comments) > 0
) sub, LATERAL unnest(sub.comments)
ORDER BY sub.from_reviewed DESC, sub.taxid, sub.acc, unnest.commentType
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
    ("entries",    ENTRIES_SQL,    ["reviewed DESC", "taxid ASC", "acc ASC"]),
    ("features",   FEATURES_SQL,  ["from_reviewed DESC", "taxid ASC", "acc ASC", "start_pos ASC"]),
    ("xrefs",      XREFS_SQL,     ["from_reviewed DESC", "taxid ASC", "acc ASC", "database ASC"]),
    ("comments",   COMMENTS_SQL,  ["from_reviewed DESC", "taxid ASC", "acc ASC", "comment_type ASC"]),
    ("references", REFERENCES_SQL, ["from_reviewed DESC", "taxid ASC", "acc ASC", "citation_type ASC"]),
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
                "gene_name", "gene_synonyms", "protein_name", "alt_protein_names",
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
            "nested": ["comment"],
        },
    },
    "references": {
        "description": "One row per literature citation or submission.",
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

def stream_to_parquet(con, sql, table_dir, batch_size, label="table"):
    """Stream DuckDB result → Parquet files in bounded-memory batches.

    DuckDB executes the query lazily and yields Arrow record batches of
    `batch_size` rows.  Each batch is written as a zstd-compressed
    Parquet file.

    The Arrow schema is determined once by DuckDB before the first batch
    is yielded — all batches share the same schema.

    Memory model: only one Arrow batch is held at a time (plus whatever
    DuckDB needs for the ORDER BY spill).

    Returns (total_rows, file_list).
    """
    eprint(f"  Querying DuckDB for {label}...")
    t0 = time.time()

    result = con.sql(sql)
    reader = result.to_arrow_reader(batch_size=batch_size)

    os.makedirs(table_dir, exist_ok=True)

    total_rows = 0
    file_num = 0
    parquet_files = []
    arrow_schema = None

    for record_batch in reader:
        arrow_tbl = pa.Table.from_batches([record_batch])
        n = arrow_tbl.num_rows
        if n == 0:
            continue

        if arrow_schema is None:
            arrow_schema = arrow_tbl.schema

        file_num += 1
        total_rows += n

        filename = f"{label}_{file_num:05d}.parquet"
        parquet_path = os.path.join(table_dir, filename)
        t1 = time.time()
        pq.write_table(arrow_tbl, parquet_path, compression="zstd",
                       row_group_size=100_000)
        parquet_files.append(filename)
        eprint(
            f"    batch {file_num}: {n:,} rows "
            f"(total {total_rows:,}, "
            f"wrote in {time.time()-t1:.1f}s)"
        )

    elapsed = time.time() - t0
    eprint(f"  {label}: {total_rows:,} rows in {file_num} files ({elapsed:.1f}s)")
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
                    "(entries, features, xrefs, comments, references)"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "--schema", default=None,
        help="Committed DuckDB schema JSON (from schema_bootstrap.py). "
             "If omitted, DuckDB auto-detects.",
    )
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
    json_read_clause = build_read_clause(jsonl_path, args.schema)

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
    staging_dir = os.path.join(outdir, ".staging")
    staging_path = os.path.join(staging_dir, "staged.parquet")

    if tables_to_write:
        eprint("\n--- PARQUET STAGING ---")
        os.makedirs(staging_dir, exist_ok=True)
        read_clause = stage_to_parquet(con, json_read_clause, staging_path)
    else:
        eprint("\n--- PARQUET STAGING (skipped — all tables already written) ---")
        read_clause = json_read_clause

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
            sql = sql_template.format(read_clause=read_clause)
            row_count, files, arrow_schema = stream_to_parquet(
                con, sql, table_dir, args.batch_size, label=name
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

    # ── Clean up staging file ──
    if os.path.exists(staging_path):
        os.remove(staging_path)
        eprint(f"\n  Removed staging file: {staging_path}")
    if os.path.isdir(staging_dir) and not os.listdir(staging_dir):
        os.rmdir(staging_dir)

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


if __name__ == "__main__":
    main()
