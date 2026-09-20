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
    datapackage.json

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
import atexit
import tempfile
import time
import json
import shutil
import hashlib
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from frictionless import Package

# ─── Release constants ──────────────────────────────────────────────────
SCHEMA_VERSION = "1.0.0"          # public schema semver (plan H.4)
MANIFEST_FORMAT_VERSION = 2       # structure of manifest.json (plan §4.4)
DATA_LICENSE = "CC-BY-4.0"        # UniProtKB data; the pipeline code is MIT
GENERATOR = "uniprot-parquet parquet_transform.py"
LICENSE_TEXT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "CC-BY-4.0.txt")
LICENSE_HEADER = (
    "This directory contains data derived from UniProtKB (https://www.uniprot.org),\n"
    "licensed under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/).\n"
    "Pipeline code: MIT, see the repository (https://github.com/ebi-uniprot/uniprot-parquet).\n"
)


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


def _human_size(nbytes: int | float) -> str:
    """Return a human-readable size string (e.g. '2.4 MB', '158.3 GB')."""
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024 or unit == "TB":
            if unit == "bytes":
                n = int(nbytes)
                return f"{n} byte" if n == 1 else f"{n} bytes"
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"  # pragma: no cover


def stage_to_parquet(con, read_clause: str, staging_path: str) -> tuple[str, int]:
    """Convert JSONL to a Parquet staging file for faster repeated reads.

    Parses the JSON once and writes a zstd-compressed Parquet file.
    Returns a (read_parquet_clause, staged_bytes) tuple — the clause
    replaces the original read_json() in all SQL templates, and
    staged_bytes is the on-disk size for compression ratio reporting.

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
    staged_bytes = os.path.getsize(staging_path)
    eprint(f"  Staged in {time.time()-t0:.1f}s ({_human_size(staged_bytes)})")
    return f"read_parquet('{safe_path}')", staged_bytes


def discover_schema_paths(staging_path: str) -> set[str]:
    """Walk the staged Parquet schema and return all valid dotted field paths.

    This is the single source of truth for what fields exist in the dataset.
    SQL builders use ``path in schema_paths`` to decide whether to reference
    a nested field or emit NULL.

    The schema comes from PyArrow (which reads the Parquet footer), so it
    reflects the union schema that DuckDB inferred during staging with
    sample_size=-1.  This means every field that appears in *any* row is
    present — the schema is the superset of all rows.

    Examples of paths returned::

        "primaryAccession"
        "organism.taxonId"
        "proteinDescription.recommendedName.ecNumbers"
        "features.ligand.note"

    For LIST<STRUCT> columns (e.g. ``features``), the element struct's
    fields are listed directly under the list column name — just as DuckDB
    exposes them after ``LATERAL unnest()``.
    """
    schema = pq.read_schema(staging_path)

    paths: set[str] = set()

    def _walk(field_or_type, prefix: str) -> None:
        if isinstance(field_or_type, pa.Schema):
            for field in field_or_type:
                _walk(field, prefix)
            return
        if isinstance(field_or_type, pa.Field):
            path = f"{prefix}.{field_or_type.name}" if prefix else field_or_type.name
            paths.add(path)
            _walk(field_or_type.type, path)
            return
        t = field_or_type
        if isinstance(t, pa.StructType):
            for i in range(t.num_fields):
                _walk(t.field(i), prefix)
        elif isinstance(t, (pa.ListType, pa.LargeListType)):
            # List element fields are accessible after unnest — keep same prefix.
            _walk(t.value_type, prefix)

    _walk(schema, "")
    return paths


def init_duckdb(memory_limit: str, threads: int | None,
                 temp_dir: str | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql(f"SET memory_limit='{_sql_escape(memory_limit)}'")
    if threads:
        con.sql(f"SET threads={threads}")
    # Priority: explicit --temp-dir > $TMPDIR (SLURM local scratch) > /tmp
    if temp_dir is None:
        temp_dir = os.environ.get("TMPDIR", "/tmp/duckdb_temp")
    # Unique per process: concurrent jobs may share --temp-dir (e.g. the
    # documented /scratch/$USER/duckdb_tmp), and a shared spill path risks
    # collisions and cross-job cleanup.
    os.makedirs(temp_dir, exist_ok=True)
    spill_dir = tempfile.mkdtemp(prefix="duckdb_spill_", dir=temp_dir)
    atexit.register(shutil.rmtree, spill_dir, ignore_errors=True)
    con.sql(f"SET temp_directory='{_sql_escape(spill_dir)}'")
    eprint(f"  DuckDB temp directory: {spill_dir}")
    return con


# ─── SQL for each table ─────────────────────────────────────────────────

# ─── Declared DuckDB types for columns with an optional source path ─────
# Every ``NULL`` fallback in the SQL builders is typed from this dict so the
# column type never depends on which optional fields the input happened to
# contain (plan G.2).  Strings are DuckDB ``DESCRIBE`` output, verbatim, taken
# from a build where the field was present.  The validator's
# ``check_schema_types`` compares each built table against this dict.
COLUMN_TYPES: dict[tuple[str, str], str] = {
    ("entries", "organism_hosts"): "STRUCT(scientificName VARCHAR, commonName VARCHAR, taxonId BIGINT, synonyms VARCHAR[])[]",
    ("entries", "go_terms"): "STRUCT(id VARCHAR, aspect VARCHAR, term VARCHAR, evidence_type VARCHAR)[]",
    ("entries", "extra_attributes"): "STRUCT(countByCommentType MAP(VARCHAR, BIGINT), countByFeatureType MAP(VARCHAR, BIGINT), uniParcId VARCHAR)",
    ("entries", "gene_locations"): 'STRUCT(geneEncodingType VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR)[]',
    # Scalar / list columns whose source a subset may lack altogether (a
    # TrEMBL-only slice has no recommendedName; an unnamed-organism slice no
    # commonName or lineage; a subset of entries without genes or secondaries).
    ("entries", "protein_name"): "VARCHAR",
    ("entries", "gene_names"): "VARCHAR[]",
    ("entries", "gene_name"): "VARCHAR",
    ("entries", "gene_synonyms"): "VARCHAR[]",
    ("entries", "alt_protein_names"): "VARCHAR[]",
    ("entries", "protein_flag"): "VARCHAR",
    ("entries", "secondary_accs"): "VARCHAR[]",
    ("entries", "organism_common"): "VARCHAR",
    ("entries", "lineage"): "VARCHAR[]",
    ("entries", "uniparc_id"): "VARCHAR",
    ("features", "description"): "VARCHAR",
    ("features", "start_modifier"): "VARCHAR",
    ("features", "end_modifier"): "VARCHAR",
    ("publications", "publication_date"): "VARCHAR",
    ("features", "feature_id"): "VARCHAR",
    ("features", "original_sequence"): "VARCHAR",
    ("features", "alternative_sequences"): "VARCHAR[]",
    ("features", "ligand_name"): "VARCHAR",
    ("features", "ligand_id"): "VARCHAR",
    ("features", "ligand_label"): "VARCHAR",
    ("features", "ligand_note"): "VARCHAR",
    ("xrefs", "isoform_id"): "VARCHAR",
    ("xrefs", "evidences"): 'STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[]',
    ("xrefs", "properties"): 'STRUCT("key" VARCHAR, "value" VARCHAR)[]',
    ("publications", "title"): "VARCHAR",
    ("publications", "authors"): "VARCHAR[]",
    ("publications", "authoring_group"): "VARCHAR[]",
    ("publications", "journal"): "VARCHAR",
    ("publications", "volume"): "VARCHAR",
    ("publications", "first_page"): "VARCHAR",
    ("publications", "last_page"): "VARCHAR",
    ("publications", "submission_database"): "VARCHAR",
    ("publications", "citation_xrefs"): 'STRUCT("database" VARCHAR, id VARCHAR)[]',
    ("publications", "reference_positions"): "VARCHAR[]",
    ("publications", "reference_comments"): 'STRUCT("value" VARCHAR, "type" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[]',
    ("publications", "evidences"): 'STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[]',
    # Residual structs (plan A13): "everything else" buckets, so their declared
    # type is the union seen on the stress fixture.  A full build whose input
    # has a sub-field these do not list stops at check_declared_types(); extend
    # the type there (DuckDB DESCRIBE output, verbatim) and rebuild.
    ('entries', 'organism_residual'): 'STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], synonyms VARCHAR[])',
    ('entries', 'protein_desc_residual'): 'STRUCT(allergenName STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[]), alternativeNames STRUCT(fullName STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[]), shortNames STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[], ecNumbers STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR)[])[], cdAntigenNames STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[], contains STRUCT(recommendedName STRUCT(fullName STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[]), shortNames STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[], ecNumbers STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR)[]), alternativeNames STRUCT(fullName STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[]), shortNames STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[], ecNumbers STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR)[])[], allergenName STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR))[], includes STRUCT(recommendedName STRUCT(fullName STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR), ecNumbers STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR)[], shortNames STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[]), alternativeNames STRUCT(fullName STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[]), shortNames STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[], ecNumbers STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR)[])[])[], innNames STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[], recommendedName STRUCT(fullName STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[]), shortNames STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[], ecNumbers STRUCT("value" VARCHAR, evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[])[]), submissionNames STRUCT(fullName STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR), ecNumbers STRUCT(evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], "value" VARCHAR)[])[])',
    ('features', 'location_sequence'): 'VARCHAR',
    ('features', 'feature_residual'): 'STRUCT(alternativeSequence STRUCT(originalSequence VARCHAR, alternativeSequences VARCHAR[]), evidences STRUCT(evidenceCode VARCHAR, "source" VARCHAR, id VARCHAR)[], featureCrossReferences STRUCT("database" VARCHAR, id VARCHAR)[], ligand STRUCT("name" VARCHAR, id VARCHAR, "label" VARCHAR, note VARCHAR), ligandPart STRUCT("name" VARCHAR, id VARCHAR, note VARCHAR))',
    ('publications', 'reference_residual'): 'STRUCT(citation STRUCT(address VARCHAR, bookName VARCHAR, editors VARCHAR[], institute VARCHAR, locator VARCHAR, patentNumber VARCHAR, publisher VARCHAR))',
}


# Source JSON path of every column in COLUMN_TYPES (dotted, list elements
# carry no segment — the same convention as discover_schema_paths).  Used by
# check_declared_types() to refuse a build whose input carries a nested field
# the declared type would silently drop.  Step 8 (plan H.2) extends this dict
# to every column for field metadata and SCHEMA.md.
COLUMN_SOURCES: dict[tuple[str, str], str] = {
    # entries — single-path columns (derived columns such as gene_name,
    # division, go_terms, *_count, pubmed_ids have no entry)
    ("entries", "acc"): "primaryAccession",
    ("entries", "id"): "uniProtkbId",
    ("entries", "reviewed"): "entryType",
    ("entries", "taxid"): "organism.taxonId",
    ("entries", "organism_name"): "organism.scientificName",
    ("entries", "gene_names"): "genes.geneName.value",
    ("entries", "protein_name"): "proteinDescription.recommendedName.fullName.value",
    ("entries", "seq_length"): "sequence.length",
    ("entries", "sequence"): "sequence.value",
    ("entries", "secondary_accs"): "secondaryAccessions",
    ("entries", "organism_common"): "organism.commonName",
    ("entries", "lineage"): "organism.lineage",
    ("entries", "gene_synonyms"): "genes.synonyms.value",
    ("entries", "alt_protein_names"): "proteinDescription.alternativeNames.fullName.value",
    ("entries", "protein_flag"): "proteinDescription.flag",
    ("entries", "protein_existence"): "proteinExistence",
    ("entries", "annotation_score"): "annotationScore",
    ("entries", "seq_mass"): "sequence.molWeight",
    ("entries", "seq_md5"): "sequence.md5",
    ("entries", "seq_crc64"): "sequence.crc64",
    ("entries", "go_ids"): "uniProtKBCrossReferences.id",
    ("entries", "xref_dbs"): "uniProtKBCrossReferences.database",
    ("entries", "proteome_ids"): "uniProtKBCrossReferences.id",
    ("entries", "keyword_ids"): "keywords.id",
    ("entries", "keyword_names"): "keywords.name",
    ("entries", "first_public"): "entryAudit.firstPublicDate",
    ("entries", "last_modified"): "entryAudit.lastAnnotationUpdateDate",
    ("entries", "last_seq_modified"): "entryAudit.lastSequenceUpdateDate",
    ("entries", "entry_version"): "entryAudit.entryVersion",
    ("entries", "seq_version"): "entryAudit.sequenceVersion",
    ("entries", "uniparc_id"): "extraAttributes.uniParcId",
    ("entries", "entry_type"): "entryType",
    ("entries", "extra_attributes"): "extraAttributes",
    ("entries", "keyword_categories"): "keywords.category",
    ("entries", "organism_residual"): "organism",
    ("entries", "protein_desc_residual"): "proteinDescription",
    ("entries", "genes_full"): "genes",
    ("entries", "keywords_full"): "keywords",
    ("entries", "organism_hosts"): "organismHosts",
    ("entries", "gene_locations"): "geneLocations",
    # features
    ("features", "acc"): "primaryAccession",
    ("features", "reviewed"): "entryType",
    ("features", "taxid"): "organism.taxonId",
    ("features", "organism_name"): "organism.scientificName",
    ("features", "seq_length"): "sequence.length",
    ("features", "type"): "features.type",
    ("features", "start_pos"): "features.location.start.value",
    ("features", "end_pos"): "features.location.end.value",
    ("features", "start_modifier"): "features.location.start.modifier",
    ("features", "end_modifier"): "features.location.end.modifier",
    ("features", "description"): "features.description",
    ("features", "evidence_codes"): "features.evidences.evidenceCode",
    ("features", "location_sequence"): "features.location.sequence",
    ("features", "feature_residual"): "features",
    # xrefs
    ("xrefs", "acc"): "primaryAccession",
    ("xrefs", "reviewed"): "entryType",
    ("xrefs", "taxid"): "organism.taxonId",
    ("xrefs", "database"): "uniProtKBCrossReferences.database",
    ("xrefs", "id"): "uniProtKBCrossReferences.id",
    # comments
    ("comments", "acc"): "primaryAccession",
    ("comments", "reviewed"): "entryType",
    ("comments", "taxid"): "organism.taxonId",
    ("comments", "comment_type"): "comments.commentType",
    ("comments", "text_value"): "comments.texts.value",
    ("comments", "comment"): "comments",
    # publications
    ("publications", "acc"): "primaryAccession",
    ("publications", "reviewed"): "entryType",
    ("publications", "taxid"): "organism.taxonId",
    ("publications", "reference_number"): "references.referenceNumber",
    ("publications", "citation_type"): "references.citation.citationType",
    ("publications", "citation_id"): "references.citation.id",
    ("publications", "publication_date"): "references.citation.publicationDate",
    ("publications", "reference_residual"): "references",
    # accession_map
    ("accession_map", "acc"): "primaryAccession | secondaryAccessions",
    ("accession_map", "primary_acc"): "primaryAccession",
    ("accession_map", "reviewed"): "entryType",
    ("accession_map", "taxid"): "organism.taxonId",
    ("features", "feature_id"): "features.featureId",
    ("features", "original_sequence"): "features.alternativeSequence.originalSequence",
    ("features", "alternative_sequences"): "features.alternativeSequence.alternativeSequences",
    ("features", "ligand_name"): "features.ligand.name",
    ("features", "ligand_id"): "features.ligand.id",
    ("features", "ligand_label"): "features.ligand.label",
    ("features", "ligand_note"): "features.ligand.note",
    ("xrefs", "isoform_id"): "uniProtKBCrossReferences.isoformId",
    ("xrefs", "evidences"): "uniProtKBCrossReferences.evidences",
    ("xrefs", "properties"): "uniProtKBCrossReferences.properties",
    ("publications", "title"): "references.citation.title",
    ("publications", "authors"): "references.citation.authors",
    ("publications", "authoring_group"): "references.citation.authoringGroup",
    ("publications", "journal"): "references.citation.journal",
    ("publications", "volume"): "references.citation.volume",
    ("publications", "first_page"): "references.citation.firstPage",
    ("publications", "last_page"): "references.citation.lastPage",
    ("publications", "submission_database"): "references.citation.submissionDatabase",
    ("publications", "citation_xrefs"): "references.citation.citationCrossReferences",
    ("publications", "reference_positions"): "references.referencePositions",
    ("publications", "reference_comments"): "references.referenceComments",
    ("publications", "evidences"): "references.evidences",
}


def _null(table: str, column: str) -> str:
    """Typed NULL fallback for an optional column.  KeyError is the intended
    failure: every fallback must have a declared type in COLUMN_TYPES."""
    return f"NULL::{COLUMN_TYPES[(table, column)]}"


def _typed(table: str, column: str, expr: str, schema_paths: set[str]) -> str:
    """SQL for an optional column: the source expression cast to its declared
    type when the source path exists, else a typed NULL.

    The cast matters even when the path exists: a struct's sub-fields are
    themselves optional (a subset build whose geneLocations carry no ``value``
    key infers a narrower struct), and DuckDB widens a struct by name, filling
    missing sub-fields with NULL.  The cast would also silently *drop* a
    sub-field the declared type does not know about, which is why main() runs
    check_declared_types() before any table is built.
    """
    if COLUMN_SOURCES[(table, column)] not in schema_paths:
        return _null(table, column)
    return f"CAST({expr} AS {COLUMN_TYPES[(table, column)]})"


# ─── Residual structs (plan A13, DEDUPLICATION_PROPOSAL.md appendix) ─────
# A residual holds every sub-field of a source struct that no convenience
# column already carries.  The kept field set is computed from the discovered
# schema (everything under the prefix minus PROMOTED), so a field UniProt adds
# lands in the residual instead of being dropped.  A struct whose fields are
# only *partly* promoted stays whole in the residual ("never split an array or
# struct across layers"): features.alternativeSequence can be {} in the source,
# which a split could not reproduce.
#
# (table, column): (source prefix, promoted child names)
RESIDUALS: dict[tuple[str, str], tuple[str, frozenset[str]]] = {
    ("entries", "organism_residual"):
        ("organism", frozenset({"taxonId", "scientificName", "commonName", "lineage"})),
    ("entries", "protein_desc_residual"):
        ("proteinDescription", frozenset({"flag"})),
    ("features", "feature_residual"):
        ("features", frozenset({"type", "location", "description", "featureId"})),
    ("publications", "reference_residual"):
        ("references", frozenset({"referenceNumber", "referencePositions",
                                  "referenceComments", "evidences", "citation"})),
    # nested residual inside reference_residual.citation
    ("publications", "reference_residual.citation"):
        ("references.citation", frozenset({"citationType", "id", "title", "authors",
                                           "authoringGroup", "publicationDate", "journal",
                                           "volume", "firstPage", "lastPage",
                                           "submissionDatabase", "citationCrossReferences"})),
}

# Structs whose every child is promoted into convenience columns and which
# therefore have no residual.  A child not listed here fails the build
# (check_promoted_paths): losslessness by construction, not by hope.
PROMOTED_WHOLE: dict[str, frozenset[str]] = {
    "": frozenset({"entryType", "primaryAccession", "secondaryAccessions", "uniProtkbId",
                   "entryAudit", "annotationScore", "organism", "organismHosts",
                   "proteinExistence", "proteinDescription", "genes", "geneLocations",
                   "keywords", "comments", "features", "references",
                   "uniProtKBCrossReferences", "sequence", "extraAttributes"}),
    "entryAudit": frozenset({"firstPublicDate", "lastAnnotationUpdateDate",
                             "lastSequenceUpdateDate", "entryVersion", "sequenceVersion"}),
    "sequence": frozenset({"value", "length", "molWeight", "md5", "crc64"}),
    "features.location": frozenset({"start", "end", "sequence"}),
    "features.location.start": frozenset({"value", "modifier"}),
    "features.location.end": frozenset({"value", "modifier"}),
    "uniProtKBCrossReferences": frozenset({"database", "id", "properties", "isoformId", "evidences"}),
}


def _children(prefix: str, schema_paths: set[str]) -> set[str]:
    """Direct child field names under a dotted prefix ('' = top level)."""
    if prefix == "":
        return {p for p in schema_paths if "." not in p}
    head = prefix + "."
    return {p[len(head):] for p in schema_paths if p.startswith(head) and "." not in p[len(head):]}


def _residual_fields(table: str, column: str, schema_paths: set[str]) -> list[str]:
    prefix, promoted = RESIDUALS[(table, column)]
    return sorted(_children(prefix, schema_paths) - promoted)


def _residual_sql(table: str, column: str, base: str, schema_paths: set[str],
                  nested: dict[str, str] | None = None) -> str:
    """struct_pack(...) of the residual fields of ``base`` (a SQL struct
    expression), cast to the declared type; a typed NULL when nothing is left.
    ``nested`` maps a promoted child name to a SQL expression for its own
    residual (used for references.citation)."""
    fields = _residual_fields(table, column, schema_paths)
    parts = [f'"{f}" := {base}."{f}"' for f in fields]
    for name, expr in (nested or {}).items():
        if expr is not None:
            parts.append(f'"{name}" := {expr}')
    if not parts:
        return _null(table, column)
    inner = "struct_pack(" + ", ".join(parts) + ")"
    if "." in column:                      # nested residual: type comes from the parent
        return inner
    return f"CAST({inner} AS {COLUMN_TYPES[(table, column)]})"


def check_promoted_paths(schema_paths: set[str]) -> None:
    """Abort the build if a fully promoted struct (PROMOTED_WHOLE) has a child
    no convenience column carries.  The fix is to promote the field or move
    the struct into a residual; never to skip the check."""
    problems = []
    for prefix, known in PROMOTED_WHOLE.items():
        extra = sorted(_children(prefix, schema_paths) - known)
        if extra:
            problems.append(f"{prefix or '<top level>'}: {extra}")
    if problems:
        raise RuntimeError("Input has fields no column carries; extend the schema:\n  "
                           + "\n  ".join(problems))


def _declared_type_paths(con, type_str: str, prefix: str) -> set[str]:
    """Dotted sub-paths covered by a DuckDB type string, discover_schema_paths style."""
    paths: set[str] = set()

    def _walk(t, pre):
        if t.id == "list":
            _walk(t.children[0][1], pre)
        elif t.id == "map":
            paths.add(pre + ".*")          # a MAP admits any key: wildcard
        elif t.id == "struct":
            for name, child in t.children:
                path = f"{pre}.{name}"
                paths.add(path)
                _walk(child, path)

    _walk(con.sql(f"SELECT NULL::{type_str}").types[0], prefix)
    return paths


def check_declared_types(con, schema_paths: set[str]) -> None:
    """Abort the build if the input has a nested field that a declared type
    (COLUMN_TYPES) would silently drop.  The fix is to extend the declared
    type, never to skip the check."""
    problems = []
    for key, type_str in COLUMN_TYPES.items():
        if key in RESIDUALS:
            src, promoted = RESIDUALS[key]
            if src not in schema_paths:
                continue
            covered = _declared_type_paths(con, type_str, src)
            actual = {p for p in schema_paths if p.startswith(src + ".")
                      and p[len(src) + 1:].split(".")[0] not in promoted}
            # a nested residual (e.g. reference_residual.citation) re-admits its own leftovers
            for (t, c), (nsrc, npromoted) in RESIDUALS.items():
                if t == key[0] and c.startswith(key[1] + ".") and nsrc.startswith(src + "."):
                    actual |= {p for p in schema_paths if p.startswith(nsrc + ".")
                               and p[len(nsrc) + 1:].split(".")[0] not in npromoted}
        else:
            src = COLUMN_SOURCES.get(key)      # derived columns have no single source
            if src is None or src not in schema_paths:
                continue
            covered = _declared_type_paths(con, type_str, src)
            actual = {p for p in schema_paths if p.startswith(src + ".")}
        wildcards = [w[:-1] for w in covered if w.endswith(".*")]
        extra = sorted(p for p in actual - covered if not any(p.startswith(w) for w in wildcards))
        if extra:
            problems.append(f"{key[0]}.{key[1]} (source {src}): input has {extra} "
                            f"but COLUMN_TYPES declares {type_str}")
    if problems:
        raise RuntimeError("COLUMN_TYPES is narrower than the input; update it:\n  "
                           + "\n  ".join(problems))


# The review flag, shared by every table builder (plan §5.2): Swiss-Prot
# entries are "UniProtKB reviewed (Swiss-Prot)", TrEMBL "UniProtKB unreviewed (TrEMBL)".
REVIEWED_EXPR = "CASE WHEN e.entryType LIKE '%Swiss-Prot%' THEN true ELSE false END"

# Optional fields that may not appear in all UniProtKB subsets.
# (e.g. organismHosts only exists in virus/parasite entries,
#  geneLocations is rare in some organisms.)
# The SQL generator substitutes a typed NULL for any that are absent.
_OPTIONAL_ENTRY_FIELDS = {"organismHosts", "geneLocations"}

# Paths every UniProtKB entry carries; the builders dereference them without
# a guard.  A dump lacking one is refused up front with a clear message
# (check_required_paths) instead of a DuckDB BinderException mid-build.
# Everything else the builders read is guarded with has() / _typed().
REQUIRED_PATHS: frozenset[str] = frozenset({
    "primaryAccession", "uniProtkbId", "entryType",
    "organism.taxonId", "organism.scientificName",
    "sequence.value", "sequence.length", "sequence.molWeight", "sequence.md5", "sequence.crc64",
    "entryAudit.firstPublicDate", "entryAudit.lastAnnotationUpdateDate",
    "entryAudit.lastSequenceUpdateDate", "entryAudit.entryVersion", "entryAudit.sequenceVersion",
    "proteinExistence", "annotationScore",
})

# Paths required only when their top-level array exists at all (a subset
# with no features has no features table; one with features has typed ones).
REQUIRED_CHILD_PATHS: dict[str, frozenset[str]] = {
    "features": frozenset({"features.type",
                           "features.location.start.value", "features.location.end.value"}),
    "uniProtKBCrossReferences": frozenset({"uniProtKBCrossReferences.database",
                                           "uniProtKBCrossReferences.id"}),
    "references": frozenset({"references.referenceNumber",
                             "references.citation.citationType", "references.citation.id"}),
}


def check_required_paths(schema_paths: set[str]) -> None:
    """Abort the build if the input lacks a field every UniProtKB entry has."""
    missing = sorted(REQUIRED_PATHS - schema_paths)
    for parent, paths in REQUIRED_CHILD_PATHS.items():
        if parent in schema_paths:
            missing += sorted(paths - schema_paths)
    if missing:
        raise RuntimeError("Input lacks fields every UniProtKB entry carries "
                           f"(is this a UniProtKB JSON dump?): {missing}")


# Element types for a child table whose source array is absent from the
# input: the builder's SQL then unnests an empty list of this shape and
# writes a zero-row table instead of failing to bind the missing column.
_EMPTY_CHILD_SOURCE: dict[str, str] = {
    "features": 'CAST([] AS STRUCT("type" VARCHAR, location STRUCT("start" STRUCT("value" BIGINT, '
                'modifier VARCHAR), "end" STRUCT("value" BIGINT, modifier VARCHAR)))[])',
    "uniProtKBCrossReferences": 'CAST([] AS STRUCT("database" VARCHAR, id VARCHAR)[])',
    "comments": "CAST([] AS MAP(VARCHAR, JSON)[])",
    "references": 'CAST([] AS STRUCT(referenceNumber BIGINT, '
                  'citation STRUCT(citationType VARCHAR, id VARCHAR))[])',
}


def _child_source(field: str, schema_paths: set[str]) -> str:
    """SQL for a child table's source array: the column, or an empty typed list."""
    if field in schema_paths:
        return f'e."{field}"'
    return _EMPTY_CHILD_SOURCE[field]


def _build_entries_sql(schema_paths: set[str]) -> str:
    """Build the entries SQL with NULLs for any fields absent from the schema.

    ``schema_paths`` is the set of all valid dotted paths discovered from the
    staged Parquet (see :func:`discover_schema_paths`).  Any optional path not
    in the set gets a typed ``NULL`` (or ``[]`` / ``0`` for list and count
    columns) in the SQL — no BinderException, no matter how exotic the
    dataset.  Only REQUIRED_PATHS are dereferenced unguarded, and
    check_required_paths() has already refused an input without them.
    """

    def has(path: str) -> bool:
        return path in schema_paths

    organism_hosts = _typed("entries", "organism_hosts", "e.organismHosts", schema_paths)
    gene_locations = _typed("entries", "gene_locations", "e.geneLocations", schema_paths)

    # EC numbers: extract from recommended, alternative, and (if present) submitted names.
    # Each naming block's struct schema may or may not include ecNumbers depending on the
    # dataset.  Only include the extraction expression when the field actually exists.
    ec_parts = []
    if has("proteinDescription.recommendedName.ecNumbers"):
        ec_parts.append(
            "list_transform(COALESCE(e.proteinDescription.recommendedName.ecNumbers, []), x -> x.value)"
        )
    if has("proteinDescription.alternativeNames.ecNumbers"):
        ec_parts.append("""flatten(list_transform(
            COALESCE(e.proteinDescription.alternativeNames, []),
            n -> list_transform(COALESCE(n.ecNumbers, []), x -> x.value)
        ))""")
    # UniProtKB API renamed submittedNames → submissionNames; handle both.
    _submitted_field = (
        "submissionNames" if has("proteinDescription.submissionNames.ecNumbers")
        else "submittedNames" if has("proteinDescription.submittedNames.ecNumbers")
        else None
    )
    if _submitted_field:
        ec_parts.append(f"""flatten(list_transform(
            COALESCE(e.proteinDescription.{_submitted_field}, []),
            n -> list_transform(COALESCE(n.ecNumbers, []), x -> x.value)
        ))""")

    if ec_parts:
        ec_numbers_expr = "list_distinct(flatten([\n        " + ",\n        ".join(ec_parts) + "\n    ]))"
    else:
        ec_numbers_expr = "CAST([] AS VARCHAR[])"

    # protein_name: COALESCE across recommendedName → submissionNames/submittedNames[1] → alternativeNames[1].
    # Swiss-Prot entries have recommendedName; TrEMBL entries typically only have submissionNames.
    # Without this fallback, protein_name is NULL for >99% of the lake (TrEMBL dominates).
    # UniProtKB API renamed submittedNames → submissionNames; handle both.
    # Every naming block is optional in a subset, so only existing ones are read.
    protein_name_parts = []
    if has("proteinDescription.recommendedName.fullName.value"):
        protein_name_parts.append("e.proteinDescription.recommendedName.fullName.value")
    _submitted_name_field = (
        "submissionNames" if has("proteinDescription.submissionNames.fullName.value")
        else "submittedNames" if has("proteinDescription.submittedNames.fullName.value")
        else None
    )
    if _submitted_name_field:
        protein_name_parts.append(f"e.proteinDescription.{_submitted_name_field}[1].fullName.value")
    if has("proteinDescription.alternativeNames.fullName.value"):
        protein_name_parts.append("(list_extract(COALESCE(e.proteinDescription.alternativeNames, []), 1)).fullName.value")
    if protein_name_parts:
        protein_name_expr = (f"CAST(COALESCE({', '.join(protein_name_parts)}) AS "
                             f"{COLUMN_TYPES[('entries', 'protein_name')]})")
    else:
        protein_name_expr = _null("entries", "protein_name")

    # Genes, secondary accessions, organism names/lineage, protein flag and
    # alternative names: all absent from some subsets.
    gene_names_expr = _typed("entries", "gene_names",
                             "list_transform(COALESCE(e.genes, []), g -> g.geneName.value)", schema_paths)
    gene_name_expr = (
        f"CAST(list_extract(list_transform(COALESCE(e.genes, []), g -> g.geneName.value), 1) AS "
        f"{COLUMN_TYPES[('entries', 'gene_name')]})"
        if has("genes.geneName.value") else _null("entries", "gene_name")
    )
    gene_synonyms_expr = _typed(
        "entries", "gene_synonyms",
        "flatten(list_transform(COALESCE(e.genes, []), g -> list_transform(COALESCE(g.synonyms, []), s -> s.value)))",
        schema_paths)
    alt_protein_names_expr = _typed(
        "entries", "alt_protein_names",
        "list_transform(COALESCE(e.proteinDescription.alternativeNames, []), x -> x.fullName.value)",
        schema_paths)
    protein_flag_expr = _typed("entries", "protein_flag", "e.proteinDescription.flag", schema_paths)
    secondary_accs_expr = _typed("entries", "secondary_accs", "e.secondaryAccessions", schema_paths)
    organism_common_expr = _typed("entries", "organism_common", "e.organism.commonName", schema_paths)
    lineage_expr = _typed("entries", "lineage", "e.organism.lineage", schema_paths)
    uniparc_id_expr = _typed("entries", "uniparc_id", "e.extraAttributes.uniParcId", schema_paths)
    # Full nested pass-through columns: no declared type (they carry whatever
    # UniProt ships), so an absent source gets a NULL of the minimal shape.
    genes_full_expr = ("e.genes" if has("genes")
                       else 'NULL::STRUCT(geneName STRUCT("value" VARCHAR))[]')
    keywords_full_expr = ("e.keywords" if has("keywords")
                          else 'NULL::STRUCT(id VARCHAR, category VARCHAR, "name" VARCHAR)[]')

    def _keyword_list(field):
        if not has(f"keywords.{field}"):
            return "CAST([] AS VARCHAR[])"
        return f"list_transform(COALESCE(e.keywords, []), x -> x.{field})"

    # Taxonomic division needs the lineage; without it only the human rule applies.
    if has("organism.lineage"):
        division_rules = """
      WHEN list_contains(e.organism.lineage, 'Rodentia')         THEN 'rodents'
      WHEN list_contains(e.organism.lineage, 'Mammalia')         THEN 'mammals'
      WHEN list_contains(e.organism.lineage, 'Vertebrata')       THEN 'vertebrates'
      WHEN list_contains(e.organism.lineage, 'Fungi')            THEN 'fungi'
      WHEN list_contains(e.organism.lineage, 'Viridiplantae')    THEN 'plants'
      WHEN list_contains(e.organism.lineage, 'Eukaryota')        THEN 'invertebrates'
      WHEN list_contains(e.organism.lineage, 'Bacteria')         THEN 'bacteria'
      WHEN list_contains(e.organism.lineage, 'Archaea')          THEN 'archaea'
      WHEN list_contains(e.organism.lineage, 'Viruses')          THEN 'viruses'"""
    else:
        division_rules = ""

    # Cross-reference shortcuts and per-array counts: [] / 0 when the array is
    # absent, matching what an entry without the array gets when it is present.
    xrefs_present = has("uniProtKBCrossReferences")
    go_ids_expr = ("""list_distinct([
        x.id
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
        IF x.database = 'GO'
    ])""" if xrefs_present else "CAST([] AS VARCHAR[])")
    xref_dbs_expr = ("""list_distinct([
        x.database
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
    ])""" if xrefs_present else "CAST([] AS VARCHAR[])")
    proteome_ids_expr = ("""list_sort(list_distinct([
        x.id
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
        IF x.database = 'Proteomes'
    ]))""" if xrefs_present else "CAST([] AS VARCHAR[])")

    def _count(field):
        if not has(field):
            return "CAST(0 AS INTEGER)"
        return f'CAST(len(COALESCE(e."{field}", [])) AS INTEGER)'

    # Residual structs (plan A13): what the convenience columns do not carry.
    organism_residual = _residual_sql("entries", "organism_residual", "e.organism", schema_paths)
    protein_desc_residual = _residual_sql("entries", "protein_desc_residual",
                                          "e.proteinDescription", schema_paths)

    # extra_attributes: input-independent shape (see the SELECT comment).
    def _count_map(field):
        if not has(f"extraAttributes.{field}"):
            return "NULL::MAP(VARCHAR, BIGINT)"
        return (f"map_from_entries(list_filter(map_entries("
                f"e.extraAttributes.{field}::MAP(VARCHAR, BIGINT)), x -> x.value IS NOT NULL))")
    extra_attributes = (
        "CAST(struct_pack("
        f"countByCommentType := {_count_map('countByCommentType')}, "
        f"countByFeatureType := {_count_map('countByFeatureType')}, "
        + ("uniParcId := e.extraAttributes.uniParcId" if has("extraAttributes.uniParcId")
           else "uniParcId := NULL::VARCHAR")
        + f") AS {COLUMN_TYPES[('entries', 'extra_attributes')]})"
    ) if has("extraAttributes") else _null("entries", "extra_attributes")

    # go_terms (plan B.1): GO xrefs carry properties GoTerm ('F:ATP binding')
    # and GoEvidenceType ('IEA:InterPro').  aspect/term are NULL, never '',
    # when GoTerm is absent.
    if has("uniProtKBCrossReferences.properties"):
        go_terms_expr = """[ struct_pack(
            id            := x.id,
            aspect        := left(list_filter(COALESCE(x.properties, []), p -> p.key = 'GoTerm')[1].value, 1),
            term          := substr(list_filter(COALESCE(x.properties, []), p -> p.key = 'GoTerm')[1].value, 3),
            evidence_type := list_filter(COALESCE(x.properties, []), p -> p.key = 'GoEvidenceType')[1].value)
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
        IF x.database = 'GO' ]"""
    else:
        go_terms_expr = _null("entries", "go_terms")

    # pubmed_ids (plan B.2): distinct PubMed ids, sorted numerically, stored as strings.
    if has("references.citation.citationCrossReferences"):
        pubmed_ids_expr = """list_transform(
        list_sort(list_transform(
            list_distinct(flatten(list_transform(
                COALESCE(e."references", []),
                r -> [ c.id FOR c IN COALESCE(r.citation.citationCrossReferences, []) IF c.database = 'PubMed' ]
            ))),
            x -> CAST(x AS BIGINT))),
        x -> CAST(x AS VARCHAR))"""
    else:
        pubmed_ids_expr = "CAST([] AS VARCHAR[])"

    return f"""
SELECT
    -- Nine hot columns first and contiguous (plan Part C): the seven UniProt
    -- default columns plus taxid and sequence.  Adjacent column chunks mean
    -- fewer range requests for remote readers of the default projection.
    e.primaryAccession                              AS acc,
    e.uniProtkbId                                   AS id,
    {REVIEWED_EXPR}                                 AS reviewed,
    e.organism.taxonId                              AS taxid,
    e.organism.scientificName                       AS organism_name,
    {gene_names_expr}                               AS gene_names,
    {protein_name_expr}                                AS protein_name,
    CAST(e.sequence.length AS INTEGER)              AS seq_length,
    e.sequence.value                                AS sequence,

    -- Primary gene name = first of gene_names (plan B.4)
    {gene_name_expr}                                AS gene_name,

    -- Identity / organism (remaining)
    {secondary_accs_expr}                           AS secondary_accs,
    {organism_common_expr}                          AS organism_common,
    {lineage_expr}                                  AS lineage,
    -- UniProt taxonomic division (plan D.5), most specific rule first.
    -- First approximation of the FTP taxonomic_divisions/ rules; the D.5
    -- correctness gate (per-division counts vs the FTP) decides the final CASE.
    -- Protists land in 'invertebrates' as on the FTP; 'unclassified' is the rest.
    CASE
      WHEN e.organism.taxonId = 9606                             THEN 'human'{division_rules}
      ELSE 'unclassified'
    END                                             AS division,

    -- Gene & protein (remaining)
    -- All gene synonyms across all genes (searchable list)
    {gene_synonyms_expr}                            AS gene_synonyms,
    -- Alternative protein names (searchable list)
    {alt_protein_names_expr}                        AS alt_protein_names,
    -- Precursor / Fragment flag (commonly used to filter incomplete sequences)
    {protein_flag_expr}                             AS protein_flag,
    -- EC numbers: extract from all naming blocks (recommended, alternative, submitted)
    {ec_numbers_expr}                               AS ec_numbers,
    e.proteinExistence                              AS protein_existence,
    e.annotationScore                               AS annotation_score,

    -- Sequence (remaining)
    CAST(e.sequence.molWeight AS INTEGER)           AS seq_mass,
    e.sequence.md5                                  AS seq_md5,
    e.sequence.crc64                                AS seq_crc64,

    -- Cross-reference shortcuts
    {go_ids_expr}                                   AS go_ids,
    {go_terms_expr}                                 AS go_terms,
    {xref_dbs_expr}                                 AS xref_dbs,
    {proteome_ids_expr}                             AS proteome_ids,
    {_keyword_list('id')}                           AS keyword_ids,
    {_keyword_list('name')}                         AS keyword_names,
    {_keyword_list('category')}                     AS keyword_categories,

    -- Versioning
    CAST(e.entryAudit.firstPublicDate AS DATE)      AS first_public,
    CAST(e.entryAudit.lastAnnotationUpdateDate AS DATE) AS last_modified,
    CAST(e.entryAudit.lastSequenceUpdateDate AS DATE)   AS last_seq_modified,
    CAST(e.entryAudit.entryVersion AS INTEGER)      AS entry_version,
    CAST(e.entryAudit.sequenceVersion AS INTEGER)   AS seq_version,

    -- Counts
    {_count('features')}                            AS feature_count,
    {_count('uniProtKBCrossReferences')}            AS xref_count,
    {_count('comments')}                            AS comment_count,
    {_count('references')}                          AS reference_count,
    {pubmed_ids_expr}                               AS pubmed_ids,
    {uniparc_id_expr}                               AS uniparc_id,

    -- Entry type (lossless round-trip — the boolean 'reviewed' loses the exact string)
    e.entryType                                     AS entry_type,
    -- Extra attributes (countByCommentType, countByFeatureType, uniParcId).
    -- The two count objects are MAP(VARCHAR, BIGINT) whatever DuckDB inferred
    -- (a STRUCT with one field per type seen, or a MAP past its map-inference
    -- threshold), so the column type does not depend on the input (plan G.2).
    {extra_attributes}                              AS extra_attributes,

    -- Residual / full nested structures (plan A13): residuals hold only what
    -- the convenience columns do not; bin/reconstruct.py rebuilds the JSON.
    -- features, xrefs, comments, and publications are in their own tables
    {organism_residual}                             AS organism_residual,
    {protein_desc_residual}                         AS protein_desc_residual,
    {genes_full_expr}                               AS genes_full,
    {keywords_full_expr}                            AS keywords_full,
    {organism_hosts}                                 AS organism_hosts,
    {gene_locations}                                 AS gene_locations

FROM {{read_clause}} e
ORDER BY reviewed DESC, e.organism.taxonId, e.primaryAccession
"""


def _build_features_sql(schema_paths: set[str]) -> str:
    """Build features SQL with NULLs for any fields absent from the schema.

    Feature structs may lack ligand, alternativeSequence, evidences, or
    featureId depending on the dataset.  ``schema_paths`` is checked for
    every nested reference so the SQL never touches a missing struct key.
    """

    def has(path: str) -> bool:
        return path in schema_paths

    feature_id = _typed("features", "feature_id", "unnest.featureId", schema_paths)

    if has("features.evidences"):
        evidence_codes = """list_transform(
        COALESCE(unnest.evidences, []),
        x -> x.evidenceCode
    )"""
    else:
        evidence_codes = "CAST([] AS VARCHAR[])"

    original_seq = _typed("features", "original_sequence", "unnest.alternativeSequence.originalSequence", schema_paths)
    alt_seqs = _typed("features", "alternative_sequences", "unnest.alternativeSequence.alternativeSequences", schema_paths)

    ligand_name = _typed("features", "ligand_name", "unnest.ligand.name", schema_paths)
    ligand_id = _typed("features", "ligand_id", "unnest.ligand.id", schema_paths)
    ligand_label = _typed("features", "ligand_label", "unnest.ligand.label", schema_paths)
    ligand_note = _typed("features", "ligand_note", "unnest.ligand.note", schema_paths)
    location_sequence = _typed("features", "location_sequence", "unnest.location.sequence", schema_paths)
    start_modifier = _typed("features", "start_modifier", "unnest.location.start.modifier", schema_paths)
    end_modifier = _typed("features", "end_modifier", "unnest.location.end.modifier", schema_paths)
    description = _typed("features", "description", "unnest.description", schema_paths)
    feature_residual = _residual_sql("features", "feature_residual", "unnest", schema_paths)
    features_src = _child_source("features", schema_paths)

    return f"""
SELECT
    sub.acc,
    sub.reviewed,
    sub.taxid,
    sub.organism_name,
    sub.seq_length,

    -- Flattened convenience columns (fast querying)
    unnest.type                                     AS type,
    CAST(unnest.location.start.value AS INTEGER)    AS start_pos,
    CAST(unnest.location.end.value AS INTEGER)      AS end_pos,
    {start_modifier}                                AS start_modifier,
    {end_modifier}                                  AS end_modifier,
    {description}                                   AS description,
    {feature_id}                                    AS feature_id,

    {evidence_codes}                                AS evidence_codes,
    {location_sequence}                             AS location_sequence,

    {original_seq}                                  AS original_sequence,
    {alt_seqs}                                      AS alternative_sequences,

    {ligand_name}                                   AS ligand_name,
    {ligand_id}                                     AS ligand_id,
    {ligand_label}                                  AS ligand_label,
    {ligand_note}                                   AS ligand_note,

    -- Residual (plan A13): evidences, featureCrossReferences, ligand, ligandPart,
    -- alternativeSequence and anything UniProt adds; bin/reconstruct.py merges
    -- it with the convenience columns above.
    {feature_residual}                              AS feature_residual

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        {REVIEWED_EXPR}                             AS reviewed,
        e.organism.taxonId                           AS taxid,
        e.organism.scientificName                    AS organism_name,
        CAST(e.sequence.length AS INTEGER)           AS seq_length,
        {features_src}                               AS features
    FROM {{read_clause}} e
    WHERE {features_src} IS NOT NULL AND len({features_src}) > 0
) sub, LATERAL unnest(sub.features)
ORDER BY sub.reviewed DESC, sub.taxid, sub.acc
"""


def _build_xrefs_sql(schema_paths: set[str]) -> str:
    """Build xrefs SQL with NULLs for any fields absent from the schema.

    Cross-reference structs may lack ``isoformId`` or ``evidences`` depending
    on the dataset.  ``schema_paths`` is checked before referencing them.
    """

    def has(path: str) -> bool:
        return path in schema_paths

    isoform_id = _typed("xrefs", "isoform_id", "unnest.isoformId", schema_paths)
    xref_evidences = _typed("xrefs", "evidences", "unnest.evidences", schema_paths)
    properties = _typed("xrefs", "properties", "unnest.properties", schema_paths)
    xrefs_src = _child_source("uniProtKBCrossReferences", schema_paths)

    return f"""
SELECT
    sub.acc,
    sub.reviewed,
    sub.taxid,

    -- Flattened convenience columns (fast querying)
    -- Note: no lossless nested `xref` column — the UniProt xref schema
    -- has exactly these five fields (database, id, properties, isoformId,
    -- evidences), all of which are captured above. A duplicate nested
    -- struct would be 100% redundant and ~410 GB at full UniProtKB scale.
    unnest.database                                 AS database,
    unnest.id                                       AS id,
    {properties}                                    AS properties,
    {isoform_id}                                    AS isoform_id,
    {xref_evidences}                                AS evidences

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        {REVIEWED_EXPR}                             AS reviewed,
        e.organism.taxonId                           AS taxid,
        {xrefs_src}                                  AS uniProtKBCrossReferences
    FROM {{read_clause}} e
    WHERE {xrefs_src} IS NOT NULL
      AND len({xrefs_src}) > 0
) sub, LATERAL unnest(sub.uniProtKBCrossReferences)
ORDER BY sub.reviewed DESC, sub.taxid, sub.acc
"""


def _build_comments_sql(schema_paths: set[str]) -> str:
    """Build comments SQL.

    ``comments`` is ``MAP(VARCHAR, JSON)[]`` in the staged schema, so
    ``discover_schema_paths`` never yields a ``comments.texts`` path and the
    column cannot be guarded with ``has()`` (plan G.1).  The ``texts`` key is
    read unconditionally: map access on a missing key yields NULL, so
    structured-only comment types (COFACTOR, INTERACTION, CATALYTIC ACTIVITY,
    ALTERNATIVE PRODUCTS, ...) get NULL rather than an error.  DISEASE and
    SUBCELLULAR LOCATION keep their prose under ``note.texts`` and are also
    NULL here; the full comment is in the ``comment`` column.
    """
    comments_src = _child_source("comments", schema_paths)   # the only optional path here

    text_value_expr = """NULLIF(array_to_string(
        from_json(unnest.texts->'$[*].value', '["VARCHAR"]'),
        chr(10) || chr(10)
    ), '')"""

    return f"""
SELECT
    sub.acc,
    sub.reviewed,
    sub.taxid,

    -- Strip embedded double quotes from commentType (JSON scalar → VARCHAR)
    -- so users can write comment_type = 'FUNCTION' instead of '"FUNCTION"'
    trim('"' FROM unnest.commentType::VARCHAR)      AS comment_type,
    -- Concatenate all text values (multi-paragraph comments have texts[1..N])
    -- texts is JSON, so extract all values and join with double newline
    {text_value_expr}                               AS text_value,
    -- Full comment as JSON (lossless — preserves original structure).
    -- Stored as VARCHAR in Parquet; the comments view casts to JSON
    -- so users get ->> operators without explicit casting.
    unnest::JSON                                    AS comment

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        {REVIEWED_EXPR}                             AS reviewed,
        e.organism.taxonId                           AS taxid,
        {comments_src}                               AS comments
    FROM {{read_clause}} e
    WHERE {comments_src} IS NOT NULL AND len({comments_src}) > 0
) sub, LATERAL unnest(sub.comments)
ORDER BY sub.reviewed DESC, sub.taxid, sub.acc
"""


def _build_publications_sql(schema_paths: set[str]) -> str:
    """Build publications SQL with NULLs for any fields absent from the schema.

    Citation structs vary by type: journal articles have ``journal``, ``volume``,
    ``firstPage``, ``lastPage``; submissions have ``submissionDatabase``; some
    have ``authoringGroup`` instead of ``authors``.  On a subset that only
    contains one citation type, many of these fields could be absent.
    """

    def has(path: str) -> bool:
        return path in schema_paths

    # Citation fields — all potentially absent depending on citation types in the dataset
    title = _typed("publications", "title", "unnest.citation.title", schema_paths)
    authors = _typed("publications", "authors", "unnest.citation.authors", schema_paths)
    authoring_group = _typed("publications", "authoring_group", "unnest.citation.authoringGroup", schema_paths)
    journal = _typed("publications", "journal", "unnest.citation.journal", schema_paths)
    volume = _typed("publications", "volume", "unnest.citation.volume", schema_paths)
    first_page = _typed("publications", "first_page", "unnest.citation.firstPage", schema_paths)
    last_page = _typed("publications", "last_page", "unnest.citation.lastPage", schema_paths)
    submission_db = _typed("publications", "submission_database", "unnest.citation.submissionDatabase", schema_paths)
    citation_xrefs = _typed("publications", "citation_xrefs", "unnest.citation.citationCrossReferences", schema_paths)
    ref_positions = _typed("publications", "reference_positions", "unnest.referencePositions", schema_paths)
    ref_comments = _typed("publications", "reference_comments", "unnest.referenceComments", schema_paths)
    ref_evidences = _typed("publications", "evidences", "unnest.evidences", schema_paths)
    citation_residual = (
        _residual_sql("publications", "reference_residual.citation", "unnest.citation", schema_paths)
        if _residual_fields("publications", "reference_residual.citation", schema_paths) else None
    )
    reference_residual = _residual_sql("publications", "reference_residual", "unnest", schema_paths,
                                       nested={"citation": citation_residual})
    publication_date = _typed("publications", "publication_date", "unnest.citation.publicationDate", schema_paths)
    references_src = _child_source("references", schema_paths)

    return f"""
SELECT
    sub.acc,
    sub.reviewed,
    sub.taxid,

    -- Flattened convenience columns (fast querying)
    CAST(unnest.referenceNumber AS INTEGER)          AS reference_number,
    unnest.citation.citationType                    AS citation_type,
    unnest.citation.id                              AS citation_id,
    {title}                                         AS title,
    {authors}                                       AS authors,
    {authoring_group}                               AS authoring_group,
    {publication_date}                              AS publication_date,
    {journal}                                       AS journal,
    {volume}                                        AS volume,
    {first_page}                                    AS first_page,
    {last_page}                                     AS last_page,
    {submission_db}                                  AS submission_database,
    {citation_xrefs}                                AS citation_xrefs,
    {ref_positions}                                  AS reference_positions,
    {ref_comments}                                   AS reference_comments,
    {ref_evidences}                                  AS evidences,

    -- Full original nested struct (lossless round-trip)
    {reference_residual}                            AS reference_residual

FROM (
    SELECT
        e.primaryAccession                           AS acc,
        {REVIEWED_EXPR}                             AS reviewed,
        e.organism.taxonId                           AS taxid,
        {references_src}                             AS "references"
    FROM {{read_clause}} e
    WHERE {references_src} IS NOT NULL AND len({references_src}) > 0
) sub, LATERAL unnest(sub."references")
ORDER BY sub.reviewed DESC, sub.taxid, sub.acc
"""


# ─── VARIANT child SQL builders (--variant-children) ───────────────────
#
# These replace the hand-extracted convenience columns with a single
# VARIANT data column per child table.  Each row still has typed filter
# columns (type, database, comment_type) for fast WHERE clauses;
# everything else is accessed via data.field VARIANT dot notation.
#
# Advantages:
#   - No schema_paths dependency — VARIANT absorbs any nested structure
#   - ~300 fewer LOC than the convenience-column builders
#   - Schema evolution "for free" — new fields appear automatically
#
# Requires: DuckDB ≥1.5 (VARIANT is a built-in type, no extension needed).


def _build_features_variant_sql() -> str:
    """Features with VARIANT data column."""
    return f"""
SELECT
    e.primaryAccession                           AS acc,
    {REVIEWED_EXPR}                             AS reviewed,
    e.organism.taxonId                           AS taxid,
    unnest.type                                  AS type,
    unnest::VARIANT                              AS data
FROM {{read_clause}} e,
LATERAL UNNEST(COALESCE(e.features, [])) AS t(unnest)
ORDER BY reviewed DESC, taxid, acc
"""


def _build_xrefs_variant_sql() -> str:
    """Cross-references with VARIANT data column."""
    return f"""
SELECT
    e.primaryAccession                           AS acc,
    {REVIEWED_EXPR}                             AS reviewed,
    e.organism.taxonId                           AS taxid,
    unnest.database                              AS database,
    unnest::VARIANT                              AS data
FROM {{read_clause}} e,
LATERAL UNNEST(COALESCE(e.uniProtKBCrossReferences, [])) AS t(unnest)
ORDER BY reviewed DESC, taxid, acc
"""


def _build_comments_variant_sql() -> str:
    """Comments with VARIANT data column.

    Comments are MAP(VARCHAR, JSON) in the JSONL schema (not a typed struct),
    so ``unnest::VARIANT`` produces VARIANT(ARRAY) with key-value pairs —
    dot notation won't work.  Going through JSON first (``unnest::JSON``)
    normalises the map to a JSON object, then ``::VARIANT`` gives us
    VARIANT(OBJECT) with proper field access.
    """
    return f"""
SELECT
    e.primaryAccession                           AS acc,
    {REVIEWED_EXPR}                             AS reviewed,
    e.organism.taxonId                           AS taxid,
    trim('"' FROM CAST(unnest.commentType AS VARCHAR)) AS comment_type,
    (unnest::JSON)::VARIANT                      AS data
FROM {{read_clause}} e,
LATERAL UNNEST(COALESCE(e.comments, [])) AS t(unnest)
ORDER BY reviewed DESC, taxid, acc
"""


def _build_publications_variant_sql() -> str:
    """Publications with VARIANT data column."""
    return f"""
SELECT
    e.primaryAccession                           AS acc,
    {REVIEWED_EXPR}                             AS reviewed,
    e.organism.taxonId                           AS taxid,
    unnest::VARIANT                              AS data
FROM {{read_clause}} e,
LATERAL UNNEST(COALESCE(e."references", [])) AS t(unnest)
ORDER BY reviewed DESC, taxid, acc
"""


def _build_accession_map_sql(schema_paths: set[str]) -> str:
    """One row per primary *and* secondary accession → current primary (plan
    Part A, B2).  Reads the staged Parquet like the other builders so it works
    under --skip-existing.  Sorted reviewed DESC first so the Part D writer
    sees one partition flip, then acc within each side."""
    has_secondary = "secondaryAccessions" in schema_paths
    primary = f"""
    SELECT e.primaryAccession AS acc, e.primaryAccession AS primary_acc, true AS is_primary,
           {REVIEWED_EXPR} AS reviewed, e.organism.taxonId AS taxid
    FROM {{read_clause}} e"""
    secondary = f"""
    UNION ALL
    SELECT s AS acc, e.primaryAccession AS primary_acc, false AS is_primary,
           {REVIEWED_EXPR} AS reviewed, e.organism.taxonId AS taxid
    FROM {{read_clause}} e, LATERAL unnest(COALESCE(e.secondaryAccessions, [])) AS t(s)""" if has_secondary else ""
    return f"""
SELECT acc, primary_acc, is_primary, reviewed, taxid FROM ({primary}{secondary})
ORDER BY reviewed DESC, acc, primary_acc
"""


# ─── Table definitions ──────────────────────────────────────────────────

# ─── Writer constants ───────────────────────────────────────────────────
TARGET_FILE_BYTES = 256 * 1024 * 1024   # per-file roll-over target. PROVISIONAL: plan F.4 decides between
                                        # 256 MB / 512 MB / 1 GB on the slice (benchmarks/bench_file_size.py)
ROW_GROUP_SIZE = 100_000
ZSTD_LEVEL = 9                          # plan H.1. PROVISIONAL: chosen from the stress-fixture sweep
                                        # (plan H.1 table, stress fixture: -7% entries, -12% xrefs
                                        # bytes for +6% stage time vs level 1); confirm on the F.1 slice.
PARTITION_KEY = "review_status"         # plan Part D: Hive directory key ...
PARTITION_VALUES = {True: "swissprot", False: "trembl"}   # ... derived from the stored `reviewed`

# (name, sort_order, partition_column).  Every table names its
# partition column explicitly so a future table cannot be written
# unpartitioned by accident.
TABLE_DEFS = [
    ("entries",      ["reviewed DESC", "taxid ASC", "acc ASC"], "reviewed"),
    # Child tables inherit (reviewed DESC, taxid ASC, acc ASC) from the
    # pre-sorted JSONL input — DuckDB's ORDER BY on these three columns is
    # essentially free (data arrives already in order after LATERAL unnest).
    # Within-protein sort keys (start_pos, database, comment_type, citation_type)
    # are deliberately omitted to avoid ~1.2 TB of sort spill at production
    # scale (~3B xref + ~1.3B feature + ~1B publication + ~400M comment rows).
    # Users who need within-protein ordering can add it at query time.
    ("features",     ["reviewed DESC", "taxid ASC", "acc ASC"], "reviewed"),
    ("xrefs",        ["reviewed DESC", "taxid ASC", "acc ASC"], "reviewed"),
    ("comments",     ["reviewed DESC", "taxid ASC", "acc ASC"], "reviewed"),
    ("publications", ["reviewed DESC", "taxid ASC", "acc ASC"], "reviewed"),
    # accession_map is acc-sorted within each review side: the lookup table
    # (plan Part A).  A real sort at production scale (~250M + secondaries rows
    # of five narrow columns; low tens of GB of spill).
    ("accession_map", ["reviewed DESC", "acc ASC", "primary_acc ASC"], "reviewed"),
]

# All table SQL is built dynamically to handle optional fields. schema_paths
# is the single source of truth — no field is referenced without first
# checking that it exists in the staged Parquet schema.
_SQL_BUILDERS = {
    "entries":      _build_entries_sql,
    "features":     _build_features_sql,
    "xrefs":        _build_xrefs_sql,
    "comments":     _build_comments_sql,
    "publications": _build_publications_sql,
    "accession_map": _build_accession_map_sql,
}

# VARIANT child tables (evaluation-only, --variant-children) — no schema_paths needed.
_VARIANT_BUILDERS = {
    "features":     _build_features_variant_sql,
    "xrefs":        _build_xrefs_variant_sql,
    "comments":     _build_comments_variant_sql,
    "publications": _build_publications_variant_sql,
}


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
                # Same order as the SELECT in _build_entries_sql (plan Part C):
                # nine hot columns first, then gene_name, then the rest.
                "acc", "id", "reviewed", "taxid", "organism_name", "gene_names",
                "protein_name", "seq_length", "sequence",
                "gene_name",
                "secondary_accs", "organism_common", "lineage", "division",
                "gene_synonyms", "alt_protein_names", "protein_flag", "ec_numbers",
                "protein_existence", "annotation_score", "seq_mass", "seq_md5", "seq_crc64",
                "go_ids", "go_terms", "xref_dbs", "proteome_ids", "keyword_ids", "keyword_names",
                "keyword_categories",
                "first_public", "last_modified", "last_seq_modified",
                "entry_version", "seq_version",
                "feature_count", "xref_count", "comment_count", "reference_count", "pubmed_ids",
                "uniparc_id", "entry_type", "extra_attributes",
            ],
            "nested": [
                "organism_residual", "protein_desc_residual", "genes_full", "keywords_full",
                "organism_hosts", "gene_locations",
            ],
        },
    },
    "features": {
        "description": "One row per positional annotation (domain, signal, transmembrane, etc). Sorted by (reviewed DESC, taxid ASC, acc ASC) for locality; use ORDER BY start_pos for position-sorted queries within a protein.",
        "primary_key": [],
        "foreign_keys": {"acc": "entries.acc", "taxid": "entries.taxid"},
        "columns": {
            "convenience": [
                "acc", "reviewed", "taxid", "organism_name", "seq_length",
                "type", "start_pos", "end_pos", "start_modifier", "end_modifier",
                "description", "feature_id", "evidence_codes", "location_sequence",
                "original_sequence", "alternative_sequences",
                "ligand_name", "ligand_id", "ligand_label", "ligand_note",
            ],
            "nested": ["feature_residual"],
        },
    },
    "xrefs": {
        "description": "One row per cross-reference to an external database (PDB, Ensembl, GO, InterPro, etc).",
        "primary_key": [],
        "foreign_keys": {"acc": "entries.acc", "taxid": "entries.taxid"},
        "columns": {
            "convenience": [
                "acc", "reviewed", "taxid",
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
                "acc", "reviewed", "taxid",
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
                "acc", "reviewed", "taxid",
                "reference_number", "citation_type", "citation_id", "title",
                "authors", "authoring_group", "publication_date",
                "journal", "volume", "first_page", "last_page",
                "submission_database", "citation_xrefs",
                "reference_positions", "reference_comments", "evidences",
            ],
            "nested": ["reference_residual"],
        },
    },
    "accession_map": {
        "description": "One row per primary or secondary accession, mapping to the current primary accession. The lookup table for point queries and for resolving retired accessions.",
        "primary_key": ["acc", "primary_acc"],
        "foreign_keys": {"primary_acc": "entries.acc"},
        "columns": {
            "convenience": ["acc", "primary_acc", "is_primary", "reviewed", "taxid"],
            "nested": [],
        },
    },
}


# ─── Core pipeline ──────────────────────────────────────────────────────

def _annotate_schema(schema: pa.Schema, label: str, file_meta: dict[str, str]) -> pa.Schema:
    """Attach description / category / source_path metadata to every field
    (plan H.2) and the release-level key/values to the schema.  A column
    without a COLUMN_DESCRIPTIONS entry is a build failure, so files and
    manifest can never disagree about what a column means."""
    cats = TABLE_META.get(label, {}).get("columns", {})
    missing = [f.name for f in schema if (label, f.name) not in COLUMN_DESCRIPTIONS]
    if missing:
        raise RuntimeError(f"{label}: no COLUMN_DESCRIPTIONS entry for {missing}")
    fields = []
    for f in schema:
        md = {"description": COLUMN_DESCRIPTIONS[(label, f.name)]}
        if f.name in cats.get("convenience", []):
            md["category"] = "convenience"
        elif f.name in cats.get("nested", []):
            md["category"] = "nested"
        src = COLUMN_SOURCES.get((label, f.name))
        if src:
            md["source_path"] = src
        fields.append(f.with_metadata(md))
    return pa.schema(fields, metadata=file_meta)


def _file_metadata(release) -> dict[str, str]:
    """Key/value metadata written into every Parquet footer (plan H.4/H.6/A7)."""
    return {"uniprot_release": release or "", "schema_version": SCHEMA_VERSION,
            "license": DATA_LICENSE, "generator": GENERATOR, "zstd_level": str(ZSTD_LEVEL)}


def _bloom_kwargs(label: str) -> dict:
    """Bloom-filter writer options (plan Part A, B1).  Deferred: PyArrow 23
    cannot write bloom filters (Phase 0.1), so this returns {} and the writer
    is identical with or without B1."""
    return {}


def stream_to_parquet(con, sql, table_dir, batch_size, label="table", sort_order=None,
                      partition_column="reviewed", release=None, sentinel_path=None):
    """Stream DuckDB result → Hive-partitioned Parquet files in bounded-memory batches.

    DuckDB executes the query lazily and yields Arrow record batches of
    `batch_size` rows.  Batches are accumulated into files of ~TARGET_FILE_BYTES
    under ``<table_dir>/review_status=<swissprot|trembl>/`` (plan Part D):
    rows arrive sorted ``reviewed DESC`` so every ``true`` row precedes every
    ``false`` row, a batch contains at most one flip, and the writer switches
    partition directory (and restarts the file counter) exactly once.  The
    ``reviewed`` column itself stays stored in every file.

    The Arrow schema is determined once by DuckDB before the first batch
    is yielded — all batches share the same schema.

    Memory model: only one Arrow batch is held at a time (plus whatever
    DuckDB needs for the ORDER BY spill).

    Atomicity: files are written to ``<partition>/.tmp/`` and moved to their
    final location only after the whole table has been written, so nothing
    is visible under the final directories until the table is complete.
    ``.tmp/`` is removed on error and any leftover from a killed run is
    removed before writing: DuckDB's ``**`` glob descends into hidden
    directories, so a stale partial file there would be double-counted by
    every ``<table>/**/*.parquet`` reader.

    The publish itself (drop stale finals, move new files in) is not atomic:
    a kill inside it leaves a mix of old and new files.  So the table's
    completion sentinel (``sentinel_path``, see ``_sentinel_path``) is deleted
    before the write starts and written, with the final file list, only
    after the publish ends; --skip-existing trusts a table only when the
    sentinel exists and lists exactly the files on disk.

    Args:
        con: DuckDB connection
        sql: SQL query to execute
        table_dir: Output directory for the table
        batch_size: Rows per Arrow batch
        label: Table name (for file names and logging)
        sort_order: List of sort order strings (e.g. ["reviewed DESC", "taxid ASC"])
                   to embed in Parquet footer metadata (PyArrow 16.0+).
        partition_column: boolean column that decides the partition directory
        release: UniProt release name, written into the footer metadata
        sentinel_path: where to write the completion sentinel (None: no sentinel)

    Returns (total_rows, file_list, arrow_schema); file paths are relative to
    ``table_dir`` (``review_status=swissprot/entries_00001.parquet``).
    """
    eprint(f"  Querying DuckDB for {label}...")
    reader = con.sql(sql).to_arrow_reader(batch_size=batch_size)
    os.makedirs(table_dir, exist_ok=True)
    t0 = time.time()
    if sentinel_path and os.path.exists(sentinel_path):
        os.remove(sentinel_path)       # the table is incomplete from here on

    total_rows = 0
    arrow_schema = None
    sorting_columns = None
    pending = []                       # (tmp_path, final_path, relative_path)
    st = {"side": None, "writer": None, "file_num": 0, "path": None}

    def part_dir(side):
        return os.path.join(table_dir, f"{PARTITION_KEY}={side}")

    def remove_tmp_dirs():
        for side in PARTITION_VALUES.values():
            shutil.rmtree(os.path.join(part_dir(side), ".tmp"), ignore_errors=True)

    def open_writer():
        st["file_num"] += 1
        tmp = os.path.join(part_dir(st["side"]), ".tmp")
        os.makedirs(tmp, exist_ok=True)
        st["path"] = os.path.join(tmp, f"{label}_{st['file_num']:05d}.parquet")
        kwargs = {"compression": "zstd", "compression_level": ZSTD_LEVEL,
                  "write_page_index": True}
        if sorting_columns:
            kwargs["sorting_columns"] = sorting_columns
        kwargs.update(_bloom_kwargs(label))
        st["writer"] = pq.ParquetWriter(st["path"], arrow_schema, **kwargs)

    def close_writer():
        if st["writer"] is None:
            return
        st["writer"].close()
        st["writer"] = None
        final = os.path.join(part_dir(st["side"]), os.path.basename(st["path"]))
        pending.append((st["path"], final, os.path.relpath(final, table_dir)))

    def write_part(tbl):
        if st["writer"] is None:
            open_writer()
        st["writer"].write_table(tbl, row_group_size=ROW_GROUP_SIZE)
        if os.path.getsize(st["path"]) >= TARGET_FILE_BYTES:
            close_writer()

    remove_tmp_dirs()                  # leftovers of a killed earlier run
    try:
        for record_batch in reader:
            tbl = pa.Table.from_batches([record_batch])
            if tbl.num_rows == 0:
                continue
            if arrow_schema is None:
                arrow_schema = _annotate_schema(tbl.schema, label, _file_metadata(release))
                sorting_columns = build_sorting_columns(sort_order, arrow_schema) if sort_order else None
            # Re-wrap every batch so the written schema carries the field metadata.
            tbl = pa.Table.from_arrays(list(tbl.columns), schema=arrow_schema)
            total_rows += tbl.num_rows

            flags = tbl.column(partition_column)
            if flags.null_count:
                raise RuntimeError(f"{label}: {partition_column} has NULLs; cannot partition")
            n_true = pc.sum(flags).as_py() or 0
            first = flags[0].as_py()
            if first and n_true < tbl.num_rows:            # the flip is inside this batch
                parts = [(True, tbl.slice(0, n_true)), (False, tbl.slice(n_true))]
            else:
                if not first and n_true:
                    raise RuntimeError(f"{label}: batch not sorted {partition_column} DESC")
                parts = [(first, tbl)]

            for flag, part in parts:
                side = PARTITION_VALUES[flag]
                if st["side"] != side:
                    if st["side"] is not None and flag:           # true after false: input is not sorted
                        raise RuntimeError(f"{label}: {partition_column} flipped back to true")
                    close_writer()
                    st["side"], st["file_num"] = side, 0
                write_part(part)
            elapsed = time.time() - t0
            eprint(f"    {label}: {total_rows:,} rows so far "
                   f"({elapsed:.0f}s, {total_rows / elapsed if elapsed else 0:,.0f} rows/s)")

        close_writer()
        # All-or-nothing publish.  First drop Parquet files a previous run left
        # under the partition directories that this run did not produce (a
        # rebuild with a larger file target or a different zstd level writes
        # fewer files); glob readers would otherwise see duplicated rows.
        new_finals = {final for _, final, _ in pending}
        for side in PARTITION_VALUES.values():
            d = part_dir(side)
            if os.path.isdir(d):
                for name in os.listdir(d):
                    path = os.path.join(d, name)
                    if name.endswith(".parquet") and path not in new_finals:
                        os.remove(path)
        for tmp_path, final_path, _ in pending:
            shutil.move(tmp_path, final_path)
        remove_tmp_dirs()
    except Exception:
        # On error, close the writer and drop the partial files: nothing of
        # this table is published, and nothing is left for a glob to find.
        if st["writer"] is not None:
            st["writer"].close()
        remove_tmp_dirs()
        raise

    files = [rel for _, _, rel in pending]
    if sentinel_path:
        _write_sentinel(sentinel_path, label, files, total_rows, release)
    eprint(f"  {label}: {total_rows:,} rows in {len(files)} files ({time.time()-t0:.1f}s)")
    return total_rows, files, arrow_schema


# ─── Per-table completion sentinels ─────────────────────────────────────
# ``<outdir>/.complete/<table>.json`` records the file list a finished write
# published.  It lives outside the table directory (which holds Parquet only:
# directory readers treat every file under ``<table>/`` as Parquet) and is
# the only thing --skip-existing trusts: a kill during the publish window
# leaves files on disk but no sentinel, so the table is rebuilt.

def _sentinel_path(outdir, table_name):
    return os.path.join(outdir, ".complete", f"{table_name}.json")


def _write_sentinel(path, table_name, files, row_count, release):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"table": table_name, "files": list(files), "row_count": row_count,
                   "release": release, "completed_at": datetime.now(timezone.utc).isoformat()},
                  f, indent=2)
    os.replace(tmp, path)              # the sentinel itself appears atomically


def _read_sentinel(path):
    """The sentinel's contents, or None if absent or unreadable."""
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _table_is_complete(outdir, table_name, table_dir):
    """True only if the sentinel exists and lists exactly the files on disk."""
    sentinel = _read_sentinel(_sentinel_path(outdir, table_name))
    if sentinel is None:
        return False
    return sentinel.get("files") == _list_table_files(table_dir)


def _list_table_files(table_dir):
    """Parquet files of a table, relative to table_dir, in partition order."""
    from glob import glob
    return sorted(os.path.relpath(p, table_dir)
                  for p in glob(os.path.join(table_dir, f"{PARTITION_KEY}=*", "*.parquet")))


def _partition_row_counts(table_dir, files):
    counts = {}
    for rel in files:
        side = rel.split("/")[0].split("=", 1)[1]
        counts[side] = counts.get(side, 0) + pq.read_metadata(os.path.join(table_dir, rel)).num_rows
    return counts


def _file_details(table_dir, rel_path):
    """Size, SHA-256, MD5 and taxid range of one Parquet file (plan F.2.1 /
    D.5).  One streamed pass while the file is still in page cache."""
    path = os.path.join(table_dir, rel_path)
    sha, md5 = hashlib.sha256(), hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
            md5.update(chunk)
    meta = pq.read_metadata(path)
    mins, maxs = [], []
    if "taxid" in meta.schema.names:
        col = meta.schema.names.index("taxid")        # top-level scalar: index is exact
        for i in range(meta.num_row_groups):
            stats = meta.row_group(i).column(col).statistics
            if stats and stats.has_min_max:
                mins.append(stats.min)
                maxs.append(stats.max)
    return {"size_bytes": os.path.getsize(path), "sha256": sha.hexdigest(), "md5": md5.hexdigest(),
            "taxid_min": min(mins) if mins else None, "taxid_max": max(maxs) if maxs else None}


def _hash_file(path):
    # Return order (sha256, md5) matches hash_file in release_manifest.py —
    # keep the two in sync.
    sha, md5 = hashlib.sha256(), hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
            md5.update(chunk)
    return sha.hexdigest(), md5.hexdigest()


def _write_checksums(outdir, manifest, extra_files):
    """lake/SHA256SUMS.txt (sha256sum -c format) and RELEASE.metalink (Metalink 4,
    RFC 5854) at the lake root (plan F.2.1).
    ``extra_files`` are lake-root files hashed after the Parquet files."""
    import xml.etree.ElementTree as ET

    entries = []                                 # (rel path from outdir, size, sha256, md5)
    for table, info in manifest["tables"].items():
        for rel in info["files"]:
            d = info["file_details"][rel]
            entries.append((f"{table}/{rel}", d["size_bytes"], d["sha256"], d["md5"]))
    for name in extra_files:
        path = os.path.join(outdir, name)
        sha, md5 = _hash_file(path)
        entries.append((name, os.path.getsize(path), sha, md5))

    sums_path = os.path.join(outdir, "SHA256SUMS.txt")
    with open(sums_path, "w") as f:
        for rel, _, sha, _ in entries:
            f.write(f"{sha}  {rel}\n")
    eprint(f"  Wrote {sums_path} ({len(entries)} files)")

    def metalink(items, path):
        ns = "urn:ietf:params:xml:ns:metalink"
        root = ET.Element("metalink", xmlns=ns)
        ET.SubElement(root, "published").text = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for rel, size, sha, md5 in items:
            fe = ET.SubElement(root, "file", name=rel)
            ET.SubElement(fe, "size").text = str(size)
            ET.SubElement(fe, "hash", type="sha-256").text = sha
            ET.SubElement(fe, "hash", type="md5").text = md5
            ET.SubElement(fe, "url").text = rel
        ET.indent(root)
        ET.ElementTree(root).write(path, encoding="UTF-8", xml_declaration=True)

    # Root only: a per-table RELEASE.metalink would break directory reads in
    # Polars / PyArrow / pandas, which treat every file under <table>/ as
    # Parquet (plan D.1 reader table).  Table directories hold Parquet only.
    metalink(entries, os.path.join(outdir, "RELEASE.metalink"))
    eprint("  Wrote RELEASE.metalink")


def _add_file_details(table_info, table_dir):
    """Fill file_details (per file) and size_bytes (sum) on a manifest table entry."""
    table_info["file_details"] = {rel: _file_details(table_dir, rel) for rel in table_info["files"]}
    table_info["size_bytes"] = sum(d["size_bytes"] for d in table_info["file_details"].values())


def _partitioning_block(name, files, table_dir, partition_column):
    """The manifest's per-table partitioning descriptor (plan D.3)."""
    return {
        "scheme": "hive",
        "keys": [{"name": PARTITION_KEY, "type": "string",
                  "values": sorted({f.split("/")[0].split("=", 1)[1] for f in files}),
                  "derived_from": partition_column,
                  "row_counts": _partition_row_counts(table_dir, files)}],
        "note": ("accession_map is excluded from any future second partition level (plan D.5)"
                 if name == "accession_map" else ""),
    }


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


# ─── Column semantic descriptions ───────────────────────────────────────
# Maps (table, column) → human-readable description for datapackage.json.
# Arrow types and nullability come from the actual Parquet schema; these
# descriptions encode the *meaning* that Parquet metadata cannot capture.

COLUMN_DESCRIPTIONS = {
    # ── entries ──
    ("entries", "acc"):                "UniProtKB primary accession (e.g. P05067). Stable identifier; unique per entry.",
    ("entries", "id"):                 "UniProtKB mnemonic entry name (e.g. A4_HUMAN). May change between releases.",
    ("entries", "reviewed"):           "True for Swiss-Prot (manually reviewed); false for TrEMBL (automated annotation).",
    ("entries", "secondary_accs"):     "List of secondary (merged/demerged) accessions that resolve to this entry.",
    ("entries", "taxid"):              "NCBI taxonomy ID of the source organism (e.g. 9606 for Homo sapiens).",
    ("entries", "organism_name"):      "Scientific name of the source organism.",
    ("entries", "organism_common"):    "Common name of the source organism (e.g. 'Human'). May be null.",
    ("entries", "lineage"):            "Full taxonomic lineage as a list of taxon names, root to species.",
    ("entries", "division"):           "UniProt taxonomic division (archaea, bacteria, fungi, human, invertebrates, mammals, plants, rodents, vertebrates, viruses, unclassified), derived from lineage and taxid.",
    ("entries", "gene_names"):         "List of primary gene names across all genes associated with this entry.",
    ("entries", "gene_name"):          "Primary gene name (first of gene_names).",
    ("entries", "gene_synonyms"):      "Flattened list of gene name synonyms across all genes.",
    ("entries", "protein_name"):       "Recommended (Swiss-Prot) or submitted (TrEMBL) full protein name.",
    ("entries", "alt_protein_names"):  "List of alternative full protein names from proteinDescription.alternativeNames.",
    ("entries", "protein_flag"):       "Protein description flag: 'Precursor', 'Fragment', 'Precursor; Fragment', or null.",
    ("entries", "ec_numbers"):         "Distinct EC (Enzyme Commission) numbers extracted from all naming blocks.",
    ("entries", "protein_existence"):  "Protein existence evidence level (e.g. 'Evidence at protein level').",
    ("entries", "annotation_score"):   "UniProt annotation score (1-5). Higher = more richly annotated.",
    ("entries", "sequence"):           "Full amino acid sequence (one-letter IUPAC codes).",
    ("entries", "seq_length"):         "Sequence length in amino acids.",
    ("entries", "seq_mass"):           "Molecular weight of the unprocessed precursor in Daltons.",
    ("entries", "seq_md5"):            "MD5 hash of the sequence (lowercase hex). Useful for deduplication.",
    ("entries", "seq_crc64"):          "CRC64 checksum of the sequence. Used by UniProt for integrity checks.",
    ("entries", "go_ids"):             "Distinct Gene Ontology term IDs (e.g. GO:0005634) from cross-references.",
    ("entries", "go_terms"):           "GO annotations as {id, aspect (P/F/C), term, evidence_type}; go_ids is the flat id list.",
    ("entries", "xref_dbs"):           "Distinct database names referenced in cross-references (e.g. PDB, Ensembl).",
    ("entries", "proteome_ids"):       "Distinct UniProt proteome ids (UP…) from cross-references, sorted.",
    ("entries", "keyword_ids"):        "UniProt keyword IDs (e.g. KW-0181).",
    ("entries", "keyword_names"):      "UniProt keyword names (e.g. 'Complete proteome').",
    ("entries", "keyword_categories"): "Keyword categories aligned with keyword_ids (e.g. Molecular function, Biological process).",
    ("entries", "first_public"):       "Date the entry was first made public in UniProtKB.",
    ("entries", "last_modified"):      "Date of the last annotation update.",
    ("entries", "last_seq_modified"):  "Date of the last sequence update.",
    ("entries", "entry_version"):      "Entry version number (incremented on any change).",
    ("entries", "seq_version"):        "Sequence version number (incremented only on sequence changes).",
    ("entries", "feature_count"):      "Number of positional features (domains, sites, variants, etc.).",
    ("entries", "xref_count"):         "Number of cross-references to external databases.",
    ("entries", "comment_count"):      "Number of comment annotations (function, disease, etc.).",
    ("entries", "reference_count"):    "Number of literature/submission references.",
    ("entries", "pubmed_ids"):         "Distinct PubMed ids cited by the entry, sorted numerically, stored as strings.",
    ("entries", "uniparc_id"):         "UniParc identifier (UPI) linking to the sequence archive.",
    ("entries", "entry_type"):         "Raw entryType string (e.g. 'UniProtKB reviewed (Swiss-Prot)'). Lossless.",
    ("entries", "extra_attributes"):   "Nested struct with countByCommentType, countByFeatureType, uniParcId.",
    ("entries", "organism_residual"): "Organism fields not in the convenience columns (synonyms, evidences); merge with taxid, organism_name, organism_common, lineage to rebuild organism.",
    ("entries", "protein_desc_residual"): "proteinDescription minus flag (kept whole: recommendedName, alternativeNames, submissionNames, contains, includes, ...); protein_name / alt_protein_names / ec_numbers are projections of it.",
    ("entries", "genes_full"): "Full genes array (geneName, synonyms, orfNames, orderedLocusNames with evidences); gene_names / gene_synonyms are projections of it.",
    ("entries", "keywords_full"): "Full keywords array (id, name, category, evidences); keyword_ids / keyword_names / keyword_categories are projections of it.",
    ("entries", "organism_hosts"):     "Full nested organismHosts array (for viruses — host organisms). May be null.",
    ("entries", "gene_locations"):     "Full nested geneLocations array (mitochondrial, plastid, etc.). May be null.",

    # ── features ──
    ("features", "acc"):               "Parent entry's primary accession. Foreign key → entries.acc.",
    ("features", "reviewed"):     "True if the parent entry is Swiss-Prot (reviewed).",
    ("features", "taxid"):             "NCBI taxonomy ID of the parent entry. Foreign key → entries.taxid.",
    ("features", "organism_name"):     "Scientific name of the parent entry's organism (denormalized for convenience).",
    ("features", "seq_length"):        "Sequence length of the parent entry (denormalized for coverage calculations).",
    ("features", "type"):              "Feature type (e.g. 'Domain', 'Signal peptide', 'Natural variant').",
    ("features", "start_pos"):         "Start position in the sequence (1-based, inclusive). Cast to integer.",
    ("features", "end_pos"):           "End position in the sequence (1-based, inclusive). Cast to integer.",
    ("features", "start_modifier"):    "Position modifier: 'EXACT', 'OUTSIDE', 'UNSURE', or null.",
    ("features", "end_modifier"):      "Position modifier: 'EXACT', 'OUTSIDE', 'UNSURE', or null.",
    ("features", "description"):       "Free-text description of the feature annotation.",
    ("features", "feature_id"):        "UniProt feature identifier (e.g. PRO_0000001234). May be null.",
    ("features", "evidence_codes"):    "List of evidence codes (e.g. ECO:0000269) supporting this feature.",
    ("features", "location_sequence"): "Sequence the feature location refers to (isoform or alternative sequence), when not the canonical one.",
    ("features", "original_sequence"): "Original amino acids before the variant/mutation. Null for non-variant features.",
    ("features", "alternative_sequences"): "List of alternative amino acid sequences for variant features.",
    ("features", "ligand_name"):       "Name of the bound ligand (for Binding site features). May be null.",
    ("features", "ligand_id"):         "ChEBI or other identifier for the ligand. May be null.",
    ("features", "ligand_label"):      "Label distinguishing multiple ligands in the same entry. May be null.",
    ("features", "ligand_note"):       "Additional note about the ligand binding. May be null.",
    ("features", "feature_residual"): "Feature fields not in the convenience columns: evidences (full), featureCrossReferences, ligand, ligandPart, alternativeSequence. bin/reconstruct.py merges it with the convenience columns.",

    # ── xrefs ──
    ("xrefs", "acc"):                  "Parent entry's primary accession. Foreign key → entries.acc.",
    ("xrefs", "reviewed"):        "True if the parent entry is Swiss-Prot (reviewed).",
    ("xrefs", "taxid"):               "NCBI taxonomy ID of the parent entry. Foreign key → entries.taxid.",
    ("xrefs", "database"):            "External database name (e.g. 'PDB', 'Ensembl', 'GO', 'InterPro').",
    ("xrefs", "id"):                  "Identifier in the external database (e.g. '1ABC' for PDB).",
    ("xrefs", "properties"):          "Database-specific key-value properties as a nested struct/list.",
    ("xrefs", "isoform_id"):          "Isoform accession if this xref is specific to an isoform. May be null.",
    ("xrefs", "evidences"):           "Evidence records supporting this cross-reference. May be null.",

    # ── comments ──
    ("comments", "acc"):               "Parent entry's primary accession. Foreign key → entries.acc.",
    ("comments", "reviewed"):     "True if the parent entry is Swiss-Prot (reviewed).",
    ("comments", "taxid"):            "NCBI taxonomy ID of the parent entry. Foreign key → entries.taxid.",
    ("comments", "comment_type"):     "Comment type (e.g. 'FUNCTION', 'DISEASE', 'SUBCELLULAR LOCATION').",
    ("comments", "text_value"):       "Concatenated free-text values (paragraphs joined by double newline). Null for structured-only comments.",
    ("comments", "comment"):          "Full comment as JSON string (lossless). Use comment->>'$.key' to extract fields.",

    # ── publications ──
    ("publications", "acc"):           "Parent entry's primary accession. Foreign key → entries.acc.",
    ("publications", "reviewed"): "True if the parent entry is Swiss-Prot (reviewed).",
    ("publications", "taxid"):        "NCBI taxonomy ID of the parent entry. Foreign key → entries.taxid.",
    ("publications", "reference_number"): "Position of this reference in the entry's reference list (1-based).",
    ("publications", "citation_type"): "Citation type: 'journal article', 'submission', 'book', 'patent', etc.",
    ("publications", "citation_id"):  "Citation identifier (PubMed ID, DOI, or other). May be null for submissions.",
    ("publications", "title"):        "Publication title. May be null for submissions without titles.",
    ("publications", "authors"):      "List of author names. May be null.",
    ("publications", "authoring_group"): "Authoring group/consortium name. May be null.",
    ("publications", "publication_date"): "Publication or submission date (string, as provided by UniProt).",
    ("publications", "journal"):      "Journal name. Null for non-journal citations.",
    ("publications", "volume"):       "Journal volume. Null for non-journal citations.",
    ("publications", "first_page"):   "First page number. Null for non-journal citations.",
    ("publications", "last_page"):    "Last page number. Null for non-journal citations.",
    ("publications", "submission_database"): "Database name for submissions (e.g. 'EMBL/GenBank/DDBJ'). Null otherwise.",
    ("publications", "citation_xrefs"): "Cross-references within the citation (PubMed, DOI, etc.).",
    ("publications", "reference_positions"): "List of reference position strings (e.g. 'NUCLEOTIDE SEQUENCE').",
    ("publications", "reference_comments"): "List of reference comment structs (scope, source, etc.).",
    ("publications", "evidences"):    "Evidence records for this reference. May be null.",
    ("publications", "reference_residual"): "Reference fields not in the convenience columns, including citation extras (bookName, editors, publisher, address, institute, patentNumber, locator). bin/reconstruct.py merges it with the convenience columns.",

    # --variant-children builds (benchmarks/VARIANT_EVALUATION.md): one VARIANT column
    ("features", "data"):     "Whole feature as a DuckDB VARIANT (--variant-children builds only).",
    ("xrefs", "data"):        "Whole cross-reference as a DuckDB VARIANT (--variant-children builds only).",
    ("comments", "data"):     "Whole comment as a DuckDB VARIANT (--variant-children builds only).",
    ("publications", "data"): "Whole reference as a DuckDB VARIANT (--variant-children builds only).",

    # accession_map
    ("accession_map", "acc"):         "Primary or secondary accession (the lookup key). A secondary accession can map to more than one primary (split entries).",
    ("accession_map", "primary_acc"): "Current primary accession the key resolves to (FK → entries.acc).",
    ("accession_map", "is_primary"):  "True when acc == primary_acc.",
    ("accession_map", "reviewed"):    "True if the primary entry is Swiss-Prot (copied from entries.reviewed).",
    ("accession_map", "taxid"):       "NCBI taxonomy ID of the primary entry (copied from entries.taxid).",
}


# ─── Arrow → Frictionless type mapping ──────────────────────────────────

def _arrow_type_to_frictionless(arrow_type_str: str) -> dict:
    """Map an Arrow type string to a Frictionless Data Package field type + format.

    Returns a dict with 'type' and optionally 'format' and 'arrayItem'.
    Frictionless spec doesn't natively support nested structs or arrays,
    so we use 'array' and 'object' as type with richer_type for the Arrow detail.
    """
    s = arrow_type_str.strip()

    # Simple scalar types
    if s in ("bool", "boolean"):
        return {"type": "boolean"}
    if s in ("int32", "int64", "uint32", "uint64", "int16", "uint16", "int8", "uint8"):
        return {"type": "integer"}
    if s in ("float", "double", "float16", "float32", "float64"):
        return {"type": "number"}
    if s in ("string", "utf8", "large_string", "large_utf8"):
        return {"type": "string"}
    if s == "date32[day]":
        return {"type": "date"}
    if s.startswith("timestamp"):
        return {"type": "datetime"}
    if s == "json":
        return {"type": "object", "format": "json"}

    # List types
    if s.startswith("list<"):
        return {"type": "array"}

    # Struct/nested types
    if s.startswith("struct<") or s.startswith("map<"):
        return {"type": "object"}

    # Fallback
    return {"type": "string", "format": "default"}


def _build_datapackage(manifest: dict, release: str) -> dict:
    """Build a Frictionless Data Package descriptor from a manifest.

    The descriptor follows https://specs.frictionlessdata.io/data-package/
    and https://specs.frictionlessdata.io/tabular-data-resource/ with
    extensions for Arrow type detail, nullability, and sort order.

    This makes the lake self-describing and machine-readable per FAIR
    principles (Findable, Accessible, Interoperable, Reusable).
    """
    resources = []

    for table_name, table_info in manifest["tables"].items():
        meta = TABLE_META.get(table_name, {})

        # Build field descriptors from the actual Parquet schema
        fields = []
        for col in table_info.get("columns", []):
            col_name = col["name"]
            frictionless = _arrow_type_to_frictionless(col["type"])

            field = {
                "name": col_name,
                "type": frictionless["type"],
                "description": COLUMN_DESCRIPTIONS.get((table_name, col_name), ""),
                "constraints": {
                    "required": not col["nullable"],
                },
                "arrowType": col["type"],
                "nullable": col["nullable"],
            }
            if "format" in frictionless:
                field["format"] = frictionless["format"]

            # Mark which category this column belongs to
            categories = meta.get("columns", {})
            if col_name in categories.get("convenience", []):
                field["columnCategory"] = "convenience"
            elif col_name in categories.get("nested", []):
                field["columnCategory"] = "nested"

            fields.append(field)

        # Build the resource descriptor
        resource = {
            "name": table_name,
            "description": meta.get("description", table_info.get("description", "")),
            "path": [f"{table_name}/{f}" for f in table_info.get("files", [])],
            "format": "parquet",
            "mediatype": "application/vnd.apache.parquet",
            "encoding": "binary",
            "compression": "zstd",
            "schema": {
                "fields": fields,
                "primaryKey": meta.get("primary_key", table_info.get("primary_key", [])),
                "foreignKeys": [
                    {
                        "fields": [fk_col],
                        "reference": {
                            "resource": ref.split(".")[0],
                            "fields": [ref.split(".")[1]] if "." in ref else [ref],
                        },
                    }
                    for fk_col, ref in meta.get("foreign_keys", table_info.get("foreign_keys", {})).items()
                ],
            },
            "rowCount": table_info.get("row_count", 0),
            "bytes": table_info.get("size_bytes"),
            "sortOrder": table_info.get("sort_order", []),
            "partitioning": table_info.get("partitioning"),
        }

        resources.append(resource)

    datapackage = {
        "$schema": "https://specs.frictionlessdata.io/schemas/data-package.json",
        "name": "uniprot-parquet",
        "title": "UniProtKB Parquet Data Lake",
        "description": (
            "A denormalized, analysis-ready Parquet representation of the complete "
            "UniProtKB database. Five sorted tables (entries, features, xrefs, "
            "comments, publications) preserve all upstream data losslessly while "
            "providing flattened convenience columns for common query patterns; "
            "accession_map resolves primary and secondary accessions. "
            "Per-file SHA-256 hashes are in SHA256SUMS.txt and manifest.json file_details."
        ),
        "homepage": "https://github.com/dlrice/uniprot-parquet",
        "version": SCHEMA_VERSION,
        "licenses": [
            {
                "name": "MIT",
                "path": "https://opensource.org/licenses/MIT",
            },
            {
                "name": "CC-BY-4.0",
                "path": "https://creativecommons.org/licenses/by/4.0/",
                "title": "UniProt data is licensed under Creative Commons Attribution 4.0",
            },
        ],
        "sources": [
            {
                "title": "UniProt Knowledgebase (UniProtKB)",
                "path": "https://www.uniprot.org/",
            },
        ],
        "contributors": [
            {
                "title": "Daniel Rice",
                "role": "author",
            },
        ],
        "keywords": [
            "UniProtKB", "UniProt", "proteomics", "bioinformatics",
            "Parquet", "data lake", "FAIR",
        ],
        "created": manifest.get("generated_at", ""),
        "uniprotRelease": release,
        "resources": resources,
    }

    return datapackage


# ─── Main ───────────────────────────────────────────────────────────────

def main():
    global TARGET_FILE_BYTES, ZSTD_LEVEL
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
    parser.add_argument(
        "--variant-children", action="store_true",
        help="Use DuckDB VARIANT columns on child tables (features, xrefs, "
             "comments, publications) instead of hand-extracted convenience "
             "columns.  Evaluation only (benchmarks/VARIANT_EVALUATION.md); "
             "requires DuckDB ≥1.5 and a PyArrow that can write VARIANT "
             "(PyArrow 23 cannot: 'Unsupported Arrow type VARIANT').",
    )
    parser.add_argument(
        "--only", default=None,
        help="Comma-separated table names to build (default: all). For benchmarks.",
    )
    parser.add_argument(
        "--target-file-bytes", type=int, default=None,
        help=f"Roll to a new Parquet file at this size (default {TARGET_FILE_BYTES}; plan F.4).",
    )
    parser.add_argument(
        "--zstd-level", type=int, default=None,
        help=f"zstd compression level for Parquet pages (default {ZSTD_LEVEL}; plan H.1).",
    )
    args = parser.parse_args()

    if args.target_file_bytes:
        TARGET_FILE_BYTES = args.target_file_bytes
    if args.zstd_level is not None:
        ZSTD_LEVEL = args.zstd_level
    table_defs = TABLE_DEFS
    if args.only:
        wanted = {t.strip() for t in args.only.split(",")}
        unknown = wanted - {d[0] for d in TABLE_DEFS}
        if unknown:
            parser.error(f"--only: unknown table(s) {sorted(unknown)}")
        table_defs = [d for d in TABLE_DEFS if d[0] in wanted]

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
    eprint(f"  Writer:    zstd level {ZSTD_LEVEL}, {_human_size(TARGET_FILE_BYTES)} files, "
           f"{ROW_GROUP_SIZE:,}-row groups" + (f", only {args.only}" if args.only else ""))
    eprint()

    # ── Init DuckDB ──
    con = init_duckdb(args.memory_limit, args.threads, args.temp_dir)
    if args.variant_children:
        eprint("  VARIANT child tables enabled (DuckDB ≥1.5 built-in type)")
    staging_dir = os.path.join(outdir, ".staging")
    staging_path = os.path.join(staging_dir, "staged.parquet")

    try:
        json_read_clause = build_read_clause(jsonl_path)

        # ── Determine which tables to write ──
        skip_set = set()
        if args.skip_existing:
            for name, _, _ in table_defs:
                table_dir = os.path.join(outdir, name)
                if not os.path.isdir(table_dir):
                    continue
                existing = _list_table_files(table_dir)
                if _table_is_complete(outdir, name, table_dir):
                    skip_set.add(name)
                    eprint(f"  SKIP {name} (complete: {len(existing)} Parquet files, --skip-existing)")
                elif existing:
                    eprint(f"  REBUILD {name}: {len(existing)} Parquet files on disk but no "
                           f"completion sentinel matching them (interrupted publish?)")

        tables_to_write = {name for name, _, _ in table_defs} - skip_set

        # ── Stage JSONL → Parquet (parse JSON once, read Parquet 5× faster) ──
        if tables_to_write:
            eprint("\n--- PARQUET STAGING ---")
            os.makedirs(staging_dir, exist_ok=True)
            read_clause, staged_bytes = stage_to_parquet(con, json_read_clause, staging_path)
        else:
            eprint("\n--- PARQUET STAGING (skipped — all tables already written) ---")
            read_clause = json_read_clause
            staged_bytes = None

        # ── Discover schema (single source of truth for all SQL generation) ──
        # Only needed when we have tables to write (staging must have occurred).
        if tables_to_write:
            schema_paths = discover_schema_paths(staging_path)
            eprint(f"  Schema: {len(schema_paths)} field paths discovered")
            if not schema_paths:
                eprint("FATAL: staging produced an empty schema — no field paths found")
                sys.exit(1)
            # Refuse to build if the input is not a UniProtKB dump, if a declared
            # type would drop a nested field (plan G.2), or if a promoted struct
            # has a child no column carries.
            check_required_paths(schema_paths)
            check_declared_types(con, schema_paths)
            check_promoted_paths(schema_paths)
        else:
            schema_paths = set()

        # Log notable absent fields for diagnostics
        missing_optional = _OPTIONAL_ENTRY_FIELDS - {p for p in schema_paths if "." not in p}
        if missing_optional:
            eprint(f"  Optional fields not in data (will be NULL): {', '.join(sorted(missing_optional))}")
        if ("proteinDescription.submittedNames" not in schema_paths
                and "proteinDescription.submissionNames" not in schema_paths):
            eprint("  Note: proteinDescription.submittedNames/submissionNames absent (normal for Swiss-Prot-only data)")
        ec_sources = [s for s in ["recommendedName", "alternativeNames", "submittedNames", "submissionNames"]
                      if f"proteinDescription.{s}.ecNumbers" in schema_paths]
        if ec_sources:
            eprint(f"  EC numbers found in: {', '.join(ec_sources)}")
        else:
            eprint("  Note: no ecNumbers in any naming block (ec_numbers column will be empty)")

        # ── Write tables ──
        t_total = time.time()
        manifest_tables = {}

        for name, sort_order, partition_column in table_defs:
            eprint(f"\n--- {name.upper()} ---")
            table_dir = os.path.join(outdir, name)

            meta = TABLE_META.get(name, {})

            if name in skip_set:
                existing = _list_table_files(table_dir)
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
                    "partitioning": _partitioning_block(name, existing, table_dir, partition_column),
                    "columns": _schema_to_dict(schema),
                    "column_categories": meta.get("columns", {}),
                }
                _add_file_details(manifest_tables[name], table_dir)
            else:
                if getattr(args, "variant_children", False) and name not in ("entries", "accession_map"):
                    sql_template = _VARIANT_BUILDERS[name]()
                else:
                    sql_template = _SQL_BUILDERS[name](schema_paths)
                sql = sql_template.format(read_clause=read_clause)
                row_count, files, arrow_schema = stream_to_parquet(
                    con, sql, table_dir, args.batch_size, label=name, sort_order=sort_order,
                    partition_column=partition_column, release=args.release,
                    sentinel_path=_sentinel_path(outdir, name),
                )
                manifest_tables[name] = {
                    "description": meta.get("description", ""),
                    "primary_key": meta.get("primary_key", []),
                    "foreign_keys": meta.get("foreign_keys", {}),
                    "files": files,
                    "row_count": row_count,
                    "sort_order": sort_order,
                    "partitioning": _partitioning_block(name, files, table_dir, partition_column),
                    "columns": _schema_to_dict(arrow_schema) if arrow_schema else [],
                    "column_categories": meta.get("columns", {}),
                }
                _add_file_details(manifest_tables[name], table_dir)

        # ── Write manifest.json ──
        manifest = {
            "format": "uniprot-parquet",
            "version": MANIFEST_FORMAT_VERSION,
            "schema_version": SCHEMA_VERSION,
            "release": args.release,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "tables": manifest_tables,
            "total_rows": sum(t["row_count"] for t in manifest_tables.values()),
            "compression": {"codec": "zstd", "level": ZSTD_LEVEL},
            "target_file_bytes": TARGET_FILE_BYTES,
            "row_group_size": ROW_GROUP_SIZE,
        }
        manifest_path = os.path.join(outdir, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        eprint(f"  Wrote {manifest_path}")

        # ── Write LICENSE (plan H.6): the data terms travel with the lake ──
        license_path = os.path.join(outdir, "LICENSE")
        with open(LICENSE_TEXT_PATH) as f:
            legal_code = f.read()
        with open(license_path, "w") as f:
            f.write(LICENSE_HEADER + "\n" + legal_code)
        eprint(f"  Wrote {license_path}")

        # ── Write datapackage.json (Frictionless Data Package descriptor) ──
        datapackage = _build_datapackage(manifest, args.release)
        datapackage_path = os.path.join(outdir, "datapackage.json")
        with open(datapackage_path, "w") as f:
            json.dump(datapackage, f, indent=2)
        eprint(f"  Wrote {datapackage_path}")

        # ── SHA256SUMS.txt and RELEASE.metalink (plan F.2.1) ──
        _write_checksums(outdir, manifest, ["manifest.json", "datapackage.json", "LICENSE"])

        # Validate against the Frictionless spec
        report = Package.validate_descriptor(datapackage)
        if report.valid:
            eprint("  Frictionless validation: PASSED")
        else:
            eprint(f"  WARNING: Frictionless validation failed "
                   f"({report.stats['errors']} errors, "
                   f"{report.stats['warnings']} warnings)")

        # ── Summary ──
        elapsed = time.time() - t_total
        eprint("\n" + "=" * 60)
        eprint(f"DONE in {elapsed:.1f}s")
        total_parquet_bytes = 0
        for name, _, _ in table_defs:
            table_dir = os.path.join(outdir, name)
            table_bytes = sum(
                os.path.getsize(os.path.join(table_dir, f))
                for f in manifest_tables[name]["files"]
            )
            total_parquet_bytes += table_bytes
            status = " (skipped)" if name in skip_set else ""
            eprint(f"  {name}: {manifest_tables[name]['row_count']:,} rows, "
                   f"{_human_size(table_bytes)}{status}")
        eprint(f"  Total: {manifest['total_rows']:,} rows, {_human_size(total_parquet_bytes)} Parquet")
        if staged_bytes and staged_bytes > 0:
            ratio = staged_bytes / total_parquet_bytes if total_parquet_bytes > 0 else 0
            eprint(f"  Compression: staged {_human_size(staged_bytes)} → "
                   f"{_human_size(total_parquet_bytes)} Parquet ({ratio:.1f}x)")
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
