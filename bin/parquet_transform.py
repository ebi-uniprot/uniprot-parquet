#!/usr/bin/env python3
"""
Transform JSONL(.zst) → sorted, Hive-partitioned Parquet tables
(entries, features, xrefs, comments, publications, accession_map).

Architecture:
  - The JSONL is staged to a Parquet file once (single JSON parse), then all
    six table queries read from Parquet instead of re-parsing JSON.  This
    roughly halves the total transform time at production scale.
  - DuckDB performs all SQL transformations (flattening, unnesting, sorting).
  - DuckDB streams Arrow record batches (bounded memory, never materialises
    the full dataset).
  - PyArrow writes each batch as a zstd-compressed Parquet file.
  - manifest.json, datapackage.json, LICENSE, SHA256SUMS.txt and
    RELEASE.metalink describe the lake.
  - The staging Parquet is cleaned up after all tables are written.

Schema:
  The input schema is never declared: DuckDB infers it from the whole file
  on every run (read_json_auto, sample_size=-1).  Every table has two kinds
  of column.  Source columns are the JSON element itself: one per top-level
  key of the entry (or feature, cross-reference, comment, reference), named
  as in UniProt's JSON and holding the value as shipped, so a key UniProt
  adds is a new column, never dropped and never a build failure.  Shortcut
  columns (TABLE_META "shortcut", COLUMN_TYPES) are snake_case, typed keys,
  UniProt default result columns and values derived from several fields.
  bin/reconstruct.py rebuilds the source JSON from the source columns
  alone, which the validator checks on every release.

Memory model:
  At any moment we hold at most --batch-size rows in Arrow memory.  DuckDB
  may spill to disk during the ORDER BY, but the Arrow → Parquet path stays
  bounded regardless of dataset size.

Output layout:
  <outdir>/
    <table>/review_status=swissprot/<table>_00001.parquet, ...
    <table>/review_status=trembl/<table>_00001.parquet, ...
    manifest.json  datapackage.json  LICENSE  SHA256SUMS.txt  RELEASE.metalink
    .complete/<table>.json          (per-table completion sentinels)

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
import shutil
import hashlib
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from frictionless import Package

from duckdb_util import run_main, _sql_escape, init_duckdb  # noqa: F401  (re-exported: validate_lake imports init_duckdb from here)

# ─── Release constants ──────────────────────────────────────────────────
SCHEMA_VERSION = "1.0.0"          # semver of the shortcut columns (see README "Schema versioning")
MANIFEST_FORMAT_VERSION = 2       # structure of manifest.json
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
        sort_order: List of strings like ["reviewed DESC", "organism_id ASC", "accession ASC"]
        arrow_schema: PyArrow schema to map column names to indices

    Returns:
        List of pq.SortingColumn objects, or None if parsing fails.
    """
    if not sort_order:
        return None

    try:
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

# Types DuckDB's JSON reader infers for a string that parses as one.  A
# JSON string is kept a string (_as_read): converting it would change its
# text (a 'T' becomes a space, an offset is folded into UTC) and the entry
# would no longer reconstruct.
_PARSED_STRING_TYPE_IDS = {"date", "time", "time with time zone", "timestamp",
                           "timestamp with time zone", "timestamp_s", "timestamp_ms",
                           "timestamp_ns", "uuid"}


def _as_read(t) -> str:
    """SQL type string of inferred type ``t`` with every string DuckDB
    parsed into a date, time, timestamp or UUID kept as VARCHAR."""
    if t.id in _PARSED_STRING_TYPE_IDS:
        return "VARCHAR"
    if t.id == "struct":
        return "STRUCT(" + ", ".join(f"{_ident(name)} {_as_read(child)}"
                                     for name, child in t.children) + ")"
    if t.id == "list":
        return _as_read(t.children[0][1]) + "[]"
    if t.id == "map":
        return f"MAP({_as_read(t.children[0][1])}, {_as_read(t.children[1][1])})"
    return str(t)


def build_read_clause(con, jsonl_path: str) -> str:
    """Build the DuckDB read_json SQL fragment for ``jsonl_path``.

    DuckDB infers column names and types from the data itself.
    This is the right approach for UniProtKB: the data is a JSON dump
    from production, so whatever schema it has is what we use.

    sample_size=-1 forces DuckDB to scan the entire file for schema
    inference, ensuring rare nested struct fields (e.g. a ligand sub-struct
    that only appears in a handful of entries) are never silently dropped.
    That scan runs here, once; the clause returned declares the inferred
    columns, with date-, time- and UUID-shaped strings kept as strings
    (_as_read), so reading through it infers nothing again.

    Every object is inferred as a STRUCT, never a MAP: by default DuckDB
    turns an object into MAP(VARCHAR, JSON) when it has many keys or when
    most of its keys are rare (proteinDescription in a TrEMBL-dominated
    dump, comments across comment types), and a MAP hides every field
    inside it from the shortcut columns and from the source columns' struct
    types.

    Raises RuntimeError if sibling keys differ only in case, which a DuckDB
    struct cannot hold.
    """
    safe_path = _sql_escape(jsonl_path)
    # maximum_object_size=512 MB: some UniProtKB JSON entries exceed
    # DuckDB's 16 MB default (e.g. titin has >34k residues + huge
    # annotation arrays).  512 MB covers the largest known entry.
    options = "format='newline_delimited', maximum_object_size=536870912"
    try:
        rel = con.sql(f"SELECT * FROM read_json_auto('{safe_path}', {options}, "
                      f"sample_size=-1, map_inference_threshold=-1, "
                      f"field_appearance_threshold=0) LIMIT 0")
    except duckdb.NotImplementedException as e:
        name = re.search(r'Duplicate name ("[^"]*") in struct', str(e))
        if not name:
            raise
        raise RuntimeError(
            f"Input has sibling keys that differ only in case (one of them {name.group(1)}): "
            f"a DuckDB struct cannot hold both, so this upstream change needs a code "
            f"decision.  DuckDB: {e}") from None
    columns = ", ".join(f"'{_sql_escape(name)}': '{_sql_escape(_as_read(t))}'"
                        for name, t in zip(rel.columns, rel.types))
    return f"read_json('{safe_path}', {options}, columns={{{columns}}})"


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


def _join(prefix: str, name: str) -> str:
    """Dotted path of field ``name`` under ``prefix`` ('' = top level).  A '.'
    or '\\' inside the name is backslash-escaped, so a key such as "x.y" is
    one segment, not x → y."""
    seg = name.replace("\\", "\\\\").replace(".", "\\.")
    return f"{prefix}.{seg}" if prefix else seg


def _segment_name(seg: str) -> str | None:
    """The field name of one path segment, or None if ``seg`` spans several
    segments (holds an unescaped '.')."""
    name, escaped = [], False
    for ch in seg:
        if escaped:
            name.append(ch)
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == ".":
            return None
        else:
            name.append(ch)
    return "".join(name)


def discover_schema_paths(staging_path: str) -> dict[str, None]:
    """Walk the staged Parquet schema and return all valid dotted field paths,
    as a dict used as an ordered set: iteration follows the input's key
    order, which the source columns keep (_children).

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
    exposes them after ``LATERAL unnest()``.  A '.' inside a field name is
    escaped (see :func:`_join`).
    """
    schema = pq.read_schema(staging_path)

    paths: dict[str, None] = {}

    def _walk(field_or_type, prefix: str) -> None:
        if isinstance(field_or_type, pa.Schema):
            for field in field_or_type:
                _walk(field, prefix)
            return
        if isinstance(field_or_type, pa.Field):
            path = _join(prefix, field_or_type.name)
            paths[path] = None
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


def _type_paths(t, prefix: str, out: dict[str, object]) -> None:
    """Record every dotted path under a DuckDB type (discover_schema_paths
    convention: list elements add no segment, a MAP's values sit under
    ``<path>.*``) mapped to its DuckDBPyType."""
    if t.id == "list":
        _type_paths(t.children[0][1], prefix, out)
    elif t.id == "map":
        value_type = t.children[1][1]
        out[prefix + ".*"] = value_type
        _type_paths(value_type, prefix + ".*", out)
    elif t.id == "struct":
        for name, child in t.children:
            path = _join(prefix, name)
            out[path] = child
            _type_paths(child, path, out)


def _unlist(t):
    """The element type of a (nested) list; any other type unchanged."""
    while t.id == "list":
        t = t.children[0][1]
    return t


def _is_json(t) -> bool:
    """True for JSON or a (nested) list of JSON."""
    return str(_unlist(t)) == "JSON"


def inferred_types(con, read_clause: str) -> dict[str, object]:
    """Every dotted path of the input (discover_schema_paths convention)
    mapped to the DuckDBPyType DuckDB inferred for it."""
    rel = con.sql(f"SELECT * FROM {read_clause} LIMIT 0")
    types: dict[str, object] = {}
    for name, t in zip(rel.columns, rel.types):
        types[_join("", name)] = t
        _type_paths(t, _join("", name), types)
    return types


def discover_json_paths(types: dict[str, object]) -> set[str]:
    """Paths of ``types`` (inferred_types) whose type is JSON or a list of
    JSON.

    DuckDB infers JSON for a field whose type conflicts between entries, and
    JSON[] for an array whose elements do.  A source column whose value
    holds one is stored whole as JSON (_source_columns): Parquet's JSON
    type, which readers see as arrow.json and bin/reconstruct.py decodes.
    """
    return {p for p, t in types.items() if _is_json(t)}


# ─── Output schema: shortcut columns + source columns ───────────────────
#
# The input schema is never declared: build_read_clause infers it from the
# whole file.  Every table has two kinds of column:
#
#   * Shortcut columns (TABLE_META "shortcut") — snake_case, each cast to its
#     declared type (COLUMN_TYPES), so a column's type never depends on what
#     DuckDB inferred or on which optional fields a build's input contained.
#     They are keys, UniProt's default result columns and values derived from
#     several fields: the contract SCHEMA_VERSION versions.  A missing
#     *required* source path fails the build (check_required_paths), as does a
#     shortcut source whose type conflicts between entries or is one the
#     column cannot hold (check_curated_sources).
#
#   * Source columns — one per top-level key of the table's JSON element,
#     named exactly as in UniProt's JSON, with the inferred type
#     (_source_columns).  A key UniProt adds becomes a column; a key whose
#     type conflicts between entries is stored as JSON.  A row's source
#     columns are the element: bin/reconstruct.py needs nothing else.

# Child table → (source array, *_index column, entries *_count column).  A
# child table holds one row per element of the entry's source array.
CHILD_TABLES: dict[str, tuple[str, str, str]] = {
    "features":     ("features", "feature_index", "feature_count"),
    "xrefs":        ("uniProtKBCrossReferences", "xref_index", "xref_count"),
    "comments":     ("comments", "comment_index", "comment_count"),
    "publications": ("references", "reference_index", "reference_count"),
}

# Declared DuckDB type of every shortcut column.  Strings are DuckDB
# ``DESCRIBE`` output, verbatim.  The validator's check_schema_types compares
# every built table against this dict.
COLUMN_TYPES: dict[tuple[str, str], str] = {
    # Key columns every table carries.
    **{(table, column): ctype
       for table in ("entries", "features", "xrefs", "comments", "publications", "accession_map")
       for column, ctype in (("accession", "VARCHAR"), ("reviewed", "BOOLEAN"), ("organism_id", "BIGINT"))},
    ("entries", "id"): "VARCHAR",
    ("entries", "organism_name"): "VARCHAR",
    ("entries", "length"): "INTEGER",
    ("entries", "division"): "VARCHAR",
    ("entries", "ec"): "VARCHAR[]",
    ("entries", "go_id"): "VARCHAR[]",
    ("entries", "xref_dbs"): "VARCHAR[]",
    ("entries", "xref_proteomes"): "VARCHAR[]",
    ("entries", "keywordid"): "VARCHAR[]",
    ("entries", "keyword"): "VARCHAR[]",
    ("entries", "keyword_categories"): "VARCHAR[]",
    ("entries", "date_created"): "DATE",
    ("entries", "date_modified"): "DATE",
    ("entries", "date_sequence_modified"): "DATE",
    **{("entries", count): "INTEGER" for _, _, count in CHILD_TABLES.values()},
    ("entries", "lit_pubmed_id"): "VARCHAR[]",
    ("features", "organism_name"): "VARCHAR",
    ("features", "length"): "INTEGER",
    ("features", "start_pos"): "INTEGER",
    ("features", "end_pos"): "INTEGER",
    ("features", "evidence_codes"): "VARCHAR[]",
    ("comments", "text_value"): "VARCHAR",
    ("accession_map", "primary_accession"): "VARCHAR",
    ("accession_map", "is_primary"): "BOOLEAN",
    # Shortcuts whose source a subset may lack altogether (a TrEMBL-only
    # slice has no recommendedName; a subset without genes or ligands):
    # _typed() emits a NULL of the declared type.
    ("entries", "go_terms"): "STRUCT(id VARCHAR, aspect VARCHAR, term VARCHAR, evidence_type VARCHAR)[]",
    ("entries", "protein_name_primary"): "VARCHAR",
    ("entries", "gene_primary"): "VARCHAR[]",
    ("entries", "gene_synonym"): "VARCHAR[]",
    ("entries", "alt_protein_names"): "VARCHAR[]",
    ("features", "ligand_id"): "VARCHAR",
    # Position of the row's element in the entry's source array (0-based):
    # restores array order, and completes each child table's sort key.
    **{(table, index): "INTEGER" for table, (_, index, _) in CHILD_TABLES.items()},
}

# Source JSON path of every shortcut column (dotted; list elements carry no
# segment, the same convention as discover_schema_paths).  Written into each
# field's Parquet metadata and SCHEMA.md; _typed() uses it to tell whether
# an optional shortcut's source exists in this build's input.  A source
# column's path is its own key (_source_path).
COLUMN_SOURCES: dict[tuple[str, str], str] = {
    # entries — single-path shortcuts (derived ones such as division,
    # go_terms, *_count, lit_pubmed_id have none: DERIVED_SOURCES)
    ("entries", "accession"): "primaryAccession",
    ("entries", "id"): "uniProtkbId",
    ("entries", "reviewed"): "entryType",
    ("entries", "organism_id"): "organism.taxonId",
    ("entries", "organism_name"): "organism.scientificName",
    ("entries", "gene_primary"): "genes.geneName.value",
    ("entries", "protein_name_primary"): "proteinDescription.recommendedName.fullName.value",
    ("entries", "length"): "sequence.length",
    ("entries", "gene_synonym"): "genes.synonyms.value",
    ("entries", "alt_protein_names"): "proteinDescription.alternativeNames.fullName.value",
    ("entries", "go_id"): "uniProtKBCrossReferences.id",
    ("entries", "xref_dbs"): "uniProtKBCrossReferences.database",
    ("entries", "xref_proteomes"): "uniProtKBCrossReferences.id",
    ("entries", "keywordid"): "keywords.id",
    ("entries", "keyword"): "keywords.name",
    ("entries", "keyword_categories"): "keywords.category",
    ("entries", "date_created"): "entryAudit.firstPublicDate",
    ("entries", "date_modified"): "entryAudit.lastAnnotationUpdateDate",
    ("entries", "date_sequence_modified"): "entryAudit.lastSequenceUpdateDate",
    # child tables: the parent entry's keys, then the element's
    **{(table, column): source
       for table in CHILD_TABLES
       for column, source in (("accession", "primaryAccession"), ("reviewed", "entryType"),
                              ("organism_id", "organism.taxonId"))},
    ("features", "organism_name"): "organism.scientificName",
    ("features", "length"): "sequence.length",
    ("features", "start_pos"): "features.location.start.value",
    ("features", "end_pos"): "features.location.end.value",
    ("features", "ligand_id"): "features.ligand.id",
    ("features", "evidence_codes"): "features.evidences.evidenceCode",
    ("comments", "text_value"): "comments.texts.value",
    # accession_map
    ("accession_map", "accession"): "primaryAccession | secondaryAccessions",
    ("accession_map", "primary_accession"): "primaryAccession",
    ("accession_map", "reviewed"): "entryType",
    ("accession_map", "organism_id"): "organism.taxonId",
}

# Every source path a shortcut reads beyond its COLUMN_SOURCES entry (a
# derived shortcut has none), for check_curated_sources.  Not written to the
# Parquet metadata or SCHEMA.md, which show such a column as "derived".
_NAMING_BLOCKS = ("recommendedName", "alternativeNames", "submissionNames", "submittedNames")
# proteinDescription arrays whose elements (domains, chains) carry naming blocks of their own.
_NESTED_NAMING = ("includes", "contains")
DERIVED_SOURCES: dict[tuple[str, str], tuple[str, ...]] = {
    ("entries", "protein_name_primary"): tuple(f"proteinDescription.{b}.fullName.value"
                                       for b in _NAMING_BLOCKS[1:]),
    ("entries", "division"): ("organism.taxonId", "organism.lineage"),
    ("entries", "ec"): tuple(f"proteinDescription.{n}{b}.ecNumbers.value"
                             for n in ("", *(f"{x}." for x in _NESTED_NAMING))
                             for b in _NAMING_BLOCKS),
    ("entries", "go_terms"): ("uniProtKBCrossReferences.id", "uniProtKBCrossReferences.database",
                              "uniProtKBCrossReferences.properties.key",
                              "uniProtKBCrossReferences.properties.value"),
    **{("entries", count): (array,) for array, _, count in CHILD_TABLES.values()},
    ("entries", "lit_pubmed_id"): ("references.citation.citationCrossReferences.database",
                                "references.citation.citationCrossReferences.id"),
    ("accession_map", "is_primary"): (),
    # array positions (COLUMN_DESCRIPTIONS names the array)
    **{(table, index): () for table, (_, index, _) in CHILD_TABLES.items()},
}

# Entry keys whose arrays are child tables of their own, not entries columns.
_CHILD_ARRAYS: frozenset[str] = frozenset(array for array, _, _ in CHILD_TABLES.values())


def _ident(name: str) -> str:
    """Quote a field name taken from the input (UniProt may add any key)."""
    return '"' + name.replace('"', '""') + '"'


def _null(table: str, column: str) -> str:
    """Typed NULL fallback for an optional curated column.  KeyError is the
    intended failure: every fallback must have a declared type in COLUMN_TYPES."""
    return f"NULL::{COLUMN_TYPES[(table, column)]}"


def _cast(table: str, column: str, expr: str) -> str:
    """SQL for a curated column: ``expr`` cast to its declared type."""
    return f"CAST({expr} AS {COLUMN_TYPES[(table, column)]})"


def _typed(table: str, column: str, expr: str, schema_paths: set[str]) -> str:
    """SQL for an optional curated column: the source expression cast to its
    declared type when the source path exists, else a typed NULL."""
    if COLUMN_SOURCES[(table, column)] not in schema_paths:
        return _null(table, column)
    return _cast(table, column, expr)


def _needs_json(prefix: str, json_paths: set[str]) -> bool:
    """True if the subtree at ``prefix`` holds a JSON-typed field."""
    return any(p == prefix or p.startswith(prefix + ".") for p in json_paths)


def _children(prefix: str, schema_paths) -> list[str]:
    """Direct child field names (unescaped) under a dotted prefix ('' = top
    level), in the order discover_schema_paths found them (the input's key
    order); sorted when ``schema_paths`` is a plain set."""
    head = prefix + "." if prefix else ""
    names = (_segment_name(p[len(head):]) for p in schema_paths if p.startswith(head))
    ordered = list(dict.fromkeys(n for n in names if n is not None))
    return sorted(ordered) if isinstance(schema_paths, (set, frozenset)) else ordered


def _source_path(table: str, key: str) -> str:
    """Dotted input path of source column ``key`` of ``table``."""
    return _join("" if table == "entries" else CHILD_TABLES[table][0], key)


def _source_keys(table: str, schema_paths) -> list[str]:
    """The top-level keys of ``table``'s JSON element in the input, in the
    input's order: the source column names (an entry's four child arrays
    excepted)."""
    prefix = "" if table == "entries" else CHILD_TABLES[table][0]
    if prefix and prefix not in schema_paths:
        # No such array in the input: the zero-row table still has the keys
        # every element carries (the _EMPTY_CHILD_SOURCE shape), so queries
        # and macros naming them bind on any lake.
        return list(_EMPTY_CHILD_KEYS[prefix])
    return [k for k in _children(prefix, schema_paths)
            if not (table == "entries" and k in _CHILD_ARRAYS)]


def json_source_columns(table: str, schema_paths, json_paths: set[str]) -> set[str]:
    """Source columns of ``table`` stored as JSON text: those whose value
    holds a field typed JSON (its type differs between entries).  The writer
    gives them Parquet's JSON type (_as_json_columns)."""
    if table not in TABLE_META or TABLE_META[table]["element"] is None:
        return set()
    return {k for k in _source_keys(table, schema_paths)
            if _needs_json(_source_path(table, k), json_paths)}


def _source_columns(table: str, base: str, schema_paths,
                    json_paths: set[str]) -> list[tuple[str, str]]:
    """(key, SQL select item) for every source column of ``table``: one per
    top-level key of its JSON element (the entry, or an element of the child
    table's source array), in the input's key order, named exactly as the
    key.  A key whose value holds a JSON-typed field (its type differs
    between entries) is stored whole as JSON, so no column mixes shapes.

    Raises RuntimeError for a key named like one of the table's shortcut
    columns (or the partition key): column names are case-insensitive in
    DuckDB, so the two could not both be columns."""
    keys = _source_keys(table, schema_paths)
    reserved = {c.lower() for c in TABLE_META[table]["columns"]["shortcut"]} | {PARTITION_KEY}
    clash = sorted(k for k in keys if k.lower() in reserved)
    if clash:
        raise RuntimeError(
            f"{table}: input key(s) {clash} have the name of a shortcut column: a JSON "
            f"key UniProt added collides with a name this lake uses, which needs a code "
            f"decision (rename the shortcut).")
    as_json = json_source_columns(table, schema_paths, json_paths)
    items = []
    for key in keys:
        expr = f"{base}.{_ident(key)}"
        if key in as_json:
            expr = f"to_json({expr})"
        items.append((key, f"{expr} AS {_ident(key)}"))
    return items


# The review flag, shared by every table builder: Swiss-Prot entries are
# "UniProtKB reviewed (Swiss-Prot)", TrEMBL "UniProtKB unreviewed (TrEMBL)".
REVIEWED_EXPR = "CASE WHEN e.entryType LIKE '%Swiss-Prot%' THEN true ELSE false END"

# Every entryType UniProt ships, with the review flag REVIEWED_EXPR gives it.
# Any other value (a rewording, a NULL) would land every entry carrying it
# in the TrEMBL partition: check_entry_types refuses the input instead.
ENTRY_TYPES: dict[str, bool] = {
    "UniProtKB reviewed (Swiss-Prot)": True,
    "UniProtKB unreviewed (TrEMBL)": False,
}


def check_entry_types(con, read_clause: str) -> None:
    """Abort the build if an entry's entryType is not one of ENTRY_TYPES."""
    values = [v for (v,) in con.sql(f"SELECT DISTINCT entryType FROM {read_clause}").fetchall()]
    unknown = sorted((v for v in values if v not in ENTRY_TYPES), key=str)
    if unknown:
        raise RuntimeError(
            f"Input has entryType value(s) {unknown}; known values are {sorted(ENTRY_TYPES)}. "
            "The review flag and the swissprot/trembl partitions depend on entryType: "
            "a reworded value needs a code decision (ENTRY_TYPES in parquet_transform.py).")

# Paths every UniProtKB entry carries; the builders dereference them without
# a guard.  A dump lacking one is refused up front with a clear message
# (check_required_paths) instead of a DuckDB BinderException mid-build.
# Everything else the builders read is guarded with has() / _typed(), and
# the source columns are whatever keys the input has.
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
    "comments": frozenset({"comments.commentType"}),
}


def check_required_paths(schema_paths: set[str]) -> None:
    """Abort the build if the input lacks a field every UniProtKB entry has."""
    missing = sorted(p for p in REQUIRED_PATHS if p not in schema_paths)
    for parent, paths in REQUIRED_CHILD_PATHS.items():
        if parent in schema_paths:
            missing += sorted(p for p in paths if p not in schema_paths)
    if missing:
        raise RuntimeError("Input lacks fields every UniProtKB entry carries "
                           f"(is this a UniProtKB JSON dump?): {missing}")


# Derived sources read only for their array length (the *_count columns):
# the elements may take any shape.
_LENGTH_ONLY: frozenset[tuple[str, str]] = frozenset(
    ("entries", count) for _, _, count in CHILD_TABLES.values())

# A list source that a scalar curated column unnests, one row per element.
_UNNESTED_SOURCES: frozenset[tuple[tuple[str, str], str]] = frozenset({
    (("accession_map", "accession"), "secondaryAccessions"),
})

def _is_object(t) -> bool:
    """True for a STRUCT or MAP, or a (nested) list of one."""
    return _unlist(t).id in ("struct", "map")


def _curated_sources() -> list[tuple[tuple[str, str], str, str]]:
    """((table, column), source path, kind) for every path a shortcut column
    reads; kind is "column" or "derived"."""
    shortcut = lambda key: key[1] in TABLE_META[key[0]]["columns"]["shortcut"]  # noqa: E731
    sources = [(key, path, "column") for key, source in COLUMN_SOURCES.items() if shortcut(key)
               for path in source.split(" | ")]
    sources += [(key, path, "derived") for key, paths in DERIVED_SOURCES.items() if shortcut(key)
                for path in paths]
    return sources


def absent_curated_sources(schema_paths) -> list[str]:
    """"table.column ← source path(s)" for every shortcut column none of
    whose source paths is in the input: it is a typed NULL (or empty)
    throughout.  Normal for a subset; in a full dump, a field UniProt
    renamed, whose new name is a new source column."""
    paths: dict[tuple[str, str], list[str]] = {}
    for key, path, _ in _curated_sources():
        paths.setdefault(key, []).append(path)
    return sorted(f"{table}.{column} ← {', '.join(ps)}"
                  for (table, column), ps in paths.items()
                  if ps and not any(p in schema_paths for p in ps))


def check_curated_sources(types: dict[str, object]) -> None:
    """Abort the build if a field a shortcut column reads has an inferred
    type (inferred_types) the column cannot hold.

    * Its type differs between entries: DuckDB infers JSON for the field, for
      an object containing it or for a field inside it.  A shortcut has one
      declared type; a CAST would keep the JSON quoting of every value, and a
      JSON-typed parent hides the source altogether (a typed NULL).
    * A plain value where an object is expected: an object containing the
      field is a scalar (keywords shipped as a string), so the field is gone.
    * An object (or a list) where a value is declared: the same shape in
      every entry, so no JSON, but a CAST to VARCHAR would store its text
      rendering.
    * An object containing the field is a MAP: no field path reaches inside
      it, so the column would be NULL throughout.

    Every path counts: COLUMN_SOURCES and DERIVED_SOURCES.  Like a missing
    required path, this is a breaking change upstream that needs a code
    decision.  Source columns take any type and are never refused."""
    def ancestors(path: str) -> list[str]:
        parts = path.split(".")
        return [".".join(parts[:i]) for i in range(1, len(parts))]

    differs, plain, shape, mapped = [], [], [], []
    for (table, column), path, kind in _curated_sources():
        label = f"{table}.{column} ← {path}"
        length_only = (table, column) in _LENGTH_ONLY
        above = next((a for a in ancestors(path) if a in types and _is_json(types[a])), None)
        below = None if length_only else next(
            (p for p in types if p.startswith(path + ".") and _is_json(types[p])), None)
        in_map = next((a for a in ancestors(path) if a in types
                       and _unlist(types[a]).id == "map"), None)
        if in_map:
            mapped.append(f"{label} ({in_map} is {types[in_map]})")
        elif path in types and _is_json(types[path]):
            differs.append(label)
        elif above or below:
            differs.append(f"{label} (inside {above})" if above else f"{label} ({below} below it)")
        elif length_only:
            continue
        elif scalar := next((a for a in ancestors(path)
                             if a in types and not _is_object(types[a])), None):
            plain.append(f"{label} ({scalar} is {types[scalar]})")
        elif path not in types:
            continue
        elif _is_object(types[path]) or (
                kind == "column" and types[path].id == "list"
                and "[]" not in COLUMN_TYPES[(table, column)]
                and ((table, column), path) not in _UNNESTED_SOURCES):
            declared = COLUMN_TYPES[(table, column)] if kind == "column" else "a value"
            shape.append(f"{label} is {types[path]}, declared {declared}")
    problems = []
    if differs:
        problems.append("Shortcut columns whose source field has a different type in "
                        f"different entries (inferred as JSON): {sorted(differs)}")
    if plain:
        problems.append("Shortcut columns whose source field is inside a plain value "
                        f"where an object is expected: {sorted(plain)}")
    if shape:
        problems.append("Shortcut columns whose source field has a type the column "
                        f"cannot hold: {sorted(shape)}")
    if mapped:
        problems.append("Shortcut columns whose source field is inside a MAP, where "
                        f"no field path reaches it: {sorted(mapped)}")
    if problems:
        raise RuntimeError("\n".join(problems))


# Element types for a child table whose source array is absent from the
# input: the builder's SQL then unnests an empty list of this shape and
# writes a zero-row table instead of failing to bind the missing column.
_EMPTY_CHILD_SOURCE: dict[str, str] = {
    "features": 'CAST([] AS STRUCT("type" VARCHAR, location STRUCT("start" STRUCT("value" BIGINT, '
                'modifier VARCHAR), "end" STRUCT("value" BIGINT, modifier VARCHAR)))[])',
    "uniProtKBCrossReferences": 'CAST([] AS STRUCT("database" VARCHAR, id VARCHAR)[])',
    "comments": 'CAST([] AS STRUCT("commentType" VARCHAR)[])',
    "references": 'CAST([] AS STRUCT(referenceNumber BIGINT, '
                  'citation STRUCT(citationType VARCHAR, id VARCHAR))[])',
}


# The keys of each _EMPTY_CHILD_SOURCE element: a zero-row child table's
# source columns.
_EMPTY_CHILD_KEYS: dict[str, tuple[str, ...]] = {
    "features": ("type", "location"),
    "uniProtKBCrossReferences": ("database", "id"),
    "comments": ("commentType",),
    "references": ("referenceNumber", "citation"),
}


def _child_source(field: str, schema_paths: set[str]) -> str:
    """SQL for a child table's source array: the column, or an empty typed list."""
    if field in schema_paths:
        return f'e."{field}"'
    return _EMPTY_CHILD_SOURCE[field]


def _child_from(table: str, schema_paths: set[str], extra: dict[str, str] | None = None) -> str:
    """FROM and ORDER BY of a child table's SQL: one row per element
    (``unnest``, 1-based ``ordinality``) of the entry's source array, beside
    ``sub``'s key columns and ``extra`` {column: source expression}."""
    array, index, _ = CHILD_TABLES[table]
    src = _child_source(array, schema_paths)
    keys = {"accession": "e.primaryAccession", "reviewed": REVIEWED_EXPR,
            "organism_id": "e.organism.taxonId", **(extra or {})}
    key_sql = "".join(f"        {_cast(table, col, expr)} AS {col},\n" for col, expr in keys.items())
    return f"""FROM (
    SELECT
{key_sql}        {src} AS elements
    FROM {{read_clause}} e
    WHERE {src} IS NOT NULL AND len({src}) > 0
) sub, LATERAL unnest(sub.elements) WITH ORDINALITY AS u("unnest", ordinality)
ORDER BY sub.reviewed DESC, sub.organism_id, sub.accession, {index}"""


def _build_entries_sql(schema_paths, json_paths: set[str]) -> str:
    """Build the entries SQL: the shortcut columns, then the entry's source
    columns (every top-level key except the four arrays that are child
    tables).

    ``schema_paths`` is the set of all valid dotted paths discovered from the
    staged Parquet (see :func:`discover_schema_paths`).  Any optional path
    not in the set gets a typed ``NULL`` (or ``[]`` / ``0`` for list and
    count columns) in the SQL — no BinderException, no matter how exotic the
    dataset.  Only REQUIRED_PATHS are dereferenced unguarded, and
    check_required_paths() has already refused an input without them.
    ``json_paths`` (discover_json_paths) marks JSON-typed input fields.
    """

    def has(path: str) -> bool:
        return path in schema_paths

    def c(column: str, expr: str) -> str:
        return _cast("entries", column, expr)

    # EC numbers: every naming block's ecNumbers (recommended, alternative,
    # submitted), of the entry and of each domain (includes[]) and chain
    # (contains[]), as UniProt's REST `ec` field.  A naming block's struct
    # may lack ecNumbers in a subset, so only existing paths are read.
    def _ec_parts(path: str, expr: str) -> list[str]:
        parts = []
        if has(f"{path}.recommendedName.ecNumbers"):
            parts.append(f"list_transform(COALESCE({expr}.recommendedName.ecNumbers, []), x -> x.value)")
        # UniProtKB API renamed submittedNames → submissionNames; read either.
        for block in _NAMING_BLOCKS[1:]:
            if has(f"{path}.{block}.ecNumbers"):
                parts.append(f"""flatten(list_transform(
            COALESCE({expr}.{block}, []),
            n -> list_transform(COALESCE(n.ecNumbers, []), x -> x.value)
        ))""")
        return parts

    ec_parts = _ec_parts("proteinDescription", "e.proteinDescription")
    for nested in _NESTED_NAMING:
        inner = _ec_parts(f"proteinDescription.{nested}", "d")
        if inner:
            ec_parts.append(f"""flatten(list_transform(
            COALESCE(e.proteinDescription.{nested}, []),
            d -> flatten([{", ".join(inner)}])
        ))""")

    if ec_parts:
        ec_numbers_expr = "list_distinct(flatten([\n        " + ",\n        ".join(ec_parts) + "\n    ]))"
    else:
        ec_numbers_expr = "[]"

    # protein_name_primary: COALESCE across recommendedName → submissionNames/submittedNames[1] → alternativeNames[1].
    # Swiss-Prot entries have recommendedName; TrEMBL entries typically only have submissionNames.
    # Without this fallback, protein_name_primary is NULL for >99% of the lake (TrEMBL dominates).
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
                             f"{COLUMN_TYPES[('entries', 'protein_name_primary')]})")
    else:
        protein_name_expr = _null("entries", "protein_name_primary")

    # Genes and alternative names: absent from some subsets.
    gene_names_expr = _typed("entries", "gene_primary",
                             "list_transform(COALESCE(e.genes, []), g -> g.geneName.value)", schema_paths)
    gene_synonyms_expr = _typed(
        "entries", "gene_synonym",
        "flatten(list_transform(COALESCE(e.genes, []), g -> list_transform(COALESCE(g.synonyms, []), s -> s.value)))",
        schema_paths)
    alt_protein_names_expr = _typed(
        "entries", "alt_protein_names",
        "list_transform(COALESCE(e.proteinDescription.alternativeNames, []), x -> x.fullName.value)",
        schema_paths)

    def _keyword_list(field):
        if not has(f"keywords.{field}"):
            return "[]"
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
    division_expr = f"""CASE
      WHEN e.organism.taxonId = 9606                             THEN 'human'{division_rules}
      ELSE 'unclassified'
    END"""

    # Cross-reference shortcuts and per-array counts: [] / 0 when the array is
    # absent, matching what an entry without the array gets when it is present.
    xrefs_present = has("uniProtKBCrossReferences")
    go_ids_expr = ("""list_distinct([
        x.id
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
        IF x.database = 'GO'
    ])""" if xrefs_present else "[]")
    xref_dbs_expr = ("""list_distinct([
        x.database
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
    ])""" if xrefs_present else "[]")
    proteome_ids_expr = ("""list_sort(list_distinct([
        x.id
        FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
        IF x.database = 'Proteomes'
    ]))""" if xrefs_present else "[]")

    def _count(field):
        return f'len(COALESCE(e."{field}", []))' if has(field) else "0"

    # go_terms: GO xrefs carry properties GoTerm ('F:ATP binding')
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

    # lit_pubmed_id: distinct PubMed ids as the source writes them, numeric ids
    # in numeric order and any other id after them: one malformed id is kept,
    # never a cast error that fails the whole table.
    if has("references.citation.citationCrossReferences"):
        pubmed_ids_expr = """list_transform(
        list_sort(list_transform(
            list_distinct(flatten(list_transform(
                COALESCE(e."references", []),
                r -> [ c.id FOR c IN COALESCE(r.citation.citationCrossReferences, []) IF c.database = 'PubMed' ]
            ))),
            x -> struct_pack(non_numeric := TRY_CAST(x AS BIGINT) IS NULL,
                             n := COALESCE(TRY_CAST(x AS BIGINT), 0), id := x))),
        x -> x.id)"""
    else:
        pubmed_ids_expr = "[]"

    # Source columns: the entry as UniProt ships it.  sequence (a required
    # path) is placed with the hot shortcuts, the rest follow them.
    sources = dict(_source_columns("entries", "e", schema_paths, json_paths))
    sequence = sources.pop("sequence")
    rest = "".join(f",\n    {sql}" for sql in sources.values())

    return f"""
SELECT
    -- UniProt's default result columns, organism_id and the sequence first and
    -- contiguous: adjacent column chunks mean fewer range requests for
    -- remote readers of the default projection.
    {c('accession', 'e.primaryAccession')}                AS accession,
    {c('id', 'e.uniProtkbId')}                      AS id,
    {c('reviewed', REVIEWED_EXPR)}                  AS reviewed,
    {c('organism_id', 'e.organism.taxonId')}              AS organism_id,
    {c('organism_name', 'e.organism.scientificName')} AS organism_name,
    {gene_names_expr}                               AS gene_primary,
    {protein_name_expr}                             AS protein_name_primary,
    {c('length', 'e.sequence.length')}          AS length,
    {sequence},

    -- Gene & protein
    {gene_synonyms_expr}                            AS gene_synonym,
    {alt_protein_names_expr}                        AS alt_protein_names,
    -- EC numbers from all naming blocks, the entry's and its domains' and chains'
    {c('ec', ec_numbers_expr)}              AS ec,
    -- UniProt taxonomic division, most specific rule first.
    -- First approximation of the FTP taxonomic_divisions/ rules; the final CASE
    -- waits on per-division counts matching the FTP (README, pending work).
    -- Protists land in 'invertebrates' as on the FTP; 'unclassified' is the rest.
    {c('division', division_expr)}                  AS division,

    -- Cross-reference and keyword shortcuts
    {c('go_id', go_ids_expr)}                      AS go_id,
    {go_terms_expr}                                 AS go_terms,
    {c('xref_dbs', xref_dbs_expr)}                  AS xref_dbs,
    {c('xref_proteomes', proteome_ids_expr)}          AS xref_proteomes,
    {c('keywordid', _keyword_list('id'))}         AS keywordid,
    {c('keyword', _keyword_list('name'))}     AS keyword,
    {c('keyword_categories', _keyword_list('category'))} AS keyword_categories,

    -- Dates (typed; entryAudit keeps the strings)
    {c('date_created', 'e.entryAudit.firstPublicDate')}  AS date_created,
    {c('date_modified', 'e.entryAudit.lastAnnotationUpdateDate')} AS date_modified,
    {c('date_sequence_modified', 'e.entryAudit.lastSequenceUpdateDate')} AS date_sequence_modified,

    -- Counts of the child tables' rows, and the cited PubMed ids
    {c('feature_count', _count('features'))}        AS feature_count,
    {c('xref_count', _count('uniProtKBCrossReferences'))} AS xref_count,
    {c('comment_count', _count('comments'))}        AS comment_count,
    {c('reference_count', _count('references'))}    AS reference_count,
    {c('lit_pubmed_id', pubmed_ids_expr)}              AS lit_pubmed_id

    -- Source columns: the rest of the entry as UniProt ships it (features,
    -- cross-references, comments and references are tables of their own).
    {rest}

FROM {{read_clause}} e
ORDER BY reviewed DESC, e.organism.taxonId, e.primaryAccession
"""


def _child_sources_sql(table: str, schema_paths, json_paths: set[str]) -> str:
    """The source columns of a child table's SELECT, each after a comma."""
    return "".join(f",\n    {sql}" for _, sql in
                   _source_columns(table, "unnest", schema_paths, json_paths))


def _build_features_sql(schema_paths, json_paths: set[str]) -> str:
    """Build features SQL: the shortcuts, then the feature's source columns.

    Feature structs may lack ligand or evidences depending on the dataset;
    ``schema_paths`` is checked before a shortcut reads a nested field.
    """
    if "features.evidences" in schema_paths:
        evidence_codes = _cast("features", "evidence_codes", """list_transform(
        COALESCE(unnest.evidences, []),
        x -> x.evidenceCode
    )""")
    else:
        evidence_codes = "CAST([] AS VARCHAR[])"
    ligand_id = _typed("features", "ligand_id", "unnest.ligand.id", schema_paths)

    return f"""
SELECT
    sub.accession,
    sub.reviewed,
    sub.organism_id,
    sub.organism_name,
    sub.length,
    {_cast("features", "feature_index", "ordinality - 1")} AS feature_index,
    {_cast("features", "start_pos", "unnest.location.start.value")} AS start_pos,
    {_cast("features", "end_pos", "unnest.location.end.value")} AS end_pos,
    {ligand_id}                                     AS ligand_id,
    {evidence_codes}                                AS evidence_codes{_child_sources_sql("features", schema_paths, json_paths)}

{_child_from("features", schema_paths, {"organism_name": "e.organism.scientificName",
                                                "length": "e.sequence.length"})}
"""


def _build_xrefs_sql(schema_paths, json_paths: set[str]) -> str:
    """Build xrefs SQL: the keys, then the cross-reference's source columns
    (database, id, properties, isoformId, evidences, ...)."""
    return f"""
SELECT
    sub.accession,
    sub.reviewed,
    sub.organism_id,
    {_cast("xrefs", "xref_index", "ordinality - 1")} AS xref_index{_child_sources_sql("xrefs", schema_paths, json_paths)}

{_child_from("xrefs", schema_paths)}
"""


def _build_comments_sql(schema_paths, json_paths: set[str]) -> str:
    """Build comments SQL: the keys, text_value, then the comment's source
    columns.  ``comments`` is read as a STRUCT list, so its columns are the
    union of every comment type's keys (commentType, texts, note, reaction,
    isoforms, ...), NULL where a comment has none.

    text_value joins the ``texts`` values; a subset whose comments carry no
    top-level ``texts`` (only SUBCELLULAR LOCATION comments, say) gets a
    typed NULL.  Structured-only comment types (COFACTOR, INTERACTION,
    CATALYTIC ACTIVITY, ALTERNATIVE PRODUCTS, ...) are NULL too, and DISEASE
    and SUBCELLULAR LOCATION keep their prose under ``note``.
    """
    if "comments.texts.value" in schema_paths:
        text_value = _cast("comments", "text_value", """NULLIF(array_to_string(
        list_transform(unnest.texts, t -> t.value), chr(10) || chr(10)), '')""")
    else:
        text_value = _null("comments", "text_value")

    return f"""
SELECT
    sub.accession,
    sub.reviewed,
    sub.organism_id,
    {_cast("comments", "comment_index", "ordinality - 1")} AS comment_index,
    -- All text values of the comment, paragraphs joined by a blank line
    {text_value}                                    AS text_value{_child_sources_sql("comments", schema_paths, json_paths)}

{_child_from("comments", schema_paths)}
"""


def _build_publications_sql(schema_paths, json_paths: set[str]) -> str:
    """Build publications SQL: the keys, then the reference's source columns
    (referenceNumber, citation, referencePositions, referenceComments,
    evidences, ...)."""
    return f"""
SELECT
    sub.accession,
    sub.reviewed,
    sub.organism_id,
    {_cast("publications", "reference_index", "ordinality - 1")} AS reference_index{_child_sources_sql("publications", schema_paths, json_paths)}

{_child_from("publications", schema_paths)}
"""


def _build_accession_map_sql(schema_paths: set[str], json_paths: set[str]) -> str:
    """One row per primary *and* secondary accession → current primary.
    Reads the staged Parquet like the other builders so it works under
    --skip-existing.  Sorted reviewed DESC first so the partitioned writer
    sees one partition flip, then accession within each side."""
    has_secondary = "secondaryAccessions" in schema_paths
    primary = f"""
    SELECT e.primaryAccession AS accession, e.primaryAccession AS primary_accession, true AS is_primary,
           {REVIEWED_EXPR} AS reviewed, e.organism.taxonId AS organism_id
    FROM {{read_clause}} e"""
    secondary = f"""
    UNION ALL
    SELECT s AS accession, e.primaryAccession AS primary_accession, false AS is_primary,
           {REVIEWED_EXPR} AS reviewed, e.organism.taxonId AS organism_id
    FROM {{read_clause}} e, LATERAL unnest(COALESCE(e.secondaryAccessions, [])) AS t(s)""" if has_secondary else ""
    return f"""
SELECT {", ".join(_cast("accession_map", c, c) + " AS " + c
                   for c in ("accession", "primary_accession", "is_primary", "reviewed", "organism_id"))}
FROM ({primary}{secondary})
ORDER BY reviewed DESC, accession, primary_accession
"""


# ─── Table definitions ──────────────────────────────────────────────────

# ─── Writer constants ───────────────────────────────────────────────────
TARGET_FILE_BYTES = 256 * 1024 * 1024   # per-file roll-over target. PROVISIONAL: 256 MB / 512 MB / 1 GB
                                        # to be decided on real data (benchmarks/bench_file_size.py)
ROW_GROUP_SIZE = 100_000
ZSTD_LEVEL = 9                          # PROVISIONAL: stress-fixture sweep gave -7% entries, -12% xrefs
                                        # bytes for +6% transform time vs level 1; confirm on real
                                        # data (benchmarks/bench_zstd.py).
PARTITION_KEY = "review_status"         # Hive directory key ...
PARTITION_VALUES = {True: "swissprot", False: "trembl"}   # ... derived from the stored `reviewed`

# (name, sort_order, partition_column).  Every table names its
# partition column explicitly so a future table cannot be written
# unpartitioned by accident.
TABLE_DEFS = [
    ("entries",      ["reviewed DESC", "organism_id ASC", "accession ASC"], "reviewed"),
    # Child tables: the entry order, then the element's position in the
    # entry's source array, so rows appear in UniProt's own order and a
    # rebuild of the same input writes the same rows in the same order.
    *((table, ["reviewed DESC", "organism_id ASC", "accession ASC", f"{index} ASC"], "reviewed")
      for table, (_, index, _) in CHILD_TABLES.items()),
    # accession_map is accession-sorted within each review side: the lookup table.
    # A real sort at production scale (~250M + secondaries rows of five
    # narrow columns; low tens of GB of spill).
    ("accession_map", ["reviewed DESC", "accession ASC", "primary_accession ASC"], "reviewed"),
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


# ─── Semantic metadata (embedded in manifest.json for agents/tools) ─────
# This is the metadata that Parquet files *don't* encode: what each table
# means, how they relate, and which columns are shortcuts.  Every other
# column of a table is a source column (the manifest lists both kinds, as
# built).  "element" names what one row's source columns hold.

TABLE_META = {
    "entries": {
        "description": "One row per protein. Primary table for most queries.",
        "element": "entry",
        "primary_key": ["accession"],
        "foreign_keys": {},
        "columns": {
            # Same order as the SELECT in _build_entries_sql (the sequence
            # source column sits after length).
            "shortcut": [
                "accession", "id", "reviewed", "organism_id", "organism_name", "gene_primary",
                "protein_name_primary", "length",
                "gene_synonym", "alt_protein_names", "ec", "division",
                "go_id", "go_terms", "xref_dbs", "xref_proteomes",
                "keywordid", "keyword", "keyword_categories",
                "date_created", "date_modified", "date_sequence_modified",
                "feature_count", "xref_count", "comment_count", "reference_count", "lit_pubmed_id",
            ],
        },
    },
    "features": {
        "description": "One row per positional annotation (domain, signal, transmembrane, etc). Sorted by (reviewed DESC, organism_id ASC, accession ASC, feature_index ASC): within a protein, features appear in UniProt's order; use ORDER BY start_pos for position order.",
        "element": "feature",
        "primary_key": ["accession", "feature_index"],
        "foreign_keys": {"accession": "entries.accession", "organism_id": "entries.organism_id"},
        "columns": {
            "shortcut": [
                "accession", "reviewed", "organism_id", "organism_name", "length", "feature_index",
                "start_pos", "end_pos", "ligand_id", "evidence_codes",
            ],
        },
    },
    "xrefs": {
        "description": "One row per cross-reference to an external database (PDB, Ensembl, GO, InterPro, etc).",
        "element": "cross-reference",
        "primary_key": ["accession", "xref_index"],
        "foreign_keys": {"accession": "entries.accession", "organism_id": "entries.organism_id"},
        "columns": {"shortcut": ["accession", "reviewed", "organism_id", "xref_index"]},
    },
    "comments": {
        "description": "One row per comment annotation (function, subcellular location, disease, etc).",
        "element": "comment",
        "primary_key": ["accession", "comment_index"],
        "foreign_keys": {"accession": "entries.accession", "organism_id": "entries.organism_id"},
        "columns": {"shortcut": ["accession", "reviewed", "organism_id", "comment_index", "text_value"]},
    },
    "publications": {
        "description": "One row per literature citation or submission (derived from UniProtKB entry.references).",
        "element": "reference",
        "primary_key": ["accession", "reference_index"],
        "foreign_keys": {"accession": "entries.accession", "organism_id": "entries.organism_id"},
        "columns": {"shortcut": ["accession", "reviewed", "organism_id", "reference_index"]},
    },
    "accession_map": {
        "description": "One row per primary or secondary accession, mapping to the current primary accession. The lookup table for point queries and for resolving retired accessions.",
        "element": None,
        "primary_key": ["accession", "primary_accession"],
        "foreign_keys": {"primary_accession": "entries.accession"},
        "columns": {"shortcut": ["accession", "primary_accession", "is_primary", "reviewed", "organism_id"]},
    },
}


def column_categories(table: str, names) -> dict[str, list[str]]:
    """{"shortcut": [...], "source": [...]}: ``names`` (a table's columns, as
    built or as a reader sees them) split by kind, each in column order.  The
    Hive partition column a reader may add is neither."""
    shortcuts = set(TABLE_META[table]["columns"]["shortcut"])
    return {"shortcut": [n for n in names if n in shortcuts],
            "source": [n for n in names if n not in shortcuts and n != PARTITION_KEY]}


def column_description(table: str, column: str) -> str:
    """The description of a column: COLUMN_DESCRIPTIONS, or for a source
    column UniProt added since, one saying so."""
    if (table, column) in COLUMN_DESCRIPTIONS:
        return COLUMN_DESCRIPTIONS[(table, column)]
    return (f"`{column}` of the UniProt JSON {TABLE_META[table]['element']}, as shipped "
            f"(new upstream; not yet described).")


def undescribed_source_columns(table: str, names) -> list[str]:
    """Source columns among ``names`` with no COLUMN_DESCRIPTIONS entry:
    fields UniProt added since this code was written."""
    return [n for n in column_categories(table, names)["source"]
            if (table, n) not in COLUMN_DESCRIPTIONS]


# ─── Core pipeline ──────────────────────────────────────────────────────

def _annotate_schema(schema: pa.Schema, label: str, file_meta: dict[str, str]) -> pa.Schema:
    """Attach description / category / source_path metadata to every field
    and the release-level key/values to the schema.  A shortcut column
    without a COLUMN_DESCRIPTIONS entry is a build failure; a source column
    UniProt added gets a description saying so (column_description)."""
    shortcuts = TABLE_META[label]["columns"]["shortcut"]
    missing = [f.name for f in schema
               if f.name in shortcuts and (label, f.name) not in COLUMN_DESCRIPTIONS]
    if missing:
        raise RuntimeError(f"{label}: no COLUMN_DESCRIPTIONS entry for {missing}")
    fields = []
    for f in schema:
        md = {"description": column_description(label, f.name)}
        if f.name in shortcuts:
            md["category"] = "shortcut"
            src = COLUMN_SOURCES.get((label, f.name))
        else:
            md["category"] = "source"
            src = _source_path(label, f.name)
        if src:
            md["source_path"] = src
        fields.append(f.with_metadata(md))
    return pa.schema(fields, metadata=file_meta)


def _file_metadata(release) -> dict[str, str]:
    """Key/value metadata written into every Parquet footer."""
    return {"uniprot_release": release or "", "schema_version": SCHEMA_VERSION,
            "license": DATA_LICENSE, "generator": GENERATOR, "zstd_level": str(ZSTD_LEVEL)}


# A batch holding more than this is cast in row slices (_to_regular_types):
# every slice's arrays stay well under the 2 GiB a 32-bit offset addresses.
MAX_CHUNK_BYTES = 1 << 30


def _regular_field(f: pa.Field) -> pa.Field:
    return f.with_type(_regular_type(f.type))


def _regular_type(t: pa.DataType) -> pa.DataType:
    """``t`` with every large_string / large_binary / large_list (at any
    depth, JSON's storage included) replaced by its 32-bit-offset form."""
    if isinstance(t, pa.JsonType):
        return pa.json_(_regular_type(t.storage_type))
    if pa.types.is_large_string(t):
        return pa.string()
    if pa.types.is_large_binary(t):
        return pa.binary()
    if pa.types.is_large_list(t) or pa.types.is_list(t):
        return pa.list_(_regular_field(t.value_field))
    if pa.types.is_map(t):
        return pa.map_(_regular_field(t.key_field), _regular_field(t.item_field),
                       keys_sorted=t.keys_sorted)
    if pa.types.is_struct(t):
        return pa.struct([_regular_field(t.field(i)) for i in range(t.num_fields)])
    return t


def _regular_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema([_regular_field(f) for f in schema], metadata=schema.metadata)


def _has_map(t: pa.DataType) -> bool:
    if pa.types.is_map(t):
        return True
    if pa.types.is_struct(t):
        return any(_has_map(t.field(i).type) for i in range(t.num_fields))
    if pa.types.is_large_list(t) or pa.types.is_list(t):
        return _has_map(t.value_type)
    return False


def _regular_array(a: pa.Array) -> pa.Array:
    """``a`` (possibly a slice) with _regular_type's type.  PyArrow's cast
    aborts on a MAP from DuckDB (its keys carry a validity bitmap, which
    MapArray validation rejects), so a type holding a MAP is rebuilt level
    by level and every other type is cast."""
    t = a.type
    if isinstance(t, pa.JsonType):
        return pa.ExtensionArray.from_storage(_regular_type(t), _regular_array(a.storage))
    if not _has_map(t):
        return a.cast(_regular_type(t))
    mask = a.is_null() if a.null_count else None
    if pa.types.is_struct(t):
        return pa.StructArray.from_arrays(
            [_regular_array(a.field(i)) for i in range(t.num_fields)],
            fields=[_regular_field(t.field(i)) for i in range(t.num_fields)], mask=mask)
    # A list or map: its own offsets (rebased to 0) over its slice of the child.
    start, end = a.offsets[0].as_py(), a.offsets[-1].as_py()
    offsets = pc.subtract(a.offsets, start).cast(pa.int32())
    if pa.types.is_map(t):
        keys = _regular_array(a.keys.slice(start, end - start))
        if keys.null_count == 0:
            keys = pa.Array.from_buffers(keys.type, len(keys), [None, *keys.buffers()[1:]],
                                         offset=keys.offset)
        return pa.MapArray.from_arrays(offsets, keys,
                                       _regular_array(a.items.slice(start, end - start)),
                                       type=_regular_type(t), mask=mask)
    return pa.ListArray.from_arrays(offsets, _regular_array(a.values.slice(start, end - start)),
                                    type=_regular_type(t), mask=mask)


def _to_regular_types(tbl: pa.Table) -> pa.Table:
    """``tbl`` (a batch DuckDB exported with arrow_large_buffer_size) with
    the regular Arrow types, so every file has the same schema as ever.  The
    conversion runs on row slices of at most MAX_CHUNK_BYTES, each becoming
    one chunk of the result."""
    target = _regular_schema(tbl.schema)
    if target == tbl.schema:
        return tbl

    def slices(t):
        if t.nbytes <= MAX_CHUNK_BYTES or t.num_rows <= 1:
            return [t]
        half = t.num_rows // 2
        return slices(t.slice(0, half)) + slices(t.slice(half))

    return pa.concat_tables([
        pa.Table.from_arrays([pa.chunked_array([_regular_array(c) for c in col.chunks], f.type)
                              for col, f in zip(s.columns, target)], schema=target)
        for s in slices(tbl)])


def _json_schema(schema: pa.Schema, names) -> pa.Schema:
    """``schema`` with the ``names`` columns typed arrow.json."""
    return pa.schema([f.with_type(pa.json_()) if f.name in names else f for f in schema],
                     metadata=schema.metadata)


def _as_json_columns(tbl: pa.Table, names) -> pa.Table:
    """``tbl`` with the ``names`` columns (JSON text, which DuckDB hands over
    as plain strings) typed arrow.json, which Parquet stores as its JSON
    logical type: every reader can tell a JSON column from a string one."""
    if not names:
        return tbl
    cols = [pa.chunked_array([pa.ExtensionArray.from_storage(pa.json_(), c) for c in col.chunks],
                             pa.json_()) if name in names else col
            for name, col in zip(tbl.column_names, tbl.columns)]
    return pa.Table.from_arrays(cols, schema=_json_schema(tbl.schema, names))


def stream_to_parquet(con, sql, table_dir, batch_size, label="table", sort_order=None,
                      partition_column="reviewed", release=None, sentinel_path=None,
                      source=None, json_columns=frozenset()):
    """Stream DuckDB result → Hive-partitioned Parquet files in bounded-memory batches.

    DuckDB executes the query lazily and yields Arrow record batches of
    `batch_size` rows.  Batches are accumulated into files of ~TARGET_FILE_BYTES
    under ``<table_dir>/review_status=<swissprot|trembl>/``:
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
        sort_order: List of sort order strings (e.g. ["reviewed DESC", "organism_id ASC"])
                   to embed in Parquet footer metadata.
        partition_column: boolean column that decides the partition directory
        release: UniProt release name, written into the footer metadata
        sentinel_path: where to write the completion sentinel (None: no sentinel)
        source: the input's fingerprint (_input_fingerprint), recorded in the sentinel
        json_columns: columns holding JSON text, written as Parquet JSON (_as_json_columns)

    Returns (total_rows, file_list, arrow_schema); file paths are relative to
    ``table_dir`` (``review_status=swissprot/entries_00001.parquet``).  A
    zero-row result still publishes one empty schema-valid file under
    ``review_status=swissprot/`` so glob and dataset readers never miss the
    table.
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
            tbl = _as_json_columns(_to_regular_types(pa.Table.from_batches([record_batch])),
                                   json_columns)
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

        if arrow_schema is None:
            # Zero rows (a tiny subset may have no entries with, say,
            # comments): still publish one empty schema-valid file so
            # ``<table>/**/*.parquet`` globs and dataset readers work.
            arrow_schema = _annotate_schema(
                _json_schema(_regular_schema(reader.schema), json_columns), label,
                _file_metadata(release))
            sorting_columns = build_sorting_columns(sort_order, arrow_schema) if sort_order else None
            st["side"] = PARTITION_VALUES[True]
            open_writer()

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
        _write_sentinel(sentinel_path, label, files, total_rows, release, source)
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


def _input_fingerprint(jsonl_path):
    """Size and mtime of the input (through symlinks, as Nextflow stages it):
    a sentinel vouches for its table only when rebuilt from the same input."""
    st = os.stat(jsonl_path)
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns}


def _write_sentinel(path, table_name, files, row_count, release, source=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"table": table_name, "files": list(files), "row_count": row_count,
                   "release": release, "input": source,
                   "completed_at": datetime.now(timezone.utc).isoformat()},
                  f, indent=2)
    os.replace(tmp, path)              # the sentinel itself appears atomically


def _read_sentinel(path):
    """The sentinel's contents, or None if absent or unreadable."""
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _table_is_complete(outdir, table_name, table_dir, source, release):
    """(complete, reason): complete only if the sentinel exists, lists exactly
    the files on disk, and was written for this input and release."""
    sentinel = _read_sentinel(_sentinel_path(outdir, table_name))
    if sentinel is None:
        return False, "no completion sentinel (interrupted publish?)"
    if sentinel.get("files") != _list_table_files(table_dir):
        return False, "completion sentinel does not match them (interrupted publish?)"
    if sentinel.get("input") != source:
        return False, "built from a different input"
    if sentinel.get("release") != release:
        return False, f"built for release {sentinel.get('release')!r}"
    return True, None


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
    """Size, SHA-256, MD5 and organism_id range of one Parquet file.  One streamed
    pass while the file is still in page cache."""
    path = os.path.join(table_dir, rel_path)
    sha, md5 = hash_file(path)
    meta = pq.read_metadata(path)
    mins, maxs = [], []
    if "organism_id" in meta.schema.names:
        col = meta.schema.names.index("organism_id")        # top-level scalar: index is exact
        for i in range(meta.num_row_groups):
            stats = meta.row_group(i).column(col).statistics
            if stats and stats.has_min_max:
                mins.append(stats.min)
                maxs.append(stats.max)
    return {"size_bytes": os.path.getsize(path), "sha256": sha, "md5": md5,
            "taxid_min": min(mins) if mins else None, "taxid_max": max(maxs) if maxs else None}


def hash_file(path, chunk_size=8 << 20):
    """(sha256, md5) hex digests of a file in one streamed pass.  The one
    hashing routine of the lake: the manifest, SHA256SUMS.txt, provenance
    (release_manifest.py) and validation (validate_lake.py) all use it."""
    sha, md5 = hashlib.sha256(), hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha.update(chunk)
            md5.update(chunk)
    return sha.hexdigest(), md5.hexdigest()


def _write_checksums(outdir, manifest, extra_files):
    """lake/SHA256SUMS.txt (sha256sum -c format) and RELEASE.metalink (Metalink 4,
    RFC 5854) at the lake root.
    ``extra_files`` are lake-root files hashed after the Parquet files."""
    import xml.etree.ElementTree as ET

    entries = []                                 # (rel path from outdir, size, sha256, md5)
    for table, info in manifest["tables"].items():
        for rel in info["files"]:
            d = info["file_details"][rel]
            entries.append((f"{table}/{rel}", d["size_bytes"], d["sha256"], d["md5"]))
    for name in extra_files:
        path = os.path.join(outdir, name)
        sha, md5 = hash_file(path)
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
    # Parquet.  Table directories hold Parquet only.
    metalink(entries, os.path.join(outdir, "RELEASE.metalink"))
    eprint("  Wrote RELEASE.metalink")


def _add_file_details(table_info, table_dir):
    """Fill file_details (per file) and size_bytes (sum) on a manifest table entry."""
    table_info["file_details"] = {rel: _file_details(table_dir, rel) for rel in table_info["files"]}
    table_info["size_bytes"] = sum(d["size_bytes"] for d in table_info["file_details"].values())


def _partitioning_block(name, files, table_dir, partition_column):
    """The manifest's per-table partitioning descriptor."""
    return {
        "scheme": "hive",
        "keys": [{"name": PARTITION_KEY, "type": "string",
                  "values": sorted({f.split("/")[0].split("=", 1)[1] for f in files}),
                  "derived_from": partition_column,
                  "row_counts": _partition_row_counts(table_dir, files)}],
        "note": ("accession_map is excluded from any future second partition level"
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


def _split_top(s: str) -> list[str]:
    """Split an Arrow type string (a manifest column "type") on the commas
    outside brackets: the fields of a struct<...> body.  Parameters such as
    timestamp[us, tz=UTC] or decimal128(10, 2) stay whole.  Field names are
    not quoted, so one containing ", " or a bracket splits wrongly."""
    parts, depth, cur = [], 0, ""
    for ch in s:
        depth += (ch in "<([") - (ch in ">)]")
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    return [p.strip() for p in parts + [cur]]


# ─── Column semantic descriptions ───────────────────────────────────────
# Maps (table, column) → human-readable description for the Parquet field
# metadata, datapackage.json and SCHEMA.md.  Every shortcut must have one; a
# source column UniProt adds gets a generated one (column_description).
# Source columns hold the JSON value as shipped: "Type follows the input".

_KEY_DESCRIPTIONS = {
    "accession": "Parent entry's primary accession. Foreign key → entries.accession.",
    "reviewed": "True if the parent entry is Swiss-Prot (reviewed).",
    "organism_id": "NCBI taxonomy ID of the parent entry. Foreign key → entries.organism_id.",
}

COLUMN_DESCRIPTIONS = {
    # ── entries: shortcuts ──
    ("entries", "accession"):                "UniProtKB primary accession (e.g. P05067). Stable identifier; unique per entry.",
    ("entries", "id"):                 "UniProtKB mnemonic entry name (e.g. A4_HUMAN). May change between releases.",
    ("entries", "reviewed"):           "True for Swiss-Prot (manually reviewed); false for TrEMBL (automated annotation).",
    ("entries", "organism_id"):              "NCBI taxonomy ID of the source organism (e.g. 9606 for Homo sapiens).",
    ("entries", "organism_name"):      "Scientific name of the source organism.",
    ("entries", "gene_primary"):         "Primary gene name of each gene of the entry, in the genes array's order; NULL for a gene with no geneName (ORF or ordered locus names only).",
    ("entries", "protein_name_primary"):       "Recommended (Swiss-Prot) or submitted (TrEMBL) full protein name.",
    ("entries", "length"):         "Sequence length in amino acids.",
    ("entries", "gene_synonym"):      "Flattened list of gene name synonyms across all genes.",
    ("entries", "alt_protein_names"):  "List of alternative full protein names from proteinDescription.alternativeNames.",
    ("entries", "ec"):         "Distinct EC (Enzyme Commission) numbers from every naming block (recommended, "
                                 "alternative, submission names) of the entry and of its domains (includes) "
                                 "and chains (contains).",
    ("entries", "division"):           "UniProt taxonomic division (archaea, bacteria, fungi, human, invertebrates, mammals, plants, rodents, vertebrates, viruses, unclassified), derived from lineage and organism_id.",
    ("entries", "go_id"):             "Distinct Gene Ontology term IDs (e.g. GO:0005634) from cross-references.",
    ("entries", "go_terms"):           "GO annotations as {id, aspect (P/F/C), term, evidence_type}; go_id is the flat id list.",
    ("entries", "xref_dbs"):           "Distinct database names referenced in cross-references (e.g. PDB, Ensembl).",
    ("entries", "xref_proteomes"):       "Distinct UniProt proteome ids (UP…) from cross-references, sorted.",
    ("entries", "keywordid"):        "UniProt keyword IDs (e.g. KW-0181).",
    ("entries", "keyword"):      "UniProt keyword names (e.g. 'Complete proteome').",
    ("entries", "keyword_categories"): "Keyword categories aligned with keywordid (e.g. Molecular function, Biological process).",
    ("entries", "date_created"):       "Date the entry was first made public in UniProtKB (entryAudit.firstPublicDate as a DATE).",
    ("entries", "date_modified"):      "Date of the last annotation update (entryAudit.lastAnnotationUpdateDate as a DATE).",
    ("entries", "date_sequence_modified"):  "Date of the last sequence update (entryAudit.lastSequenceUpdateDate as a DATE).",
    ("entries", "feature_count"):      "Number of positional features (rows in features).",
    ("entries", "xref_count"):         "Number of cross-references to external databases (rows in xrefs).",
    ("entries", "comment_count"):      "Number of comment annotations (rows in comments).",
    ("entries", "reference_count"):    "Number of literature/submission references (rows in publications).",
    ("entries", "lit_pubmed_id"):         "Distinct PubMed ids cited by the entry, as strings: numeric ids in numeric order, then any non-numeric id.",
    # ── entries: source columns (the entry's keys) ──
    ("entries", "entryType"):          "Entry type, e.g. 'UniProtKB reviewed (Swiss-Prot)'. Type follows the input.",
    ("entries", "primaryAccession"):   "Primary accession, as shipped (the accession shortcut holds the same value). Type follows the input.",
    ("entries", "secondaryAccessions"): "Secondary (merged/demerged) accessions that resolve to this entry. Type follows the input.",
    ("entries", "uniProtkbId"):        "Mnemonic entry name (the same value as id). Type follows the input.",
    ("entries", "entryAudit"):         "Dates and versions: firstPublicDate, lastAnnotationUpdateDate, lastSequenceUpdateDate, entryVersion, sequenceVersion. Type follows the input.",
    ("entries", "annotationScore"):    "UniProt annotation score (1-5). Type follows the input.",
    ("entries", "organism"):           "Source organism: scientificName, commonName, synonyms, taxonId, lineage, evidences. Type follows the input.",
    ("entries", "organismHosts"):      "Host organisms (of viruses). Type follows the input.",
    ("entries", "proteinExistence"):   "Protein existence evidence level (e.g. '1: Evidence at protein level'). Type follows the input.",
    ("entries", "proteinDescription"): "Protein names: recommendedName, alternativeNames, submissionNames, flag, contains, includes, ... Type follows the input.",
    ("entries", "sequence"):           "The sequence: value (amino acids, one-letter codes), length, molWeight, crc64, md5. Read sequence.value for the amino acids. Type follows the input.",
    ("entries", "genes"):              "Genes: geneName, synonyms, orfNames, orderedLocusNames (with evidences). Type follows the input.",
    ("entries", "geneLocations"):      "Gene locations (mitochondrion, plastid, plasmid, ...). Type follows the input.",
    ("entries", "keywords"):           "Keywords: id, category, name, evidences. Type follows the input.",
    ("entries", "extraAttributes"):    "countByCommentType, countByFeatureType, uniParcId, ... Type follows the input.",

    # ── features ──
    **{("features", k): v for k, v in _KEY_DESCRIPTIONS.items()},
    ("features", "organism_name"):     "Scientific name of the parent entry's organism (denormalized for convenience).",
    ("features", "length"):        "Sequence length of the parent entry (denormalized for coverage calculations).",
    ("features", "feature_index"):     "Position of this feature in the entry's features array (0-based). ORDER BY feature_index gives UniProt's order.",
    ("features", "start_pos"):         "Start position in the sequence (1-based, inclusive): location.start.value.",
    ("features", "end_pos"):           "End position in the sequence (1-based, inclusive): location.end.value.",
    ("features", "ligand_id"):         "ChEBI or other identifier of the bound ligand (ligand.id). May be null.",
    ("features", "evidence_codes"):    "Evidence codes (e.g. ECO:0000269) of evidences, in order.",
    ("features", "type"):              "Feature type (e.g. 'Domain', 'Signal peptide', 'Natural variant'). Type follows the input.",
    ("features", "location"):          "Location: start and end ({value, modifier}), and sequence (an isoform) when not the canonical one. Type follows the input.",
    ("features", "description"):       "Free-text description of the feature. Type follows the input.",
    ("features", "featureId"):         "UniProt feature identifier (e.g. PRO_0000001234). Type follows the input.",
    ("features", "evidences"):         "Evidence records (evidenceCode, source, id). Type follows the input.",
    ("features", "featureCrossReferences"): "Cross-references of the feature (e.g. dbSNP). Type follows the input.",
    ("features", "ligand"):            "Bound ligand of a binding site: name, id, label, note. Type follows the input.",
    ("features", "ligandPart"):        "Part of the ligand that binds: name, id, label, note. Type follows the input.",
    ("features", "alternativeSequence"): "Variant sequences: originalSequence and alternativeSequences. Type follows the input.",

    # ── xrefs ──
    **{("xrefs", k): v for k, v in _KEY_DESCRIPTIONS.items()},
    ("xrefs", "xref_index"):          "Position of this cross-reference in the entry's uniProtKBCrossReferences array (0-based).",
    ("xrefs", "database"):            "External database name (e.g. 'PDB', 'Ensembl', 'GO', 'InterPro'). Type follows the input.",
    ("xrefs", "id"):                  "Identifier in the external database (e.g. '1ABC' for PDB). Type follows the input.",
    ("xrefs", "properties"):          "Database-specific properties, a list of {key, value}. Type follows the input.",
    ("xrefs", "isoformId"):           "Isoform accession when the cross-reference is specific to an isoform. Type follows the input.",
    ("xrefs", "evidences"):           "Evidence records supporting this cross-reference. Type follows the input.",

    # ── comments ──
    **{("comments", k): v for k, v in _KEY_DESCRIPTIONS.items()},
    ("comments", "comment_index"):    "Position of this comment in the entry's comments array (0-based).",
    ("comments", "text_value"):       "The texts' values joined by a blank line. Null for comments without texts (structured types, and DISEASE / SUBCELLULAR LOCATION, whose prose is in note).",
    ("comments", "commentType"):      "Comment type (e.g. 'FUNCTION', 'DISEASE', 'SUBCELLULAR LOCATION'). Type follows the input.",
    ("comments", "texts"):            "Free-text paragraphs ({value, evidences}). Type follows the input.",
    ("comments", "note"):             "Note of the comment (texts). Type follows the input.",
    ("comments", "molecule"):         "Isoform or chain the comment applies to. Type follows the input.",
    ("comments", "reaction"):         "Catalysed reaction (CATALYTIC ACTIVITY): name, reactionCrossReferences, ecNumber. Type follows the input.",
    ("comments", "physiologicalReactions"): "Physiological direction of the reaction (CATALYTIC ACTIVITY). Type follows the input.",
    ("comments", "cofactors"):        "Cofactors (COFACTOR). Type follows the input.",
    ("comments", "interactions"):     "Binary interactions (INTERACTION). Type follows the input.",
    ("comments", "subcellularLocations"): "Subcellular locations, topologies and orientations (SUBCELLULAR LOCATION). Type follows the input.",
    ("comments", "events"):           "Alternative-products events, e.g. 'Alternative splicing' (ALTERNATIVE PRODUCTS). Type follows the input.",
    ("comments", "isoforms"):         "Isoforms: name, isoformIds, isoformSequenceStatus, sequenceIds (ALTERNATIVE PRODUCTS). Type follows the input.",
    ("comments", "disease"):          "Disease: diseaseId, acronym, description, diseaseCrossReference (DISEASE). Type follows the input.",
    ("comments", "resourceName"):     "Name of an online resource (WEB RESOURCE). Type follows the input.",
    ("comments", "resourceUrl"):      "URL of an online resource (WEB RESOURCE). Type follows the input.",
    ("comments", "ftp"):              "Whether the web resource is an FTP site (WEB RESOURCE). Type follows the input.",
    ("comments", "sequenceCautionType"): "Kind of sequence conflict (SEQUENCE CAUTION). Type follows the input.",
    ("comments", "sequence"):         "Sequence the caution refers to (SEQUENCE CAUTION). Type follows the input.",
    ("comments", "evidences"):        "Evidence records of the comment. Type follows the input.",
    ("comments", "method"):           "Measurement method (MASS SPECTROMETRY). Type follows the input.",
    ("comments", "molWeight"):        "Measured molecular weight (MASS SPECTROMETRY). Type follows the input.",
    ("comments", "molWeightError"):   "Error of the measured weight (MASS SPECTROMETRY). Type follows the input.",
    ("comments", "kineticParameters"): "Kinetic parameters (BIOPHYSICOCHEMICAL PROPERTIES). Type follows the input.",
    ("comments", "phDependence"):     "pH dependence (BIOPHYSICOCHEMICAL PROPERTIES). Type follows the input.",
    ("comments", "temperatureDependence"): "Temperature dependence (BIOPHYSICOCHEMICAL PROPERTIES). Type follows the input.",
    ("comments", "redoxPotential"):   "Redox potential (BIOPHYSICOCHEMICAL PROPERTIES). Type follows the input.",
    ("comments", "absorption"):       "Absorption (BIOPHYSICOCHEMICAL PROPERTIES). Type follows the input.",
    ("comments", "locationType"):     "Location type of an RNA editing site (RNA EDITING). Type follows the input.",
    ("comments", "positions"):        "Edited positions (RNA EDITING). Type follows the input.",

    # ── publications ──
    **{("publications", k): v for k, v in _KEY_DESCRIPTIONS.items()},
    ("publications", "reference_index"): "Position of this reference in the entry's references array (0-based).",
    ("publications", "referenceNumber"): "Number of the reference in the entry's reference list. Type follows the input.",
    ("publications", "citation"):      "The citation: citationType, id, title, authors, authoringGroup, publicationDate, journal, volume, firstPage, lastPage, submissionDatabase, citationCrossReferences, ... Type follows the input.",
    ("publications", "referencePositions"): "What the reference is cited for (e.g. 'NUCLEOTIDE SEQUENCE'). Type follows the input.",
    ("publications", "referenceComments"): "Reference comments (strain, tissue, plasmid, ...). Type follows the input.",
    ("publications", "evidences"):    "Evidence records of this reference. Type follows the input.",

    # accession_map
    ("accession_map", "accession"):         "Primary or secondary accession (the lookup key). A secondary accession can map to more than one primary (split entries).",
    ("accession_map", "primary_accession"): "Current primary accession the key resolves to (FK → entries.accession).",
    ("accession_map", "is_primary"):  "True when accession == primary_accession.",
    ("accession_map", "reviewed"):    "True if the primary entry is Swiss-Prot (copied from entries.reviewed).",
    ("accession_map", "organism_id"):       "NCBI taxonomy ID of the primary entry (copied from entries.organism_id).",
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
    if s in ("json", "extension<arrow.json>"):
        return {"type": "any"}                 # any JSON value

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
                "description": column_description(table_name, col_name),
                "constraints": {
                    "required": not col["nullable"],
                },
                "arrowType": col["type"],
                "nullable": col["nullable"],
            }
            if "format" in frictionless:
                field["format"] = frictionless["format"]

            # Which kind of column this is: a shortcut, or a source column
            field["columnCategory"] = ("shortcut" if col_name in meta["columns"]["shortcut"]
                                       else "source")

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
            "An analysis-ready Parquet representation of the complete UniProtKB "
            "database. Five sorted tables (entries, features, xrefs, comments, "
            "publications) hold UniProt's JSON as it is shipped, one column per "
            "key (source columns), next to typed shortcut columns for common "
            "query patterns; accession_map resolves primary and secondary "
            "accessions. "
            "Per-file SHA-256 hashes are in SHA256SUMS.txt and manifest.json file_details. "
            "The data is CC BY 4.0; the pipeline code that builds it (homepage) is MIT."
        ),
        "homepage": "https://github.com/ebi-uniprot/uniprot-parquet",
        "version": SCHEMA_VERSION,
        # The package's licenses are its data's: the MIT license covers only
        # the pipeline code, so it is not listed here.
        "licenses": [
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
                    "(entries, features, xrefs, comments, publications, accession_map)"
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
        help="Skip tables whose completion sentinel (.complete/<table>.json) "
             "lists exactly the Parquet files on disk. Enables resume after OOM.",
    )
    parser.add_argument(
        "--only", default=None,
        help="Comma-separated table names to build (default: all). For benchmarks.",
    )
    parser.add_argument(
        "--target-file-bytes", type=int, default=None,
        help=f"Roll to a new Parquet file at this size (default {TARGET_FILE_BYTES}).",
    )
    parser.add_argument(
        "--zstd-level", type=int, default=None,
        help=f"zstd compression level for Parquet pages (default {ZSTD_LEVEL}).",
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
    staging_dir = os.path.join(outdir, ".staging")
    staging_path = os.path.join(staging_dir, "staged.parquet")

    try:
        # ── Determine which tables to write ──
        skip_set = set()
        source = _input_fingerprint(jsonl_path)
        if args.skip_existing:
            for name, _, _ in table_defs:
                table_dir = os.path.join(outdir, name)
                if not os.path.isdir(table_dir):
                    continue
                existing = _list_table_files(table_dir)
                complete, reason = _table_is_complete(outdir, name, table_dir, source, args.release)
                if complete:
                    skip_set.add(name)
                    eprint(f"  SKIP {name} (complete: {len(existing)} Parquet files, --skip-existing)")
                elif existing:
                    eprint(f"  REBUILD {name}: {len(existing)} Parquet files on disk, {reason}")

        tables_to_write = {name for name, _, _ in table_defs} - skip_set

        # ── Stage JSONL → Parquet (parse JSON once, read Parquet 5× faster) ──
        if tables_to_write:
            eprint("\n--- PARQUET STAGING ---")
            os.makedirs(staging_dir, exist_ok=True)
            # Schema inference scans the whole JSONL, so only when staging.
            read_clause, staged_bytes = stage_to_parquet(
                con, build_read_clause(con, jsonl_path), staging_path)
        else:
            eprint("\n--- PARQUET STAGING (skipped — all tables already written) ---")
            staged_bytes = None

        # ── Discover schema (single source of truth for all SQL generation) ──
        # Only needed when we have tables to write (staging must have occurred).
        if tables_to_write:
            schema_paths = discover_schema_paths(staging_path)
            eprint(f"  Schema: {len(schema_paths)} field paths discovered")
            if not schema_paths:
                eprint("FATAL: staging produced an empty schema — no field paths found")
                sys.exit(1)
            # Refuse to build only if the input is not a UniProtKB dump (a
            # shortcut's required source is missing).  Fields UniProt adds are
            # not an error: they become source columns.
            check_required_paths(schema_paths)
            check_entry_types(con, read_clause)
            types = inferred_types(con, read_clause)
            json_paths = discover_json_paths(types)
            if json_paths:
                eprint(f"  Fields whose type varies between entries (their source column "
                       f"is stored as JSON): {sorted(json_paths)}")
            check_curated_sources(types)
            absent = absent_curated_sources(schema_paths)
            if absent:
                eprint("  WARNING: shortcut sources absent from input (these columns are "
                       "NULL or empty throughout; a renamed field is a new source column):")
                for line in absent:
                    eprint(f"    {line}")
            for table in ("entries", *CHILD_TABLES):
                prefix = "" if table == "entries" else CHILD_TABLES[table][0]
                keys = [k for k in _children(prefix, schema_paths)
                        if not (table == "entries" and k in _CHILD_ARRAYS)]
                new = undescribed_source_columns(table, keys)
                if new:
                    eprint(f"  New UniProt fields in {table} (source columns without a "
                           f"description): {new}")
        else:
            schema_paths, json_paths = set(), set()
        if ("proteinDescription.submittedNames" not in schema_paths
                and "proteinDescription.submissionNames" not in schema_paths):
            eprint("  Note: proteinDescription.submittedNames/submissionNames absent (normal for Swiss-Prot-only data)")
        ec_sources = [s for s in ["recommendedName", "alternativeNames", "submittedNames", "submissionNames"]
                      if f"proteinDescription.{s}.ecNumbers" in schema_paths]
        if ec_sources:
            eprint(f"  EC numbers found in: {', '.join(ec_sources)}")
        else:
            eprint("  Note: no ecNumbers in any naming block (ec column will be empty)")

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
                    "column_categories": column_categories(name, schema.names),
                }
                _add_file_details(manifest_tables[name], table_dir)
            else:
                sql = _SQL_BUILDERS[name](schema_paths, json_paths).format(read_clause=read_clause)
                row_count, files, arrow_schema = stream_to_parquet(
                    con, sql, table_dir, args.batch_size, label=name, sort_order=sort_order,
                    partition_column=partition_column, release=args.release,
                    sentinel_path=_sentinel_path(outdir, name), source=source,
                    json_columns=json_source_columns(name, schema_paths, json_paths),
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
                    "column_categories": column_categories(
                        name, arrow_schema.names if arrow_schema else []),
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

        # ── Write LICENSE: the data terms travel with the lake ──
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

        # ── SHA256SUMS.txt and RELEASE.metalink ──
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
    run_main(main)
