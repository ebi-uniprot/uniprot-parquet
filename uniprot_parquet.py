"""
UniProtKB Parquet Data Lake — single-file Python client.

Wraps DuckDB with pre-configured views and macros so you never have
to think about read_parquet() globs, httpfs setup, or join patterns.

Usage (local):

    from uniprot_parquet import connect
    con = connect("/data/uniprot/2026_01/lake")

    con.sql("SELECT * FROM entries WHERE organism_id = 9606 LIMIT 5").show()
    con.sql("SELECT * FROM protein_card('P04637')").show()
    con.sql("SELECT * FROM organism_features(9606, 'Domain')").show()

Usage (remote — EBI FTP):

    con = connect("https://ftp.ebi.ac.uk/.../2026_01/lake")

That's it. The returned object is a standard duckdb.DuckDBPyConnection
with six views (entries, features, xrefs, comments, publications, accession_map)
and seven macros ready to use.

Requirements: pip install duckdb
"""

import json
import os
import re
from typing import Any, NotRequired, TypedDict, overload

import duckdb

# ── Views ───────────────────────────────────────────────────────────────
# Every table is Hive-partitioned as <table>/review_status=swissprot|trembl/.
# Views read the files with hive_partitioning = true and REPLACE the stored
# `reviewed` column with (review_status = 'swissprot'): the two are equal by
# construction (validator check 19), and deriving `reviewed` from the path
# lets DuckDB prune whole files on `WHERE reviewed = true` before opening any
# TrEMBL footer (verified with EXPLAIN ANALYZE, 2026-09-17: 1 file read vs 2
# without the partition key).  `review_status` itself is EXCLUDEd so the view
# schema equals the stored schema.
#
# connect() builds each view over the explicit file list from manifest.json
# so a partial copy of the lake (e.g. entries/ only) works and a
# plain HTTP server needs no directory listing; the glob form below is the
# fallback when the manifest cannot be read.
TABLE_NAMES = ["entries", "features", "xrefs", "comments", "publications", "accession_map"]

_VIEW_REPLACE: dict[str, str] = {}   # per-table REPLACE list, when one differs from the default


class TableStatus(TypedDict):
    """One table's entry in tables()."""
    row_count: int
    present: bool
    files_present: int
    files_total: int


class TableOverview(TypedDict):
    """One table's entry in schema() with no table named."""
    description: str
    row_count: int
    primary_key: list[str]
    sort_order: list[str]
    foreign_keys: NotRequired[dict[str, str]]
    join_hint: NotRequired[str]


class ColumnInfo(TypedDict):
    """One column in schema(lake_path, table)["columns"]."""
    name: str
    type: str
    nullable: bool
    description: str
    category: NotRequired[str]


class TableSchema(TypedDict):
    """schema(lake_path, table): one table's columns."""
    description: str
    row_count: int
    primary_key: list[str]
    foreign_keys: dict[str, str]
    sort_order: list[str]
    shortcut_columns: list[str]
    source_columns: list[str]
    all_columns: list[str]
    columns: list[ColumnInfo]


def _q(s: object) -> str:
    """Escape a value for interpolation inside a single-quoted SQL literal."""
    return str(s).replace("'", "''")


def _view_sql(table: str, source: str) -> str:
    """CREATE VIEW over a read_parquet source (a glob or an explicit file list)."""
    replace = _VIEW_REPLACE.get(table, "(review_status = 'swissprot') AS reviewed")
    return (f"CREATE OR REPLACE VIEW {table} AS SELECT * EXCLUDE (review_status) REPLACE ({replace})\n"
            f"    FROM read_parquet({source}, hive_partitioning = true)")


def _missing_view_sql(table: str, columns: list[str]) -> str:
    """A stub view that fails loudly at query time when a table is not in this
    lake copy.  It keeps the table's real column names (from manifest.json)
    so the macros that join it still bind at creation; selecting from it
    raises the message."""
    msg = (f"table \"{table}\" is not in this lake copy; download lake/{table}/ "
           f"(see README \"Download\")")
    # error() both as every column and as the filter: the optimiser folds a
    # NULL column away (a filtered query would silently return nothing) and a
    # bare count(*) never touches a column, so both are needed to raise always.
    cols = ", ".join(f"error('{msg}')::VARCHAR AS \"{c}\"" for c in columns) or f"error('{msg}') AS _"
    return f"CREATE OR REPLACE VIEW {table} AS SELECT {cols} WHERE error('{msg}')"


# ── Macros (embedded so this file is entirely self-contained) ───────────
# Macros are bound when they are created, so they name only columns every
# lake has: shortcut columns and the source keys every UniProtKB element
# carries (parquet_transform REQUIRED_PATHS / REQUIRED_CHILD_PATHS); other
# source columns are reached through x.* or the row's JSON.  A missing table
# is a stub view (_missing_view_sql), so creation still succeeds and calling
# the macro fails with the stub's message.

_MACRO_SQL = """\

-- Annotation card for a single protein: its one entries row.  Filters on
-- accession alone (the accession column of every row group).  README
-- "Looking up by accession" has the pruned two-step lookup.
CREATE OR REPLACE MACRO protein_card(target_acc) AS TABLE (
    SELECT
        e.accession, e.gene_primary, e.protein_name_primary, e.organism_scientific_name, e.organism_id,
        e.reviewed, e.length, e.proteinExistence, e.annotationScore,
        e.go_id, e.keyword, e.ec,
        e.feature_count, e.xref_count, e.comment_count, e.reference_count
    FROM entries e
    WHERE e.accession = target_acc
);

-- Features by type for an organism (every features column)
CREATE OR REPLACE MACRO organism_features(target_taxid, feature_type) AS TABLE (
    SELECT f.*
    FROM features f
    WHERE f.organism_id = target_taxid AND f.type = feature_type
    ORDER BY f.accession, f.start_pos
);

-- Cross-references filtered by database(s) for an organism
CREATE OR REPLACE MACRO organism_xrefs(target_taxid, databases) AS TABLE (
    SELECT x.* EXCLUDE (reviewed, organism_id, xref_index)
    FROM xrefs x
    WHERE x.organism_id = target_taxid AND list_contains(databases, x.database)
    ORDER BY x.accession, x.database
);

-- Comments by type for an organism (plain strings: 'FUNCTION', 'SUBCELLULAR LOCATION', etc.).
-- text_value is NULL for types whose prose is under `note` (SUBCELLULAR
-- LOCATION, DISEASE) and for structured ones (COFACTOR, CATALYTIC ACTIVITY, ...).
CREATE OR REPLACE MACRO organism_comments(target_taxid, ctype) AS TABLE (
    SELECT c.accession, c.commentType, c.text_value
    FROM comments c
    WHERE c.organism_id = target_taxid AND c.commentType = ctype
    ORDER BY c.accession
);

-- Entries joined with features for an organism (filter first, join second)
CREATE OR REPLACE MACRO entries_with_features(target_taxid) AS TABLE (
    SELECT e.accession, e.gene_primary, e.protein_name_primary, e.reviewed,
           f.* EXCLUDE (accession, reviewed, organism_id, organism_scientific_name, length)
    FROM entries e
    JOIN features f ON f.accession = e.accession AND f.organism_id = e.organism_id
    WHERE e.organism_id = target_taxid
    ORDER BY e.accession, f.start_pos
);

-- Entries joined with xrefs for an organism + specific databases
CREATE OR REPLACE MACRO entries_with_xrefs(target_taxid, databases) AS TABLE (
    SELECT e.accession, e.gene_primary, e.protein_name_primary, e.reviewed,
           x.* EXCLUDE (accession, reviewed, organism_id, xref_index)
    FROM entries e
    JOIN xrefs x ON x.accession = e.accession AND x.organism_id = e.organism_id
    WHERE e.organism_id = target_taxid AND list_contains(databases, x.database)
    ORDER BY e.accession, x.database
);

-- Unnest isoforms for a single protein (from ALTERNATIVE PRODUCTS comments)
CREATE OR REPLACE MACRO unnest_isoforms(target_acc) AS TABLE (
    SELECT
        i.accession,
        e.gene_primary,
        iso.name.value                  AS isoform_name,
        unnest(iso.isoformIds)          AS isoform_id,
        iso.isoformSequenceStatus       AS sequence_status,
        iso.sequenceIds                 AS variant_sequence_ids
    FROM (
        SELECT
            c.accession,
            unnest(from_json(to_json(c)->'$.isoforms', '[{"name":{"value":"VARCHAR"},"isoformIds":["VARCHAR"],"isoformSequenceStatus":"VARCHAR","sequenceIds":["VARCHAR"]}]')) AS iso
        FROM comments c
        WHERE c.commentType = 'ALTERNATIVE PRODUCTS'
          AND c.accession = target_acc
    ) i
    JOIN entries e ON e.accession = i.accession
);
"""  # noqa: E501 (unnest_isoforms' from_json type literal is one SQL string)


def _split_sql(sql: str) -> list[str]:
    """Split SQL text on semicolons, respecting single-quoted strings.

    Naive str.split(';') would break on semicolons inside string
    literals (e.g. JSON schemas in from_json() calls).  This tracks
    quote state so those inner semicolons are preserved.
    """
    statements = []
    current: list[str] = []
    in_quote = False
    for char in sql:
        if char == "'" and not in_quote:
            in_quote = True
            current.append(char)
        elif char == "'" and in_quote:
            in_quote = False
            current.append(char)
        elif char == ";" and not in_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(char)
    # Catch any trailing statement without a final semicolon
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)
    return statements


def connect(
    lake_path: str,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with views and macros pointed at a lake.

    Parameters
    ----------
    lake_path : str
        Path to the lake directory.  Can be:
        - A local filesystem path:  "/data/uniprot/2026_01/lake"
        - An HTTP(S) URL:           "https://ftp.ebi.ac.uk/.../lake"
        - An S3 URI:                "s3://bucket/uniprot/2026_01/lake"
    memory_limit : str, optional
        DuckDB memory limit (e.g. "4GB").  Defaults to DuckDB's own default.
    threads : int, optional
        DuckDB thread count.  Defaults to DuckDB's own default.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A connection with views (entries, features, xrefs, comments, publications)
        and macros (protein_card, organism_features, organism_xrefs,
        organism_comments, entries_with_features, entries_with_xrefs,
        unnest_isoforms) ready to use.
    """
    con = duckdb.connect()

    if memory_limit:
        con.sql(f"SET memory_limit = '{_q(memory_limit)}'")
    if threads:
        con.sql(f"SET threads = {threads}")

    # Normalise: strip trailing slash so globs work
    base = lake_path.rstrip("/")

    # Auto-install httpfs for remote paths
    if base.startswith("http://") or base.startswith("https://") or base.startswith("s3://"):
        con.sql("INSTALL httpfs; LOAD httpfs;")

    # Views: over the manifest's explicit file lists when the manifest is
    # readable (partial lakes, plain HTTP), else over globs.
    m = _read_manifest(base)
    if m is None:
        for table in TABLE_NAMES:
            con.sql(_view_sql(table, f"'{_q(base)}/{table}/*/*.parquet'"))
    else:
        for table, info in m.get("tables", {}).items():
            files = _present_files(con, base, table, info.get("files", []))
            if not files:
                con.sql(_missing_view_sql(table, [c["name"] for c in info.get("columns", [])]))
                continue
            file_list = "[" + ", ".join(f"'{_q(f)}'" for f in files) + "]"
            con.sql(_view_sql(table, file_list))

    # Macros.  Split on semicolons that aren't inside single-quoted strings,
    # so JSON schemas like '[{"name":{"value":"VARCHAR"}}]' stay intact.
    for statement in _split_sql(_MACRO_SQL):
        con.sql(statement)

    return con


def _is_remote(base: str) -> bool:
    return base.startswith(("http://", "https://", "s3://"))


def _read_manifest(base: str) -> dict[str, Any] | None:
    """manifest.json from a local dir, http(s) URL or s3 URI; None if unreachable."""
    return _read_json(base, "manifest.json")


# A definite "this copy does not hold the file": HTTP 404, or 403 (what S3
# answers for a missing object without list permission), or no match.
_NOT_FOUND = re.compile(r"\b(?:403|404)\b|Not Found|No files found")


def _remote_readable(con: duckdb.DuckDBPyConnection, url: str) -> bool:
    """True if ``url`` can be read; False if the server says it is not there.
    Any other failure (a 5xx, a rate limit, a timeout that outlasted httpfs's
    retries) raises: taking it as "absent" would build views that silently
    miss the file's rows."""
    try:
        con.sql(f"SELECT 1 FROM read_parquet('{_q(url)}') LIMIT 0")
        return True
    except duckdb.Error as e:
        if getattr(e, "status_code", None) in (403, 404) or _NOT_FOUND.search(str(e)):
            return False
        raise RuntimeError(f"Could not tell whether {url} is in this copy of the lake "
                           f"(the request failed: {e}). Retry connect().") from e


def _present_files(con: duckdb.DuckDBPyConnection, base: str, table: str, rels: list[str]) -> list[str]:
    """The table's files that exist in *this* copy of the lake, as full paths.

    A copy may hold a whole table, one partition (a Swiss-Prot-only rsync) or
    a few files (a per-organism download from files_for_organism_id).  Locally every
    file is checked.  Remotely the first file of each partition directory is
    probed: if it is there the whole partition is taken as present (a full or
    partition-level mirror, one request per partition); if it is not, every
    other file of that partition is probed individually, so a hosted
    per-organism subset (entries_00017.parquet alone) is still found at the
    cost of one request per listed file of that partition.  Only a definite
    not-found counts as absent: any other probe failure raises
    (_remote_readable).
    """
    if not rels:
        return []
    if not _is_remote(base):
        return [f"{base}/{table}/{rel}" for rel in rels
                if os.path.exists(os.path.join(base, table, rel))]
    by_part: dict[str, list[str]] = {}
    for rel in rels:
        by_part.setdefault(rel.rsplit("/", 1)[0] if "/" in rel else "", []).append(rel)
    present: list[str] = []
    for part_rels in by_part.values():
        first, rest = part_rels[0], part_rels[1:]
        if _remote_readable(con, f"{base}/{table}/{first}"):
            present.extend(f"{base}/{table}/{rel}" for rel in part_rels)
        else:
            present.extend(f"{base}/{table}/{rel}" for rel in rest
                           if _remote_readable(con, f"{base}/{table}/{rel}"))
    return present


def _read_json(base: str, name: str) -> dict[str, Any] | None:
    """A JSON sidecar (manifest.json, datapackage.json) from a local dir,
    http(s) URL or s3 URI; None if unreachable or invalid."""
    data: dict[str, Any]
    try:
        if base.startswith(("http://", "https://")):
            import urllib.request
            with urllib.request.urlopen(f"{base}/{name}", timeout=30) as r:
                data = json.loads(r.read())
                return data
        if base.startswith("s3://"):
            con = duckdb.connect()
            con.sql("INSTALL httpfs; LOAD httpfs;")
            row = con.sql(f"SELECT content FROM read_text('{_q(base)}/{name}')").fetchone()
            if row is None:   # as before: subscripting None raised, caught below
                return None
            data = json.loads(row[0])
            return data
        with open(os.path.join(base, name)) as f:
            data = json.load(f)
            return data
    except Exception:
        return None


def manifest(lake_path: str) -> dict[str, Any]:
    """Read the lake's manifest.json and return it as a dict.

    Useful for inspecting table schemas, row counts, sort orders,
    and file lists without running any queries.

    Parameters
    ----------
    lake_path : str
        Path to the lake directory: local, http(s):// or s3://.

    Returns
    -------
    dict
        The parsed manifest with keys like "tables", "release", etc.
    """
    base = lake_path.rstrip("/")
    manifest_path = f"{base}/manifest.json"
    if _is_remote(base):
        m = _read_manifest(base)
        if m is None:
            raise FileNotFoundError(f"manifest.json could not be fetched from {manifest_path}")
        return m
    try:
        with open(manifest_path) as f:
            data: dict[str, Any] = json.load(f)
            return data
    except FileNotFoundError:
        raise FileNotFoundError(
            f"manifest.json not found at {manifest_path}. "
            f"Is '{lake_path}' a valid lake directory?"
        ) from None
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"manifest.json at {manifest_path} is not valid JSON: {exc}"
        ) from None


def tables(lake_path: str) -> dict[str, TableStatus]:
    """Return {table_name: {"row_count", "present", "files_present", "files_total"}}.

    ``present`` says whether any of the table's files are in *this* copy of
    the lake (a partial download may hold entries/ and accession_map/ only, or
    only the review_status=swissprot partitions); ``files_present`` vs
    ``files_total`` shows a partial table.

    >>> tables("/data/uniprot/2026_01/lake")
    {'entries': {'row_count': 248799253, 'present': True, 'files_present': 1200, 'files_total': 1200}, ...}
    """
    base = lake_path.rstrip("/")
    m = manifest(base)
    con = duckdb.connect()
    if _is_remote(base):
        con.sql("INSTALL httpfs; LOAD httpfs;")
    out: dict[str, TableStatus] = {}
    for name, info in m.get("tables", {}).items():
        rels = info.get("files", [])
        present = _present_files(con, base, name, rels)
        out[name] = {"row_count": info["row_count"], "present": bool(present),
                     "files_present": len(present), "files_total": len(rels)}
    return out


def files_for_organism_id(lake_path: str, organism_id: int, table: str = "entries") -> list[str]:
    """Relative paths of the files of ``table`` that can contain rows for one organism.

    Every partition of entries and of the child tables is sorted
    (organism_id ASC, accession ASC, ...) and manifest.json records each
    file's organism_id range, so the files holding one organism are
    contiguous and usually one or two per side: an organism-level download
    without a layout change.

    accession_map is sorted by accession, so every file holds every organism:
    it raises ValueError (download all of accession_map, which is small).

    >>> files_for_organism_id("/data/uniprot/2026_01/lake", 9606)
    ['entries/review_status=swissprot/entries_00001.parquet', 'entries/review_status=trembl/entries_00017.parquet']
    """
    m = manifest(lake_path)
    info = m["tables"][table]
    keys = [k.split()[0] for k in info.get("sort_order", [])]
    if "organism_id" not in keys[:2]:
        raise ValueError(f"{table} is sorted by {', '.join(keys) or 'nothing'}, not by organism_id: "
                         f"any of its files can hold any organism (download them all)")
    fd = info["file_details"]
    return [f"{table}/{rel}" for rel, d in fd.items()
            if d.get("organism_id_min") is not None and d.get("organism_id_max") is not None
            and d["organism_id_min"] <= organism_id <= d["organism_id_max"]]


def _join_hint(name: str, foreign_keys: dict[str, str]) -> str:
    """JOIN clause(s) from the tables ``name``'s foreign keys point at:
    ``JOIN features ON features.accession = entries.accession AND
    features.organism_id = entries.organism_id``.  Built from each key's
    target, not USING: accession_map.primary_accession joins
    entries.accession, a column of another name."""
    by_target: dict[str, list[str]] = {}
    for column, ref in foreign_keys.items():
        ref_table, ref_column = ref.split(".", 1)
        by_target.setdefault(ref_table, []).append(f"{name}.{column} = {ref_table}.{ref_column}")
    return " ".join(f"JOIN {name} ON {' AND '.join(conds)}" for conds in by_target.values())


@overload
def schema(lake_path: str, table: None = None) -> dict[str, TableOverview]: ...
@overload
def schema(lake_path: str, table: str) -> TableSchema: ...
def schema(lake_path: str, table: str | None = None) -> dict[str, TableOverview] | TableSchema:
    """Describe the lake schema for LLM agents and interactive exploration.

    With no arguments, returns a compact overview of all tables with their
    descriptions, row counts, keys, and relationships.

    With a table name, returns detailed column info for that table,
    distinguishing shortcut columns (typed, snake_case) from source columns
    (UniProt's JSON keys, as shipped).

    Parameters
    ----------
    lake_path : str
        Path to the lake directory: local, http(s):// or s3://.
    table : str, optional
        Table name to inspect.  If omitted, returns overview of all tables.

    Returns
    -------
    dict
        Schema information: {table: TableOverview} with no table named, else
        that table's TableSchema.
    """
    m = manifest(lake_path)
    tbls = m.get("tables", {})

    if table is None:
        overview: dict[str, TableOverview] = {}
        for name, info in tbls.items():
            entry: TableOverview = {
                "description": info.get("description", ""),
                "row_count": info.get("row_count", 0),
                "primary_key": info.get("primary_key", []),
                "sort_order": info.get("sort_order", []),
            }
            fk = info.get("foreign_keys", {})
            if fk:
                entry["foreign_keys"] = fk
                entry["join_hint"] = _join_hint(name, fk)
            overview[name] = entry
        return overview

    if table not in tbls:
        raise ValueError(
            f"Unknown table '{table}'. "
            f"Available: {', '.join(tbls.keys())}"
        )

    info = tbls[table]
    cats = info.get("column_categories", {})

    # Enrich with semantic descriptions from datapackage.json if available
    dp = _load_datapackage(lake_path)
    desc_map: dict[str, str] = {}
    type_map: dict[str, str] = {}
    if dp:
        for resource in dp.get("resources", []):
            if resource["name"] == table:
                for field in resource["schema"]["fields"]:
                    desc_map[field["name"]] = field.get("description", "")
                    type_map[field["name"]] = field.get("arrowType", "")
                break

    columns: list[ColumnInfo] = []
    for col in info.get("columns", []):
        col_name = col["name"]
        column: ColumnInfo = {
            "name": col_name,
            "type": col.get("type", type_map.get(col_name, "")),
            "nullable": col.get("nullable", True),
            "description": desc_map.get(col_name, ""),
        }
        if col_name in cats.get("shortcut", []):
            column["category"] = "shortcut"
        elif col_name in cats.get("source", []):
            column["category"] = "source"
        columns.append(column)

    return {
        "description": info.get("description", ""),
        "row_count": info.get("row_count", 0),
        "primary_key": info.get("primary_key", []),
        "foreign_keys": info.get("foreign_keys", {}),
        "sort_order": info.get("sort_order", []),
        "shortcut_columns": cats.get("shortcut", []),
        "source_columns": cats.get("source", []),
        "all_columns": [c["name"] for c in info.get("columns", [])],
        "columns": columns,
    }


def _load_datapackage(lake_path: str) -> dict[str, Any] | None:
    """Load datapackage.json (local, http(s) or s3) if it exists, else None."""
    return _read_json(lake_path.rstrip("/"), "datapackage.json")


def datapackage(lake_path: str) -> dict[str, Any]:
    """Read the lake's datapackage.json (Frictionless Data Package descriptor).

    This is the machine-readable schema documentation for the lake,
    following the Frictionless Data Package spec. It contains Arrow types,
    nullability constraints, semantic descriptions, primary/foreign keys,
    sort orders, and licensing for every column in every table.

    Parameters
    ----------
    lake_path : str
        Path to the lake directory: local, http(s):// or s3://.

    Returns
    -------
    dict
        The parsed Frictionless Data Package descriptor.

    Raises
    ------
    FileNotFoundError
        If datapackage.json is not found in the lake directory.
    """
    dp = _load_datapackage(lake_path)
    if dp is None:
        raise FileNotFoundError(
            f"datapackage.json not found at {lake_path}. "
            f"Rebuild the lake with the latest parquet_transform.py to generate it."
        )
    return dp
