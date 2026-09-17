"""
UniProtKB Parquet Data Lake — single-file Python client.

Wraps DuckDB with pre-configured views and macros so you never have
to think about read_parquet() globs, httpfs setup, or join patterns.

Usage (local):

    from uniprot_parquet import connect
    con = connect("/data/uniprot/2026_01/lake")

    con.sql("SELECT * FROM entries WHERE taxid = 9606 LIMIT 5").show()
    con.sql("SELECT * FROM protein_card('P04637')").show()
    con.sql("SELECT * FROM organism_features(9606, 'Domain')").show()

Usage (remote — EBI FTP):

    con = connect("https://ftp.ebi.ac.uk/.../2026_01/lake")

That's it. The returned object is a standard duckdb.DuckDBPyConnection
with six views (entries, features, xrefs, comments, publications, accession_map)
and seven macros ready to use.

Requirements: pip install duckdb
"""

from __future__ import annotations

import json
import os

import duckdb


# ── Views ───────────────────────────────────────────────────────────────
# Every table is Hive-partitioned as <table>/review_status=swissprot|trembl/
# (plan Part D).  Views read the files with hive_partitioning = true and
# REPLACE the stored `reviewed` column with (review_status = 'swissprot'): the
# two are equal by construction (validator check 19), and deriving `reviewed`
# from the path lets DuckDB prune whole files on `WHERE reviewed = true`
# before opening any TrEMBL footer (verified with EXPLAIN ANALYZE, D.3 spike,
# 2026-09-17: 1 file read vs 2 without the partition key).  `review_status`
# itself is EXCLUDEd so the view schema equals the stored schema.
#
# connect() builds each view over the explicit file list from manifest.json
# (plan F.2.2) so a partial copy of the lake (e.g. entries/ only) works and a
# plain HTTP server needs no directory listing; the glob form below is the
# fallback when the manifest cannot be read.
TABLE_NAMES = ["entries", "features", "xrefs", "comments", "publications", "accession_map"]

_VIEW_REPLACE = {
    # comments: the stored VARCHAR JSON is exposed as JSON so -> / ->> work
    "comments": "(review_status = 'swissprot') AS reviewed, comment::JSON AS comment",
}


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
# Macros are bound at call time, so they can be created even when a table
# they join is missing; calling one then fails with the stub view's message.

_MACRO_SQL = """\

-- Annotation card for a single protein
CREATE OR REPLACE MACRO protein_card(target_acc) AS TABLE (
    SELECT
        e.acc, e.gene_name, e.protein_name, e.organism_name, e.taxid,
        e.reviewed, e.seq_length, e.protein_existence, e.annotation_score,
        e.go_ids, e.keyword_names, e.ec_numbers,
        e.feature_count, e.xref_count, e.comment_count, e.reference_count
    FROM entries e
    WHERE e.acc = target_acc
);

-- Features by type for an organism
CREATE OR REPLACE MACRO organism_features(target_taxid, feature_type) AS TABLE (
    SELECT f.acc, f.type, f.start_pos, f.end_pos, f.description,
           f.feature_id, f.evidence_codes
    FROM features f
    WHERE f.taxid = target_taxid AND f.type = feature_type
    ORDER BY f.acc, f.start_pos
);

-- Cross-references filtered by database(s) for an organism
CREATE OR REPLACE MACRO organism_xrefs(target_taxid, databases) AS TABLE (
    SELECT x.acc, x.database, x.id, x.properties
    FROM xrefs x
    WHERE x.taxid = target_taxid AND list_contains(databases, x.database)
    ORDER BY x.acc, x.database
);

-- Comments by type for an organism (plain strings: 'FUNCTION', 'SUBCELLULAR LOCATION', etc.)
CREATE OR REPLACE MACRO organism_comments(target_taxid, ctype) AS TABLE (
    SELECT c.acc, c.comment_type, c.text_value
    FROM comments c
    WHERE c.taxid = target_taxid AND c.comment_type = ctype
    ORDER BY c.acc
);

-- Entries joined with features for an organism (filter first, join second)
CREATE OR REPLACE MACRO entries_with_features(target_taxid) AS TABLE (
    SELECT e.acc, e.gene_name, e.protein_name, e.reviewed,
           f.type, f.start_pos, f.end_pos, f.description, f.feature_id
    FROM entries e
    JOIN features f ON f.acc = e.acc AND f.taxid = e.taxid
    WHERE e.taxid = target_taxid
    ORDER BY e.acc, f.start_pos
);

-- Entries joined with xrefs for an organism + specific databases
CREATE OR REPLACE MACRO entries_with_xrefs(target_taxid, databases) AS TABLE (
    SELECT e.acc, e.gene_name, e.protein_name, e.reviewed,
           x.database, x.id, x.properties
    FROM entries e
    JOIN xrefs x ON x.acc = e.acc AND x.taxid = e.taxid
    WHERE e.taxid = target_taxid AND list_contains(databases, x.database)
    ORDER BY e.acc, x.database
);

-- Unnest isoforms for a single protein (from ALTERNATIVE PRODUCTS comments)
CREATE OR REPLACE MACRO unnest_isoforms(target_acc) AS TABLE (
    SELECT
        i.acc,
        e.gene_name,
        iso.name.value                  AS isoform_name,
        unnest(iso.isoformIds)          AS isoform_id,
        iso.isoformSequenceStatus       AS sequence_status,
        iso.sequenceIds                 AS variant_sequence_ids
    FROM (
        SELECT
            acc,
            unnest(
                from_json(
                    comment->'$.isoforms',
                    '[{"name":{"value":"VARCHAR"},"isoformIds":["VARCHAR"],"isoformSequenceStatus":"VARCHAR","sequenceIds":["VARCHAR"]}]'
                )
            ) AS iso
        FROM comments
        WHERE comment_type = 'ALTERNATIVE PRODUCTS'
          AND acc = target_acc
    ) i
    JOIN entries e ON e.acc = i.acc
);
"""


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
        con.sql(f"SET memory_limit = '{memory_limit}'")
    if threads:
        con.sql(f"SET threads = {threads}")

    # Normalise: strip trailing slash so globs work
    base = lake_path.rstrip("/")

    # Auto-install httpfs for remote paths
    if base.startswith("http://") or base.startswith("https://"):
        con.sql("INSTALL httpfs; LOAD httpfs;")
    elif base.startswith("s3://"):
        con.sql("INSTALL httpfs; LOAD httpfs;")

    # Views: over the manifest's explicit file lists when the manifest is
    # readable (partial lakes, plain HTTP), else over globs.
    m = _read_manifest(base)
    if m is None:
        for table in TABLE_NAMES:
            con.sql(_view_sql(table, f"'{base}/{table}/*/*.parquet'"))
    else:
        for table, info in m.get("tables", {}).items():
            files = [f"{base}/{table}/{rel}" for rel in info.get("files", [])]
            if not files or not _table_present(con, table, files):
                con.sql(_missing_view_sql(table, [c["name"] for c in info.get("columns", [])]))
                continue
            file_list = "[" + ", ".join(f"'{f}'" for f in files) + "]"
            con.sql(_view_sql(table, file_list))

    # Macros.  Split on semicolons that aren't inside single-quoted strings,
    # so JSON schemas like '[{"name":{"value":"VARCHAR"}}]' stay intact.
    for statement in _split_sql(_MACRO_SQL):
        con.sql(statement)

    return con


def _is_remote(base: str) -> bool:
    return base.startswith(("http://", "https://", "s3://"))


def _read_manifest(base: str) -> dict | None:
    """manifest.json from a local dir, http(s) URL or s3 URI; None if unreachable."""
    try:
        if base.startswith(("http://", "https://")):
            import urllib.request
            with urllib.request.urlopen(f"{base}/manifest.json", timeout=30) as r:
                return json.loads(r.read())
        if base.startswith("s3://"):
            con = duckdb.connect()
            con.sql("INSTALL httpfs; LOAD httpfs;")
            return json.loads(con.sql(f"SELECT content FROM read_text('{base}/manifest.json')").fetchone()[0])
        with open(os.path.join(base, "manifest.json")) as f:
            return json.load(f)
    except Exception:
        return None


def _table_present(con, table: str, files: list[str]) -> bool:
    """Is the table's first file readable from this lake copy?"""
    first = files[0]
    if not _is_remote(first):
        return os.path.exists(first)
    try:
        con.sql(f"SELECT 1 FROM read_parquet('{first}') LIMIT 0")
        return True
    except (duckdb.IOException, duckdb.HTTPException, duckdb.Error):
        return False


def manifest(lake_path: str) -> dict:
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
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"manifest.json not found at {manifest_path}. "
            f"Is '{lake_path}' a valid lake directory?"
        ) from None
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"manifest.json at {manifest_path} is not valid JSON: {exc}"
        ) from None


def tables(lake_path: str) -> dict[str, dict]:
    """Return {table_name: {"row_count": n, "present": bool}} from the manifest.

    ``present`` says whether the table's files are in *this* copy of the lake
    (a partial download may hold entries/ and accession_map/ only).

    >>> tables("/data/uniprot/2026_01/lake")
    {'entries': {'row_count': 248799253, 'present': True}, 'features': {...}, ...}
    """
    base = lake_path.rstrip("/")
    m = manifest(base)
    con = duckdb.connect()
    if _is_remote(base):
        con.sql("INSTALL httpfs; LOAD httpfs;")
    out = {}
    for name, info in m.get("tables", {}).items():
        files = [f"{base}/{name}/{rel}" for rel in info.get("files", [])]
        out[name] = {"row_count": info["row_count"],
                     "present": bool(files) and _table_present(con, name, files)}
    return out


def files_for_taxid(lake_path: str, taxid: int, table: str = "entries") -> list[str]:
    """Relative paths of the files that can contain rows for one organism.

    Every partition is sorted (taxid ASC, acc ASC) and manifest.json records
    each file's taxid range, so the files holding one organism are contiguous
    and usually one or two per side — an organism-level download without a
    layout change (plan D.5).

    >>> files_for_taxid("/data/uniprot/2026_01/lake", 9606)
    ['entries/review_status=swissprot/entries_00001.parquet', 'entries/review_status=trembl/entries_00017.parquet']
    """
    m = manifest(lake_path)
    fd = m["tables"][table]["file_details"]
    return [f"{table}/{rel}" for rel, d in fd.items()
            if d.get("taxid_min") is not None and d["taxid_min"] <= taxid <= d["taxid_max"]]


def schema(lake_path: str, table: str | None = None) -> dict | str:
    """Describe the lake schema for LLM agents and interactive exploration.

    With no arguments, returns a compact overview of all tables with their
    descriptions, row counts, keys, and relationships.

    With a table name, returns detailed column info for that table,
    distinguishing convenience columns from full nested structs.

    Parameters
    ----------
    lake_path : str
        Path to the lake directory: local, http(s):// or s3://.
    table : str, optional
        Table name to inspect.  If omitted, returns overview of all tables.

    Returns
    -------
    dict | str
        Schema information.
    """
    m = manifest(lake_path)
    tbls = m.get("tables", {})

    if table is None:
        overview = {}
        for name, info in tbls.items():
            entry = {
                "description": info.get("description", ""),
                "row_count": info.get("row_count", 0),
                "primary_key": info.get("primary_key", []),
                "sort_order": info.get("sort_order", []),
            }
            fk = info.get("foreign_keys", {})
            if fk:
                entry["foreign_keys"] = fk
                entry["join_hint"] = (
                    f"JOIN {name} USING ({', '.join(fk.keys())})"
                )
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
    desc_map = {}
    type_map = {}
    if dp:
        for resource in dp.get("resources", []):
            if resource["name"] == table:
                for field in resource["schema"]["fields"]:
                    desc_map[field["name"]] = field.get("description", "")
                    type_map[field["name"]] = field.get("arrowType", "")
                break

    columns = []
    for col in info.get("columns", []):
        col_name = col["name"]
        entry = {
            "name": col_name,
            "type": col.get("type", type_map.get(col_name, "")),
            "nullable": col.get("nullable", True),
            "description": desc_map.get(col_name, ""),
        }
        if col_name in cats.get("convenience", []):
            entry["category"] = "convenience"
        elif col_name in cats.get("nested", []):
            entry["category"] = "nested"
        columns.append(entry)

    return {
        "description": info.get("description", ""),
        "row_count": info.get("row_count", 0),
        "primary_key": info.get("primary_key", []),
        "foreign_keys": info.get("foreign_keys", {}),
        "sort_order": info.get("sort_order", []),
        "convenience_columns": cats.get("convenience", []),
        "nested_columns": cats.get("nested", []),
        "all_columns": [c["name"] for c in info.get("columns", [])],
        "columns": columns,
    }


def _load_datapackage(lake_path: str) -> dict | None:
    """Load datapackage.json if it exists, else return None."""
    dp_path = os.path.join(lake_path, "datapackage.json")
    try:
        with open(dp_path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def datapackage(lake_path: str) -> dict:
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
