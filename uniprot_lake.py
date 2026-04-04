"""
UniProtKB Parquet Data Lake — single-file Python client.

Wraps DuckDB with pre-configured views and macros so you never have
to think about read_parquet() globs, httpfs setup, or join patterns.

Usage (local):

    from uniprot_lake import connect
    con = connect("/data/uniprot/2026_01/lake")

    con.sql("SELECT * FROM entries WHERE taxid = 9606 LIMIT 5").show()
    con.sql("SELECT * FROM protein_card('P04637')").show()
    con.sql("SELECT * FROM organism_features(9606, 'Domain')").show()

Usage (remote — EBI FTP):

    con = connect("https://ftp.ebi.ac.uk/.../2026_01/lake")

That's it. The returned object is a standard duckdb.DuckDBPyConnection
with five views (entries, features, xrefs, comments, refs) and six
macros ready to use.

Requirements: pip install duckdb
"""

from __future__ import annotations

import json
import os

import duckdb


# ── Setup SQL (embedded so this file is entirely self-contained) ────────
# Views create clean table names over read_parquet() globs.
# Macros provide parameterised shortcuts for common query patterns.
# {BASE} is replaced at runtime with the actual lake path.

_SETUP_SQL = """\
-- Base views
CREATE OR REPLACE VIEW entries    AS SELECT * FROM read_parquet('{BASE}/entries/*.parquet');
CREATE OR REPLACE VIEW features   AS SELECT * FROM read_parquet('{BASE}/features/*.parquet');
CREATE OR REPLACE VIEW xrefs      AS SELECT * FROM read_parquet('{BASE}/xrefs/*.parquet');
CREATE OR REPLACE VIEW comments   AS SELECT * FROM read_parquet('{BASE}/comments/*.parquet');
CREATE OR REPLACE VIEW refs       AS SELECT * FROM read_parquet('{BASE}/references/*.parquet');

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

-- Comments by type for an organism
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
"""


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
        A connection with views (entries, features, xrefs, comments, refs)
        and macros (protein_card, organism_features, organism_xrefs,
        organism_comments, entries_with_features, entries_with_xrefs)
        ready to use.
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

    # Create views and macros
    sql = _SETUP_SQL.replace("{BASE}", base)
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            con.sql(statement)

    return con


def manifest(lake_path: str) -> dict:
    """Read the lake's manifest.json and return it as a dict.

    Useful for inspecting table schemas, row counts, sort orders,
    and file lists without running any queries.

    Parameters
    ----------
    lake_path : str
        Path to the lake directory (local only for now).

    Returns
    -------
    dict
        The parsed manifest with keys like "tables", "release", etc.
    """
    manifest_path = os.path.join(lake_path, "manifest.json")
    with open(manifest_path) as f:
        return json.load(f)


def tables(lake_path: str) -> dict[str, int]:
    """Return a {table_name: row_count} dict from the manifest.

    Quick way to see what's in the lake without querying anything.

    >>> tables("/data/uniprot/2026_01/lake")
    {'entries': 248799253, 'features': 1234567890, ...}
    """
    m = manifest(lake_path)
    return {name: info["row_count"] for name, info in m.get("tables", {}).items()}


def schema(lake_path: str, table: str | None = None) -> dict | str:
    """Describe the lake schema for LLM agents and interactive exploration.

    With no arguments, returns a compact overview of all tables with their
    descriptions, row counts, keys, and relationships.

    With a table name, returns detailed column info for that table,
    distinguishing convenience columns from full nested structs.

    Parameters
    ----------
    lake_path : str
        Path to the lake directory (local only for now).
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
    return {
        "description": info.get("description", ""),
        "row_count": info.get("row_count", 0),
        "primary_key": info.get("primary_key", []),
        "foreign_keys": info.get("foreign_keys", {}),
        "sort_order": info.get("sort_order", []),
        "convenience_columns": cats.get("convenience", []),
        "nested_columns": cats.get("nested", []),
        "all_columns": [c["name"] for c in info.get("columns", [])],
    }
