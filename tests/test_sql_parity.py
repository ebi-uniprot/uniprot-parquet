"""setup_views.sql and the SQL embedded in uniprot_parquet.py must not drift.

The client is deliberately a self-contained single file, so it embeds copies
of the view and macro SQL instead of loading setup_views.sql at runtime
(AUDIT.md Q-L10).  These tests assert the two definitions stay semantically
identical: same view statements, same macro names, same macro bodies —
compared token-wise, so formatting and comments may differ freely.
"""

import os

import uniprot_parquet as up

SQL_PATH = os.path.join(os.path.dirname(__file__), "..", "setup_views.sql")

MACRO_NAMES = [
    "protein_card", "organism_features", "organism_xrefs", "organism_comments",
    "entries_with_features", "entries_with_xrefs", "unnest_isoforms",
]


def _strip_comments(sql: str) -> str:
    """Remove -- line comments, respecting single-quoted strings."""
    out = []
    i, n = 0, len(sql)
    in_quote = False
    while i < n:
        ch = sql[i]
        if in_quote:
            out.append(ch)
            if ch == "'":
                in_quote = False
            i += 1
        elif ch == "'":
            in_quote = True
            out.append(ch)
            i += 1
        elif ch == "-" and sql[i:i + 2] == "--":
            while i < n and sql[i] != "\n":
                i += 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _normalize(stmt: str) -> str:
    return " ".join(_strip_comments(stmt).split())


def _statements(sql: str) -> dict[tuple[str, str], str]:
    """{(kind, name): normalized statement} for every CREATE in the SQL."""
    stmts = {}
    for stmt in up._split_sql(_strip_comments(sql)):
        norm = _normalize(stmt)
        if not norm:
            continue
        words = norm.split()
        assert words[:3] == ["CREATE", "OR", "REPLACE"], f"unexpected statement: {norm[:80]}"
        kind, name = words[3], words[4].split("(")[0]
        stmts[(kind, name)] = norm
    return stmts


def _file_statements() -> dict[tuple[str, str], str]:
    with open(SQL_PATH) as f:
        return _statements(f.read())


def _client_statements() -> dict[tuple[str, str], str]:
    # Regenerate the client's view statements with the .sql file's ${BASE}
    # placeholder so the source paths compare equal; macros embed verbatim.
    sql = ";\n".join(
        up._view_sql(t, f"'${{BASE}}/{t}/*/*.parquet'") for t in up.TABLE_NAMES
    ) + ";\n" + up._MACRO_SQL
    return _statements(sql)


def test_same_views_and_macros_defined():
    file_keys = set(_file_statements())
    client_keys = set(_client_statements())
    assert file_keys == client_keys
    assert {n for k, n in file_keys if k == "VIEW"} == set(up.TABLE_NAMES)
    assert {n for k, n in file_keys if k == "MACRO"} == set(MACRO_NAMES)


def test_statement_bodies_match():
    file_stmts = _file_statements()
    client_stmts = _client_statements()
    for key in sorted(file_stmts):
        assert file_stmts[key] == client_stmts[key], (
            f"{key[0]} {key[1]} differs between setup_views.sql and "
            f"uniprot_parquet.py:\n  file:   {file_stmts[key]}\n"
            f"  client: {client_stmts[key]}"
        )
