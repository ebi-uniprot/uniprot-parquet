"""Client tests: uniprot_parquet.connect() on full and
partial lakes, the helpers, and every README example that is cheap to run."""

import os
import pathlib
import re
import shutil
from typing import Any, NoReturn, cast

import duckdb
import pytest

import uniprot_parquet as up


def _scalar(rel: duckdb.DuckDBPyRelation) -> Any:
    # The first value of the first row: the queries here (count(*)) always give one row
    return cast(tuple[Any, ...], rel.fetchone())[0]


@pytest.fixture(scope="module")
def full_lake(parquet_lake: dict[str, str]) -> str:
    return parquet_lake["lake_dir"]


@pytest.fixture(scope="module")
def fixture_acc(full_lake: str) -> dict[str, Any]:
    con = up.connect(full_lake)
    accession, organism_id = cast(tuple[str, int], con.sql(
        "SELECT accession, organism_id FROM entries WHERE reviewed ORDER BY accession LIMIT 1").fetchone())
    iso = con.sql("SELECT accession FROM comments WHERE commentType = 'ALTERNATIVE PRODUCTS' LIMIT 1").fetchone()
    return {"accession": accession, "organism_id": organism_id, "isoform_acc": iso[0] if iso else None}


def test_connect_full_lake(full_lake: str, fixture_acc: dict[str, Any]) -> None:
    con = up.connect(full_lake)
    m = up.manifest(full_lake)
    for name, info in m["tables"].items():
        assert _scalar(con.sql(f"SELECT count(*) FROM {name}")) == info["row_count"], name
    accession, organism_id = fixture_acc["accession"], fixture_acc["organism_id"]
    assert len(con.sql(f"SELECT * FROM protein_card('{accession}')").fetchall()) == 1
    con.sql(f"SELECT * FROM organism_features({organism_id}, 'Chain')").fetchall()
    con.sql(f"SELECT * FROM organism_xrefs({organism_id}, ['GO'])").fetchall()
    con.sql(f"SELECT * FROM organism_comments({organism_id}, 'FUNCTION')").fetchall()
    con.sql(f"SELECT * FROM entries_with_features({organism_id})").fetchall()
    con.sql(f"SELECT * FROM entries_with_xrefs({organism_id}, ['GO'])").fetchall()
    iso = fixture_acc["isoform_acc"]
    if iso:
        assert len(con.sql(f"SELECT * FROM unnest_isoforms('{iso}')").fetchall()) > 0
    else:
        assert _scalar(con.sql(f"SELECT count(*) FROM unnest_isoforms('{accession}')")) == 0


def _copy_partial(src: str, dst: str, tables: list[str]) -> str:
    for name in tables:
        shutil.copytree(os.path.join(src, name), os.path.join(dst, name))
    for f in ("manifest.json", "datapackage.json"):
        shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    return dst


def test_connect_entries_only(full_lake: str, fixture_acc: dict[str, Any], tmp_path: pathlib.Path) -> None:
    lake = _copy_partial(full_lake, str(tmp_path / "partial"), ["entries", "accession_map"])
    con = up.connect(lake)
    m = up.manifest(lake)
    assert _scalar(con.sql("SELECT count(*) FROM entries")) == m["tables"]["entries"]["row_count"]
    assert len(con.sql(f"SELECT * FROM protein_card('{fixture_acc['accession']}')").fetchall()) == 1
    with pytest.raises(Exception, match="not in this lake copy"):
        con.sql("SELECT count(*) FROM features").fetchall()
    with pytest.raises(Exception, match="not in this lake copy"):
        con.sql(f"SELECT * FROM organism_features({fixture_acc['organism_id']}, 'Chain')").fetchall()
    t = up.tables(lake)
    assert t["entries"]["present"] is True and t["features"]["present"] is False
    assert t["features"]["row_count"] == m["tables"]["features"]["row_count"]


def test_connect_swissprot_only_copy(full_lake: str, tmp_path: pathlib.Path) -> None:
    """A review_status=swissprot-only rsync (all tables, one partition each) works."""
    lake = str(tmp_path / "sp")
    m = up.manifest(full_lake)
    for name, info in m["tables"].items():
        for rel in info["files"]:
            if rel.startswith("review_status=swissprot/"):
                os.makedirs(os.path.dirname(os.path.join(lake, name, rel)), exist_ok=True)
                shutil.copy(os.path.join(full_lake, name, rel), os.path.join(lake, name, rel))
    for f in ("manifest.json", "datapackage.json"):
        shutil.copy(os.path.join(full_lake, f), os.path.join(lake, f))
    con = up.connect(lake)
    sp = m["tables"]["entries"]["partitioning"]["keys"][0]["row_counts"]["swissprot"]
    assert _scalar(con.sql("SELECT count(*) FROM entries")) == sp
    assert _scalar(con.sql("SELECT count(*) FROM entries WHERE NOT reviewed")) == 0
    assert _scalar(con.sql("SELECT count(*) FROM features")) > 0
    t = up.tables(lake)["entries"]
    assert t["present"] and t["files_present"] < t["files_total"]


def test_connect_files_for_organism_id_copy(full_lake: str, fixture_acc: dict[str, Any],
                                            tmp_path: pathlib.Path) -> None:
    """A per-organism download (files_for_organism_id) of entries, plus the
    whole accession_map, works."""
    lake = str(tmp_path / "organism_id")
    amap = up.manifest(full_lake)["tables"]["accession_map"]["files"]
    files = up.files_for_organism_id(full_lake, fixture_acc["organism_id"]) + \
        [f"accession_map/{rel}" for rel in amap]
    for rel in files:
        os.makedirs(os.path.dirname(os.path.join(lake, rel)), exist_ok=True)
        shutil.copy(os.path.join(full_lake, rel), os.path.join(lake, rel))
    for f in ("manifest.json", "datapackage.json"):
        shutil.copy(os.path.join(full_lake, f), os.path.join(lake, f))
    con = up.connect(lake)
    assert _scalar(con.sql(f"SELECT count(*) FROM entries WHERE organism_id = {fixture_acc['organism_id']}")) > 0


def test_connect_without_manifest(full_lake: str, tmp_path: pathlib.Path) -> None:
    lake = str(tmp_path / "nomanifest")
    shutil.copytree(full_lake, lake)
    os.remove(os.path.join(lake, "manifest.json"))
    con = up.connect(lake)
    assert _scalar(con.sql("SELECT count(*) FROM entries")) > 0
    assert _scalar(con.sql("SELECT count(*) FROM features WHERE reviewed")) > 0


def test_files_for_organism_id(full_lake: str, fixture_acc: dict[str, Any]) -> None:
    organism_id = fixture_acc["organism_id"]
    files = up.files_for_organism_id(full_lake, organism_id)
    assert files
    fd = up.manifest(full_lake)["tables"]["entries"]["file_details"]
    for f in files:
        rel = f.split("/", 1)[1]
        assert fd[rel]["organism_id_min"] <= organism_id <= fd[rel]["organism_id_max"]
    assert up.files_for_organism_id(full_lake, -1) == []
    assert up.files_for_organism_id(full_lake, organism_id, table="features")


def test_files_for_organism_id_refuses_accession_map(full_lake: str) -> None:
    """accession_map is sorted by accession: no file subset is one organism's."""
    with pytest.raises(ValueError, match="accession_map is sorted by"):
        up.files_for_organism_id(full_lake, 9606, table="accession_map")


def test_files_for_organism_id_null_bounds(full_lake: str, tmp_path: pathlib.Path) -> None:
    """A foreign/hand-edited manifest with organism_id_max: null must be skipped, not crash."""
    import json
    m = up.manifest(full_lake)
    fd = m["tables"]["entries"]["file_details"]
    rel = next(iter(fd))
    fd[rel]["organism_id_max"] = None
    lake = tmp_path / "nullbounds"
    lake.mkdir()
    (lake / "manifest.json").write_text(json.dumps(m))
    files = up.files_for_organism_id(str(lake), 9606)
    assert f"entries/{rel}" not in files


def test_connect_path_with_quote(full_lake: str, tmp_path: pathlib.Path) -> None:
    """A lake path containing an apostrophe works (paths are SQL-escaped)."""
    lake = str(tmp_path / "o'brien" / "lake")
    shutil.copytree(full_lake, lake)
    con = up.connect(lake)  # manifest branch: explicit file list
    assert _scalar(con.sql("SELECT count(*) FROM entries")) > 0
    os.remove(os.path.join(lake, "manifest.json"))
    con = up.connect(lake)  # fallback branch: glob
    assert _scalar(con.sql("SELECT count(*) FROM entries")) > 0


def test_entries_view_has_no_duplicate_or_partition_column(full_lake: str) -> None:
    con = up.connect(full_lake)
    names = [r[0] for r in con.sql("DESCRIBE entries").fetchall()]
    assert len(names) == len(set(names))
    assert "review_status" not in names


def test_split_sql_respects_quotes() -> None:
    stmts = up._split_sql("SELECT 'a;b' AS x; SELECT 2;\nSELECT '[{\"k\":\"v;w\"}]'")
    assert stmts == ["SELECT 'a;b' AS x", "SELECT 2", "SELECT '[{\"k\":\"v;w\"}]'"]


def test_schema_join_hints_execute(full_lake: str) -> None:
    """Every join_hint schema() gives is SQL that runs, from the table its
    foreign keys point at (accession_map joins entries on primary_accession,
    a column entries does not have)."""
    con = up.connect(full_lake)
    hints = {name: info for name, info in up.schema(full_lake).items() if "join_hint" in info}
    assert set(hints) >= {"features", "xrefs", "comments", "publications", "accession_map"}
    for name, info in hints.items():
        target = {ref.split(".")[0] for ref in info["foreign_keys"].values()}
        assert target == {"entries"}, (name, target)
        assert _scalar(con.sql(f"SELECT count(*) FROM entries {info['join_hint']}")) > 0, name


def test_schema_helper(full_lake: str) -> None:
    overview = up.schema(full_lake)
    assert set(overview) >= {"entries", "features", "accession_map"}
    detail = up.schema(full_lake, "entries")
    assert "gene_primary" in detail["shortcut_columns"]
    assert "proteinDescription" in detail["source_columns"]
    assert {c["name"]: c["category"] for c in detail["columns"]}["sequence"] == "source"


README = os.path.join(os.path.dirname(__file__), "..", "README.md")


def _readme_sql_examples() -> list[str]:
    """SQL from the README: every statement in a ```sql block and every
    con.sql(...) call in a ```python block that queries the views / macros."""
    with open(README) as f:
        text = f.read()
    stmts = []
    for block in re.findall(r"```sql\n(.*?)```", text, re.S):
        for stmt in up._split_sql(block):
            body = "\n".join(ln for ln in stmt.splitlines() if not ln.strip().startswith("--")).strip()
            if body.upper().startswith("SELECT") and "BASE}" not in body:
                stmts.append(body)
    for block in re.findall(r"```python\n(.*?)```", text, re.S):
        for m in re.finditer(r'con\.sql\((?:"""(.*?)"""|"(.*?)")\)', block, re.S):
            body = (m.group(1) or m.group(2)).strip()
            if body.upper().startswith("SELECT") and "read_parquet" not in body:
                stmts.append(body)
    return stmts


@pytest.mark.parametrize("stmt", _readme_sql_examples())
def test_readme_examples(full_lake: str, stmt: str) -> None:
    """Every README SQL example runs against the fixture lake (values may not exist; it must not error)."""
    con = up.connect(full_lake)
    con.sql(stmt).fetchall()


def test_present_files_remote_probing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remote copies: one probe per partition when its first file is there; a
    per-file fallback finds a hosted per-organism subset whose first file is
    missing (files_for_organism_id gives e.g. entries_00017.parquet alone)."""
    base = "https://example.org/lake"
    rels = [f"review_status=swissprot/entries_{i:05d}.parquet" for i in (1, 2, 3)] + \
        [f"review_status=trembl/entries_{i:05d}.parquet" for i in (1, 2, 17, 18)]
    hosted = {f"{base}/entries/review_status=swissprot/entries_{i:05d}.parquet" for i in (1, 2, 3)} | \
        {f"{base}/entries/review_status=trembl/entries_00017.parquet"}
    probes: list[str] = []

    def fake_probe(con: duckdb.DuckDBPyConnection, url: str) -> bool:
        probes.append(url)
        return url in hosted

    monkeypatch.setattr(up, "_remote_readable", fake_probe)
    # None for the connection: the probe it would be handed to is faked
    no_con = cast(duckdb.DuckDBPyConnection, None)
    present = up._present_files(no_con, base, "entries", rels)
    assert present == sorted(hosted, key=lambda u: (("trembl" in u), u))
    # swissprot: first file present → one probe; trembl: first missing → the other three probed
    assert len(probes) == 1 + 4
    assert up._present_files(no_con, base, "entries", []) == []


class _FailingConnection:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def sql(self, query: str) -> NoReturn:
        raise self.error


@pytest.mark.parametrize("error", [
    duckdb.HTTPException('HTTP Error: Unable to connect to URL "https://h/x.parquet": 404 (Not Found).'),
    duckdb.HTTPException('HTTP Error: Unable to connect to URL "s3://b/x.parquet": 403 (Forbidden).'),
    duckdb.IOException('IO Error: No files found that match the pattern "https://h/x.parquet"'),
])
def test_remote_probe_not_found_means_absent(error: Exception) -> None:
    """A definite not-found is a file this copy does not hold (a partial or
    per-organism copy): left out of the views."""
    con = cast(duckdb.DuckDBPyConnection, _FailingConnection(error))   # a stand-in: sql() raises ``error``
    assert up._remote_readable(con, "https://h/x.parquet") is False


@pytest.mark.parametrize("error", [
    duckdb.HTTPException('HTTP Error: Unable to connect to URL "https://h/x.parquet": 503 (Service Unavailable).'),
    duckdb.IOException("IO Error: Could not establish connection error for HTTP HEAD to 'https://h/x.parquet'"),
    duckdb.HTTPException('HTTP Error: Unable to connect to URL "https://h/x.parquet": 429 (Too Many Requests).'),
])
def test_remote_probe_failure_is_not_taken_as_absent(error: Exception) -> None:
    """Any other failure (a 5xx, a rate limit, a timeout) says nothing about
    whether the file exists: connect() stops rather than build views that
    silently miss the file's rows."""
    con = cast(duckdb.DuckDBPyConnection, _FailingConnection(error))   # a stand-in: sql() raises ``error``
    with pytest.raises(RuntimeError, match=r"https://h/x.parquet"):
        up._remote_readable(con, "https://h/x.parquet")


# Shortcuts named like a UniProt REST return field and holding its value,
# checked by hand against rest.uniprot.org/configure/uniprotkb/result-fields
# and sample REST responses (2026-10-01).  A new shortcut with a REST name
# goes here only once its values are checked to match REST's.
REST_NAMED_SHORTCUTS = {
    "accession", "id", "reviewed", "organism_id", "gene_primary", "gene_synonym", "length", "ec",
    "go_id", "keyword", "keywordid", "date_created", "date_modified", "date_sequence_modified",
    "lit_pubmed_id",
}
# REST names a shortcut reuses for a different value (README says so).
REST_NAME_EXCEPTIONS = {"feature_count", "comment_count"}
# Every other REST return field name a shortcut must not take: those that
# hold a different value from a similarly named lake column.
REST_NAMES_NOT_USED = {"organism_name", "protein_name", "xref_proteomes"}


def test_readme_lists_the_rest_named_shortcuts() -> None:
    """README §2's list of shortcuts that share a REST field name (and its
    value) is exactly the curated set, and no shortcut takes a REST name
    known to hold something else."""
    import re

    from parquet_transform import TABLE_META
    with open(README) as f:
        text = f.read()
    sentence = text[text.index("holds that field's value:"):]
    sentence = sentence[:sentence.index("(as typed values")]
    assert set(re.findall(r"`(\w+)`", sentence)) == REST_NAMED_SHORTCUTS
    exceptions = text[text.index("Two names are the exception"):]
    exceptions = exceptions[:exceptions.index("\n")]
    assert set(re.findall(r"`(\w+)`", exceptions)) >= REST_NAME_EXCEPTIONS
    shortcuts = {c for meta in TABLE_META.values() for c in meta["columns"]["shortcut"]}
    assert shortcuts >= REST_NAMED_SHORTCUTS
    assert not shortcuts & REST_NAMES_NOT_USED
