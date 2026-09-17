"""Client tests (AUDIT A15, plan F.2.2): uniprot_parquet.connect() on full and
partial lakes, the helpers, and every README example that is cheap to run."""

import os
import re
import shutil

import pytest

import uniprot_parquet as up


@pytest.fixture(scope="module")
def full_lake(parquet_lake):
    return parquet_lake["lake_dir"]


@pytest.fixture(scope="module")
def fixture_acc(full_lake):
    con = up.connect(full_lake)
    acc, taxid = con.sql("SELECT acc, taxid FROM entries WHERE reviewed ORDER BY acc LIMIT 1").fetchone()
    iso = con.sql("SELECT acc FROM comments WHERE comment_type = 'ALTERNATIVE PRODUCTS' LIMIT 1").fetchone()
    return {"acc": acc, "taxid": taxid, "isoform_acc": iso[0] if iso else None}


def test_connect_full_lake(full_lake, fixture_acc):
    con = up.connect(full_lake)
    m = up.manifest(full_lake)
    for name, info in m["tables"].items():
        assert con.sql(f"SELECT count(*) FROM {name}").fetchone()[0] == info["row_count"], name
    acc, taxid = fixture_acc["acc"], fixture_acc["taxid"]
    assert len(con.sql(f"SELECT * FROM protein_card('{acc}')").fetchall()) == 1
    con.sql(f"SELECT * FROM organism_features({taxid}, 'Chain')").fetchall()
    con.sql(f"SELECT * FROM organism_xrefs({taxid}, ['GO'])").fetchall()
    con.sql(f"SELECT * FROM organism_comments({taxid}, 'FUNCTION')").fetchall()
    con.sql(f"SELECT * FROM entries_with_features({taxid})").fetchall()
    con.sql(f"SELECT * FROM entries_with_xrefs({taxid}, ['GO'])").fetchall()
    iso = fixture_acc["isoform_acc"]
    if iso:
        assert len(con.sql(f"SELECT * FROM unnest_isoforms('{iso}')").fetchall()) > 0
    else:
        assert con.sql(f"SELECT count(*) FROM unnest_isoforms('{acc}')").fetchone()[0] == 0


def _copy_partial(src, dst, tables):
    for name in tables:
        shutil.copytree(os.path.join(src, name), os.path.join(dst, name))
    for f in ("manifest.json", "datapackage.json"):
        shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    return dst


def test_connect_entries_only(full_lake, fixture_acc, tmp_path):
    lake = _copy_partial(full_lake, str(tmp_path / "partial"), ["entries", "accession_map"])
    con = up.connect(lake)
    m = up.manifest(lake)
    assert con.sql("SELECT count(*) FROM entries").fetchone()[0] == m["tables"]["entries"]["row_count"]
    assert len(con.sql(f"SELECT * FROM protein_card('{fixture_acc['acc']}')").fetchall()) == 1
    with pytest.raises(Exception, match="not in this lake copy"):
        con.sql("SELECT count(*) FROM features").fetchall()
    with pytest.raises(Exception, match="not in this lake copy"):
        con.sql(f"SELECT * FROM organism_features({fixture_acc['taxid']}, 'Chain')").fetchall()
    t = up.tables(lake)
    assert t["entries"]["present"] is True and t["features"]["present"] is False
    assert t["features"]["row_count"] == m["tables"]["features"]["row_count"]


def test_connect_without_manifest(full_lake, tmp_path):
    lake = str(tmp_path / "nomanifest")
    shutil.copytree(full_lake, lake)
    os.remove(os.path.join(lake, "manifest.json"))
    con = up.connect(lake)
    assert con.sql("SELECT count(*) FROM entries").fetchone()[0] > 0
    assert con.sql("SELECT count(*) FROM features WHERE reviewed").fetchone()[0] > 0


def test_files_for_taxid(full_lake, fixture_acc):
    taxid = fixture_acc["taxid"]
    files = up.files_for_taxid(full_lake, taxid)
    assert files
    fd = up.manifest(full_lake)["tables"]["entries"]["file_details"]
    for f in files:
        rel = f.split("/", 1)[1]
        assert fd[rel]["taxid_min"] <= taxid <= fd[rel]["taxid_max"]
    assert up.files_for_taxid(full_lake, -1) == []


def test_entries_view_has_single_gene_name(full_lake):
    con = up.connect(full_lake)
    names = [r[0] for r in con.sql("DESCRIBE entries").fetchall()]
    assert names.count("gene_name") == 1
    assert "review_status" not in names


def test_split_sql_respects_quotes():
    stmts = up._split_sql("SELECT 'a;b' AS x; SELECT 2;\nSELECT '[{\"k\":\"v;w\"}]'")
    assert stmts == ["SELECT 'a;b' AS x", "SELECT 2", "SELECT '[{\"k\":\"v;w\"}]'"]


def test_schema_helper(full_lake):
    overview = up.schema(full_lake)
    assert set(overview) >= {"entries", "features", "accession_map"}
    detail = up.schema(full_lake, "entries")
    assert "gene_name" in detail["convenience_columns"]
    assert "organism_residual" in detail["nested_columns"]


README = os.path.join(os.path.dirname(__file__), "..", "README.md")


def _readme_sql_examples():
    """SQL from the README: every statement in a ```sql block and every
    con.sql(...) call in a ```python block that queries the views / macros."""
    with open(README) as f:
        text = f.read()
    stmts = []
    for block in re.findall(r"```sql\n(.*?)```", text, re.S):
        for stmt in up._split_sql(block):
            body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
            if body.upper().startswith("SELECT") and "BASE}" not in body:
                stmts.append(body)
    for block in re.findall(r"```python\n(.*?)```", text, re.S):
        for m in re.finditer(r'con\.sql\((?:"""(.*?)"""|"(.*?)")\)', block, re.S):
            body = (m.group(1) or m.group(2)).strip()
            if body.upper().startswith("SELECT") and "read_parquet" not in body:
                stmts.append(body)
    return stmts


@pytest.mark.parametrize("stmt", _readme_sql_examples())
def test_readme_examples(full_lake, stmt):
    """Every README SQL example runs against the fixture lake (values may not exist; it must not error)."""
    con = up.connect(full_lake)
    con.sql(stmt).fetchall()
