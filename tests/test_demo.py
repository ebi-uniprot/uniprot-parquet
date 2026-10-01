"""The demo notebook's code runs against a fixture lake (no Jupyter needed):
each code cell is executed in one shared namespace, as a kernel would."""

import ast
import json
import os
import shutil

import pytest

from conftest import PROJECT_ROOT

NOTEBOOK = os.path.join(PROJECT_ROOT, "demo", "demo.ipynb")


def _run_cell(src, ns, name):
    """Execute a cell; the value of its last expression, as Jupyter shows it."""
    tree = ast.parse(src)
    last = tree.body.pop() if tree.body and isinstance(tree.body[-1], ast.Expr) else None
    exec(compile(tree, name, "exec"), ns)
    if last is not None:
        return eval(compile(ast.Expression(last.value), name, "eval"), ns)
    return None


@pytest.fixture(scope="module")
def cell_results(small_lake, tmp_path_factory):
    """{cell index: (source, value of its last expression)} for every code
    cell; plot cells are skipped when matplotlib is not installed."""
    try:
        import matplotlib  # noqa: F401
        plots = True
    except ImportError:
        plots = False
    demo = tmp_path_factory.mktemp("demo")
    shutil.copytree(small_lake["lake_dir"], demo / "lake" / "2026_03" / "lake")
    with open(NOTEBOOK) as f:
        cells = json.load(f)["cells"]
    ns, results = {}, {}
    cwd = os.getcwd()
    os.chdir(demo)
    try:
        for i, cell in enumerate(cells):
            if cell["cell_type"] != "code":
                continue
            src = "".join(cell["source"])
            if ".plot" in src and not plots:
                continue
            results[i] = (src, _run_cell(src, ns, f"cell{i}"))
    finally:
        os.chdir(cwd)
    return results


def test_every_code_cell_runs(cell_results):
    assert cell_results


def _cell(cell_results, marker):
    return next(value for src, value in cell_results.values() if src.startswith(marker))


def test_feature_coverage_cell(cell_results):
    """The coverage cell shows proteins, and counts each residue once, so no
    coverage exceeds 100%."""
    df = _cell(cell_results, "# Feature coverage")
    assert len(df) > 0
    assert (df["coverage_pct"] <= 100).all(), df


def test_isoform_cell_shows_its_table(cell_results):
    df = _cell(cell_results, "# Unnest isoforms")
    assert df is not None and len(df) > 0


def test_isoform_census_counts_isoforms_per_protein(cell_results, small_lake):
    """The census ranks proteins by their number of isoforms (one row per
    protein), not by the isoform IDs of a single isoform."""
    import uniprot_parquet as up
    df = _cell(cell_results, "# Proteome-wide isoform census")
    con = up.connect(small_lake["lake_dir"])
    most = con.sql("SELECT max(len(isoforms)) FROM comments "
                   "WHERE commentType = 'ALTERNATIVE PRODUCTS'").fetchone()[0]
    assert most > 1                                          # precondition
    assert df["n_isoforms"].iloc[0] == most
    assert df["accession"].is_unique
