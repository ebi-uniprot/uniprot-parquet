"""Shared fixtures for UniProtKB Parquet data lake tests.

Run locally with:
    pytest tests/ -v                   # ~4K entries, ~2 min
    pytest tests/ -v --stress          # ~15K entries, ~8 min

Fixture data:
  tests/fixtures/diverse.json.gz        — ~4K diverse entries (default)
  tests/fixtures/diverse_stress.json.gz — ~15K entries (stress scale)
  Not committed to git. Run `python tests/fetch_fixtures.py` to download
  (auto-runs on first test).
"""

import gzip
import json
import os
import subprocess
import sys

import pytest

# Make bin/ importable
BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
sys.path.insert(0, BIN_DIR)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
DIVERSE_JSON_GZ = os.path.join(FIXTURE_DIR, "diverse.json.gz")
STRESS_JSON_GZ = os.path.join(FIXTURE_DIR, "diverse_stress.json.gz")
SMALL_JSON_GZ = os.path.join(FIXTURE_DIR, "small.json.gz")
FETCH_SCRIPT = os.path.join(os.path.dirname(__file__), "fetch_fixtures.py")


def pytest_addoption(parser):
    parser.addoption(
        "--stress", action="store_true", default=False,
        help="Use ~13K-entry stress dataset instead of ~4K default",
    )


def _ensure_fixture(scale="default"):
    """Download test fixtures from UniProtKB REST API if not already present."""
    target = STRESS_JSON_GZ if scale == "stress" else DIVERSE_JSON_GZ
    if os.path.exists(target):
        return target
    print(f"\nTest fixture not found: {target}")
    print("Fetching from UniProtKB REST API (one-time download)...\n")
    cmd = [sys.executable, FETCH_SCRIPT, "--scale", scale]
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if result.returncode != 0 or not os.path.exists(target):
        pytest.fail(f"Failed to fetch test fixtures (scale={scale}). Run `python tests/fetch_fixtures.py` manually.")
    return target


@pytest.fixture(scope="session")
def fixture_json_gz(request):
    """Path to the fixture. Uses stress dataset when running with --stress."""
    scale = "stress" if request.config.getoption("--stress") else "default"
    return _ensure_fixture(scale)


def write_jsonl_zst(entries, dst):
    """Write ``entries`` to ``dst`` as JSONL.zst; ``dst``."""
    import orjson
    import zstandard as zstd

    with open(dst, "wb") as fout, zstd.ZstdCompressor(level=3).stream_writer(fout) as writer:
        for entry in entries:
            writer.write(orjson.dumps(entry) + b"\n")
    return dst


def _json_gz_to_jsonl_zst(src, dst):
    """Convert a {"results": [...]} JSON.gz fixture to JSONL.zst."""
    with gzip.open(src, "rt") as f:
        return write_jsonl_zst(json.load(f)["results"], dst)


def run_bin(script, *args):
    """Run bin/``script`` with ``args`` (bin/ importable); the CompletedProcess."""
    env = {**os.environ, "PYTHONPATH": BIN_DIR + ":" + os.environ.get("PYTHONPATH", "")}
    return subprocess.run([sys.executable, os.path.join(BIN_DIR, script), *map(str, args)],
                          env=env, capture_output=True, text=True)


def run_transform(jsonl_path, outdir, release="test_2026", extra_args=()):
    """Run bin/parquet_transform.py as a subprocess; fail the test on error."""
    result = run_bin("parquet_transform.py", jsonl_path, "--outdir", outdir,
                     "--memory-limit", "4GB", "--batch-size", "500", "--release", release,
                     *extra_args)
    if result.returncode != 0:
        pytest.fail(f"parquet_transform failed:\n{result.stderr}")
    return result


@pytest.fixture(scope="session")
def small_jsonl(fixture_json_gz, tmp_path_factory):
    """Convert fixture JSON → JSONL.zst once for the entire test session."""
    out_dir = tmp_path_factory.mktemp("jsonl")
    return _json_gz_to_jsonl_zst(fixture_json_gz, str(out_dir / "diverse.jsonl.zst"))


@pytest.fixture(scope="session")
def parquet_lake(small_jsonl, tmp_path_factory):
    """Run the full parquet_transform pipeline once, return lake directory path."""
    lake_dir = tmp_path_factory.mktemp("lake")
    outdir = str(lake_dir / "output")
    result = run_transform(small_jsonl, outdir)
    return {
        "lake_dir": outdir,
        "stderr": result.stderr,
    }


@pytest.fixture(scope="session")
def small_lake(tmp_path_factory):
    """A lake built from tests/fixtures/small.json.gz (52 entries, no
    geneLocations); exercises the typed-NULL fallbacks."""
    out_dir = tmp_path_factory.mktemp("small")
    jsonl_path = _json_gz_to_jsonl_zst(SMALL_JSON_GZ, str(out_dir / "small.jsonl.zst"))
    outdir = str(out_dir / "lake")
    run_transform(jsonl_path, outdir, release="small_2026")
    return {"lake_dir": outdir, "jsonl": jsonl_path}


# ─── Round-trip fixtures (shared by test_roundtrip.py and test_reconstruct.py) ──
# Module scope, not session: the row dicts are several GB on the stress
# fixture and must be released at module teardown so the later validator
# subprocess (test_validate.py) has memory to run.


def load_original_entries(fixture_path):
    """Load the original JSON entries from the fixture, keyed by accession."""
    with gzip.open(fixture_path, "rt") as f:
        data = json.load(f)
    return {e["primaryAccession"]: e for e in data["results"]}


def table_files(lake_dir, name):
    """Every Parquet file of a table (recursive over the Hive partition dirs),
    sorted so review_status=swissprot files precede review_status=trembl."""
    import glob
    return sorted(glob.glob(os.path.join(lake_dir, name, "**", "*.parquet"), recursive=True))


def open_table(lake_dir, name):
    """PyArrow dataset over the explicit file list: no Hive column is added,
    so tests see exactly the stored schema (validate_lake.open_table does the same)."""
    import pyarrow.dataset as ds
    return ds.dataset(table_files(lake_dir, name), format="parquet")


def group_by_acc(table):
    """An Arrow table's rows as dicts (reconstruct.rows_from_arrow: JSON
    columns decoded), grouped by accession: {accession: [row, ...]}."""
    from reconstruct import rows_from_arrow
    grouped = {}
    for row in rows_from_arrow(table):
        grouped.setdefault(row["accession"], []).append(row)
    return grouped


def _rows_by_acc(lake_dir, name):
    """All rows of a table as dicts, grouped by accession."""
    return group_by_acc(open_table(lake_dir, name).to_table())


@pytest.fixture(scope="module")
def originals(fixture_json_gz):
    """Original JSON entries keyed by accession."""
    return load_original_entries(fixture_json_gz)


@pytest.fixture(scope="module")
def lake(parquet_lake):
    return parquet_lake["lake_dir"]


@pytest.fixture(scope="module")
def lake_entries(lake):
    """All entries from the Parquet lake, keyed by accession."""
    from reconstruct import rows_from_arrow
    return {row["accession"]: row for row in rows_from_arrow(open_table(lake, "entries").to_table())}


@pytest.fixture(scope="module")
def lake_features(lake):
    return _rows_by_acc(lake, "features")


@pytest.fixture(scope="module")
def lake_xrefs(lake):
    return _rows_by_acc(lake, "xrefs")


@pytest.fixture(scope="module")
def lake_comments(lake):
    return _rows_by_acc(lake, "comments")


@pytest.fixture(scope="module")
def lake_publications(lake):
    return _rows_by_acc(lake, "publications")
