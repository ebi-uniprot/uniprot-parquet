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

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
DIVERSE_JSON_GZ = os.path.join(FIXTURE_DIR, "diverse.json.gz")
STRESS_JSON_GZ = os.path.join(FIXTURE_DIR, "diverse_stress.json.gz")
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


@pytest.fixture(scope="session")
def small_jsonl(fixture_json_gz, tmp_path_factory):
    """Convert fixture JSON → JSONL.zst once for the entire test session."""
    out_dir = tmp_path_factory.mktemp("jsonl")
    jsonl_path = str(out_dir / "diverse.jsonl.zst")

    with gzip.open(fixture_json_gz, "rt") as f:
        data = json.load(f)

    import orjson
    import zstandard as zstd

    cctx = zstd.ZstdCompressor(level=3)
    with open(jsonl_path, "wb") as fout:
        with cctx.stream_writer(fout) as writer:
            for entry in data["results"]:
                writer.write(orjson.dumps(entry) + b"\n")

    return jsonl_path


@pytest.fixture(scope="session")
def parquet_lake(small_jsonl, tmp_path_factory):
    """Run the full parquet_transform pipeline once, return lake directory path."""
    lake_dir = tmp_path_factory.mktemp("lake")
    outdir = str(lake_dir / "output")

    transform_script = os.path.join(BIN_DIR, "parquet_transform.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable, transform_script, small_jsonl,
        "--outdir", outdir,
        "--memory-limit", "4GB",
        "--batch-size", "500",
        "--release", "test_2026",
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(f"parquet_transform failed:\n{result.stderr}")

    return {
        "lake_dir": outdir,
        "stderr": result.stderr,
    }
