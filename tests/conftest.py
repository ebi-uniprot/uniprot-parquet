"""Shared fixtures for UniProtKB Iceberg pipeline tests."""

import os
import sys
import gzip
import json
import subprocess
import pytest

# Make bin/ importable
BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
sys.path.insert(0, BIN_DIR)

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
SMALL_JSON_GZ = os.path.join(PROJECT_ROOT, "test_json", "small.json.gz")
SCHEMA_JSON = os.path.join(PROJECT_ROOT, "schema.json")


@pytest.fixture(scope="session")
def small_jsonl(tmp_path_factory):
    """Convert small.json.gz → small.jsonl.zst once for the entire test session."""
    out_dir = tmp_path_factory.mktemp("jsonl")
    jsonl_path = str(out_dir / "small.jsonl.zst")

    with gzip.open(SMALL_JSON_GZ, "rt") as f:
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
def small_json_entries():
    """Load the raw JSON entries for assertion comparisons."""
    with gzip.open(SMALL_JSON_GZ, "rt") as f:
        data = json.load(f)
    return data["results"]


@pytest.fixture(scope="session")
def iceberg_lake(small_jsonl, tmp_path_factory):
    """Run the full iceberg_transform pipeline once, return (catalog, warehouse) paths."""
    lake_dir = tmp_path_factory.mktemp("lake")
    warehouse = str(lake_dir / "warehouse")
    catalog_db = str(lake_dir / "catalog.db")
    catalog_uri = f"sqlite:///{catalog_db}"

    transform_script = os.path.join(BIN_DIR, "iceberg_transform.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")

    # Schema is now inferred from the data — no --schema needed for the
    # Iceberg schema.  We still pass --schema for the DuckDB read_json
    # column hints (avoids sampling issues on tiny test files).
    cmd = [
        sys.executable, transform_script, small_jsonl,
        "--schema", SCHEMA_JSON,
        "--catalog-uri", catalog_uri,
        "--warehouse", warehouse,
        "--namespace", "uniprot",
        "--memory-limit", "4GB",
        "--batch-size", "50",
        "--release", "test_2026",
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(f"iceberg_transform failed:\n{result.stderr}")

    return {
        "catalog_uri": catalog_uri,
        "warehouse": warehouse,
        "stderr": result.stderr,
    }
