"""Shared fixtures for UniProtKB Parquet data lake tests."""

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
SMALL_JSON_GZ = os.path.join(os.path.dirname(__file__), "fixtures", "small.json.gz")
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
def parquet_lake(small_jsonl, tmp_path_factory):
    """Run the full parquet_transform pipeline once, return lake directory path."""
    lake_dir = tmp_path_factory.mktemp("lake")
    outdir = str(lake_dir / "output")

    transform_script = os.path.join(BIN_DIR, "parquet_transform.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable, transform_script, small_jsonl,
        "--schema", SCHEMA_JSON,
        "--outdir", outdir,
        "--memory-limit", "4GB",
        "--batch-size", "50",
        "--release", "test_2026",
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(f"parquet_transform failed:\n{result.stderr}")

    return {
        "lake_dir": outdir,
        "stderr": result.stderr,
    }
