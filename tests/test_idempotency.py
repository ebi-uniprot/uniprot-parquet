"""Test that running the pipeline twice on the same output produces identical results.

This verifies idempotency: a second run overwrites the previous Parquet files,
so row counts remain correct (no duplication).
"""

import os
import sys
import subprocess

import pyarrow.dataset as ds
import pytest

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")

EXPECTED_ENTRIES = 44
TABLE_NAMES = ["entries", "features", "xrefs", "comments", "publications"]


def _run_transform(small_jsonl, outdir, extra_args=None):
    """Run parquet_transform.py once and return the subprocess result."""
    transform_script = os.path.join(BIN_DIR, "parquet_transform.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable, transform_script, small_jsonl,
        "--outdir", outdir,
        "--memory-limit", "4GB",
        "--batch-size", "50",
        "--release", "idempotency_test",
    ]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, env=env, capture_output=True, text=True)


def _get_row_counts(outdir):
    """Return dict of {table_name: row_count} from Parquet files."""
    counts = {}
    for name in TABLE_NAMES:
        table_dir = os.path.join(outdir, name)
        if os.path.isdir(table_dir):
            dataset = ds.dataset(table_dir, format="parquet")
            counts[name] = dataset.count_rows()
        else:
            counts[name] = 0
    return counts


def test_idempotency(small_jsonl, tmp_path_factory):
    """Running the pipeline twice on the same output should not duplicate data."""
    lake_dir = tmp_path_factory.mktemp("idempotency")
    outdir = str(lake_dir / "output")

    # First run
    result1 = _run_transform(small_jsonl, outdir)
    if result1.returncode != 0:
        pytest.fail(f"First run failed:\n{result1.stderr}")
    counts1 = _get_row_counts(outdir)

    # Second run — same output directory
    result2 = _run_transform(small_jsonl, outdir)
    if result2.returncode != 0:
        pytest.fail(f"Second run failed:\n{result2.stderr}")
    counts2 = _get_row_counts(outdir)

    # Row counts must be identical (not doubled)
    for name in TABLE_NAMES:
        assert counts1[name] == counts2[name], (
            f"{name}: first run had {counts1[name]} rows, "
            f"second run had {counts2[name]} rows (expected identical)"
        )

    # Sanity: entries count should match expected
    assert counts2["entries"] == EXPECTED_ENTRIES


def test_skip_existing_preserves_tables(small_jsonl, tmp_path_factory):
    """--skip-existing skips tables that already have Parquet files."""
    lake_dir = tmp_path_factory.mktemp("skip_existing")
    outdir = str(lake_dir / "output")

    # First run — writes all tables
    result1 = _run_transform(small_jsonl, outdir)
    if result1.returncode != 0:
        pytest.fail(f"First run failed:\n{result1.stderr}")
    counts1 = _get_row_counts(outdir)

    # Record modification times from first run
    mtimes1 = {}
    for name in TABLE_NAMES:
        table_dir = os.path.join(outdir, name)
        files = sorted(f for f in os.listdir(table_dir) if f.endswith(".parquet"))
        if files:
            mtimes1[name] = os.path.getmtime(os.path.join(table_dir, files[0]))

    # Second run with --skip-existing — should skip all tables
    result2 = _run_transform(
        small_jsonl, outdir, extra_args=["--skip-existing"]
    )
    if result2.returncode != 0:
        pytest.fail(f"Second run failed:\n{result2.stderr}")

    # Verify skip messages in stderr
    for name in TABLE_NAMES:
        assert f"SKIP {name}" in result2.stderr, (
            f"Expected SKIP message for {name} in stderr"
        )

    # Row counts must be identical
    counts2 = _get_row_counts(outdir)
    for name in TABLE_NAMES:
        assert counts1[name] == counts2[name]

    # File modification times must be the same (files were not rewritten)
    for name in TABLE_NAMES:
        table_dir = os.path.join(outdir, name)
        files = sorted(f for f in os.listdir(table_dir) if f.endswith(".parquet"))
        if files:
            mtime2 = os.path.getmtime(os.path.join(table_dir, files[0]))
            assert mtime2 == mtimes1[name], (
                f"{name}: file was modified — table was rewritten instead of skipped"
            )
