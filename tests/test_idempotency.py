"""Test that running the pipeline twice on the same output produces identical results.

This verifies idempotency: a second run overwrites the previous Parquet files,
so row counts remain correct (no duplication).
"""

import gzip
import json
import os
import shutil
import subprocess
import sys

import pyarrow.dataset as ds
import pytest

from conftest import table_files

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")

TABLE_NAMES = ["entries", "features", "xrefs", "comments", "publications", "accession_map"]


def _expected_entries_from_jsonl(jsonl_path):
    import zstandard as zstd
    with open(jsonl_path, "rb") as f:
        return zstd.ZstdDecompressor().stream_reader(f).read().count(b"\n")


def _expected_entries(fixture_path):
    """Derive expected entry count from the fixture."""
    with gzip.open(fixture_path, "rt") as f:
        data = json.load(f)
    return len(data["results"])


def _run_transform(small_jsonl, outdir, extra_args=None):
    """Run parquet_transform.py once and return the subprocess result."""
    transform_script = os.path.join(BIN_DIR, "parquet_transform.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable, transform_script, small_jsonl,
        "--outdir", outdir,
        "--memory-limit", "4GB",
        "--batch-size", "500",
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


def test_idempotency(small_jsonl, fixture_json_gz, tmp_path_factory):
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
    assert counts2["entries"] == _expected_entries(fixture_json_gz)


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
        files = table_files(outdir, name)
        if files:
            mtimes1[name] = os.path.getmtime(files[0])

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
        files = table_files(outdir, name)
        if files:
            mtime2 = os.path.getmtime(files[0])
            assert mtime2 == mtimes1[name], (
                f"{name}: file was modified — table was rewritten instead of skipped"
            )


def _sentinel(outdir, name):
    return os.path.join(outdir, ".complete", f"{name}.json")


def test_skip_existing_rebuilds_without_sentinel(small_jsonl, tmp_path):
    """Files on disk are not proof of a complete table: a run killed during the
    publish window leaves Parquet files but no completion sentinel, and
    --skip-existing must rebuild rather than trust (and hash) them."""
    outdir = str(tmp_path / "lake")
    assert _run_transform(small_jsonl, outdir).returncode == 0
    with open(_sentinel(outdir, "entries")) as f:
        sentinel = json.load(f)
    assert sentinel["files"] == sorted(
        os.path.relpath(p, os.path.join(outdir, "entries")) for p in table_files(outdir, "entries"))
    n = _expected_entries_from_jsonl(small_jsonl)
    assert sentinel["row_count"] == n

    os.remove(_sentinel(outdir, "entries"))
    mtime_before = os.path.getmtime(table_files(outdir, "entries")[0])
    result = _run_transform(small_jsonl, outdir, extra_args=["--skip-existing"])
    assert result.returncode == 0, result.stderr
    assert "REBUILD entries" in result.stderr
    assert "SKIP features" in result.stderr
    assert os.path.getmtime(table_files(outdir, "entries")[0]) > mtime_before
    assert os.path.exists(_sentinel(outdir, "entries"))
    assert _get_row_counts(outdir)["entries"] == n


def test_skip_existing_rebuilds_on_file_list_mismatch(small_jsonl, tmp_path):
    """A table whose files on disk differ from what its sentinel recorded (here
    one partition file lost) is rebuilt, restoring the full row count."""
    outdir = str(tmp_path / "lake")
    assert _run_transform(small_jsonl, outdir).returncode == 0
    n = _expected_entries_from_jsonl(small_jsonl)
    os.remove(table_files(outdir, "entries")[-1])
    assert _get_row_counts(outdir)["entries"] < n
    result = _run_transform(small_jsonl, outdir, extra_args=["--skip-existing"])
    assert result.returncode == 0, result.stderr
    assert "REBUILD entries" in result.stderr
    assert _get_row_counts(outdir)["entries"] == n


def test_rebuild_with_fewer_files_removes_stale_ones(small_jsonl, tmp_path):
    """A rebuild that writes fewer files per partition deletes the previous run's extras."""
    outdir = str(tmp_path / "lake")
    _run_transform(small_jsonl, outdir, extra_args=["--target-file-bytes", "200000"])
    many = table_files(outdir, "entries")
    assert len(many) > 2, "expected several small files per partition"
    _run_transform(small_jsonl, outdir)                  # default target: one file per partition
    few = table_files(outdir, "entries")
    assert len(few) == 2, few
    with open(os.path.join(outdir, "manifest.json")) as f:
        manifest = json.load(f)
    assert sorted(os.path.relpath(f, os.path.join(outdir, "entries")) for f in few) == \
        sorted(manifest["tables"]["entries"]["files"])
    assert _get_row_counts(outdir)["entries"] == _expected_entries_from_jsonl(small_jsonl)


def test_rebuild_removes_leftover_tmp_files(small_jsonl, tmp_path):
    """A partial file a killed run left under <partition>/.tmp/ is removed by the
    next run.  DuckDB's ** glob descends into hidden directories, so a leftover
    would otherwise be double-counted by every <table>/**/*.parquet reader."""
    import duckdb
    outdir = str(tmp_path / "lake")
    _run_transform(small_jsonl, outdir)
    entries_dir = os.path.join(outdir, "entries")
    stale = os.path.join(entries_dir, "review_status=trembl", ".tmp", "entries_00002.parquet")
    os.makedirs(os.path.dirname(stale))
    shutil.copy(table_files(outdir, "entries")[-1], stale)
    n = _expected_entries_from_jsonl(small_jsonl)
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{entries_dir}/**/*.parquet')").fetchone()[0] > n
    _run_transform(small_jsonl, outdir)
    assert not os.path.exists(os.path.dirname(stale))
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{entries_dir}/**/*.parquet')").fetchone()[0] == n
