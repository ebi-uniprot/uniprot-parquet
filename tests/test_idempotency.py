"""Test that running the pipeline twice on the same warehouse produces identical results.

This verifies the idempotency fix: ensure_table drops and recreates tables
rather than appending to them.  Without this, a second run would duplicate
all data.
"""

import os
import sys
import subprocess

import pytest
from pyiceberg.catalog.sql import SqlCatalog

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
SCHEMA_JSON = os.path.join(os.path.dirname(__file__), "..", "schema.json")

EXPECTED_ENTRIES = 44
TABLE_NAMES = ["entries", "features", "xrefs", "comments"]


def _run_transform(small_jsonl, warehouse, catalog_db, extra_args=None):
    """Run iceberg_transform.py once and return the subprocess result."""
    transform_script = os.path.join(BIN_DIR, "iceberg_transform.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable, transform_script, small_jsonl,
        "--schema", SCHEMA_JSON,
        "--catalog-uri", f"sqlite:///{catalog_db}",
        "--warehouse", warehouse,
        "--namespace", "uniprotkb",
        "--memory-limit", "4GB",
        "--batch-size", "50",
        "--release", "idempotency_test",
    ]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, env=env, capture_output=True, text=True)


def _get_row_counts(catalog_uri, warehouse):
    """Return dict of {table_name: row_count} from Iceberg metadata."""
    catalog = SqlCatalog(
        "uniprot", **{"uri": catalog_uri, "warehouse": warehouse}
    )
    counts = {}
    for name in TABLE_NAMES:
        table = catalog.load_table(f"uniprotkb.{name}")
        snapshot = table.current_snapshot()
        if snapshot and snapshot.summary:
            counts[name] = int(snapshot.summary["total-records"])
        else:
            counts[name] = 0
    return counts


def test_idempotency(small_jsonl, tmp_path_factory):
    """Running the pipeline twice on the same warehouse should not duplicate data."""
    lake_dir = tmp_path_factory.mktemp("idempotency")
    warehouse = str(lake_dir / "warehouse")
    catalog_db = str(lake_dir / "catalog.db")
    catalog_uri = f"sqlite:///{catalog_db}"

    # First run
    result1 = _run_transform(small_jsonl, warehouse, catalog_db)
    if result1.returncode != 0:
        pytest.fail(f"First run failed:\n{result1.stderr}")
    counts1 = _get_row_counts(catalog_uri, warehouse)

    # Second run — same warehouse, same catalog
    result2 = _run_transform(small_jsonl, warehouse, catalog_db)
    if result2.returncode != 0:
        pytest.fail(f"Second run failed:\n{result2.stderr}")
    counts2 = _get_row_counts(catalog_uri, warehouse)

    # Row counts must be identical (not doubled)
    for name in TABLE_NAMES:
        assert counts1[name] == counts2[name], (
            f"{name}: first run had {counts1[name]} rows, "
            f"second run had {counts2[name]} rows (expected identical)"
        )

    # Sanity: entries count should match expected
    assert counts2["entries"] == EXPECTED_ENTRIES


def test_single_append_snapshot_after_rerun(small_jsonl, tmp_path_factory):
    """After two runs, each table should still have exactly one APPEND snapshot."""
    from pyiceberg.table.snapshots import Operation

    lake_dir = tmp_path_factory.mktemp("idempotency_snapshots")
    warehouse = str(lake_dir / "warehouse")
    catalog_db = str(lake_dir / "catalog.db")
    catalog_uri = f"sqlite:///{catalog_db}"

    # Run twice
    for i in range(2):
        result = _run_transform(small_jsonl, warehouse, catalog_db)
        if result.returncode != 0:
            pytest.fail(f"Run {i+1} failed:\n{result.stderr}")

    catalog = SqlCatalog(
        "uniprot", **{"uri": catalog_uri, "warehouse": warehouse}
    )
    for name in TABLE_NAMES:
        table = catalog.load_table(f"uniprotkb.{name}")
        snapshots = list(table.metadata.snapshots)
        append_snapshots = [
            s for s in snapshots
            if s.summary and s.summary.operation == Operation.APPEND
        ]
        assert len(append_snapshots) == 1, (
            f"{name}: expected 1 APPEND snapshot after re-run, "
            f"got {len(append_snapshots)}"
        )


def test_skip_existing_preserves_tables(small_jsonl, tmp_path_factory):
    """--skip-existing skips tables that already have data, preserving row counts."""
    lake_dir = tmp_path_factory.mktemp("skip_existing")
    warehouse = str(lake_dir / "warehouse")
    catalog_db = str(lake_dir / "catalog.db")
    catalog_uri = f"sqlite:///{catalog_db}"

    # First run — writes all tables
    result1 = _run_transform(small_jsonl, warehouse, catalog_db)
    if result1.returncode != 0:
        pytest.fail(f"First run failed:\n{result1.stderr}")
    counts1 = _get_row_counts(catalog_uri, warehouse)

    # Record snapshot IDs from first run
    catalog = SqlCatalog(
        "uniprot", **{"uri": catalog_uri, "warehouse": warehouse}
    )
    snapshots1 = {}
    for name in TABLE_NAMES:
        table = catalog.load_table(f"uniprotkb.{name}")
        snapshots1[name] = table.current_snapshot().snapshot_id

    # Second run with --skip-existing — should skip all tables
    result2 = _run_transform(
        small_jsonl, warehouse, catalog_db, extra_args=["--skip-existing"]
    )
    if result2.returncode != 0:
        pytest.fail(f"Second run failed:\n{result2.stderr}")

    # Verify skip messages in stderr
    for name in TABLE_NAMES:
        assert f"SKIP {name}" in result2.stderr, (
            f"Expected SKIP message for {name} in stderr"
        )

    # Row counts must be identical
    counts2 = _get_row_counts(catalog_uri, warehouse)
    for name in TABLE_NAMES:
        assert counts1[name] == counts2[name]

    # Snapshot IDs must be the same (tables were not recreated)
    catalog2 = SqlCatalog(
        "uniprot", **{"uri": catalog_uri, "warehouse": warehouse}
    )
    for name in TABLE_NAMES:
        table = catalog2.load_table(f"uniprotkb.{name}")
        assert table.current_snapshot().snapshot_id == snapshots1[name], (
            f"{name}: snapshot ID changed — table was recreated instead of skipped"
        )
