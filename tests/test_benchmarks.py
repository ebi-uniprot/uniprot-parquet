"""Tests for the benchmark measurements that README and RUNBOOK rely on:
they must measure the partitioned lake the pipeline writes."""

import json
import os
import sys
from typing import Any

import pyarrow.parquet as pq
from conftest import PROJECT_ROOT

sys.path.insert(0, os.path.join(PROJECT_ROOT, "benchmarks"))


def _manifest(lake_dir: str) -> dict[str, Any]:
    with open(os.path.join(lake_dir, "manifest.json")) as f:
        manifest: dict[str, Any] = json.load(f)
        return manifest


def test_analyze_storage_reads_every_partition(small_lake: dict[str, str]) -> None:
    """Every table's bytes and rows, under its review_status= partitions,
    with one column entry per column (not one per row group)."""
    from bench_baseline import analyze_storage
    lake = small_lake["lake_dir"]
    manifest = _manifest(lake)
    storage = analyze_storage(lake)
    assert set(storage["tables"]) == set(manifest["tables"])
    for table, info in storage["tables"].items():
        files = [os.path.join(lake, table, rel) for rel in manifest["tables"][table]["files"]]
        assert info["total_bytes"] == sum(os.path.getsize(f) for f in files), table
        assert info["total_rows"] == manifest["tables"][table]["row_count"], table
        names = [c["name"] for c in info["columns"]]
        assert len(names) == len(set(names)) == pq.read_metadata(files[0]).num_columns, table
    assert storage["totals"]["total_parquet_bytes"] > 0


def test_extrapolated_file_count_follows_the_target(small_lake: dict[str, str]) -> None:
    """A full release's file count scales with bytes per row and the target,
    one partial file per partition at most, not with the sample's file count."""
    from _bench_common import table_files
    from bench_file_size import extrapolate_files
    files = table_files(small_lake["lake_dir"], "entries")
    total = sum(os.path.getsize(f) for f in files)
    rows = sum(pq.read_metadata(f).num_rows for f in files)
    target, scale = 256 * 1024 * 1024, 100_000
    n = extrapolate_files(files, target, full_rows=rows * scale)
    partitions = {os.path.dirname(f) for f in files}
    assert total * scale / target <= n <= total * scale / target + len(partitions)
    assert extrapolate_files(files, target * 4, full_rows=rows * scale) < n


def test_every_query_runs(small_lake: dict[str, str], tmp_path: Any) -> None:
    """Every query in bench_queries.py runs on the fixture lake (which has no
    human entry, so the values are chosen from the lake), and the two-step
    lookups return what their single-step forms return."""
    from bench_queries import QUERIES, main
    main(["--lake", small_lake["lake_dir"], "--label", "test", "--out", str(tmp_path), "--runs", "2"])
    with open(tmp_path / "queries_test_latest.json") as f:
        results = json.load(f)
    rows = {r["name"]: r for r in results["queries"]}
    assert set(rows) == {qy.name for qy in QUERIES}
    problems = {n: r.get("error") or r.get("timeout") or r.get("skipped")
                for n, r in rows.items() if "first_s" not in r}
    assert problems == {}
    assert all(r["runs"] == 2 and "warm_median_s" in r for r in rows.values())
    for single, two in (("entry_single_step", "entry_two_step"), ("features_single_step", "features_two_step"),
                        ("batch_single_step", "batch_two_step")):
        assert rows[single]["rows"] == rows[two]["rows"] > 0, single
        assert rows[single]["result_bytes"] == rows[two]["result_bytes"], single
    assert len(results["meta"]["params"]["batch"]) == min(100, _manifest(small_lake["lake_dir"])
                                                          ["tables"]["entries"]["row_count"])
