"""Tests for bin/sort_jsonl.py beyond the end-to-end runs elsewhere."""

import pathlib

from conftest import run_bin


def test_duckdb_out_of_memory_exits_75(small_jsonl: str, tmp_path: pathlib.Path) -> None:
    """A DuckDB out-of-memory error exits 75 (EX_TEMPFAIL), which the
    workflow retries with twice the memory; other failures exit 1."""
    result = run_bin("sort_jsonl.py", small_jsonl, "-o", tmp_path / "out.jsonl.zst",
                     "--memory-limit", "50MB", "--threads", "2")
    assert result.returncode == 75, result.stderr[-2000:]
    assert "DuckDB ran out of memory" in result.stderr
