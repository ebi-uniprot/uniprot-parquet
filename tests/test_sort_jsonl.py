"""Tests for bin/sort_jsonl.py beyond the end-to-end runs elsewhere."""

import json
import pathlib
import random
from typing import Any

import pytest
from conftest import run_bin, write_jsonl_zst
from sort_jsonl import max_object_size, parse_size, read_lines


def test_duckdb_out_of_memory_exits_75(small_jsonl: str, tmp_path: pathlib.Path) -> None:
    """A DuckDB out-of-memory error exits 75 (EX_TEMPFAIL), which the
    workflow retries with twice the memory; other failures exit 1."""
    result = run_bin("sort_jsonl.py", small_jsonl, "-o", tmp_path / "out.jsonl.zst",
                     "--memory-limit", "50MB", "--threads", "2")
    assert result.returncode == 75, result.stderr[-2000:]
    assert "DuckDB ran out of memory" in result.stderr


def _lines(path: pathlib.Path) -> list[bytes]:
    return list(read_lines(str(path)))


def _sort(src: str | pathlib.Path, out: pathlib.Path, *extra: str) -> str:
    """sort_jsonl.py ``src`` -> ``out``, which must succeed; its stderr."""
    result = run_bin("sort_jsonl.py", src, "-o", out, "--threads", "2", *extra)
    assert result.returncode == 0, result.stderr[-2000:]
    return result.stderr


def _entries() -> list[dict[str, Any]]:
    """Entries that exercise every sort key: Swiss-Prot and TrEMBL (and no
    entryType, which is TrEMBL), taxon IDs that sort differently as numbers
    and as text, a missing and a non-numeric taxon ID (both NULL, last),
    accessions that differ only in case, and a missing accession (last in
    its taxon).  No two share every key: ties have no order to compare.
    Padded so that a small --chunk-bytes cuts many chunks."""
    entries: list[dict[str, Any]] = []
    for i, taxon in enumerate([9, 10, 9606, 100000, None, "abc"]):
        for j, entry_type in enumerate(("UniProtKB reviewed (Swiss-Prot)", "UniProtKB unreviewed (TrEMBL)", None)):
            # one missing accession per (reviewed, taxon ID) key: not for "abc"
            # (NULL, as None is) nor for a missing entryType (TrEMBL, as j == 1 is)
            missing = [None] if taxon != "abc" and entry_type else []
            for accession in [f"P{i}{j}A", f"p{i}{j}a", f"Q{i}{j}0", *missing]:
                entry: dict[str, Any] = {"pad": "x" * 500}
                if accession is not None:
                    entry["primaryAccession"] = accession
                if entry_type:
                    entry["entryType"] = entry_type
                if taxon is not None:
                    entry["organism"] = {"taxonId": taxon}
                entries.append(entry)
    random.Random(1).shuffle(entries)
    assert len({_expected_order(e) for e in entries}) == len(entries)
    return entries


def _expected_order(entry: dict[str, Any]) -> tuple[Any, ...]:
    """The SQL's ORDER BY as a Python key."""
    taxon = (entry.get("organism") or {}).get("taxonId")
    taxon = taxon if isinstance(taxon, int) else None
    accession = entry.get("primaryAccession")
    return ("Swiss-Prot" not in (entry.get("entryType") or ""),
            taxon is None, taxon or 0, accession is None, (accession or "").encode())


def test_merged_runs_match_one_sort(tmp_path: pathlib.Path) -> None:
    """Many chunks, sorted and merged, give the same bytes as one sort of the
    whole input, in the ORDER BY's order."""
    src = write_jsonl_zst(_entries(), str(tmp_path / "in.jsonl.zst"))
    whole, merged = tmp_path / "whole.jsonl.zst", tmp_path / "merged.jsonl.zst"
    assert "runs merged" not in _sort(src, whole, "--memory-limit", "2GB")
    assert "runs merged" in _sort(src, merged, "--memory-limit", "2GB", "--chunk-bytes", "20KB")
    assert _lines(merged) == _lines(whole)
    entries = [json.loads(line) for line in _lines(merged)]
    assert entries == sorted(_entries(), key=_expected_order)


def test_merged_runs_match_one_sort_on_real_entries(small_jsonl: str, tmp_path: pathlib.Path) -> None:
    """The ~4.6K-entry fixture (~190 MB of JSONL) in 40 MB chunks at 1 GB:
    the same bytes as one sort with memory to spare."""
    whole, merged = tmp_path / "whole.jsonl.zst", tmp_path / "merged.jsonl.zst"
    _sort(small_jsonl, whole, "--memory-limit", "4GB")
    stderr = _sort(small_jsonl, merged, "--memory-limit", "1GB", "--chunk-bytes", "40MB")
    assert "runs merged" in stderr
    assert _lines(merged) == _lines(whole)


def test_a_last_line_without_newline_is_kept(tmp_path: pathlib.Path) -> None:
    src = tmp_path / "in.jsonl"
    src.write_bytes(b'{"primaryAccession": "B"}\n{"primaryAccession": "A"}')
    out = tmp_path / "out.jsonl.zst"
    _sort(src, out, "--memory-limit", "1GB", "--chunk-bytes", "1")
    assert _lines(out) == [b'{"primaryAccession": "A"}\n', b'{"primaryAccession": "B"}\n']


@pytest.mark.parametrize("text, size", [
    ("6912MB", 6_912_000_000), ("16GB", 16 * 10**9), ("1.5GiB", 3 * 2**29), ("20KB", 20_000), ("512", 512),
])
def test_parse_size(text: str, size: int) -> None:
    assert parse_size(text) == size


def test_max_object_size() -> None:
    """DuckDB's 16 MiB default unless a line needs more, then whole MiB above it."""
    assert max_object_size(0) == max_object_size(16 << 20) - (1 << 20) == 16 << 20
    assert max_object_size(40_000_000) == 39 << 20 > 40_000_000
