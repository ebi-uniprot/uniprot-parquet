"""Tests for bin/verify_release.py: a published release directory checked
against its own metadata (the workflow's last step before RELEASE_COMPLETE)."""

import json
import os
import pathlib
import shutil
import subprocess

import pytest
from conftest import run_bin


@pytest.fixture
def release(small_lake: dict[str, str], tmp_path: pathlib.Path) -> pathlib.Path:
    """A release directory as the workflow publishes it: lake/, the sorted
    JSONL, the validation reports, SCHEMA.md and provenance.json, plus the
    .pending marker release_manifest.py writes."""
    rel = tmp_path / "2026_01"
    shutil.copytree(small_lake["lake_dir"], rel / "lake",
                    ignore=shutil.ignore_patterns(".complete"))
    shutil.copy(small_lake["jsonl"], rel / "sorted.jsonl.zst")
    (rel / "validation_report.txt").write_text("VERDICT: ALL CHECKS PASSED\n")
    (rel / "validation_report.json").write_text('{"passed": true}\n')
    (rel / "SCHEMA.md").write_text("# schema\n")
    source = tmp_path / "input_manifest.json"
    source.write_text(json.dumps({"filename": "uniprot.json.gz", "size_bytes": 10, "sha256": "ab" * 32,
                                  "entry_count": 52, "expected_count": None}))
    result = run_bin("release_manifest.py", "--lake", rel / "lake",
                     "--input-jsonl", rel / "sorted.jsonl.zst", "--release", "2026_01",
                     "--input-manifest", source, "--nextflow-version", "26.04.0",
                     "--param", "process_memory=96 GB", "--param", "duckdb_pct=75",
                     "-o", rel / "provenance.json", "--complete-marker", rel / ".RELEASE_COMPLETE.pending",
                     "--published", rel / "sorted.jsonl.zst", rel / "SCHEMA.md",
                     rel / "validation_report.txt", rel / "validation_report.json")
    assert result.returncode == 0, result.stderr
    return rel


def _verify(rel: pathlib.Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return run_bin("verify_release.py", rel, "--marker", rel / ".RELEASE_COMPLETE.pending", *extra)


def _first_parquet(rel: pathlib.Path) -> pathlib.Path:
    return next(p for p in sorted((rel / "lake").rglob("*.parquet")))


def test_complete_release_passes(release: pathlib.Path) -> None:
    for extra in ((), ("--full",)):
        result = _verify(release, *extra)
        assert result.returncode == 0, result.stdout + result.stderr


def test_provenance_records_published_sidecars(release: pathlib.Path) -> None:
    with open(release / "provenance.json") as f:
        published = json.load(f)["published"]
    assert sorted(published) == ["SCHEMA.md", "sorted.jsonl.zst",
                                 "validation_report.json", "validation_report.txt"]
    assert published["SCHEMA.md"]["size_bytes"] == len("# schema\n")


def test_missing_parquet_file_fails(release: pathlib.Path) -> None:
    path = _first_parquet(release)
    path.unlink()
    result = _verify(release)
    assert result.returncode == 1
    assert f"{path.relative_to(release / 'lake')}: missing" in result.stdout


def test_wrong_size_fails(release: pathlib.Path) -> None:
    path = _first_parquet(release)
    with open(path, "ab") as f:
        f.write(b"x")
    result = _verify(release)
    assert result.returncode == 1 and "manifest.json says" in result.stdout


def test_extra_parquet_file_fails(release: pathlib.Path) -> None:
    path = _first_parquet(release)
    shutil.copy(path, path.parent / "stray_00099.parquet")
    result = _verify(release)
    assert result.returncode == 1 and "stray_00099.parquet: not listed in SHA256SUMS.txt" in result.stdout


def test_truncated_jsonl_fails(release: pathlib.Path) -> None:
    path = release / "sorted.jsonl.zst"
    os.truncate(path, os.path.getsize(path) // 2)
    result = _verify(release)
    assert result.returncode == 1 and "sorted.jsonl.zst:" in result.stdout


def test_same_size_corruption_needs_full(release: pathlib.Path) -> None:
    """A flipped byte keeps the size: only --full sees it in a large file,
    and the small metadata files are always hashed."""
    path = _first_parquet(release)
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(bytes(data))
    assert _verify(release, "--full").returncode == 1
    (release / "SCHEMA.md").write_text("# schemX\n")
    result = _verify(release)
    assert result.returncode == 1 and "SCHEMA.md: SHA-256 differs" in result.stdout


def test_marker_of_another_lake_fails(release: pathlib.Path) -> None:
    marker = release / ".RELEASE_COMPLETE.pending"
    marker.write_text(marker.read_text().replace("sha256sums_sha256: ", "sha256sums_sha256: 0"))
    result = _verify(release)
    assert result.returncode == 1 and "marker" in result.stdout


def test_provenance_identifies_source_tools_and_params(release: pathlib.Path) -> None:
    with open(release / "provenance.json") as f:
        provenance = json.load(f)
    assert provenance["inputs"]["source"]["sha256"] == "ab" * 32
    assert provenance["inputs"]["source"]["filename"] == "uniprot.json.gz"
    import duckdb
    import pyarrow
    tools = provenance["tools"]
    assert tools["duckdb"] == duckdb.__version__ and tools["pyarrow"] == pyarrow.__version__
    assert tools["nextflow"] == "26.04.0" and tools["python"].count(".") == 2
    assert provenance["params"] == {"release": "2026_01", "process_memory": "96 GB", "duckdb_pct": "75"}
    assert provenance["writer"]["row_group_size"] > 0
