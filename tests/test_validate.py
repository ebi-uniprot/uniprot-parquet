"""Tests for the production validation script (validate_lake.py).

Runs the full transform pipeline, then runs the validator against the
resulting lake + source JSONL.  Verifies that all checks pass.
"""

import json
import os
import shutil
import sys
import subprocess

import pytest

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")


def test_validate_passes_on_good_lake(small_jsonl, parquet_lake, tmp_path):
    """The validator should pass on a correctly built lake."""
    validate_script = os.path.join(BIN_DIR, "validate_lake.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")

    report_path = str(tmp_path / "validation_report.txt")

    cmd = [
        sys.executable, validate_script,
        "--lake", parquet_lake["lake_dir"],
        "--jsonl", small_jsonl,
        "--spot-check-n", "10",
        "-o", report_path,
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    # Print stderr for debugging if it fails
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        if os.path.exists(report_path):
            with open(report_path) as f:
                print("REPORT:", f.read())

    assert result.returncode == 0, (
        f"Validator failed on a correct lake:\n{result.stderr}"
    )

    # Verify report file was created and contains PASS
    assert os.path.exists(report_path)
    with open(report_path) as f:
        report = f.read()
    assert "ALL CHECKS PASSED" in report
    assert "[FAIL]" not in report

    # validation_report.json beside the text report (plan F.2.1)
    json_path = os.path.splitext(report_path)[0] + ".json"
    assert os.path.exists(json_path)
    with open(json_path) as f:
        data = json.load(f)
    assert data["passed"] is True
    assert data["checks"] and all(c["passed"] for c in data["checks"])
    assert any(c["name"].startswith("reconstruction matches JSONL") for c in data["checks"])
    assert any("file hashes match" in c["name"] for c in data["checks"])


def _run_validator(lake, jsonl, report_path, *extra):
    return subprocess.run(
        [sys.executable, os.path.join(BIN_DIR, "validate_lake.py"), "--lake", lake, "--jsonl", jsonl,
         "--spot-check-n", "10", "-o", report_path, *extra],
        capture_output=True, text=True,
    )


def _checks(report_path):
    with open(os.path.splitext(report_path)[0] + ".json") as f:
        return {c["name"]: c for c in json.load(f)["checks"]}


def test_validate_expected_count(small_jsonl, parquet_lake, tmp_path):
    """--expected-count (STREAM_JSONL's pre-sort count) must match both the JSONL
    and the entries table: it is the only anchor independent of the sorted file."""
    import pyarrow.dataset as ds
    from conftest import table_files
    n = ds.dataset(table_files(parquet_lake["lake_dir"], "entries"), format="parquet").count_rows()
    name = "JSONL line count and entries count == expected count"

    ok = str(tmp_path / "ok.txt")
    assert _run_validator(parquet_lake["lake_dir"], small_jsonl, ok, "--expected-count", str(n)).returncode == 0
    assert _checks(ok)[name]["passed"] is True

    bad = str(tmp_path / "bad.txt")
    assert _run_validator(parquet_lake["lake_dir"], small_jsonl, bad, "--expected-count", str(n + 1)).returncode != 0
    assert _checks(bad)[name]["passed"] is False


def test_validate_fails_on_file_outside_manifest(small_jsonl, parquet_lake, tmp_path):
    """A Parquet file the manifest does not list — here a writer's leftover under
    a hidden .tmp/ directory, which DuckDB's ** glob would read — fails validation."""
    lake = str(tmp_path / "lake")
    shutil.copytree(parquet_lake["lake_dir"], lake)
    part = os.path.join(lake, "entries", "review_status=swissprot")
    src = os.path.join(part, sorted(f for f in os.listdir(part) if f.endswith(".parquet"))[0])
    os.makedirs(os.path.join(part, ".tmp"))
    shutil.copy(src, os.path.join(part, ".tmp", "entries_00009.parquet"))
    report_path = str(tmp_path / "validation_report.txt")
    result = subprocess.run(
        [sys.executable, os.path.join(BIN_DIR, "validate_lake.py"), "--lake", lake, "--jsonl", small_jsonl,
         "--spot-check-n", "10", "-o", report_path],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    with open(os.path.splitext(report_path)[0] + ".json") as f:
        checks = {c["name"]: c for c in json.load(f)["checks"]}
    assert checks["entries manifest files match disk"]["passed"] is False


def test_release_manifest_writes_complete_marker_last(small_jsonl, parquet_lake, tmp_path):
    """bin/release_manifest.py writes provenance.json, then RELEASE_COMPLETE (plan H.5)."""
    import hashlib
    import time

    script = os.path.join(BIN_DIR, "release_manifest.py")
    out = tmp_path / "provenance.json"
    env = os.environ.copy()
    env["PYTHONPATH"] = BIN_DIR + ":" + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, script, "--lake", parquet_lake["lake_dir"], "--input-jsonl", small_jsonl,
         "--release", "test_2026", "-o", str(out)],
        env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    marker = tmp_path / "RELEASE_COMPLETE"
    assert out.exists() and marker.exists()
    assert os.path.getmtime(marker) >= os.path.getmtime(out)

    text = marker.read_text()
    fields = dict(line.split(": ", 1) for line in text.strip().splitlines())
    assert fields["release"] == "test_2026"
    with open(os.path.join(parquet_lake["lake_dir"], "manifest.json")) as f:
        assert fields["schema_version"] == json.load(f)["schema_version"]
    with open(os.path.join(parquet_lake["lake_dir"], "SHA256SUMS.txt"), "rb") as f:
        assert fields["sha256sums_sha256"] == hashlib.sha256(f.read()).hexdigest()
    assert fields["completed_at"].endswith("Z")

    with open(out) as f:
        prov = json.load(f)
    assert "sha256" in prov["inputs"]["jsonl"] and "md5" in prov["inputs"]["jsonl"]
    assert set(prov["tables"]) >= {"entries", "accession_map"}
    assert prov["tables"]["entries"]["total_size_bytes"] > 0


def _head(cwd):
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=cwd, text=True).strip()


def test_git_info_ignores_working_directory(tmp_path, monkeypatch):
    """Provenance records the pipeline checkout's commit, not whatever repo the
    Nextflow task work dir happens to be in (or none at all)."""
    from release_manifest import git_info

    repo_head = _head(os.path.join(BIN_DIR, ".."))

    monkeypatch.chdir(tmp_path)                     # outside any repo
    assert git_info()["commit"] == repo_head

    other = tmp_path / "other"                      # inside an unrelated repo
    other.mkdir()
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run(["git", "init", "-q"], cwd=other, check=True)
    subprocess.run(["git", "commit", "-q", "--allow-empty", "-m", "x"], cwd=other, env=env, check=True)
    assert _head(other) != repo_head
    monkeypatch.chdir(other)
    assert git_info()["commit"] == repo_head


@pytest.mark.parametrize("sidecar", ["manifest.json", "SHA256SUMS.txt"])
def test_release_manifest_refuses_marker_without_sidecars(small_jsonl, parquet_lake, tmp_path, sidecar):
    """No RELEASE_COMPLETE for a lake missing manifest.json or SHA256SUMS.txt:
    the marker is what mirrors trust, so it must not stamp a half-built lake."""
    lake = str(tmp_path / "lake")
    shutil.copytree(parquet_lake["lake_dir"], lake)
    os.remove(os.path.join(lake, sidecar))
    script = os.path.join(BIN_DIR, "release_manifest.py")
    out = tmp_path / "provenance.json"
    marker = tmp_path / "RELEASE_COMPLETE"
    result = subprocess.run(
        [sys.executable, script, "--lake", lake, "--input-jsonl", small_jsonl,
         "--release", "test_2026", "-o", str(out), "--complete-marker", str(marker)],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert sidecar in result.stderr
    assert not marker.exists()
