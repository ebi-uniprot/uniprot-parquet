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

    # validation_report.json beside the text report
    json_path = os.path.splitext(report_path)[0] + ".json"
    assert os.path.exists(json_path)
    with open(json_path) as f:
        data = json.load(f)
    assert data["passed"] is True
    assert data["checks"] and all(c["passed"] for c in data["checks"])
    assert any(c["name"].startswith("reconstruction matches JSONL") for c in data["checks"])
    assert any("file hashes match" in c["name"] for c in data["checks"])


def _run_validator(lake, jsonl, report_path, *extra):
    from conftest import run_bin
    return run_bin("validate_lake.py", "--lake", lake, "--jsonl", jsonl,
                   "--spot-check-n", "10", "-o", report_path, *extra)


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


def test_schema_diff_is_informational(small_jsonl, parquet_lake, tmp_path):
    """--previous-manifest reports removed, added and retyped columns as
    warnings; the lake still passes."""
    with open(os.path.join(parquet_lake["lake_dir"], "manifest.json")) as f:
        previous = json.load(f)
    cols = previous["tables"]["entries"]["columns"]
    by_name = {c["name"]: c for c in cols}
    by_name["organism"]["type"] = "struct<taxonId: int64, oldField: string>"
    previous["tables"]["entries"]["columns"] = [c for c in cols if c["name"] != "gene_primary"] + [
        {"name": "retired_column", "type": "string", "nullable": True}]
    prev_path = str(tmp_path / "previous_manifest.json")
    with open(prev_path, "w") as f:
        json.dump(previous, f)

    report_path = str(tmp_path / "report.txt")
    result = _run_validator(parquet_lake["lake_dir"], small_jsonl, report_path,
                            "--previous-manifest", prev_path)
    with open(report_path) as f:
        text = f.read()
    assert result.returncode == 0, text
    assert "[WARN] entries: SHORTCUT column added: gene_primary" in text
    assert "[WARN] entries: column removed: retired_column" in text
    assert "[WARN] entries: column retyped: organism" in text
    assert "fields removed: oldField" in text
    assert "[WARN] shortcut schema changed but schema_version did not" in text
    with open(os.path.splitext(report_path)[0] + ".json") as f:
        data = json.load(f)
    assert data["passed"] is True and len(data["warnings"]) >= 4


def test_schema_diff_flags_removed_curated_column(small_jsonl, parquet_lake, tmp_path):
    """A shortcut removed since the previous release is tagged SHORTCUT (from
    the previous manifest's column_categories: it is no longer in
    TABLE_META) and flags the unchanged schema_version."""
    with open(os.path.join(parquet_lake["lake_dir"], "manifest.json")) as f:
        previous = json.load(f)
    entries = previous["tables"]["entries"]
    entries["columns"].append({"name": "old_curated_col", "type": "string", "nullable": True})
    entries["column_categories"]["shortcut"].append("old_curated_col")
    prev_path = str(tmp_path / "previous_manifest.json")
    with open(prev_path, "w") as f:
        json.dump(previous, f)

    report_path = str(tmp_path / "report.txt")
    result = _run_validator(parquet_lake["lake_dir"], small_jsonl, report_path,
                            "--previous-manifest", prev_path)
    with open(report_path) as f:
        text = f.read()
    assert result.returncode == 0, text
    assert "[WARN] entries: SHORTCUT column removed: old_curated_col" in text
    assert "[WARN] shortcut schema changed but schema_version did not" in text


@pytest.mark.parametrize("change", ["added", "removed", "retyped"])
def test_schema_diff_source_column_needs_no_version(small_jsonl, parquet_lake, tmp_path, change):
    """A source column follows UniProt's JSON: one added (UniProt added a
    key), removed or retyped is reported but needs no schema_version bump."""
    with open(os.path.join(parquet_lake["lake_dir"], "manifest.json")) as f:
        previous = json.load(f)
    cols = previous["tables"]["entries"]["columns"]
    if change == "added":
        previous["tables"]["entries"]["columns"] = [c for c in cols if c["name"] != "geneLocations"]
    elif change == "removed":
        cols.append({"name": "retiredKey", "type": "string", "nullable": True})
    else:
        next(c for c in cols if c["name"] == "organism")["type"] = "struct<oldField: string>"
    prev_path = str(tmp_path / "previous_manifest.json")
    with open(prev_path, "w") as f:
        json.dump(previous, f)

    report_path = str(tmp_path / "report.txt")
    result = _run_validator(parquet_lake["lake_dir"], small_jsonl, report_path,
                            "--previous-manifest", prev_path)
    with open(report_path) as f:
        text = f.read()
    assert result.returncode == 0, text
    assert f"entries: column {change}:" in text
    assert "but schema_version did not" not in text, text


def test_arrow_type_paths_keep_bracketed_parameters():
    """Commas inside [...] and (...) do not split a struct's fields."""
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    from parquet_transform import _split_top
    from validate_lake import _arrow_type_paths
    t = "struct<updated: timestamp[us, tz=UTC], price: decimal128(10, 2), s: struct<x: int64, y: string>>"
    assert _split_top(t[len("struct<"):-1]) == [
        "updated: timestamp[us, tz=UTC]", "price: decimal128(10, 2)", "s: struct<x: int64, y: string>"]
    assert _arrow_type_paths(t) == {"updated", "price", "s", "s.x", "s.y"}


@pytest.mark.xfail(strict=True, reason="Arrow type strings do not quote field names, so a key "
                   "containing ', ' reads as two fields; the schema diff is informational")
def test_arrow_type_paths_key_with_comma():
    import pyarrow as pa
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    from validate_lake import _arrow_type_paths
    assert _arrow_type_paths(str(pa.struct([("x, y", pa.int64())]))) == {"x, y"}


def test_reconstruction_error_is_a_failed_check(small_lake, monkeypatch):
    """An exception while rebuilding an entry fails check 16 for that entry
    instead of crashing the validator before it writes a report."""
    import gzip
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    import reconstruct
    import validate_lake
    from conftest import SMALL_JSON_GZ

    def broken(*args, **kwargs):
        raise ValueError("boom")
    monkeypatch.setattr(reconstruct, "reconstruct_entry", broken)
    with gzip.open(SMALL_JSON_GZ, "rt") as f:
        sampled = json.load(f)["results"]
    report = validate_lake.ValidationReport()
    validate_lake.check_reconstruction(report, small_lake["lake_dir"], sampled, 5)
    failed = [r for r in report.records if not r["passed"]]
    assert failed and "rebuild raised ValueError: boom" in failed[0]["detail"]


def test_schema_diff_missing_previous_manifest_warns(small_jsonl, parquet_lake, tmp_path):
    report_path = str(tmp_path / "report.txt")
    result = _run_validator(parquet_lake["lake_dir"], small_jsonl, report_path,
                            "--previous-manifest", str(tmp_path / "nope.json"))
    assert result.returncode == 0
    with open(report_path) as f:
        assert "[WARN] schema diff skipped: cannot read a manifest" in f.read()


def _rewrite_first_file(lake, table, transform):
    """Rewrite a table's first Parquet file through ``transform(arrow_table)``
    and refresh the manifest's details for it, so only the data check can fail."""
    import pyarrow.parquet as pq
    from conftest import table_files
    from parquet_transform import _file_details
    path = table_files(lake, table)[0]
    tbl = pq.read_table(path)
    pq.write_table(transform(tbl), path)
    with open(os.path.join(lake, "manifest.json")) as f:
        manifest = json.load(f)
    table_dir = os.path.join(lake, table)
    rel = os.path.relpath(path, table_dir)
    manifest["tables"][table]["file_details"][rel].update(_file_details(table_dir, rel))
    with open(os.path.join(lake, "manifest.json"), "w") as f:
        json.dump(manifest, f)


def test_validate_fails_on_broken_array_positions(small_jsonl, parquet_lake, tmp_path):
    """A duplicated *_index within an accession fails check 5."""
    import pyarrow as pa
    lake = str(tmp_path / "lake")
    shutil.copytree(parquet_lake["lake_dir"], lake)

    def zero_second_index(tbl):
        idx = tbl.column("feature_index").to_pylist()
        idx[1] = idx[0]
        i = tbl.schema.get_field_index("feature_index")
        return tbl.set_column(i, tbl.schema.field(i), pa.array(idx, pa.int32()))
    _rewrite_first_file(lake, "features", zero_second_index)

    report_path = str(tmp_path / "report.txt")
    assert _run_validator(lake, small_jsonl, report_path).returncode != 0
    checks = _checks(report_path)
    assert checks["features.feature_index is 0..feature_count-1 for every accession"]["passed"] is False


def test_validate_fails_on_child_count_mismatch(small_jsonl, parquet_lake, tmp_path):
    """Contiguous indices are not enough: an accession's number of child rows
    must equal its entries count (here one entry claims an extra feature)."""
    import pyarrow as pa
    lake = str(tmp_path / "lake")
    shutil.copytree(parquet_lake["lake_dir"], lake)

    def bump_first_feature_count(tbl):
        counts = tbl.column("feature_count").to_pylist()
        i = next(k for k, n in enumerate(counts) if n > 0)
        counts[i] += 1
        col = tbl.schema.get_field_index("feature_count")
        return tbl.set_column(col, tbl.schema.field(col), pa.array(counts, pa.int32()))
    _rewrite_first_file(lake, "entries", bump_first_feature_count)

    report_path = str(tmp_path / "report.txt")
    assert _run_validator(lake, small_jsonl, report_path).returncode != 0
    check = _checks(report_path)["features.feature_index is 0..feature_count-1 for every accession"]
    assert check["passed"] is False and "rows for" in check["detail"]


def test_validate_fails_on_unsorted_child_table(small_jsonl, parquet_lake, tmp_path):
    """Check 5's single pass over a child table still sees rows out of
    (reviewed, organism_id, accession, *_index) order: here two accessions' runs swap."""
    import pyarrow as pa
    lake = str(tmp_path / "lake")
    shutil.copytree(parquet_lake["lake_dir"], lake)

    def swap_first_two_runs(tbl):
        accession = tbl.column("accession").to_pylist()
        first, second = accession[0], next(a for a in accession if a != accession[0])
        run = lambda a: [i for i, x in enumerate(accession) if x == a]  # noqa: E731
        order = run(second) + run(first) + [i for i in range(len(accession)) if accession[i] not in (first, second)]
        return tbl.take(pa.array(order))
    _rewrite_first_file(lake, "features", swap_first_two_runs)

    report_path = str(tmp_path / "report.txt")
    assert _run_validator(lake, small_jsonl, report_path).returncode != 0
    checks = _checks(report_path)
    check = checks["features sorted by (reviewed DESC, organism_id ASC, accession ASC, feature_index ASC)"]
    assert check["passed"] is False and "disorder at row" in check["detail"]


def test_sort_check_reads_each_child_table_once(small_lake, monkeypatch):
    """Check 5 streams each child table once (the sort order and the array
    positions come from the same pass): a child table is billions of rows."""
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    import validate_lake

    reads = {}
    real_open = validate_lake.open_table

    class Counting:
        def __init__(self, name, dataset):
            self._name, self._dataset = name, dataset

        def to_batches(self, *args, **kwargs):
            reads[self._name] = reads.get(self._name, 0) + 1
            return self._dataset.to_batches(*args, **kwargs)

        def __getattr__(self, attr):
            return getattr(self._dataset, attr)

    monkeypatch.setattr(validate_lake, "open_table",
                        lambda lake, name: Counting(name, real_open(lake, name)))
    report = validate_lake.ValidationReport()
    validate_lake.check_sort_order(report, small_lake["lake_dir"])
    assert report.passed(), report.full_report()
    for child in validate_lake.CHILD_INDEX:
        assert reads[child] == 1, f"{child} read {reads[child]} times"


def test_release_manifest_writes_complete_marker_last(small_jsonl, parquet_lake, tmp_path):
    """bin/release_manifest.py writes provenance.json, then RELEASE_COMPLETE."""
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


def test_validator_decompresses_jsonl_once(small_lake, tmp_path, monkeypatch):
    """main() makes exactly one pass over the source JSONL: the fused
    count+sample pass feeds every check that needs source entries."""
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    import validate_lake

    calls = {"open": 0}
    real_open = validate_lake._open_jsonl_lines

    def counting_open(path):
        calls["open"] += 1
        return real_open(path)

    def fail_count(path):
        raise AssertionError("count_jsonl_lines must not be called from main()")

    monkeypatch.setattr(validate_lake, "_open_jsonl_lines", counting_open)
    monkeypatch.setattr(validate_lake, "count_jsonl_lines", fail_count)

    report = str(tmp_path / "report.txt")
    monkeypatch.setattr(sys, "argv", [
        "validate_lake.py",
        "--lake", small_lake["lake_dir"],
        "--jsonl", small_lake["jsonl"],
        "--spot-check-n", "5",
        "-o", report,
    ])
    with pytest.raises(SystemExit) as exc:
        validate_lake.main()
    assert exc.value.code == 0
    assert calls["open"] == 1, f"JSONL decompressed {calls['open']} times, expected 1"


def _small_entries():
    import gzip
    from conftest import SMALL_JSON_GZ
    with gzip.open(SMALL_JSON_GZ, "rt") as f:
        return json.load(f)["results"]


def test_validator_catches_a_value_the_lake_cannot_hold(tmp_path):
    """The ground truth is the dump's data, not DuckDB's parse of it: an
    integer too wide for BIGINT/HUGEINT is inferred DOUBLE and stored rounded,
    and reconstruction (check 16) must report it, as it would if SORT_JSONL
    wrote the same rounded value into sorted.jsonl.zst."""
    from conftest import run_bin, run_transform
    entries = _small_entries()
    entries[0]["bigCount"] = 12345678901234567890123
    src = tmp_path / "in.jsonl"
    src.write_text("".join(json.dumps(e) + "\n" for e in entries))   # orjson cannot write it
    sorted_jsonl = tmp_path / "sorted.jsonl.zst"
    result = run_bin("sort_jsonl.py", src, "-o", sorted_jsonl, "--memory-limit", "4GB")
    assert result.returncode == 0, result.stderr
    lake = str(tmp_path / "lake")
    run_transform(sorted_jsonl, lake)
    report = str(tmp_path / "report.txt")
    result = run_bin("validate_lake.py", "--lake", lake, "--jsonl", sorted_jsonl,
                     "--spot-check-n", "100", "-o", report)
    assert result.returncode == 1
    failed = [name for name, c in _checks(report).items() if not c["passed"]]
    assert any(name.startswith("reconstruction matches JSONL") for name in failed), failed


def test_check_15_requires_text_on_every_row(tmp_path):
    """README check 15: every text-bearing comment has a text_value.  One
    FUNCTION comment without texts fails it, however many others have one."""
    from conftest import run_transform, write_jsonl_zst
    entries = _small_entries()
    functions = [c for e in entries for c in e.get("comments", [])
                 if c["commentType"] == "FUNCTION"]
    assert len(functions) > 1
    del functions[0]["texts"]
    jsonl = write_jsonl_zst(entries, str(tmp_path / "in.jsonl.zst"))
    lake = str(tmp_path / "lake")
    run_transform(jsonl, lake)
    report = str(tmp_path / "report.txt")
    result = _run_validator(lake, jsonl, report, "--spot-check-n", "100")
    assert result.returncode == 1
    failed = [name for name, c in _checks(report).items() if not c["passed"]]
    assert failed == ["comments.text_value populated for FUNCTION"], failed


def test_validator_duckdb_settings(small_lake, tmp_path, monkeypatch):
    """--memory-limit / --threads / --temp-dir reach every DuckDB connection
    the validator opens (VALIDATE sizes DuckDB to its task, not the node)."""
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    import validate_lake

    calls = []
    real_init = validate_lake.init_duckdb

    def spy(memory_limit, threads, temp_dir=None):
        calls.append((memory_limit, threads, temp_dir))
        return real_init(memory_limit, threads, temp_dir)

    monkeypatch.setattr(validate_lake, "init_duckdb", spy)
    report = str(tmp_path / "report.txt")
    monkeypatch.setattr(sys, "argv", [
        "validate_lake.py", "--lake", small_lake["lake_dir"], "--jsonl", small_lake["jsonl"],
        "--spot-check-n", "5", "--memory-limit", "1GB", "--threads", "2",
        "--temp-dir", str(tmp_path / "spill"), "-o", report,
    ])
    with pytest.raises(SystemExit) as exc:
        validate_lake.main()
    assert exc.value.code == 0
    assert calls and set(calls) == {("1GB", 2, str(tmp_path / "spill"))}
    with open(validate_lake.__file__) as f:
        assert "duckdb.connect(" not in f.read()
    with open(report) as f:
        assert "DuckDB: memory_limit 1GB, threads 2" in f.read()


@pytest.mark.parametrize("digest", ["sha256", "md5"])
def test_validate_fails_on_wrong_file_digest(small_lake, tmp_path, digest):
    """A file whose SHA-256 or MD5 differs from the manifest's fails check 8."""
    lake = str(tmp_path / "lake")
    shutil.copytree(small_lake["lake_dir"], lake)
    manifest_path = os.path.join(lake, "manifest.json")
    with open(manifest_path) as f:
        manifest = json.load(f)
    details = manifest["tables"]["xrefs"]["file_details"]
    rel = sorted(details)[0]
    details[rel][digest] = "0" * len(details[rel][digest])
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)
    report_path = str(tmp_path / "report.txt")
    result = _run_validator(lake, small_lake["jsonl"], report_path, "--threads", "3")
    assert result.returncode != 0
    failed = [c for name, c in _checks(report_path).items()
              if name.startswith("xrefs:") and "file hashes match" in name]
    assert failed and failed[0]["passed"] is False
    assert f"{rel}: {digest}" in failed[0]["detail"]


# ─── Check 21: shortcut values; check 3: entryType; check 20: --release ───


def _set_column(tbl, name, fn):
    """``tbl`` with column ``name`` replaced by fn(old values), same type."""
    import pyarrow as pa
    i = tbl.schema.get_field_index(name)
    field = tbl.schema.field(i)
    return tbl.set_column(i, field, pa.array([fn(v) for v in tbl.column(name).to_pylist()], field.type))


def _failed(report_path):
    return [name for name, c in _checks(report_path).items() if not c["passed"]]


@pytest.mark.parametrize("table, column, fn", [
    ("entries", "protein_name_primary", lambda v: "WRONG"),
    ("entries", "go_id", lambda v: []),
    ("entries", "lit_pubmed_id", lambda v: []),
    ("entries", "date_created", lambda v: None),
    ("entries", "reviewed", lambda v: not v),
    ("features", "organism_name", lambda v: "x"),
    ("features", "start_pos", lambda v: None if v is None else v + 1),
])
def test_wrong_shortcut_values_fail_check_21(small_lake, tmp_path, table, column, fn):
    """A shortcut that is wrong in every row of a file fails check 21, even
    though every entry still reconstructs from its source columns."""
    lake = str(tmp_path / "lake")
    shutil.copytree(small_lake["lake_dir"], lake)
    _rewrite_first_file(lake, table, lambda tbl: _set_column(tbl, column, fn))
    report_path = str(tmp_path / "report.txt")
    result = _run_validator(lake, small_lake["jsonl"], report_path, "--spot-check-n", "100")
    assert result.returncode == 1
    failed = _failed(report_path)
    check = f"{table} shortcut values match the JSONL"
    assert any(name.startswith(check) for name in failed), failed
    detail = next(c["detail"] for name, c in _checks(report_path).items() if name.startswith(check))
    assert f"{table}.{column} " in detail


def test_unknown_entry_type_fails_check_3(small_lake, tmp_path):
    lake = str(tmp_path / "lake")
    shutil.copytree(small_lake["lake_dir"], lake)
    _rewrite_first_file(lake, "entries", lambda tbl: _set_column(
        tbl, "entryType", lambda v: v.replace("Swiss-Prot", "SwissProt")))
    report_path = str(tmp_path / "report.txt")
    assert _run_validator(lake, small_lake["jsonl"], report_path).returncode == 1
    check = _checks(report_path)["entries.entryType values are known"]
    assert check["passed"] is False and "SwissProt" in check["detail"]


@pytest.mark.parametrize("release", [True, False])
def test_empty_shortcut_fails_check_20_in_release_mode(small_lake, tmp_path, release):
    """go_id empty in every row: a failure for a release (--release), a
    warning for a subset."""
    from conftest import table_files
    import pyarrow.parquet as pq
    lake = str(tmp_path / "lake")
    shutil.copytree(small_lake["lake_dir"], lake)
    for i in range(len(table_files(lake, "entries"))):
        # _rewrite_first_file rewrites file 0: rotate through every file.
        files = table_files(lake, "entries")
        path = files[i]
        tbl = pq.read_table(path)
        pq.write_table(_set_column(tbl, "go_id", lambda v: []), path)
    report_path = str(tmp_path / "report.txt")
    extra = ("--release",) if release else ()
    _run_validator(lake, small_lake["jsonl"], report_path, *extra)
    failed = _failed(report_path)
    with open(report_path) as f:
        text = f.read()
    if release:
        assert "entries.go_id has values" in failed, failed
    else:
        assert "entries.go_id has values" not in failed
        assert "[WARN] entries.go_id has no values" in text


def test_good_lake_passes_in_release_mode(small_lake, tmp_path):
    report_path = str(tmp_path / "report.txt")
    result = _run_validator(small_lake["lake_dir"], small_lake["jsonl"], report_path,
                            "--release", "--spot-check-n", "100")
    assert result.returncode == 0, result.stderr[-2000:]
    names = _checks(report_path)
    for table in ("entries", "features", "xrefs", "comments", "publications", "accession_map"):
        assert any(n.startswith(f"{table} shortcut values match the JSONL") for n in names), table


# ─── Check 18: accession_map mappings ─────────────────────────────────


def _rotate_secondary_primaries(tbl):
    """Each secondary row points at the next secondary row's primary."""
    import pyarrow as pa
    primary = tbl.column("is_primary").to_pylist()
    targets = tbl.column("primary_accession").to_pylist()
    rows = [i for i, p in enumerate(primary) if not p]
    rotated = list(targets)
    distinct = sorted({targets[i] for i in rows})
    for i in rows:
        rotated[i] = distinct[(distinct.index(targets[i]) + 1) % len(distinct)]
    i = tbl.schema.get_field_index("primary_accession")
    return tbl.set_column(i, tbl.schema.field(i), pa.array(rotated, tbl.schema.field(i).type))


def _repoint_first_primary(tbl):
    import pyarrow as pa
    primary = tbl.column("is_primary").to_pylist()
    targets = tbl.column("primary_accession").to_pylist()
    first, other = [i for i, p in enumerate(primary) if p][:2]
    targets[first] = targets[other]
    i = tbl.schema.get_field_index("primary_accession")
    return tbl.set_column(i, tbl.schema.field(i), pa.array(targets, tbl.schema.field(i).type))


def _flip_first_secondary_reviewed(tbl):
    import pyarrow as pa
    primary = tbl.column("is_primary").to_pylist()
    reviewed = tbl.column("reviewed").to_pylist()
    first = primary.index(False)
    reviewed[first] = not reviewed[first]
    i = tbl.schema.get_field_index("reviewed")
    return tbl.set_column(i, tbl.schema.field(i), pa.array(reviewed, pa.bool_()))


@pytest.mark.parametrize("mutate, check", [
    (_rotate_secondary_primaries, "accession_map secondary rows map each secondary accession to its entry"),
    (_repoint_first_primary, "accession_map primary rows have accession == primary_accession"),
    (_flip_first_secondary_reviewed, "accession_map (reviewed, organism_id) match the entry of primary_accession"),
])
def test_wrong_accession_map_mapping_fails_check_18(small_lake, tmp_path, mutate, check):
    """Counts, duplicates and orphans all still agree: only the mapping is wrong."""
    lake = str(tmp_path / "lake")
    shutil.copytree(small_lake["lake_dir"], lake)
    _rewrite_first_file(lake, "accession_map", mutate)
    report_path = str(tmp_path / "report.txt")
    assert _run_validator(lake, small_lake["jsonl"], report_path).returncode == 1
    checks = _checks(report_path)
    assert check in checks, sorted(checks)
    assert checks[check]["passed"] is False


# ─── Stratified sample (Swiss-Prot and TrEMBL reservoirs) ──────────────


def _synthetic_jsonl(path, n_reviewed, n_unreviewed):
    """JSONL.zst lines with only the keys the sampler reads, TrEMBL and
    Swiss-Prot interleaved (the sampler must not rely on the sort)."""
    from conftest import write_jsonl_zst
    entries = [{"primaryAccession": f"T{i}", "entryType": "UniProtKB unreviewed (TrEMBL)"}
               for i in range(n_unreviewed)]
    for i in range(n_reviewed):
        entries.insert(i * 37 % (len(entries) + 1),
                       {"primaryAccession": f"S{i}", "entryType": "UniProtKB reviewed (Swiss-Prot)"})
    return write_jsonl_zst(entries, str(path))


def test_sample_is_stratified(tmp_path):
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    from validate_lake import sample_and_count_jsonl
    jsonl = _synthetic_jsonl(tmp_path / "in.jsonl.zst", 10, 5000)
    sample, count, unclassified = sample_and_count_jsonl(jsonl, 5)
    assert count == 5010 and unclassified == 0
    accessions = [e["primaryAccession"] for e in sample]
    assert len(accessions) == len(set(accessions)) == 10
    assert sum(a.startswith("S") for a in accessions) == 5
    sample, _, _ = sample_and_count_jsonl(jsonl, 5, n_reviewed=8)
    assert sum(e["primaryAccession"].startswith("S") for e in sample) == 8


def test_sample_single_stratum(tmp_path):
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    from validate_lake import sample_and_count_jsonl
    jsonl = _synthetic_jsonl(tmp_path / "in.jsonl.zst", 0, 300)
    sample, count, unclassified = sample_and_count_jsonl(jsonl, 5)
    assert (len(sample), count, unclassified) == (5, 300, 0)


def test_sample_counts_lines_without_entry_type(tmp_path):
    sys.path.insert(0, os.path.abspath(BIN_DIR))
    from conftest import write_jsonl_zst
    from validate_lake import sample_and_count_jsonl
    jsonl = write_jsonl_zst([{"primaryAccession": "A"}, {"primaryAccession": "B",
                             "entryType": "UniProtKB unreviewed (TrEMBL)"}],
                            str(tmp_path / "in.jsonl.zst"))
    assert sample_and_count_jsonl(jsonl, 5)[1:] == (2, 1)


def test_report_shows_both_strata(small_lake, tmp_path):
    report_path = str(tmp_path / "report.txt")
    result = _run_validator(small_lake["lake_dir"], small_lake["jsonl"], report_path,
                            "--spot-check-n", "5")
    assert result.returncode == 0, result.stderr[-2000:]
    names = list(_checks(report_path))
    assert "reconstruction matches JSONL for 5/5 Swiss-Prot and 5/5 TrEMBL entries" in names, names
    assert any(n.startswith("entries shortcut values match the JSONL") and "5 Swiss-Prot + 5 TrEMBL" in n
               for n in names), names
    assert "all 10 sampled accessions found in lake (5 Swiss-Prot + 5 TrEMBL)" in names, names


def test_duckdb_out_of_memory_exits_75(small_lake, tmp_path):
    report = str(tmp_path / "report.txt")
    result = _run_validator(small_lake["lake_dir"], small_lake["jsonl"], report,
                            "--memory-limit", "50MB", "--threads", "2")
    assert result.returncode == 75, result.stderr[-2000:]
    assert "DuckDB ran out of memory" in result.stderr
