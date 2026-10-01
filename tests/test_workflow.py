"""End-to-end tests of upjson2lake.nf on the small fixture.

Skipped unless ``nextflow`` is on PATH.  Every run uses -resume in one launch
directory (as run_lake.sh does), so the fixture's baseline run is reused and
each test only re-runs what its params change.
"""

import json
import os
import shutil
import subprocess
import sys

import pytest

from conftest import PROJECT_ROOT, SMALL_JSON_GZ

WORKFLOW = os.path.join(PROJECT_ROOT, "upjson2lake.nf")

pytestmark = pytest.mark.skipif(shutil.which("nextflow") is None, reason="nextflow not on PATH")


def _launch(launch_dir, release, *extra, workflow=WORKFLOW):
    """Run ``workflow`` from ``launch_dir`` into ``launch_dir/out/<release>``;
    the CompletedProcess."""
    # The bin/ scripts resolve python3 from PATH: use the interpreter running the tests.
    env = {**os.environ, "PATH": os.path.dirname(sys.executable) + os.pathsep + os.environ["PATH"]}
    env.setdefault("NXF_OFFLINE", "true")
    cmd = ["nextflow", "run", str(workflow), "-c", "no_reports.config", "-profile", "local",
           "-ansi-log", "false", "-resume",
           "--outdir", "out", "--release", release, "--process_memory", "4 GB", *extra]
    return subprocess.run(cmd, cwd=launch_dir, env=env, capture_output=True, text=True)


def _run(launch_dir, release, *extra, workflow=WORKFLOW):
    """As _launch, which must succeed; the validation report text."""
    result = _launch(launch_dir, release, *extra, workflow=workflow)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    with open(os.path.join(launch_dir, "out", release, "validation_report.txt")) as f:
        return f.read()


@pytest.fixture(scope="module")
def launch_dir(tmp_path_factory):
    """A launch directory holding a baseline release ``prev`` built from the
    small fixture, to diff later runs against."""
    d = tmp_path_factory.mktemp("nf_launch")
    # The run's timeline / report / trace / DAG are not under test.
    (d / "no_reports.config").write_text(
        "trace.enabled = false\nreport.enabled = false\n"
        "timeline.enabled = false\ndag.enabled = false\n")
    _run(d, "prev")
    return d


def _copy_manifest(launch_dir, subdir):
    dst = launch_dir / subdir / "manifest.json"
    dst.parent.mkdir()
    shutil.copy(launch_dir / "out" / "prev" / "lake" / "manifest.json", dst)
    return dst


class TestPreviousManifest:
    """--previous_manifest reaches VALIDATE however it is given, so the
    report's schema diff is never silently skipped."""

    def test_relative_path(self, launch_dir):
        text = _run(launch_dir, "rel", "--previous_manifest", "out/prev/lake/manifest.json")
        assert "schema diff skipped" not in text
        assert "previous: release prev" in text

    def test_path_with_spaces(self, launch_dir):
        manifest = _copy_manifest(launch_dir, "prev release")
        text = _run(launch_dir, "spaced", "--previous_manifest", str(manifest))
        assert "schema diff skipped" not in text
        assert "previous: release prev" in text

    def test_resume_rereads_changed_manifest(self, launch_dir):
        manifest = _copy_manifest(launch_dir, "editable")
        _run(launch_dir, "resumed", "--previous_manifest", str(manifest))
        data = json.loads(manifest.read_text())
        data["release"] = "prev-edited"
        manifest.write_text(json.dumps(data, indent=2))
        text = _run(launch_dir, "resumed", "--previous_manifest", str(manifest))
        assert "previous: release prev-edited" in text

    def test_manifest_of_the_release_being_built_is_refused(self, launch_dir):
        """PARQUET_TRANSFORM republishes that file while VALIDATE reads it,
        so the schema diff would compare the release with itself."""
        result = _launch(launch_dir, "prev", "--previous_manifest", "out/prev/lake/manifest.json")
        assert result.returncode != 0
        assert "inside the release being built" in result.stdout + result.stderr


def test_stale_spill_of_a_killed_attempt_is_reclaimed(launch_dir, tmp_path):
    """An attempt killed by the OOM killer never runs its cleanup, so its
    spill stays in --duckdb_temp; the next attempt of the same release and
    process removes it before spilling, and nothing else there is touched."""
    temp = tmp_path / "duckdb tmp"
    stale = [temp / f"spill.{p}" / "duckdb_spill_old" / "junk"
             for p in ("SORT_JSONL", "PARQUET_TRANSFORM", "VALIDATE")]
    other = temp / "other" / "keep"
    for f in (*stale, other):
        f.parent.mkdir(parents=True)
        f.write_text("x")
    # An input of its own, so -resume cannot reuse the baseline's SORT_JSONL.
    inputfile = tmp_path / "input.json.gz"
    shutil.copy(SMALL_JSON_GZ, inputfile)
    _run(launch_dir, "spill", "--duckdb_temp", str(temp), "--inputfile", str(inputfile))
    for f in stale:
        assert not f.exists(), f
    assert other.exists()


def test_published_lake_has_no_build_sentinels(launch_dir):
    """The transform's .complete/ sentinels are build bookkeeping for manual
    --skip-existing re-runs: not listed in SHA256SUMS.txt, not published."""
    lake = launch_dir / "out" / "prev" / "lake"
    assert (lake / "manifest.json").exists()
    assert not (lake / ".complete").exists()


def test_validate_duckdb_is_sized_to_its_task(launch_dir):
    """VALIDATE hands DuckDB the task's memory share and cpus, not the node's."""
    with open(launch_dir / "out" / "prev" / "validation_report.txt") as f:
        assert "DuckDB: memory_limit 3072MB, threads " in f.read()   # 75% of 4 GB, in MB


# ─── Release marker and the published copy ───────────────────────────


@pytest.fixture(scope="module")
def project_copy(tmp_path_factory):
    """A copy of the pipeline (workflow, config, bin/) whose scripts a test
    may change without touching the checkout."""
    d = tmp_path_factory.mktemp("project")
    shutil.copy(WORKFLOW, d)
    shutil.copy(os.path.join(PROJECT_ROOT, "nextflow.config"), d)
    shutil.copytree(os.path.join(PROJECT_ROOT, "bin"), d / "bin",
                    ignore=shutil.ignore_patterns("__pycache__"))
    return d


class TestReleaseMarker:
    """RELEASE_COMPLETE appears only for a release whose published copy is
    whole, and never survives a failed re-run."""

    def test_success_leaves_only_the_marker(self, launch_dir):
        release = launch_dir / "out" / "prev"
        assert (release / "RELEASE_COMPLETE").exists()
        assert not (release / ".RELEASE_COMPLETE.pending").exists()

    def test_failed_run_leaves_no_marker_and_removes_a_stale_one(self, launch_dir):
        release = launch_dir / "out" / "failing"
        release.mkdir(parents=True)
        (release / "RELEASE_COMPLETE").write_text("stale\n")
        result = _launch(launch_dir, "failing", "--expected_count", "1")
        assert result.returncode != 0
        assert not (release / "RELEASE_COMPLETE").exists()
        assert not (release / ".RELEASE_COMPLETE.pending").exists()

    def test_resume_republishes_a_damaged_release(self, launch_dir):
        """A head job killed mid-publish is recovered with -resume: every task
        is cached, the published copy is rewritten whole, and the marker is
        written only after verify_release.py passed on it."""
        from conftest import run_bin
        _run(launch_dir, "damaged")
        release = launch_dir / "out" / "damaged"
        victim = sorted((release / "lake").rglob("*.parquet"))[0]
        victim.unlink()
        jsonl = release / "sorted.jsonl.zst"
        os.truncate(jsonl, jsonl.stat().st_size // 2)
        assert run_bin("verify_release.py", release).returncode == 1
        _run(launch_dir, "damaged")
        assert victim.exists()
        assert (release / "RELEASE_COMPLETE").exists()
        result = run_bin("verify_release.py", release, "--full")
        assert result.returncode == 0, result.stdout

    def test_failed_verification_writes_no_marker(self, launch_dir, project_copy):
        stub = project_copy / "bin" / "verify_release.py"
        original = stub.read_text()
        stub.write_text("#!/usr/bin/env python3\nimport sys\nprint('FAIL: stub')\nsys.exit(1)\n")
        try:
            result = _launch(launch_dir, "unverified", "--inputfile", SMALL_JSON_GZ,
                             workflow=project_copy / "upjson2lake.nf")
        finally:
            stub.write_text(original)
        assert result.returncode == 0, result.stdout[-3000:]
        release = launch_dir / "out" / "unverified"
        assert (release / "provenance.json").exists()
        assert not (release / "RELEASE_COMPLETE").exists()
        assert not (release / ".RELEASE_COMPLETE.pending").exists()
        assert "FAIL: stub" in result.stdout + result.stderr


def test_changed_bin_module_reruns_validate(launch_dir, project_copy):
    """VALIDATE imports reconstruct.py, which Nextflow does not hash: a change
    to any bin/ script must still re-run VALIDATE and PROVENANCE under
    -resume, never republish the old verdict.  The transform is cached."""
    workflow = project_copy / "upjson2lake.nf"
    _run(launch_dir, "digest", "--inputfile", SMALL_JSON_GZ, workflow=workflow)
    module = project_copy / "bin" / "reconstruct.py"
    original = module.read_text()
    module.write_text(original + "\n# changed\n")
    try:
        result = _launch(launch_dir, "digest", "--inputfile", SMALL_JSON_GZ, workflow=workflow)
    finally:
        module.write_text(original)
    out = result.stdout
    assert result.returncode == 0, out[-3000:]
    # STREAM_JSONL, SORT_JSONL and PARQUET_TRANSFORM cached; VALIDATE and PROVENANCE run.
    assert "completed=2 failed=0 cached=3" in out, out[-1000:]


def _sibling_imports(script, seen=None):
    """The bin/ modules ``script`` imports, directly or through each other."""
    import ast
    bin_dir = os.path.join(PROJECT_ROOT, "bin")
    seen = set() if seen is None else seen
    with open(os.path.join(bin_dir, script)) as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        names = ([a.name for a in node.names] if isinstance(node, ast.Import)
                 else [node.module] if isinstance(node, ast.ImportFrom) and node.module else [])
        for name in names:
            module = name.split(".")[0] + ".py"
            if module not in seen and os.path.exists(os.path.join(bin_dir, module)):
                seen.add(module)
                _sibling_imports(module, seen)
    return seen


def test_bin_modules_list_every_imported_module():
    """bin_modules(<process>) names exactly the bin/ modules the process's
    scripts import, so a change to one re-runs the process under -resume."""
    import re
    with open(WORKFLOW) as f:
        text = f.read()
    table = text[text.index("def bin_modules("):]
    table = table[:table.index("\n}\n")]
    lists = {p: set(re.findall(r"'([^']+)'", names))
             for p, names in re.findall(r"(\w+):\s*\[([^]]*)\]", table)}
    assert set(lists) == {"STREAM_JSONL", "SORT_JSONL", "PARQUET_TRANSFORM", "VALIDATE", "PROVENANCE"}
    for process, listed in lists.items():
        body = text[text.index(f"process {process} {{"):]
        body = body[:body.index("\n}\n")]
        scripts = set(re.findall(r"\b(\w+\.py)\b", body))
        imported = set().union(*(_sibling_imports(s) for s in scripts)) - scripts
        assert listed == imported, f"{process}: bin_modules lists {sorted(listed)}, scripts import {sorted(imported)}"
        if listed:
            assert f"bin_digest('{process}')" in text, process


def test_duckdb_out_of_memory_is_retried_with_more_memory(launch_dir, tmp_path):
    """--process_memory 2.5 GB gives DuckDB 1920MB at attempt 1, too little
    to sort or transform the small fixture: each exits 75 and is re-submitted
    at 5 GB (DuckDB 3840MB), and the run succeeds."""
    import re
    # An input of its own, so -resume cannot reuse the baseline's SORT_JSONL.
    inputfile = tmp_path / "input.json.gz"
    shutil.copy(SMALL_JSON_GZ, inputfile)
    result = _launch(launch_dir, "oom", "--process_memory", "2.5 GB", "--inputfile", str(inputfile))
    assert result.returncode == 0, result.stdout[-3000:]
    # The trace file is off in these tests (its collector needs /proc/<pid>/io);
    # .nextflow.log records each attempt.
    with open(launch_dir / ".nextflow.log") as f:
        log = f.read()
    for process in ("SORT_JSONL", "PARQUET_TRANSFORM"):
        assert re.search(rf"name: {process} \(\w+\); status: COMPLETED; exit: 75;", log), process
        assert f"Re-submitted process > {process}" in log, process
        attempts = re.findall(rf"name: {process} \(\w+\); status: COMPLETED; exit: (\d+); "
                              rf"error: -; workDir: (\S+)\]", log)
        limits = [re.search(r"--memory-limit (\S+)", open(os.path.join(d, ".command.sh")).read()).group(1)
                  for _, d in attempts]
        assert [(e, m) for (e, _), m in zip(attempts, limits)] == [("75", "1920MB"), ("0", "3840MB")]
    assert (launch_dir / "out" / "oom" / "RELEASE_COMPLETE").exists()


def test_provenance_identifies_the_source_dump(launch_dir):
    """provenance.json names the dump the release was built from (its
    SHA-256), the tool versions and the run parameters."""
    import hashlib
    with open(launch_dir / "out" / "prev" / "provenance.json") as f:
        provenance = json.load(f)
    with open(SMALL_JSON_GZ, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    assert provenance["inputs"]["source"]["sha256"] == digest
    assert provenance["inputs"]["source"]["entry_count"] == 52
    assert provenance["tools"]["nextflow"] and provenance["tools"]["duckdb"]
    assert provenance["params"]["release"] == "prev"
    assert provenance["params"]["process_memory"] == "4 GB"
