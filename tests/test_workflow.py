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


def _launch(launch_dir, release, *extra):
    """Run the workflow from ``launch_dir`` into ``launch_dir/out/<release>``;
    the CompletedProcess."""
    # The bin/ scripts resolve python3 from PATH: use the interpreter running the tests.
    env = {**os.environ, "PATH": os.path.dirname(sys.executable) + os.pathsep + os.environ["PATH"]}
    env.setdefault("NXF_OFFLINE", "true")
    cmd = ["nextflow", "run", WORKFLOW, "-c", "no_reports.config", "-profile", "local",
           "-ansi-log", "false", "-resume",
           "--outdir", "out", "--release", release, "--process_memory", "4 GB", *extra]
    return subprocess.run(cmd, cwd=launch_dir, env=env, capture_output=True, text=True)


def _run(launch_dir, release, *extra):
    """As _launch, which must succeed; the validation report text."""
    result = _launch(launch_dir, release, *extra)
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
        assert "DuckDB: memory_limit 3GB, threads " in f.read()
