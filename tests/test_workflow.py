"""End-to-end tests of upjson2lake.nf on the small fixture.

Skipped unless ``nextflow`` is on PATH; with REQUIRE_NEXTFLOW set (CI's
workflow job), a missing nextflow fails them instead.  The static check that
bin_modules() matches the scripts' imports is in test_bin_modules.py, which
always runs.  Every run uses -resume in one launch
directory (as run_lake.sh does), so the fixture's baseline run is reused and
each test only re-runs what its params change.
"""

import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
from typing import cast

import pytest
from conftest import PROJECT_ROOT, SMALL_JSON_GZ, make_parts_output

WORKFLOW = os.path.join(PROJECT_ROOT, "upjson2lake.nf")
TABLES = ("entries", "features", "xrefs", "comments", "publications", "accession_map")

pytestmark = pytest.mark.skipif(shutil.which("nextflow") is None and not os.environ.get("REQUIRE_NEXTFLOW"),
                                reason="nextflow not on PATH")


def _environ() -> dict[str, str]:
    """os.environ but the shell's NXF_WORK (RUNBOOK stage 6): each launch
    directory keeps its own work/, out of the cluster's shared one."""
    return {k: v for k, v in os.environ.items() if k != "NXF_WORK"}


def _launch(launch_dir: pathlib.Path, release: str, *extra: str,
            workflow: str | pathlib.Path = WORKFLOW) -> subprocess.CompletedProcess[str]:
    """Run ``workflow`` from ``launch_dir`` into ``launch_dir/out/<release>``;
    the CompletedProcess."""
    # The bin/ scripts resolve python3 from PATH: use the interpreter running the tests.
    env = {**_environ(), "PATH": os.path.dirname(sys.executable) + os.pathsep + os.environ["PATH"]}
    env.setdefault("NXF_OFFLINE", "true")
    cmd = ["nextflow", "run", str(workflow), "-c", "no_reports.config", "-profile", "local",
           "-ansi-log", "false", "-resume",
           "--outdir", "out", "--release", release, "--process_memory", "4 GB", *extra]
    return subprocess.run(cmd, cwd=launch_dir, env=env, capture_output=True, text=True)


def _run(launch_dir: pathlib.Path, release: str, *extra: str, workflow: str | pathlib.Path = WORKFLOW) -> str:
    """As _launch, which must succeed; the validation report text."""
    result = _launch(launch_dir, release, *extra, workflow=workflow)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    with open(os.path.join(launch_dir, "out", release, "validation_report.txt")) as f:
        return f.read()


@pytest.fixture(scope="module")
def launch_dir(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """A launch directory holding a baseline release ``prev`` built from the
    small fixture, to diff later runs against."""
    d = tmp_path_factory.mktemp("nf_launch")
    # The run's timeline / report / trace / DAG are not under test.
    (d / "no_reports.config").write_text(
        "trace.enabled = false\nreport.enabled = false\n"
        "timeline.enabled = false\ndag.enabled = false\n")
    _run(d, "prev")
    return d


def _copy_manifest(launch_dir: pathlib.Path, subdir: str) -> pathlib.Path:
    dst = launch_dir / subdir / "manifest.json"
    dst.parent.mkdir()
    shutil.copy(launch_dir / "out" / "prev" / "lake" / "manifest.json", dst)
    return dst


class TestPreviousManifest:
    """--previous_manifest reaches VALIDATE however it is given, so the
    report's schema diff is never silently skipped."""

    def test_relative_path(self, launch_dir: pathlib.Path) -> None:
        text = _run(launch_dir, "rel", "--previous_manifest", "out/prev/lake/manifest.json")
        assert "schema diff skipped" not in text
        assert "previous: release prev" in text

    def test_path_with_spaces(self, launch_dir: pathlib.Path) -> None:
        manifest = _copy_manifest(launch_dir, "prev release")
        text = _run(launch_dir, "spaced", "--previous_manifest", str(manifest))
        assert "schema diff skipped" not in text
        assert "previous: release prev" in text

    def test_resume_rereads_changed_manifest(self, launch_dir: pathlib.Path) -> None:
        manifest = _copy_manifest(launch_dir, "editable")
        _run(launch_dir, "resumed", "--previous_manifest", str(manifest))
        data = json.loads(manifest.read_text())
        data["release"] = "prev-edited"
        manifest.write_text(json.dumps(data, indent=2))
        text = _run(launch_dir, "resumed", "--previous_manifest", str(manifest))
        assert "previous: release prev-edited" in text

    def test_manifest_of_the_release_being_built_is_refused(self, launch_dir: pathlib.Path) -> None:
        """ASSEMBLE_LAKE republishes that file while VALIDATE reads it,
        so the schema diff would compare the release with itself."""
        result = _launch(launch_dir, "prev", "--previous_manifest", "out/prev/lake/manifest.json")
        assert result.returncode != 0
        assert "inside the release being built" in result.stdout + result.stderr


@pytest.mark.parametrize("count", [(), ("abc",)])
def test_expected_count_must_be_a_whole_number(launch_dir: pathlib.Path, count: tuple[str, ...]) -> None:
    """--expected_count $N with N unset arrives as true: refused before any
    task runs, and before the release's RELEASE_COMPLETE is removed."""
    marker = launch_dir / "out" / "prev" / "RELEASE_COMPLETE"
    result = _launch(launch_dir, "prev", "--expected_count", *count)
    assert result.returncode != 0
    assert "must be the release's entry count" in result.stdout + result.stderr
    assert marker.exists()


def _spill_dir_name(release_dir: pathlib.Path, process: str) -> str:
    """spill_setup's directory for a release directory and process:
    <release>.<process>.<first 8 hex digits of md5(absolute release dir)>."""
    import hashlib
    digest = hashlib.md5(str(release_dir).encode()).hexdigest()[:8]
    return f"{release_dir.name}.{process}.{digest}"


def _group(pattern: str, text: str) -> str:
    # re.search(pattern, text).group(1), typed: still an AttributeError if nothing matches
    return cast(re.Match[str], re.search(pattern, text)).group(1)


def test_stale_spill_of_a_killed_attempt_is_reclaimed(launch_dir: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """An attempt killed by the OOM killer never runs its cleanup, so its
    spill stays in --duckdb_temp; the next attempt of the same release and
    process removes it before spilling, and nothing else there is touched:
    not even the live spill of a run of the same release label into another
    outdir, which shares --duckdb_temp."""
    temp = tmp_path / "duckdb tmp"
    processes = ("SORT_JSONL", "INFER_SCHEMA", "STAGE_PARQUET", "BUILD_TABLE.entries", "BUILD_TABLE.xrefs", "VALIDATE")
    stale = [temp / _spill_dir_name(launch_dir / "out" / "spill", p) / "duckdb_spill_old" / "junk"
             for p in processes]
    other = [temp / "other" / "keep"] + [
        temp / _spill_dir_name(tmp_path / "elsewhere" / "spill", p) / "duckdb_spill_live" / "keep"
        for p in processes]
    for f in (*stale, *other):
        f.parent.mkdir(parents=True)
        f.write_text("x")
    # An input of its own, so -resume cannot reuse the baseline's SORT_JSONL.
    inputfile = tmp_path / "input.json.gz"
    shutil.copy(SMALL_JSON_GZ, inputfile)
    _run(launch_dir, "spill", "--duckdb_temp", str(temp), "--inputfile", str(inputfile))
    for f in stale:
        assert not f.exists(), f
    for f in other:
        assert f.exists(), f


def test_published_lake_has_no_build_sentinels(launch_dir: pathlib.Path) -> None:
    """The transform's .complete/ sentinels are build bookkeeping for manual
    --skip-existing re-runs: not listed in SHA256SUMS.txt, not published."""
    lake = launch_dir / "out" / "prev" / "lake"
    assert (lake / "manifest.json").exists()
    assert not (lake / ".complete").exists()


def test_validate_duckdb_is_sized_to_its_task(launch_dir: pathlib.Path) -> None:
    """VALIDATE hands DuckDB the task's memory share and cpus, not the node's."""
    with open(launch_dir / "out" / "prev" / "validation_report.txt") as f:
        assert "DuckDB: memory_limit 3072MB, threads " in f.read()   # 75% of 4 GB, in MB


def test_rerun_of_the_tables_reuses_schema_and_staging(launch_dir: pathlib.Path) -> None:
    """The input's columns and its staged Parquet come from the cache when
    only the tables are built again: here a new release label, which only
    BUILD_TABLE's and ASSEMBLE_LAKE's commands name."""
    _run(launch_dir, "relabel")
    with open(launch_dir / ".nextflow.log") as f:
        log = f.read()
    for process in ("STREAM_JSONL", "SORT_JSONL", "INFER_SCHEMA", "STAGE_PARQUET"):
        assert f"Cached process > {process} (" in log, process
    built = re.findall(r"\[(\w\w/\w+)\] Submitted process > BUILD_TABLE \((\w+)\)", log)
    assert sorted(t for _, t in built) == sorted(TABLES)
    # and each read the staged file it was handed
    for hash_prefix, table in built:
        (workdir,) = (launch_dir / "work").glob(f"{hash_prefix}*")
        assert "PARQUET STAGING (reused:" in (workdir / ".command.err").read_text(), table
        assert "--presorted" in (workdir / ".command.sh").read_text(), table


def test_a_table_whose_output_is_gone_is_built_alone(launch_dir: pathlib.Path) -> None:
    """Each table is cached on its own: when one task's result is missing (as
    after its failure), -resume builds that table again and takes the other
    five from the cache.  The release is re-assembled and validated."""
    _run(launch_dir, "onetable")
    with open(launch_dir / ".nextflow.log") as f:
        first = f.read()
    built = dict((t, h) for h, t in re.findall(r"\[(\w\w/\w+)\] Submitted process > BUILD_TABLE \((\w+)\)", first))
    (xrefs_dir,) = (launch_dir / "work").glob(f"{built['xrefs']}*")
    shutil.rmtree(xrefs_dir / "table_xrefs")
    report = _run(launch_dir, "onetable")
    assert "VERDICT: ALL CHECKS PASSED" in report
    with open(launch_dir / ".nextflow.log") as f:
        log = f.read()
    assert re.findall(r"Submitted process > BUILD_TABLE \((\w+)\)", log) == ["xrefs"]
    assert sorted(re.findall(r"Cached process > BUILD_TABLE \((\w+)\)", log)) == sorted(set(TABLES) - {"xrefs"})
    assert "Submitted process > ASSEMBLE_LAKE (" in log


def test_duckdb_pct_from_the_command_line(launch_dir: pathlib.Path) -> None:
    """--duckdb_pct arrives as a string: every DuckDB process still gets that
    share of its task's memory (it once failed: Long.multiply(String))."""
    report = _run(launch_dir, "pct", "--duckdb_pct", "50")
    assert "DuckDB: memory_limit 2048MB, threads " in report               # 50% of 4 GB
    with open(launch_dir / ".nextflow.log") as f:
        log = f.read()
    for process in ("SORT_JSONL", "BUILD_TABLE", "VALIDATE"):
        workdir = re.findall(rf"submitted process {process}.*workDir: (\S+)", log)
        if workdir:            # not cached: its command must carry the 50% limit
            assert "--memory-limit 2048MB" in (pathlib.Path(workdir[-1]) / ".command.sh").read_text(), process


@pytest.mark.parametrize("pct", ["abc", "0", "150"])
def test_duckdb_pct_must_be_a_percentage(launch_dir: pathlib.Path, pct: str) -> None:
    marker = launch_dir / "out" / "prev" / "RELEASE_COMPLETE"
    result = _launch(launch_dir, "prev", "--duckdb_pct", pct)
    assert result.returncode != 0
    assert "--duckdb_pct must be" in result.stdout + result.stderr
    assert marker.exists()


# ─── Release marker and the published copy ───────────────────────────


@pytest.fixture(scope="module")
def project_copy(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
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

    def test_success_leaves_only_the_marker(self, launch_dir: pathlib.Path) -> None:
        release = launch_dir / "out" / "prev"
        assert (release / "RELEASE_COMPLETE").exists()
        assert not (release / ".RELEASE_COMPLETE.pending").exists()

    def test_failed_run_leaves_no_marker_and_removes_a_stale_one(self, launch_dir: pathlib.Path) -> None:
        release = launch_dir / "out" / "failing"
        release.mkdir(parents=True)
        (release / "RELEASE_COMPLETE").write_text("stale\n")
        result = _launch(launch_dir, "failing", "--expected_count", "1")
        assert result.returncode != 0
        assert not (release / "RELEASE_COMPLETE").exists()
        assert not (release / ".RELEASE_COMPLETE.pending").exists()

    def test_resume_republishes_a_damaged_release(self, launch_dir: pathlib.Path) -> None:
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
        assert result.returncode == 0, result.stdout + result.stderr

    def test_failed_verification_writes_no_marker(self, launch_dir: pathlib.Path, project_copy: pathlib.Path) -> None:
        stub = project_copy / "bin" / "verify_release.py"
        original = stub.read_text()
        stub.write_text("#!/usr/bin/env python3\nimport sys\nprint('FAIL: stub')\nsys.exit(1)\n")
        try:
            result = _launch(launch_dir, "unverified", "--inputfile", SMALL_JSON_GZ,
                             workflow=project_copy / "upjson2lake.nf")
        finally:
            stub.write_text(original)
        assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
        release = launch_dir / "out" / "unverified"
        assert (release / "provenance.json").exists()
        assert not (release / "RELEASE_COMPLETE").exists()
        assert not (release / ".RELEASE_COMPLETE.pending").exists()
        assert "FAIL: stub" in result.stdout + result.stderr


def test_changed_bin_module_reruns_validate(launch_dir: pathlib.Path, project_copy: pathlib.Path) -> None:
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
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    # Everything to the lake cached (each BUILD_TABLE task); VALIDATE and PROVENANCE run.
    # From .nextflow.log, not the console: which lines the console shows depends
    # on where Nextflow runs (under an AI agent it prints only a summary).
    with open(launch_dir / ".nextflow.log") as f:
        log = f.read()
    for process in ("STREAM_JSONL", "SORT_JSONL", "INFER_SCHEMA", "STAGE_PARQUET", "ASSEMBLE_LAKE"):
        assert f"Cached process > {process} (" in log, process
    assert sorted(re.findall(r"Cached process > BUILD_TABLE \((\w+)\)", log)) == sorted(TABLES)
    for process in ("VALIDATE", "PROVENANCE"):
        assert f"Submitted process > {process} (" in log, process


def test_duckdb_out_of_memory_is_retried_with_more_memory(launch_dir: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """--process_memory 2.5 GB gives DuckDB 1920MB at attempt 1, too little
    to stage the small fixture (the JSON reader's 1 GiB buffer per thread): it
    exits 75 and is re-submitted at 5 GB (DuckDB 3840MB), and the run
    succeeds.  (The other DuckDB processes fit in 1920MB; SORT_JSONL's exit 75
    is tested in test_sort_jsonl.py.)"""
    # An input of its own, so -resume cannot reuse the baseline's SORT_JSONL.
    inputfile = tmp_path / "input.json.gz"
    shutil.copy(SMALL_JSON_GZ, inputfile)
    result = _launch(launch_dir, "oom", "--process_memory", "2.5 GB", "--inputfile", str(inputfile))
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    # The trace file is off in these tests (its collector needs /proc/<pid>/io);
    # .nextflow.log records each attempt.
    with open(launch_dir / ".nextflow.log") as f:
        log = f.read()
    for process in ("STAGE_PARQUET",):
        assert re.search(rf"name: {process} \(\w+\); status: COMPLETED; exit: 75;", log), process
        assert f"Re-submitted process > {process}" in log, process
        attempts = re.findall(rf"name: {process} \(\w+\); status: COMPLETED; exit: (\d+); "
                              rf"error: -; workDir: (\S+)\]", log)
        limits = [_group(r"--memory-limit (\S+)", (pathlib.Path(d) / ".command.sh").read_text())
                  for _, d in attempts]
        assert [(e, m) for (e, _), m in zip(attempts, limits, strict=True)] == [("75", "1920MB"), ("0", "3840MB")]
    assert (launch_dir / "out" / "oom" / "RELEASE_COMPLETE").exists()


def test_provenance_identifies_the_source_dump(launch_dir: pathlib.Path) -> None:
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


# ─── --input_parts: a dat2jsonl output instead of a REST dump ──────────


def _decompressed(path: pathlib.Path) -> bytes:
    import zstandard as zstd
    with open(path, "rb") as f:
        return zstd.ZstdDecompressor().stream_reader(f).read()


def _parquet_tables(release_dir: pathlib.Path) -> dict[str, tuple[object, dict[bytes, bytes]]]:
    """Each Parquet file of the lake: its data, and its schema metadata but
    uniprot_release (the run's release label, so not comparable across runs)."""
    import pyarrow.parquet as pq
    lake = release_dir / "lake"
    out = {}
    for path in sorted(lake.glob("*/*/*.parquet")):
        table = pq.read_table(path)
        meta = dict(table.schema.metadata or {})
        assert meta.pop(b"uniprot_release") == release_dir.name.encode()
        out[str(path.relative_to(lake))] = (table.replace_schema_metadata(None), meta)
    return out


def test_parts_build_the_same_lake_as_the_dump(launch_dir: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """FROM_PARTS joins a dat2jsonl output's parts in plan order; given the
    lines STREAM_JSONL makes from the baseline's dump, the release is the
    same: sorted JSONL byte for byte, every Parquet file's data and metadata
    (but its release label), validation passed.  provenance.json names the
    dat2jsonl output as the source."""
    parts = tmp_path / "dat2jsonl"
    make_parts_output(SMALL_JSON_GZ, str(parts), n_parts=3)
    _run(launch_dir, "parts", "--input_parts", str(parts), "--expected_count", "52")
    out = launch_dir / "out"
    assert (out / "parts" / "RELEASE_COMPLETE").exists()
    assert _decompressed(out / "parts" / "sorted.jsonl.zst") == _decompressed(out / "prev" / "sorted.jsonl.zst")
    tables, baseline = _parquet_tables(out / "parts"), _parquet_tables(out / "prev")
    assert tables and tables.keys() == baseline.keys()
    for name, (table, meta) in tables.items():
        assert table.equals(baseline[name][0]) and meta == baseline[name][1], name  # type: ignore[attr-defined]
    with open(out / "parts" / "provenance.json") as f:
        source = json.load(f)["inputs"]["source"]
    assert source["kind"] == "dat2jsonl" and source["directory"] == str(parts.resolve())
    assert source["entry_count"] == source["expected_count"] == 52 and source["parts"] == 3


def test_parts_whose_check_has_not_passed_are_refused(launch_dir: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """An output without COMPLETE (dat2jsonl's check failed or never ran) is
    refused before anything runs, and no release appears."""
    parts = tmp_path / "dat2jsonl"
    make_parts_output(SMALL_JSON_GZ, str(parts), complete=False)
    result = _launch(launch_dir, "unchecked", "--input_parts", str(parts))
    assert result.returncode != 0
    assert "COMPLETE is missing" in result.stdout + result.stderr
    assert not (launch_dir / "out" / "unchecked" / "RELEASE_COMPLETE").exists()


def test_parts_whose_count_is_not_the_expected_one_fail(launch_dir: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """FROM_PARTS stops when the joined parts do not hold --expected_count
    entries, like STREAM_JSONL."""
    parts = tmp_path / "dat2jsonl"
    make_parts_output(SMALL_JSON_GZ, str(parts))
    result = _launch(launch_dir, "miscounted", "--input_parts", str(parts), "--expected_count", "53")
    assert result.returncode != 0
    assert "52 entries, expected 53" in result.stdout + result.stderr
    assert not (launch_dir / "out" / "miscounted" / "RELEASE_COMPLETE").exists()


# ─── --input_dat: the release's flat files, converted in the pipeline ──

DAT2JSONL_JAR = pathlib.Path(PROJECT_ROOT) / "dat2jsonl" / "target" / "dat2jsonl.jar"


def _release_dir(tmp_path: pathlib.Path, dat_name: str = "uniprot_sprot.dat") -> pathlib.Path:
    """A release directory with dat2jsonl's files present (empty): enough for the checks."""
    src = tmp_path / "uniprot"
    (src / "go").mkdir(parents=True)
    (src / "uniparc_map").mkdir()
    for name in ("keywlist.txt", "humdisease.txt", "subcell.txt", "go/GO.dr_extended", dat_name):
        (src / name).touch()
    return src


@pytest.mark.parametrize(("extra", "why"), [
    ((), "--input_dat needs --gaf_eco"),
    (("--input_parts", "x"), "two inputs: give one"),
])
def test_input_dat_is_refused_without_its_parts(launch_dir: pathlib.Path, tmp_path: pathlib.Path,
                                                extra: tuple[str, ...], why: str) -> None:
    src = _release_dir(tmp_path)
    result = _launch(launch_dir, "datrefused", "--input_dat", str(src),
                     "--dat_files", str(src / "uniprot_sprot.dat"), *extra)
    assert result.returncode != 0
    assert why in result.stdout + result.stderr


def test_input_dat_refuses_a_compressed_dat(launch_dir: pathlib.Path, tmp_path: pathlib.Path) -> None:
    src = _release_dir(tmp_path, "uniprot_sprot.dat.gz")
    result = _launch(launch_dir, "datrefused", "--input_dat", str(src),
                     "--dat_files", str(src / "uniprot_sprot.dat.gz"))
    assert result.returncode != 0
    assert "byte ranges need an uncompressed .dat" in result.stdout + result.stderr


@pytest.mark.skipif(not DAT2JSONL_JAR.exists() or not os.environ.get("DAT2JSONL_TEST_SRC"),
                    reason="needs dat2jsonl/target/dat2jsonl.jar and DAT2JSONL_TEST_SRC: a release directory "
                           "(keywlist.txt, humdisease.txt, subcell.txt, go/GO.dr_extended, uniparc_map/, "
                           "uniprot_sprot.dat) holding gaf-eco-mapping.txt too")
def test_input_dat_builds_the_lake(launch_dir: pathlib.Path) -> None:
    """UNIPARC_ACTIVE, DAT2JSONL over several byte ranges and FROM_DAT turn a
    release's flat file into a validated lake; provenance.json records the
    stage, the jar and uniprot-core version, and the coverage check."""
    src = pathlib.Path(os.environ["DAT2JSONL_TEST_SRC"])
    dat = src / "uniprot_sprot.dat"
    with open(dat, "rb") as f:
        entries = sum(1 for line in f if line == b"//\n")
    chunk_gb = dat.stat().st_size / 3 / 1e9 + 1e-9        # three ranges
    _run(launch_dir, "dat", "--input_dat", str(src), "--dat_files", str(dat),
         "--gaf_eco", str(src / "gaf-eco-mapping.txt"), "--dat2jsonl_jar", str(DAT2JSONL_JAR),
         "--dat_chunk_gb", f"{chunk_gb:.12f}", "--dat_cpus", "2", "--dat_memory", "2 GB",
         "--expected_count", str(entries))
    out = launch_dir / "out" / "dat"
    assert (out / "RELEASE_COMPLETE").exists()
    with open(out / "provenance.json") as f:
        source = json.load(f)["inputs"]["source"]
    assert source["kind"] == "dat2jsonl stage" and source["entry_count"] == entries
    assert source["parts"] == 3 and source["check"][-1] == "CHECK PASSED"
    assert any(line.startswith("uniprot-core: ") for line in source["inputs"])


# ─── The SLURM profiles, on the stand-in tests/fake_slurm ──────────────


def test_prod_profile_on_stand_in_slurm(tmp_path: pathlib.Path) -> None:
    """The prod profile through Nextflow's slurm executor (sbatch, squeue and
    scancel stood in by tests/fake_slurm): every process is submitted with
    the requests RUNBOOK 8a lists; DuckDB's out-of-memory error is retried
    with twice the memory and the time, capped at --max_time; the job's
    output lands in .command.log with beforeScript's job id; and the log
    names each submitted task's work directory (the RUNBOOK's wd helper)."""
    launch = tmp_path / "launch"
    launch.mkdir()
    (launch / "no_reports.config").write_text(
        "trace.enabled = false\nreport.enabled = false\n"
        "timeline.enabled = false\ndag.enabled = false\n")
    state = tmp_path / "slurm"
    fake = os.path.join(PROJECT_ROOT, "tests", "fake_slurm")
    env = {**_environ(), "FAKE_SLURM_DIR": str(state), "NXF_OFFLINE": "true",
           "PATH": os.pathsep.join([fake, os.path.dirname(sys.executable), os.environ["PATH"]])}
    result = subprocess.run(
        ["nextflow", "run", WORKFLOW, "-c", "no_reports.config", "-profile", "prod", "-ansi-log", "false",
         "--inputfile", SMALL_JSON_GZ, "--outdir", "out", "--release", "slurm",
         "--process_memory", "2.5 GB", "--max_time", "30h", "--expected_count", "52"],
        cwd=launch, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    assert (launch / "out" / "slurm" / "RELEASE_COMPLETE").exists()

    def requests(job: pathlib.Path) -> tuple[str, str | None, str, str, str | None]:
        text = job.read_text()
        name = _group(r"#SBATCH -J nf-([A-Z_]+)", text).rstrip("_")
        opts = dict(re.findall(r"#SBATCH (-c|-t|--mem|-p) (\S+)", text))
        return name, opts.get("-c"), opts["-t"], opts["--mem"], opts.get("-p")
    jobs = [requests(j) for j in sorted(state.glob("*.sbatch"), key=lambda p: int(p.stem))]
    assert jobs == [
        # no -p: the cluster's default partition (--queue names one)
        ("STREAM_JSONL", "4", "48:00:00", "4096M", None),
        ("SORT_JSONL", "4", "24:00:00", "2560M", None),
        ("INFER_SCHEMA", "4", "24:00:00", "2560M", None),       # 16 GB, but at most --process_memory
        ("STAGE_PARQUET", "4", "24:00:00", "2560M", None),      # DuckDB out of memory: exit 75
        ("STAGE_PARQUET", "4", "30:00:00", "5120M", None),      # retry: twice memory; 48h capped at --max_time
        *[("BUILD_TABLE", "4", "30:00:00", "2560M", None)] * 6,  # one per table; 48h capped at --max_time
        ("ASSEMBLE_LAKE", "2", "24:00:00", "2560M", None),
        ("VALIDATE", "8", "24:00:00", "2560M", None),
        ("PROVENANCE", None, "24:00:00", "1024M", None),
    ], jobs
    assert all("--mail-type=FAIL" in j.read_text() for j in state.glob("*.sbatch"))

    log = (launch / ".nextflow.log").read_text()
    workdir = re.findall(r"submitted process VALIDATE.*workDir: (\S+)", log)[-1]
    assert "SLURM Job ID: " in (pathlib.Path(workdir) / ".command.log").read_text()


def test_build_table_memory_requests(tmp_path: pathlib.Path) -> None:
    """BUILD_TABLE asks for 32 GB per table (2026_03's peaked at 2.7-22 GB),
    except accession_map, the one table sorted, which gets --process_memory."""
    launch = tmp_path / "launch"
    launch.mkdir()
    (launch / "no_reports.config").write_text(
        "trace.enabled = false\nreport.enabled = false\n"
        "timeline.enabled = false\ndag.enabled = false\n")
    state = tmp_path / "slurm"
    fake = os.path.join(PROJECT_ROOT, "tests", "fake_slurm")
    env = {**_environ(), "FAKE_SLURM_DIR": str(state), "NXF_OFFLINE": "true",
           "PATH": os.pathsep.join([fake, os.path.dirname(sys.executable), os.environ["PATH"]])}
    result = subprocess.run(
        ["nextflow", "run", WORKFLOW, "-c", "no_reports.config", "-profile", "prod", "-ansi-log", "false",
         "--inputfile", SMALL_JSON_GZ, "--outdir", "out", "--release", "slurm",
         "--process_memory", "40 GB", "--expected_count", "52"],
        cwd=launch, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    memory = {}
    for job in state.glob("*.sbatch"):
        text = job.read_text()
        name = _group(r"#SBATCH -J (\S+)", text)
        if name.startswith("nf-BUILD_TABLE"):
            memory[next(t for t in TABLES if t in name)] = _group(r"#SBATCH --mem (\S+)", text)
    assert memory == {**dict.fromkeys(TABLES, "32768M"), "accession_map": "40960M"}, memory
