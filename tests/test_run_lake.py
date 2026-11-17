"""Tests for run_lake.sh's argument handling, against a stub ``nextflow``
that records its arguments instead of running the pipeline."""

import os
import pathlib
import subprocess
from collections.abc import Callable

import pytest
from conftest import PROJECT_ROOT

# run(*args, mode="small", marker=True) -> CompletedProcess, path of the recorded nextflow args
Stub = tuple[Callable[..., subprocess.CompletedProcess[str]], pathlib.Path]


@pytest.fixture
def stub(tmp_path: pathlib.Path) -> Stub:
    """(run(*args, mode="small") -> CompletedProcess, path of the recorded
    nextflow args)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    args_file = tmp_path / "nextflow_args"
    nextflow = bin_dir / "nextflow"
    # Like a successful run, it writes <outdir>/<release>/RELEASE_COMPLETE,
    # unless NO_MARKER is set (a run whose published copy failed verification).
    nextflow.write_text(f"""#!/usr/bin/env bash
printf "%s\\n" "$@" > "{args_file}"
while [[ $# -gt 0 ]]; do
    case $1 in --outdir) outdir=$2 ;; --release) release=$2 ;; esac
    shift
done
[[ -n "${{NO_MARKER:-}}" ]] || {{ mkdir -p "$outdir/$release" && touch "$outdir/$release/RELEASE_COMPLETE"; }}
""")
    nextflow.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}

    def run(*args: str, mode: str = "small", marker: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", os.path.join(PROJECT_ROOT, "run_lake.sh"), mode,
             "--outdir", str(tmp_path / "out"), *args],
            cwd=PROJECT_ROOT, env={**env, **({} if marker else {"NO_MARKER": "1"})},
            capture_output=True, text=True)
    return run, args_file


@pytest.mark.parametrize("count", ["--release", "abc", "0"])
def test_expected_count_must_be_a_whole_number(stub: Stub, count: str) -> None:
    """--expected-count $EXPECTED with EXPECTED unset takes the next flag as the count."""
    run, args_file = stub
    result = run("--expected-count", count, "--release", "rel")
    assert result.returncode != 0
    assert "must be the release's entry count" in result.stdout + result.stderr
    assert not args_file.exists(), "nextflow was launched"


def test_queue_reaches_nextflow_only_when_given(stub: Stub) -> None:
    run, args_file = stub
    assert run().returncode == 0
    assert "--queue" not in args_file.read_text().splitlines()
    assert run("--queue", "research").returncode == 0
    args = args_file.read_text().splitlines()
    assert args[args.index("--queue") + 1] == "research"


class TestInputParts:
    """--input-parts: a finished dat2jsonl output instead of a REST dump."""

    def test_checked_output_reaches_nextflow_instead_of_the_dump(self, stub: Stub, tmp_path: pathlib.Path) -> None:
        run, args_file = stub
        parts = tmp_path / "dat2jsonl"
        parts.mkdir()
        (parts / "COMPLETE").touch()
        result = run("--input-parts", str(parts), "--expected-count", "52", "--first-release", mode="full")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "(dat2jsonl parts)" in result.stdout
        args = args_file.read_text().splitlines()
        assert args[args.index("--input_parts") + 1] == str(parts)
        assert "--inputfile" not in args

    def test_output_without_complete_is_refused(self, stub: Stub, tmp_path: pathlib.Path) -> None:
        run, args_file = stub
        parts = tmp_path / "dat2jsonl"
        parts.mkdir()
        result = run("--input-parts", str(parts))
        assert result.returncode != 0
        assert "COMPLETE is missing" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    def test_missing_directory_is_refused(self, stub: Stub, tmp_path: pathlib.Path) -> None:
        run, args_file = stub
        result = run("--input-parts", str(tmp_path / "nowhere"))
        assert result.returncode != 0
        assert "dat2jsonl output not found" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    def test_with_input_is_refused(self, stub: Stub, tmp_path: pathlib.Path) -> None:
        run, args_file = stub
        result = run("--input-parts", str(tmp_path), "--input", "tests/fixtures/small.json.gz")
        assert result.returncode != 0
        assert "give one" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"


class TestInputDat:
    """--input-dat: the release's flat files, converted in the pipeline."""

    @pytest.fixture
    def release(self, tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
        """(release directory, gaf-eco-mapping.txt, jar): stand-ins, enough for the checks."""
        src = tmp_path / "uniprot"
        src.mkdir()
        gaf, jar = tmp_path / "gaf-eco-mapping.txt", tmp_path / "dat2jsonl.jar"
        gaf.touch()
        jar.touch()
        return src, gaf, jar

    def test_reaches_nextflow_with_its_files(self, stub: Stub, release: tuple[pathlib.Path, ...]) -> None:
        run, args_file = stub
        src, gaf, jar = release
        result = run("--input-dat", str(src), "--gaf-eco", str(gaf), "--dat2jsonl-jar", str(jar),
                     "--expected-count", "52", "--first-release", mode="full")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "(flat files, dat2jsonl here)" in result.stdout
        args = args_file.read_text().splitlines()
        assert args[args.index("--input_dat") + 1] == str(src)
        assert args[args.index("--gaf_eco") + 1] == str(gaf)
        assert args[args.index("--dat2jsonl_jar") + 1] == str(jar)
        assert "--inputfile" not in args and "--input_parts" not in args

    def test_without_gaf_eco_is_refused(self, stub: Stub, release: tuple[pathlib.Path, ...]) -> None:
        run, args_file = stub
        src, _, jar = release
        result = run("--input-dat", str(src), "--dat2jsonl-jar", str(jar))
        assert result.returncode != 0
        assert "needs --gaf-eco" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    def test_missing_jar_is_refused(self, stub: Stub, release: tuple[pathlib.Path, ...]) -> None:
        run, args_file = stub
        src, gaf, jar = release
        result = run("--input-dat", str(src), "--gaf-eco", str(gaf), "--dat2jsonl-jar", str(jar.with_name("no.jar")))
        assert result.returncode != 0
        assert "jar not found" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    def test_missing_directory_is_refused(self, stub: Stub, release: tuple[pathlib.Path, ...]) -> None:
        run, args_file = stub
        src, gaf, jar = release
        result = run("--input-dat", str(src / "nowhere"), "--gaf-eco", str(gaf), "--dat2jsonl-jar", str(jar))
        assert result.returncode != 0
        assert "Release directory not found" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    @pytest.mark.parametrize("other", [("--input", "tests/fixtures/small.json.gz"), ("--input-parts", "x")])
    def test_with_another_input_is_refused(self, stub: Stub, release: tuple[pathlib.Path, ...],
                                           other: tuple[str, str]) -> None:
        run, args_file = stub
        src, gaf, _ = release
        result = run("--input-dat", str(src), "--gaf-eco", str(gaf), *other)
        assert result.returncode != 0
        assert "give one" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    def test_gaf_eco_without_input_dat_is_refused(self, stub: Stub, release: tuple[pathlib.Path, ...]) -> None:
        run, args_file = stub
        result = run("--gaf-eco", str(release[1]))
        assert result.returncode != 0
        assert "go with --input-dat" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"


class TestPreviousManifest:
    def test_missing_file_is_refused_by_name(self, stub: Stub) -> None:
        run, args_file = stub
        result = run("--previous-manifest", "datalake/2026_3/lake/manifest.json")
        assert result.returncode != 0
        assert "datalake/2026_3/lake/manifest.json" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    def test_existing_file_reaches_nextflow(self, stub: Stub) -> None:
        run, args_file = stub
        manifest = "tests/fixtures/small.json.gz"   # any existing file, relative to the launch dir
        result = run("--previous-manifest", manifest)
        assert result.returncode == 0, result.stdout + result.stderr
        args = args_file.read_text().splitlines()
        passed = args[args.index("--previous_manifest") + 1]
        assert os.path.samefile(os.path.join(PROJECT_ROOT, passed), os.path.join(PROJECT_ROOT, manifest))

    def test_manifest_of_the_release_being_built_is_refused(self, stub: Stub, tmp_path: pathlib.Path) -> None:
        """The run republishes that manifest while VALIDATE reads it, so the
        schema diff would compare the release with itself."""
        run, args_file = stub
        manifest = tmp_path / "out" / "rel" / "lake" / "manifest.json"
        manifest.parent.mkdir(parents=True)
        manifest.write_text("{}")
        result = run("--release", "rel", "--previous-manifest", str(manifest))
        assert result.returncode != 0
        assert "inside the release being built" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    def test_manifest_of_another_release_in_outdir_is_accepted(self, stub: Stub, tmp_path: pathlib.Path) -> None:
        run, _ = stub
        manifest = tmp_path / "out" / "rel_2" / "lake" / "manifest.json"   # shares a prefix with "rel"
        manifest.parent.mkdir(parents=True)
        manifest.write_text("{}")
        result = run("--release", "rel", "--previous-manifest", str(manifest))
        assert result.returncode == 0, result.stdout + result.stderr


class TestFullModeSchemaDiff:
    """A full release's report always compares its schema with the previous
    release's (README §2), unless it is declared the first release."""

    FULL = ("--expected-count", "52", "--input", "tests/fixtures/small.json.gz")

    def test_refused_without_previous_manifest(self, stub: Stub) -> None:
        run, args_file = stub
        result = run(*self.FULL, mode="full")
        assert result.returncode != 0
        assert "--previous-manifest" in result.stdout + result.stderr
        assert "--first-release" in result.stdout + result.stderr
        assert not args_file.exists(), "nextflow was launched"

    def test_first_release_launches_without_diff(self, stub: Stub) -> None:
        run, args_file = stub
        result = run(*self.FULL, "--first-release", mode="full")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "--previous_manifest" not in args_file.read_text().splitlines()

    def test_previous_manifest_launches(self, stub: Stub) -> None:
        run, args_file = stub
        result = run(*self.FULL, "--previous-manifest", "tests/fixtures/small.json.gz", mode="full")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "--previous_manifest" in args_file.read_text().splitlines()

    def test_both_flags_are_refused(self, stub: Stub) -> None:
        run, args_file = stub
        result = run(*self.FULL, "--first-release",
                     "--previous-manifest", "tests/fixtures/small.json.gz", mode="full")
        assert result.returncode != 0
        assert not args_file.exists(), "nextflow was launched"

    def test_small_mode_needs_neither(self, stub: Stub) -> None:
        run, _ = stub
        assert run().returncode == 0


class TestReleaseMarker:
    def test_success_needs_the_marker(self, stub: Stub) -> None:
        """Nextflow exits 0 when the published copy fails verification (the
        check runs in onComplete): run_lake.sh goes by RELEASE_COMPLETE."""
        run, _ = stub
        result = run(marker=False)
        assert result.returncode != 0
        assert "RELEASE_COMPLETE" in result.stdout + result.stderr

    def test_marker_means_success(self, stub: Stub) -> None:
        run, _ = stub
        result = run()
        assert result.returncode == 0, result.stdout + result.stderr


def test_max_time_reaches_nextflow(stub: Stub) -> None:
    """A queue whose wall-time limit is under 72h needs a lower cap for the
    retries' doubled time."""
    run, args_file = stub
    result = run("--max-time", "48h")
    assert result.returncode == 0, result.stdout + result.stderr
    args = args_file.read_text().splitlines()
    assert args[args.index("--max_time") + 1] == "48h"


def _header_examples(mode: str) -> list[list[str]]:
    """The ``./run_lake.sh <mode> ...`` usage lines of run_lake.sh's header,
    as argument lists (lines abbreviated with '...' excluded)."""
    import shlex
    with open(os.path.join(PROJECT_ROOT, "run_lake.sh")) as f:
        header = [line.lstrip("#").split("#")[0].strip() for line in f
                  if line.startswith("#   ./run_lake.sh")]
    return [shlex.split(line)[1:] for line in header
            if line.split()[1:2] == [mode] and "..." not in line]


@pytest.mark.parametrize("example", _header_examples("full"), ids=" ".join)
def test_full_mode_header_examples_launch(example: list[str], stub: Stub, tmp_path: pathlib.Path) -> None:
    """Each full-mode example in the header works as written, from a
    directory holding the dump and the previous release under the default
    outdir (datalake/)."""
    _, args_file = stub
    cwd = tmp_path / "launch"
    (cwd / "datalake" / "2026_03" / "lake").mkdir(parents=True)
    (cwd / "datalake" / "2026_03" / "lake" / "manifest.json").write_text("{}")
    (cwd / "UniProtKB.json.gz").write_bytes(b"")
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}"}
    result = subprocess.run(["bash", os.path.join(PROJECT_ROOT, "run_lake.sh"), *example],
                            cwd=cwd, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert args_file.exists(), "nextflow was not launched"
