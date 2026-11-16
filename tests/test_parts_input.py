"""bin/parts_input.py: a dat2jsonl output as FROM_PARTS's input.  It must
refuse an output whose check has not passed, give the parts in plan order,
and refuse a joined JSONL whose line count is not the parts' own or the
expected one."""

import json
import os
import pathlib
import subprocess

import zstandard as zstd
from conftest import SMALL_JSON_GZ, make_parts_output, run_bin


def _join(outdir: pathlib.Path, dst: pathlib.Path) -> int:
    """FROM_PARTS's join: the listed parts concatenated; the lines in them."""
    listed = run_bin("parts_input.py", "parts", outdir)
    assert listed.returncode == 0, listed.stderr
    with open(dst, "wb") as out:
        for path in listed.stdout.splitlines():
            out.write(pathlib.Path(path).read_bytes())
    with open(dst, "rb") as f:
        return sum(1 for _ in zstd.ZstdDecompressor().stream_reader(f).read().splitlines())


def test_parts_in_plan_order_join_to_the_entries(tmp_path: pathlib.Path) -> None:
    lines = make_parts_output(SMALL_JSON_GZ, str(tmp_path / "out"), n_parts=3)
    listed = run_bin("parts_input.py", "parts", tmp_path / "out").stdout.splitlines()
    assert [os.path.basename(p) for p in listed] == [f"part-0000{i}.jsonl.zst" for i in (1, 2, 3)]
    joined = tmp_path / "uniprot.jsonl.zst"
    _join(tmp_path / "out", joined)
    # zstd frames concatenate: one stream, the lines in order
    with open(joined, "rb") as f:
        assert zstd.ZstdDecompressor().stream_reader(f).read() == b"".join(lines)


def test_manifest_records_the_source(tmp_path: pathlib.Path) -> None:
    lines = make_parts_output(SMALL_JSON_GZ, str(tmp_path / "out"))
    joined = tmp_path / "uniprot.jsonl.zst"
    n = _join(tmp_path / "out", joined)
    result = run_bin("parts_input.py", "manifest", tmp_path / "out", "--jsonl", joined,
                     "--entry-count", n, "--expected-count", len(lines), "-o", tmp_path / "m.json")
    assert result.returncode == 0, result.stderr
    m = json.loads((tmp_path / "m.json").read_text())
    import hashlib
    assert m["kind"] == "dat2jsonl"
    assert m["entry_count"] == m["expected_count"] == len(lines) == 52
    assert m["jsonl_sha256"] == hashlib.sha256(joined.read_bytes()).hexdigest()
    assert m["parts"] == 2
    assert m["dat"] == [{"path": os.path.abspath(SMALL_JSON_GZ), "entries": 52, "parts": 2,
                         "size_bytes": os.path.getsize(SMALL_JSON_GZ)}]
    assert "uniprot-core: 1.0.43" in m["inputs"] and m["check"][-1] == "CHECK PASSED"


def _refused(result: subprocess.CompletedProcess[str], why: str) -> None:
    assert result.returncode == 1 and why in result.stderr, result.stderr


def test_output_without_complete_is_refused(tmp_path: pathlib.Path) -> None:
    make_parts_output(SMALL_JSON_GZ, str(tmp_path / "out"), complete=False)
    _refused(run_bin("parts_input.py", "parts", tmp_path / "out"), "COMPLETE is missing")


def test_failed_check_is_refused(tmp_path: pathlib.Path) -> None:
    make_parts_output(SMALL_JSON_GZ, str(tmp_path / "out"))
    (tmp_path / "out" / "CHECK.txt").write_text("GAP x: ...\nCHECK FAILED\n")
    _refused(run_bin("parts_input.py", "parts", tmp_path / "out"), "does not end with CHECK PASSED")


def test_missing_part_is_refused(tmp_path: pathlib.Path) -> None:
    make_parts_output(SMALL_JSON_GZ, str(tmp_path / "out"))
    (tmp_path / "out" / "parts" / "part-00002.jsonl.zst").unlink()
    _refused(run_bin("parts_input.py", "parts", tmp_path / "out"), "part-00002")


def test_not_a_dat2jsonl_output_is_refused(tmp_path: pathlib.Path) -> None:
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "COMPLETE").touch()
    (tmp_path / "out" / "CHECK.txt").write_text("CHECK PASSED\n")
    _refused(run_bin("parts_input.py", "parts", tmp_path / "out"), "no plan.tsv")


def test_count_unlike_the_parts_or_the_expected_one_is_refused(tmp_path: pathlib.Path) -> None:
    make_parts_output(SMALL_JSON_GZ, str(tmp_path / "out"))
    joined = tmp_path / "uniprot.jsonl.zst"
    n = _join(tmp_path / "out", joined)
    args = ("manifest", tmp_path / "out", "--jsonl", joined, "-o", tmp_path / "m.json")
    _refused(run_bin("parts_input.py", *args, "--entry-count", n - 1), "their stats say 52")
    _refused(run_bin("parts_input.py", *args, "--entry-count", n, "--expected-count", n + 1), "expected 53")
    assert not (tmp_path / "m.json").exists()
