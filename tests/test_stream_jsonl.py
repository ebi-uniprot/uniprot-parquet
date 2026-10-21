"""Tests for bin/stream_jsonl.py (JSON.gz → JSONL streamer)."""

import gzip
import json
import os
import pathlib
import subprocess
import sys
from collections.abc import Iterator
from typing import Any

import pytest
import stream_jsonl

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
SMALL_JSON_GZ = os.path.join(FIXTURE_DIR, "small.json.gz")


def test_streams_every_entry_and_writes_count_file(tmp_path: pathlib.Path) -> None:
    count_file = tmp_path / "entry_count.txt"
    with gzip.open(SMALL_JSON_GZ, "rb") as f:
        raw = f.read()
    result = subprocess.run(
        [sys.executable, os.path.join(BIN_DIR, "stream_jsonl.py"), "--count-file", str(count_file)],
        input=raw, capture_output=True,
    )
    assert result.returncode == 0, result.stderr.decode()
    lines = result.stdout.splitlines()
    assert len(lines) == 52
    assert count_file.read_text() == "52"


def test_input_manifest_identifies_the_source_file(tmp_path: pathlib.Path) -> None:
    """--source names the file the stream was decompressed from: its name,
    size and SHA-256 (hashed while the stream is read) go to the
    --input-manifest sidecar, with the counts."""
    import hashlib
    manifest = tmp_path / "input_manifest.json"
    with gzip.open(SMALL_JSON_GZ, "rb") as f:
        raw = f.read()
    result = subprocess.run(
        [sys.executable, os.path.join(BIN_DIR, "stream_jsonl.py"), "--expected-count", "52",
         "--source", SMALL_JSON_GZ, "--input-manifest", str(manifest)],
        input=raw, capture_output=True,
    )
    assert result.returncode == 0, result.stderr.decode()
    with open(SMALL_JSON_GZ, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    assert json.loads(manifest.read_text()) == {
        "filename": "small.json.gz", "size_bytes": os.path.getsize(SMALL_JSON_GZ),
        "sha256": digest, "entry_count": 52, "expected_count": 52}


def _stream_without_c_backend(raw: bytes, *args: str) -> subprocess.CompletedProcess[bytes]:
    """stream_jsonl.py run as if ijson's yajl2_c could not be imported."""
    script = os.path.join(BIN_DIR, "stream_jsonl.py")
    code = ("import runpy, sys; sys.modules['ijson.backends.yajl2_c'] = None; "
            f"sys.argv = [{script!r}, *sys.argv[1:]]; runpy.run_path({script!r}, run_name='__main__')")
    return subprocess.run([sys.executable, "-c", code, *args], input=raw, capture_output=True)


def test_pure_python_ijson_is_refused_unless_allowed(tmp_path: pathlib.Path) -> None:
    """Without the C backend the parse is ~11x slower: refused at once (no
    output, no count file) unless --allow-pure-python, which gives the same lines."""
    with gzip.open(SMALL_JSON_GZ, "rb") as f:
        raw = f.read()
    count_file = tmp_path / "entry_count.txt"
    refused = _stream_without_c_backend(raw, "--count-file", str(count_file))
    assert refused.returncode == 1
    assert "C backend (yajl2_c) is not available" in refused.stderr.decode()
    assert refused.stdout == b"" and not count_file.exists()

    allowed = _stream_without_c_backend(raw, "--allow-pure-python", "--count-file", str(count_file))
    assert allowed.returncode == 0, allowed.stderr.decode()
    assert "pure-Python backend" in allowed.stderr.decode()
    assert allowed.stdout == _stream(raw).stdout
    assert count_file.read_text() == "52"


def test_interrupt_is_fatal_and_leaves_no_count_file(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch,
                                                     capsys: pytest.CaptureFixture[str]) -> None:
    """SIGINT mid-stream must not fall through to the success path: no DONE,
    no count file, non-zero exit — otherwise the caller's post-hoc line-count
    check compares a truncated stream against its own truncated count."""
    count_file = tmp_path / "entry_count.txt"

    def interrupted(*args: Any, **kwargs: Any) -> Iterator[dict[str, str]]:
        yield {"primaryAccession": "P00001"}
        raise KeyboardInterrupt

    # stream_jsonl's ijson (the yajl2_c backend when it is there), by import path
    monkeypatch.setattr("stream_jsonl.ijson.items", interrupted)
    monkeypatch.setattr(stream_jsonl, "WRITE", lambda b: None)
    monkeypatch.setattr(sys, "argv", ["stream_jsonl.py", "--count-file", str(count_file)])

    with pytest.raises(SystemExit) as exc:
        stream_jsonl.main()
    assert exc.value.code != 0
    assert not count_file.exists()
    err = capsys.readouterr().err
    assert "interrupted after 1 entries" in err
    assert "DONE" not in err


# ─── Data preservation: each line holds its results element's data exactly ──

def _exact(text: str) -> Any:
    """``text`` parsed with nothing lost: objects as (key, value) pair lists
    (key order and duplicate keys kept), numbers tagged with their JSON kind
    so an integer never equals a decimal of the same value (2 vs 2.0), and
    non-integers as exact Decimals (1.50 equals 1.5: formatting may change)."""
    from decimal import Decimal
    return json.loads(text, parse_int=lambda s: ("int", int(s)),
                      parse_float=lambda s: ("num", Decimal(s)), object_pairs_hook=list)


def _stream(raw: bytes, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run([sys.executable, os.path.join(BIN_DIR, "stream_jsonl.py"), *args],
                          input=raw, capture_output=True)


def _elements(raw: bytes) -> list[str]:
    """The source text of each element of ``raw``'s top-level results array."""
    text = raw.decode()
    decoder, i = json.JSONDecoder(), text.index('"results"')
    i = text.index("[", i) + 1
    out: list[str] = []
    while True:
        while text[i] in " \t\r\n,":
            i += 1
        if text[i] == "]":
            return out
        _, j = decoder.raw_decode(text, i)
        out.append(text[i:j])
        i = j


@pytest.mark.parametrize("fixture", ["small.json.gz", "diverse.json.gz"])
def test_every_line_holds_its_element_exactly(fixture: str) -> None:
    path = os.path.join(FIXTURE_DIR, fixture)
    if not os.path.exists(path):
        pytest.skip(f"{fixture} not fetched")
    with gzip.open(path, "rb") as f:
        raw = f.read()
    result = _stream(raw)
    assert result.returncode == 0, result.stderr.decode()
    lines = result.stdout.decode().splitlines()
    elements = _elements(raw)
    assert len(lines) == len(elements)
    for line, element in zip(lines, elements, strict=True):
        assert _exact(line) == _exact(element)


# Values UniProt's JSON could carry that a parse → serialise round trip can
# change: number text, escapes, empty objects, layout.
PRESERVED = b"""{"meta": {"n": 2}, "results": [
  {"primaryAccession": "P00001", "a": 1.50, "b": 0.12345678901234567890123,
   "c": 1e5, "d": -0.0, "e": 2, "f": 9223372036854775807, "u": 18446744073709551615,
   "s": "caf\\u00e9 a\\/b \\ud83d\\ude00", "g": [{}, {"k": 1}], "h": {}},
  {
    "primaryAccession": "P00002",
    "nested": {"x": [1.0, 2.25, 1E-7]}
  }
], "tail": true}"""


@pytest.mark.parametrize("original, rewritten", [
    ('{"e": 2}', '{"e": 2.0}'),
    ('{"c": 100000}', '{"c": 1E+5}'),
    ('{"d": 0}', '{"d": -0.0}'),
    ('{"g": [{"k": 1}]}', '{"g": [{"k": 1.0}]}'),
])
def test_exact_tells_an_integer_from_a_decimal(original: str, rewritten: str) -> None:
    """2 and 2.0 are equal Python numbers but different JSON data: a stream
    that rewrote integers as decimals must fail the preservation tests."""
    assert _exact(original) != _exact(rewritten)


def test_values_are_preserved_exactly() -> None:
    result = _stream(PRESERVED)
    assert result.returncode == 0, result.stderr.decode()
    lines = result.stdout.decode().splitlines()
    elements = _elements(PRESERVED)
    assert len(lines) == 2
    for line, element in zip(lines, elements, strict=True):
        assert "\n" not in line
        assert _exact(line) == _exact(element)


@pytest.mark.parametrize("element, why", [
    (b'{"primaryAccession": "P00001", "s": "\\ud800"}', "lone surrogate"),
    (b'{"primaryAccession": "P00001", "s": "x\\udc00"}', "lone surrogate"),
    (b'{"primaryAccession": "P00001", "n": 18446744073709551616}', "integer"),
    (b'{"primaryAccession": "P00001", "n": -9223372036854775809}', "integer"),
    (b'{"primaryAccession": "P00001", "n": }', "malformed"),
])
def test_data_that_cannot_be_carried_exactly_is_fatal(element: bytes, why: str, tmp_path: pathlib.Path) -> None:
    """Nothing is silently changed or dropped: the stream stops, exits 1 and
    writes no count file."""
    count_file = tmp_path / "entry_count.txt"
    good = b'{"primaryAccession": "P00000"}'
    result = _stream(b'{"results": [' + good + b", " + element + b"]}",
                     "--count-file", str(count_file))
    assert result.returncode == 1, why
    assert "FATAL" in result.stderr.decode(), why
    assert not count_file.exists(), why
