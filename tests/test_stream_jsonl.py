"""Tests for bin/stream_jsonl.py (JSON.gz → JSONL streamer)."""

import gzip
import json
import os
import subprocess
import sys

import pytest

import stream_jsonl

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
SMALL_JSON_GZ = os.path.join(FIXTURE_DIR, "small.json.gz")


def test_streams_every_entry_and_writes_count_file(tmp_path):
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


def test_interrupt_is_fatal_and_leaves_no_count_file(tmp_path, monkeypatch, capsys):
    """SIGINT mid-stream must not fall through to the success path: no DONE,
    no count file, non-zero exit — otherwise the caller's post-hoc line-count
    check compares a truncated stream against its own truncated count."""
    count_file = tmp_path / "entry_count.txt"

    def interrupted(*args, **kwargs):
        yield {"primaryAccession": "P00001"}
        raise KeyboardInterrupt

    monkeypatch.setattr(stream_jsonl.ijson, "items", interrupted)
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

def _exact(text):
    """``text`` parsed with nothing lost: objects as (key, value) pair lists
    (key order and duplicate keys kept), non-integers as exact Decimals."""
    from decimal import Decimal
    return json.loads(text, parse_float=Decimal, object_pairs_hook=list)


def _stream(raw, *args):
    return subprocess.run([sys.executable, os.path.join(BIN_DIR, "stream_jsonl.py"), *args],
                          input=raw, capture_output=True)


def _elements(raw):
    """The source text of each element of ``raw``'s top-level results array."""
    text = raw.decode()
    decoder, i = json.JSONDecoder(), text.index('"results"')
    i = text.index("[", i) + 1
    out = []
    while True:
        while text[i] in " \t\r\n,":
            i += 1
        if text[i] == "]":
            return out
        _, j = decoder.raw_decode(text, i)
        out.append(text[i:j])
        i = j


@pytest.mark.parametrize("fixture", ["small.json.gz", "diverse.json.gz"])
def test_every_line_holds_its_element_exactly(fixture):
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
    for line, element in zip(lines, elements):
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


def test_values_are_preserved_exactly():
    result = _stream(PRESERVED)
    assert result.returncode == 0, result.stderr.decode()
    lines = result.stdout.decode().splitlines()
    elements = _elements(PRESERVED)
    assert len(lines) == 2
    for line, element in zip(lines, elements):
        assert "\n" not in line
        assert _exact(line) == _exact(element)


@pytest.mark.parametrize("element, why", [
    (b'{"primaryAccession": "P00001", "s": "\\ud800"}', "lone surrogate"),
    (b'{"primaryAccession": "P00001", "s": "x\\udc00"}', "lone surrogate"),
    (b'{"primaryAccession": "P00001", "n": 18446744073709551616}', "integer"),
    (b'{"primaryAccession": "P00001", "n": -9223372036854775809}', "integer"),
    (b'{"primaryAccession": "P00001", "n": }', "malformed"),
])
def test_data_that_cannot_be_carried_exactly_is_fatal(element, why, tmp_path):
    """Nothing is silently changed or dropped: the stream stops, exits 1 and
    writes no count file."""
    count_file = tmp_path / "entry_count.txt"
    good = b'{"primaryAccession": "P00000"}'
    result = _stream(b'{"results": [' + good + b", " + element + b"]}",
                     "--count-file", str(count_file))
    assert result.returncode == 1, why
    assert "FATAL" in result.stderr.decode(), why
    assert not count_file.exists(), why
