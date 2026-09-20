"""Tests for bin/stream_jsonl.py (JSON.gz → JSONL streamer)."""

import gzip
import os
import subprocess
import sys

import pytest

import stream_jsonl

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
SMALL_JSON_GZ = os.path.join(os.path.dirname(__file__), "fixtures", "small.json.gz")


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
