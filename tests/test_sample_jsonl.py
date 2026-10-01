"""Tests for bin/sample_jsonl.py (the measurement-slice sampler)."""

import json
import os
import subprocess
import sys

import pytest

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
SCRIPT = os.path.join(BIN_DIR, "sample_jsonl.py")


def _run(args: list[str], stdin: bytes | None = None) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run([sys.executable, SCRIPT, *args], input=stdin,
                          capture_output=True, check=True)


@pytest.fixture(scope="module")
def sides(small_jsonl: str) -> tuple[set[str], set[str]]:
    """(reviewed accs, unreviewed accs) from the fixture JSONL."""
    import io

    import zstandard as zstd
    reviewed: set[str] = set()
    unreviewed: set[str] = set()
    with open(small_jsonl, "rb") as f:
        for line in io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(f), encoding="utf-8"):
            e = json.loads(line)
            (reviewed if "Swiss-Prot" in e["entryType"] else unreviewed).add(e["primaryAccession"])
    return reviewed, unreviewed


def _accs(out: bytes) -> list[str]:
    return [json.loads(ln)["primaryAccession"] for ln in out.decode().splitlines()]


def test_exact_n_from_zst_path(small_jsonl: str, sides: tuple[set[str], set[str]]) -> None:
    out = _run([small_jsonl, "--n", "50"]).stdout
    accs = _accs(out)
    assert len(accs) == 50 and len(set(accs)) == 50
    assert set(accs) <= sides[0] | sides[1]


def test_where_filters_before_sampling(small_jsonl: str, sides: tuple[set[str], set[str]]) -> None:
    reviewed, unreviewed = sides
    sp = _accs(_run([small_jsonl, "--n", "30", "--where", "swissprot"]).stdout)
    tr = _accs(_run([small_jsonl, "--n", "30", "--where", "trembl"]).stdout)
    assert sp and set(sp) <= reviewed
    assert tr and set(tr) <= unreviewed


def test_seed_is_deterministic(small_jsonl: str) -> None:
    a = _run([small_jsonl, "--n", "20", "--seed", "7"]).stdout
    b = _run([small_jsonl, "--n", "20", "--seed", "7"]).stdout
    c = _run([small_jsonl, "--n", "20", "--seed", "8"]).stdout
    assert a == b and a != c


def test_reads_plain_jsonl_from_stdin() -> None:
    raw = b"".join(b'{"entryType":"UniProtKB unreviewed (TrEMBL)","i":%d}\n' % i for i in range(500))
    out = _run(["--n", "510"], stdin=raw)
    assert len(out.stdout.splitlines()) == 500
    assert b"WARNING" in out.stderr  # fewer lines than --n


def test_reservoir_never_exceeds_n() -> None:
    sys.path.insert(0, BIN_DIR)
    import random

    from sample_jsonl import reservoir_sample
    lines = (b'{"entryType":"UniProtKB reviewed (Swiss-Prot)","i":%d}' % i for i in range(10_000))
    sample, seen = reservoir_sample(lines, 100, "swissprot", random.Random(1))
    assert seen == 10_000 and len(sample) == 100 and len(set(sample)) == 100
