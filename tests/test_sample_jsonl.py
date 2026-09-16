"""Tests for bin/sample_jsonl.py (the F.1 slice sampler)."""

import os
import sys
import json
import subprocess

import pytest

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
SCRIPT = os.path.join(BIN_DIR, "sample_jsonl.py")


def _run(args, stdin=None):
    return subprocess.run([sys.executable, SCRIPT, *args], input=stdin,
                          capture_output=True, check=True)


@pytest.fixture(scope="module")
def sides(small_jsonl):
    """(reviewed accs, unreviewed accs) from the fixture JSONL."""
    import zstandard as zstd
    reviewed, unreviewed = set(), set()
    with open(small_jsonl, "rb") as f:
        text = zstd.ZstdDecompressor().stream_reader(f).read().decode()
    for line in text.splitlines():
        e = json.loads(line)
        (reviewed if "Swiss-Prot" in e["entryType"] else unreviewed).add(e["primaryAccession"])
    return reviewed, unreviewed


def _accs(out):
    return [json.loads(l)["primaryAccession"] for l in out.decode().splitlines()]


def test_exact_n_from_zst_path(small_jsonl, sides):
    out = _run([small_jsonl, "--n", "50"]).stdout
    accs = _accs(out)
    assert len(accs) == 50 and len(set(accs)) == 50
    assert set(accs) <= sides[0] | sides[1]


def test_where_filters_before_sampling(small_jsonl, sides):
    reviewed, unreviewed = sides
    sp = _accs(_run([small_jsonl, "--n", "30", "--where", "swissprot"]).stdout)
    tr = _accs(_run([small_jsonl, "--n", "30", "--where", "trembl"]).stdout)
    assert sp and set(sp) <= reviewed
    assert tr and set(tr) <= unreviewed


def test_seed_is_deterministic(small_jsonl):
    a = _run([small_jsonl, "--n", "20", "--seed", "7"]).stdout
    b = _run([small_jsonl, "--n", "20", "--seed", "7"]).stdout
    c = _run([small_jsonl, "--n", "20", "--seed", "8"]).stdout
    assert a == b and a != c


def test_reads_plain_jsonl_from_stdin(small_jsonl):
    import zstandard as zstd
    with open(small_jsonl, "rb") as f:
        raw = zstd.ZstdDecompressor().stream_reader(f).read()
    n_lines = raw.count(b"\n")
    out = _run(["--n", str(n_lines + 10)], stdin=raw)
    assert len(out.stdout.splitlines()) == n_lines
    assert b"WARNING" in out.stderr  # fewer lines than --n


def test_reservoir_never_exceeds_n():
    sys.path.insert(0, BIN_DIR)
    import random
    from sample_jsonl import reservoir_sample
    lines = (b'{"entryType":"UniProtKB reviewed (Swiss-Prot)","i":%d}' % i for i in range(10_000))
    sample, seen = reservoir_sample(lines, 100, "swissprot", random.Random(1))
    assert seen == 10_000 and len(sample) == 100 and len(set(sample)) == 100
