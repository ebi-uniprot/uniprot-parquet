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


def _numbered(count: int) -> bytes:
    return b"".join(b'{"entryType":"UniProtKB unreviewed (TrEMBL)","i":%d}\n' % i for i in range(count))


def test_total_draws_exactly_n_in_input_order() -> None:
    """--total: the same contract as the reservoir, written as chosen."""
    out = _run(["--n", "37", "--total", "500", "--seed", "3"], stdin=_numbered(500))
    picked = [json.loads(ln)["i"] for ln in out.stdout.splitlines()]
    assert len(picked) == 37 and picked == sorted(set(picked))
    assert b"WARNING" not in out.stderr
    again = _run(["--n", "37", "--total", "500", "--seed", "3"], stdin=_numbered(500)).stdout
    assert again == out.stdout


def test_total_is_uniform() -> None:
    """Every line is chosen with probability n / total."""
    sys.path.insert(0, BIN_DIR)
    import random

    from sample_jsonl import selection_sample
    lines = [b'{"entryType":"UniProtKB reviewed (Swiss-Prot)","i":%d}' % i for i in range(50)]
    counts = [0] * 50
    rng = random.Random(1)
    for _ in range(4000):
        kept: list[bytes] = []
        assert selection_sample(lines, 10, 50, "all", rng, kept.append) == (10, 50)
        for line in kept:
            counts[json.loads(line)["i"]] += 1
    assert all(650 < c < 950 for c in counts), counts   # 800 expected per line


@pytest.mark.parametrize("total, kept, note", [(600, None, b"kept"), (300, 40, b"first --total")])
def test_wrong_total_warns(total: int, kept: int | None, note: bytes) -> None:
    """A --total above the input's count keeps fewer than --n; below it, only the
    first --total lines are eligible (still exactly --n)."""
    out = _run(["--n", "40", "--total", str(total)], stdin=_numbered(500))
    picked = [json.loads(ln)["i"] for ln in out.stdout.splitlines()]
    assert b"WARNING" in out.stderr and note in out.stderr
    if kept is None:
        assert len(picked) < 40
    else:
        assert len(picked) == kept and max(picked) < total
