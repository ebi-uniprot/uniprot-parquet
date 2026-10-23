#!/usr/bin/env python3
"""
Reservoir-sample entries from a UniProtKB JSONL stream.

Reads JSONL (optionally .zst-compressed) from a path or stdin, keeps a
uniform random sample of --n lines, and writes them to stdout as JSONL.

Memory: without --total, a reservoir holds all --n sampled lines until the
input ends: 2M UniProtKB entries are 20-30 GB (the dump opens with
Swiss-Prot's large entries).  With --total, the number of matching lines in
the input (a release's entry count), selection sampling (Knuth's
Algorithm S) draws exactly --n of them in one pass and writes each as it is
chosen, in input order, in constant memory.

--where filters on entry type before sampling, using a fast substring test
on the raw line ("UniProtKB reviewed (Swiss-Prot)" / "UniProtKB unreviewed
(TrEMBL)") so the JSON is never parsed.

Built for the measurement slice (README, "Open decisions and pending work"):
    pigz -dc UniProtKB.json.gz | python3 stream_jsonl.py \\
      | python3 sample_jsonl.py --n 1000000 --where trembl --seed 1 > trembl_1m.jsonl

Usage:
    sample_jsonl.py [PATH|-] --n N [--total N] [--where trembl|swissprot|all] [--seed S]
"""

import argparse
import random
import sys
from collections.abc import Callable, Iterable, Iterator
from typing import BinaryIO

SWISSPROT_MARK = b"Swiss-Prot"
TREMBL_MARK = b"TrEMBL"
LOG_INTERVAL = 1_000_000


def _open_input(path: str | None) -> BinaryIO:
    if path in (None, "-"):
        return sys.stdin.buffer
    if path.endswith(".zst"):
        import zstandard as zstd
        return zstd.ZstdDecompressor().stream_reader(open(path, "rb"))
    return open(path, "rb")


def _line_iter(stream: BinaryIO) -> Iterator[bytes]:
    """Yield complete lines from a binary stream (works on zstd readers)."""
    buf = b""
    while True:
        chunk = stream.read(1 << 20)
        if not chunk:
            break
        buf += chunk
        *lines, buf = buf.split(b"\n")
        yield from lines
    if buf:
        yield buf


def _matches(line: bytes, where: str) -> bool:
    if where == "all":
        return True
    # entryType is near the start of every line: "entryType":"UniProtKB reviewed (Swiss-Prot)"
    head = line[:256]
    if where == "swissprot":
        return SWISSPROT_MARK in head
    return TREMBL_MARK in head


def reservoir_sample(lines: Iterable[bytes], n: int, where: str,
                     rng: random.Random) -> tuple[list[bytes], int]:
    """Algorithm R over an iterator of raw lines. Returns (sample, seen)."""
    sample: list[bytes] = []
    seen = 0
    for line in lines:
        if not line or not _matches(line, where):
            continue
        seen += 1
        if len(sample) < n:
            sample.append(line)
        else:
            j = rng.randrange(seen)
            if j < n:
                sample[j] = line
        if seen % LOG_INTERVAL == 0:
            print(f"  sample_jsonl: {seen:,} matching lines seen", file=sys.stderr)
    return sample, seen


def selection_sample(lines: Iterable[bytes], n: int, total: int, where: str,
                     rng: random.Random, write: Callable[[bytes], object]) -> tuple[int, int]:
    """Algorithm S: each of the first ``total`` matching lines is chosen with
    probability (still needed) / (still to come), so exactly ``n`` are,
    uniformly, if the input has ``total``; each goes to ``write`` as it is
    chosen.  Lines past ``total`` are counted, never chosen.  Returns
    (kept, seen)."""
    kept = seen = 0
    for line in lines:
        if not line or not _matches(line, where):
            continue
        seen += 1
        if seen <= total and rng.random() * (total - seen + 1) < n - kept:
            write(line)
            kept += 1
        if seen % LOG_INTERVAL == 0:
            print(f"  sample_jsonl: {seen:,} matching lines seen, {kept:,} kept", file=sys.stderr)
    return kept, seen


def main() -> None:
    parser = argparse.ArgumentParser(description="Reservoir-sample lines from JSONL(.zst)")
    parser.add_argument("path", nargs="?", default="-",
                        help="JSONL or JSONL.zst path; '-' or omitted reads stdin")
    parser.add_argument("--n", type=int, required=True, help="Sample size")
    parser.add_argument("--where", choices=["trembl", "swissprot", "all"], default="all",
                        help="Restrict to one review side before sampling")
    parser.add_argument("--seed", type=int, default=1, help="RNG seed (default 1)")
    parser.add_argument("--total", type=int, default=None,
                        help="Matching lines in the input (e.g. the release's entry count): draw exactly "
                             "--n in one pass, in constant memory, instead of holding a reservoir of --n lines")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out = sys.stdout.buffer
    lines = _line_iter(_open_input(args.path))
    if args.total is not None:
        kept, seen = selection_sample(lines, args.n, args.total, args.where, rng,
                                      lambda line: out.write(line + b"\n"))
    else:
        sample, seen = reservoir_sample(lines, args.n, args.where, rng)
        for line in sample:
            out.write(line)
            out.write(b"\n")
        kept = len(sample)
    out.flush()
    print(f"  sample_jsonl: DONE — kept {kept:,} of {seen:,} matching lines "
          f"(where={args.where}, seed={args.seed})", file=sys.stderr)
    if args.total is not None and seen != args.total:
        print(f"  sample_jsonl: WARNING — {seen:,} matching lines, not --total {args.total:,}: "
              + ("drew from the first --total only" if seen > args.total else f"kept {kept:,}, not --n"),
              file=sys.stderr)
    elif seen < args.n:
        print(f"  sample_jsonl: WARNING — fewer matching lines ({seen:,}) than --n ({args.n:,})",
              file=sys.stderr)


if __name__ == "__main__":
    main()
