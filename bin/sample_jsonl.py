#!/usr/bin/env python3
"""
Reservoir-sample entries from a UniProtKB JSONL stream.

Reads JSONL (optionally .zst-compressed) from a path or stdin, keeps a
uniform random sample of --n lines, and writes them to stdout as JSONL.
Streams: never holds more than --n lines in memory.

--where filters on entry type before sampling, using a fast substring test
on the raw line ("UniProtKB reviewed (Swiss-Prot)" / "UniProtKB unreviewed
(TrEMBL)") so the JSON is never parsed.

Built for the F.1 measurement slice (PLAN_SCHEMA_V2.md):
    pigz -dc UniProtKB.json.gz | python3 stream_jsonl.py \\
      | python3 sample_jsonl.py --n 1000000 --where trembl --seed 1 > trembl_1m.jsonl

Usage:
    sample_jsonl.py [PATH|-] --n N [--where trembl|swissprot|all] [--seed S]
"""

import sys
import random
import argparse

SWISSPROT_MARK = b"Swiss-Prot"
TREMBL_MARK = b"TrEMBL"
LOG_INTERVAL = 1_000_000


def _open_input(path):
    if path in (None, "-"):
        return sys.stdin.buffer
    if path.endswith(".zst"):
        import zstandard as zstd
        return zstd.ZstdDecompressor().stream_reader(open(path, "rb"))
    return open(path, "rb")


def _line_iter(stream):
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


def _matches(line, where):
    if where == "all":
        return True
    # entryType is near the start of every line: "entryType":"UniProtKB reviewed (Swiss-Prot)"
    head = line[:256]
    if where == "swissprot":
        return SWISSPROT_MARK in head
    return TREMBL_MARK in head


def reservoir_sample(lines, n, where, rng):
    """Algorithm R over an iterator of raw lines. Returns (sample, seen)."""
    sample = []
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


def main():
    parser = argparse.ArgumentParser(description="Reservoir-sample lines from JSONL(.zst)")
    parser.add_argument("path", nargs="?", default="-",
                        help="JSONL or JSONL.zst path; '-' or omitted reads stdin")
    parser.add_argument("--n", type=int, required=True, help="Sample size")
    parser.add_argument("--where", choices=["trembl", "swissprot", "all"], default="all",
                        help="Restrict to one review side before sampling")
    parser.add_argument("--seed", type=int, default=1, help="RNG seed (default 1)")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    sample, seen = reservoir_sample(_line_iter(_open_input(args.path)), args.n, args.where, rng)
    out = sys.stdout.buffer
    for line in sample:
        out.write(line)
        out.write(b"\n")
    out.flush()
    print(f"  sample_jsonl: DONE — kept {len(sample):,} of {seen:,} matching lines "
          f"(where={args.where}, seed={args.seed})", file=sys.stderr)
    if seen < args.n:
        print(f"  sample_jsonl: WARNING — fewer matching lines ({seen:,}) than --n ({args.n:,})",
              file=sys.stderr)


if __name__ == "__main__":
    main()
