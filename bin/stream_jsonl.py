#!/usr/bin/env python3
"""
Streaming UniProtKB JSON → JSONL converter.

Reads a UniProtKB JSON dump from stdin (typically piped from pigz -dc),
extracts each entry from the "results" array via streaming (ijson), and
writes one JSON line per entry to stdout (via orjson).

Each line holds its entry's data exactly: the same keys in the same order,
strings unchanged, and every number with its exact value (a non-integer is
parsed as a Decimal and written with its own digits, not rounded through a
double).  Only the formatting changes: one compact line, raw UTF-8 rather
than \\u escapes, and an exponent written in Decimal's notation (1e5 as
1E+5, the same value).

Integrity checks (each exits 1 at once, with no count file):
  - Input the JSONL cannot carry exactly: a lone surrogate escape (the
    parser would turn it into '?'), an integer orjson cannot write (below
    -2**63 or above 2**64 - 1), malformed JSON.

Not checked: a key repeated in one object, of which only the last value
is kept.  UniProt's serialiser does not produce them, and refusing them
costs a Python call per key (~40% of this step's time).
  - If --expected-count is provided, the output count must match exactly.
  - If --count-file is provided, the final count is written to that file
    so the caller can verify end-to-end (e.g. against zstd output lines).
  - If --source and --input-manifest are provided, the source file's name,
    size and SHA-256 (hashed in a thread while the stream is read), the
    entry count and --expected-count go to that JSON sidecar, which
    provenance.json records: the release names the dump it was built from.

Usage (standalone):
    pigz -dc input.json.gz | python3 stream_jsonl.py | zstd -3 -T0 -o output.jsonl.zst

Usage (with count assertion):
    pigz -dc input.json.gz | python3 stream_jsonl.py --expected-count 248799253 | ...

Usage (with sidecar count file for post-hoc verification):
    pigz -dc input.json.gz | python3 stream_jsonl.py --count-file entry_count.txt | ...
"""

import argparse
import hashlib
import json
import os
import re
import sys
import threading
import time
from collections.abc import Callable
from decimal import Decimal
from typing import BinaryIO

try:
    import ijson.backends.yajl2_c as ijson
except ImportError:
    import ijson
    print("Warning: yajl2_c backend not found; falling back to pure Python ijson.",
          file=sys.stderr)

import orjson

DUMPS = orjson.dumps
OPT = orjson.OPT_APPEND_NEWLINE
WRITE = sys.stdout.buffer.write
LOG_INTERVAL = 1_000_000  # log progress every N entries


def _exact_number(value: object) -> orjson.Fragment:
    """orjson default: a Decimal (ijson, use_float=False) written with its
    own digits."""
    if isinstance(value, Decimal):
        return orjson.Fragment(str(value))
    raise TypeError(f"Type is not JSON serializable: {type(value).__name__}")


# A surrogate escape, high (\uD800-\uDBFF) or low (\uDC00-\uDFFF).  Rare,
# so each match is judged in Python (_SurrogateGuard._lone).
_SURROGATE_ESCAPE = re.compile(rb"\\u[dD][89a-fA-F][0-9a-fA-F]{2}")
_LOW_ESCAPE = re.compile(rb"\\u[dD][c-fC-F][0-9a-fA-F]{2}")


class LoneSurrogate(ValueError):
    pass


class _SurrogateGuard:
    """A binary stream that refuses lone surrogate escapes, which yajl
    decodes to '?' (and orjson could not write).  Each chunk is scanned with
    the tail of the one before, so an escape split between reads is seen;
    an escape too close to the end to judge is judged on the next read."""

    CONTEXT = 64        # bytes of the previous chunk kept for the next scan
    LOOKAHEAD = 12      # a high surrogate is judged with the escape after it

    def __init__(self, raw: BinaryIO) -> None:
        self.raw = raw
        self.tail = b""
        self.tail_offset = 0      # stream offset of tail[0]
        self.judged = 0           # escapes starting before this offset were judged
        self.paired_low = -1      # stream offset of the low escape after a judged high

    @staticmethod
    def _is_escape(buf: bytes, i: int) -> bool:
        """True if the backslash at buf[i] starts an escape (is not itself
        escaped by an odd run of backslashes before it)."""
        run = 0
        while i - run - 1 >= 0 and buf[i - run - 1] == 0x5C:
            run += 1
        return run % 2 == 0

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        chunk = self.raw.read(n)
        buf = self.tail + chunk
        limit = len(buf) if not chunk else len(buf) - self.LOOKAHEAD
        for m in _SURROGATE_ESCAPE.finditer(buf):
            i = m.start()
            at = self.tail_offset + i
            if (i >= limit or at < self.judged or at == self.paired_low
                    or not self._is_escape(buf, i)):
                continue
            if buf[i + 3] in b"89abAB" and _LOW_ESCAPE.match(buf, i + 6):
                self.paired_low = at + 6
                continue
            raise LoneSurrogate(f"lone surrogate escape {m.group().decode()!r} at byte {at:,}")
        self.judged = max(self.judged, self.tail_offset + limit)
        keep = min(len(buf), self.CONTEXT)
        self.tail_offset += len(buf) - keep
        self.tail = buf[len(buf) - keep:]
        return chunk


def _sha256_in_thread(path: str) -> Callable[[], str]:
    """Start hashing ``path`` in a thread (hashlib releases the GIL on large
    buffers, so it runs beside the parse); the returned function joins it
    and gives the hex digest."""
    result: dict[str, str] = {}

    def run() -> None:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8 << 20), b""):
                h.update(chunk)
        result["sha256"] = h.hexdigest()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    def finish() -> str:
        thread.join()
        if "sha256" not in result:
            raise RuntimeError(f"hashing {path} failed")
        return result["sha256"]
    return finish


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Streaming UniProtKB JSON → JSONL converter"
    )
    parser.add_argument(
        "--expected-count", type=int, default=None,
        help="If set, assert that exactly this many entries were written. "
             "Exit 1 on mismatch (catches silent data loss).",
    )
    parser.add_argument(
        "--count-file", default=None,
        help="Write the final entry count to this file (one integer). "
             "Enables post-hoc verification by the caller.",
    )
    parser.add_argument(
        "--source", default=None,
        help="The file the stream on stdin was decompressed from (the UniProtKB dump): "
             "named, sized and hashed into --input-manifest.",
    )
    parser.add_argument(
        "--input-manifest", default=None,
        help="Write a JSON sidecar identifying --source (filename, size_bytes, sha256) "
             "with entry_count and expected_count.",
    )
    args = parser.parse_args()
    if bool(args.source) != bool(args.input_manifest):
        parser.error("--source and --input-manifest go together")
    source_sha256 = _sha256_in_thread(args.source) if args.source else None

    count = 0
    t0 = time.time()

    try:
        for item in ijson.items(_SurrogateGuard(sys.stdin.buffer), "results.item",
                                use_float=False, buf_size=1 << 20):
            WRITE(DUMPS(item, default=_exact_number, option=OPT))
            count += 1
            if count % LOG_INTERVAL == 0:
                elapsed = time.time() - t0
                rate = count / elapsed if elapsed > 0 else 0
                print(f"  stream_jsonl: {count:,} entries ({rate:,.0f}/s)",
                      file=sys.stderr)
    except BrokenPipeError:
        print(f"  stream_jsonl: FATAL — BrokenPipeError after {count:,} entries "
              f"(downstream consumer died)", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        # Must not fall through to the success path: the count file would
        # record a truncated stream that the caller then treats as complete.
        print(f"  stream_jsonl: FATAL — interrupted after {count:,} entries",
              file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        # An error raised inside ijson's C reader (a lone surrogate) arrives
        # as the cause of a SystemError.
        cause = e.__cause__ if isinstance(e, SystemError) and e.__cause__ else e
        print(f"  stream_jsonl: FATAL — {type(cause).__name__} in entry {count + 1:,} "
              f"(after {count:,} written): {cause}", file=sys.stderr)
        sys.exit(1)

    elapsed = time.time() - t0
    print(f"  stream_jsonl: DONE — {count:,} entries in {elapsed:.1f}s",
          file=sys.stderr)
    if args.expected_count is not None and count != args.expected_count:
        print(f"  stream_jsonl: FATAL — count mismatch: "
              f"wrote {count:,}, expected {args.expected_count:,}",
              file=sys.stderr)
        sys.exit(1)

    if args.count_file:
        with open(args.count_file, "w") as f:
            f.write(str(count))
        print(f"  stream_jsonl: wrote count ({count:,}) to {args.count_file}",
              file=sys.stderr)

    if args.input_manifest:
        assert source_sha256 is not None   # --source came with --input-manifest (checked above)
        source = {"filename": os.path.basename(args.source),
                  "size_bytes": os.path.getsize(args.source),
                  "sha256": source_sha256(),
                  "entry_count": count,
                  "expected_count": args.expected_count}
        with open(args.input_manifest, "w") as f:
            json.dump(source, f, indent=2)
        print(f"  stream_jsonl: source {source['filename']} sha256 {source['sha256']} "
              f"→ {args.input_manifest}", file=sys.stderr)


if __name__ == "__main__":
    main()
