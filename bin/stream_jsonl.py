#!/usr/bin/env python3
"""
Streaming UniProtKB JSON → JSONL converter.

Reads a UniProtKB JSON dump from stdin (typically piped from pigz -dc),
extracts each entry from the "results" array via streaming (ijson), and
writes one JSON line per entry to stdout (via orjson).

Integrity checks:
  - Malformed entries that fail JSON serialisation are logged to stderr
    and skipped (not silently dropped).
  - Entry count and error count are reported to stderr on completion.
  - Exit code 0 if all entries were processed, 1 if any were skipped.
  - If --expected-count is provided, the output count must match exactly
    or the script exits 1.
  - If --count-file is provided, the final count is written to that file
    so the caller can verify end-to-end (e.g. against zstd output lines).

Usage (standalone):
    pigz -dc input.json.gz | python3 stream_jsonl.py | zstd -3 -T0 -o output.jsonl.zst

Usage (with count assertion):
    pigz -dc input.json.gz | python3 stream_jsonl.py --expected-count 248799253 | ...

Usage (with sidecar count file for post-hoc verification):
    pigz -dc input.json.gz | python3 stream_jsonl.py --count-file entry_count.txt | ...
"""

import sys
import time
import argparse

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


def main():
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
    args = parser.parse_args()

    count = 0
    errors = 0
    t0 = time.time()

    try:
        for item in ijson.items(sys.stdin.buffer, "results.item", use_float=True):
            try:
                WRITE(DUMPS(item, option=OPT))
                count += 1
                if count % LOG_INTERVAL == 0:
                    elapsed = time.time() - t0
                    rate = count / elapsed if elapsed > 0 else 0
                    print(f"  stream_jsonl: {count:,} entries ({rate:,.0f}/s)",
                          file=sys.stderr)
            except (TypeError, orjson.JSONEncodeError) as e:
                errors += 1
                acc = item.get("primaryAccession", "unknown") if isinstance(item, dict) else "unknown"
                print(f"  stream_jsonl: SKIPPED entry {acc}: {e}",
                      file=sys.stderr)
    except BrokenPipeError:
        pass
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"  stream_jsonl: FATAL — {type(e).__name__} after {count:,} entries: {e}",
              file=sys.stderr)
        sys.exit(1)

    elapsed = time.time() - t0
    print(f"  stream_jsonl: DONE — {count:,} entries in {elapsed:.1f}s",
          file=sys.stderr)
    if errors:
        print(f"  stream_jsonl: WARNING — {errors:,} entries skipped due to errors",
              file=sys.stderr)
        sys.exit(1)

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


if __name__ == "__main__":
    main()
