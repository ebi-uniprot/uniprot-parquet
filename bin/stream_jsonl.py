#!/usr/bin/env python3
"""
Stream UniProtKB JSON(.gz) → zstd-compressed chunked JSONL files.

Handles input format:
  1. JSON with {"results": [...]} wrapper - streamed via ijson

Output chunks are .jsonl.zst (zstandard compressed).
DuckDB reads these natively.

Usage:
    stream_jsonl.py <input.json[l][.gz]> -o <outdir> -b <batch_size>

Output:
    outdir/chunk_000000.jsonl.zst
    outdir/chunk_000001.jsonl.zst
    ...
"""

import sys
import gzip
import json
import argparse
import time
from pathlib import Path
import zstandard as zstd


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def stream_records(path):
    """Yield entries from {"results": [...]} JSON file. Handles .gz transparently."""
    try:
        import ijson
    except ImportError:
        eprint("ERROR: ijson not installed. Run: pip install ijson")
        sys.exit(1)

    is_gz = str(path).endswith('.gz')
    opener = gzip.open if is_gz else open

    with opener(path, 'rb') as fh:
        yield from ijson.items(fh, 'results.item', use_float=True)


def open_zst_writer(path):
    """Open a zstd-compressed text writer."""
    cctx = zstd.ZstdCompressor(level=3)
    fh = open(path, 'wb')
    return cctx.stream_writer(fh), fh


def main():
    parser = argparse.ArgumentParser(description='Stream UniProtKB JSON to chunked JSONL.zst')
    parser.add_argument('input', help='Input JSON file with {"results": [...]} (optionally .gz)')
    parser.add_argument('-o', '--outdir', default='jsonl_chunks', help='Output directory')
    parser.add_argument('-b', '--batch-size', type=int, default=500000,
                        help='Records per chunk file (default: 500000)')
    args = parser.parse_args()

    input_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    eprint(f"Input: {input_path}")
    eprint(f"Output: {outdir}/")
    eprint(f"Batch size: {args.batch_size:,}")

    chunk_idx = 0
    record_count = 0
    total_count = 0
    writer = None
    raw_fh = None
    t_start = time.time()

    try:
        for record in stream_records(input_path):
            if record_count == 0:
                chunk_name = f"chunk_{chunk_idx:06d}.jsonl.zst"
                writer, raw_fh = open_zst_writer(outdir / chunk_name)
                eprint(f"  Writing {chunk_name}...")

            line = json.dumps(record, separators=(',', ':')) + '\n'
            writer.write(line.encode('utf-8'))
            record_count += 1
            total_count += 1

            if record_count >= args.batch_size:
                writer.close()
                raw_fh.close()
                eprint(f"    -> {record_count:,} records")
                chunk_idx += 1
                record_count = 0
                writer = None
                raw_fh = None

        if writer is not None:
            writer.close()
            raw_fh.close()
            eprint(f"    -> {record_count:,} records (final)")

    except Exception:
        if writer is not None:
            writer.close()
            raw_fh.close()
        raise

    elapsed = time.time() - t_start
    n_chunks = chunk_idx + (1 if record_count > 0 else 0)
    if n_chunks == 0:
        n_chunks = 1  # edge case: empty input still produces chunk count
    eprint(f"\nDone: {total_count:,} records in {n_chunks} chunks ({elapsed:.1f}s)")


if __name__ == '__main__':
    main()
