#!/usr/bin/env python3
"""
Stream UniProtKB JSON.gz → chunked JSONL files.

Handles two input formats:
  1. Newline-delimited JSON (JSONL) - one record per line
  2. JSON object with {"results": [...]} wrapper (the UniProtKB download format)

For the wrapper format, uses ijson for streaming to avoid loading 156GB into memory.
Falls back to line-by-line if ijson is not available and input is already JSONL.

Usage:
    stream_jsonl.py <input.json[l][.gz]> -o <outdir> -b <batch_size>

Output:
    outdir/chunk_000000.jsonl
    outdir/chunk_000001.jsonl
    ...
"""

import os
import sys
import gzip
import json
import argparse
import time
from pathlib import Path


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def open_input(path):
    """Open file, handling .gz transparently."""
    if str(path).endswith('.gz'):
        return gzip.open(path, 'rt', encoding='utf-8')
    return open(path, 'r', encoding='utf-8')


def stream_json_wrapper(fh):
    """
    Stream records from a {"results": [...]} JSON structure using ijson.
    This is memory-efficient for the 156GB UniProtKB download.
    """
    try:
        import ijson
    except ImportError:
        eprint("ERROR: ijson not installed. Install with: pip install ijson")
        eprint("       For best performance: pip install ijson[yajl2]")
        sys.exit(1)

    # ijson streams items from the 'results' array one at a time
    for record in ijson.items(fh, 'results.item'):
        yield record


def stream_jsonl(fh):
    """Stream records from newline-delimited JSON (JSONL)."""
    for line in fh:
        line = line.strip()
        if line:
            yield json.loads(line)


def detect_format(path):
    """
    Peek at the file to determine if it's JSONL or JSON wrapper.
    JSONL starts with '{' on first line (a complete record).
    JSON wrapper starts with '{' but has a "results" key.
    """
    with open_input(path) as fh:
        first_chars = fh.read(100).strip()
        if first_chars.startswith('['):
            return 'array'
        # Check if first line is a complete JSON object (JSONL)
        # vs a wrapper object with "results" key
        if '"results"' in first_chars:
            return 'wrapper'
        # If it starts with { and first line parses as complete JSON, it's JSONL
        try:
            with open_input(path) as fh2:
                first_line = fh2.readline().strip()
                json.loads(first_line)
                return 'jsonl'
        except (json.JSONDecodeError, ValueError):
            return 'wrapper'


def main():
    parser = argparse.ArgumentParser(description='Stream UniProtKB JSON to chunked JSONL')
    parser.add_argument('input', help='Input JSON/JSONL file (optionally .gz)')
    parser.add_argument('-o', '--outdir', default='jsonl_chunks', help='Output directory')
    parser.add_argument('-b', '--batch-size', type=int, default=500000,
                        help='Records per chunk file (default: 500000)')
    args = parser.parse_args()

    input_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    batch_size = args.batch_size

    eprint(f"Input: {input_path}")
    eprint(f"Output: {outdir}/")
    eprint(f"Batch size: {batch_size:,}")

    # Detect format
    fmt = detect_format(input_path)
    eprint(f"Detected format: {fmt}")

    fh = open_input(input_path)

    if fmt == 'jsonl':
        records = stream_jsonl(fh)
    elif fmt == 'wrapper':
        # For wrapper format, need binary mode for ijson
        fh.close()
        if str(input_path).endswith('.gz'):
            fh = gzip.open(input_path, 'rb')
        else:
            fh = open(input_path, 'rb')
        records = stream_json_wrapper(fh)
    elif fmt == 'array':
        # Bare JSON array - use ijson
        fh.close()
        if str(input_path).endswith('.gz'):
            fh = gzip.open(input_path, 'rb')
        else:
            fh = open(input_path, 'rb')
        try:
            import ijson
            records = ijson.items(fh, 'item')
        except ImportError:
            eprint("ERROR: ijson required for JSON array format")
            sys.exit(1)
    else:
        eprint(f"Unknown format: {fmt}")
        sys.exit(1)

    chunk_idx = 0
    record_count = 0
    total_count = 0
    outfile = None
    t_start = time.time()

    try:
        for record in records:
            # Open new chunk file if needed
            if record_count == 0:
                chunk_name = f"chunk_{chunk_idx:06d}.jsonl"
                outpath = outdir / chunk_name
                outfile = open(outpath, 'w')
                eprint(f"  Writing {chunk_name}...")

            outfile.write(json.dumps(record, separators=(',', ':')) + '\n')
            record_count += 1
            total_count += 1

            # Rotate to next chunk
            if record_count >= batch_size:
                outfile.close()
                eprint(f"    -> {record_count:,} records")
                chunk_idx += 1
                record_count = 0
                outfile = None

        # Close final chunk
        if outfile and not outfile.closed:
            outfile.close()
            eprint(f"    -> {record_count:,} records (final)")

    finally:
        fh.close()

    elapsed = time.time() - t_start
    eprint(f"\nDone: {total_count:,} records in {chunk_idx + 1} chunks ({elapsed:.1f}s)")
    # Write manifest for downstream processes
    manifest = outdir / 'manifest.txt'
    with open(manifest, 'w') as f:
        f.write(f"total_records={total_count}\n")
        f.write(f"total_chunks={chunk_idx + 1}\n")
        f.write(f"batch_size={batch_size}\n")


if __name__ == '__main__':
    main()
