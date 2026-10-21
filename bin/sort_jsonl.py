#!/usr/bin/env python3
"""
Sort a UniProtKB JSONL(.zst) file by reviewed status (Swiss-Prot first),
then organism taxon ID, then accession.

Output is a zstd-compressed JSONL file holding the input's lines, byte for
byte, reordered.  Each line is read as raw text (read_ndjson_objects) and
the sort keys are extracted from it; nothing is parsed into DuckDB's
inferred schema and serialised again, which would add a null for every key
an entry lacks, reorder keys and rewrite numbers.  The lines are written
back with a CSV COPY that neither quotes nor escapes: JSON text never holds
the \x01 delimiter or a raw line break, so each line is one field as is.

An external merge sort, in bounded memory.  DuckDB's ORDER BY barely spills
with whole entries as its payload: it needs memory about the size of its
input (a 3.3 GiB subset failed at 6 GB with 4 threads), far short of the
full release.  So the input is cut into chunks of --chunk-bytes (by default
the memory limit / (2 + threads), which sorts in memory with room to spare),
DuckDB sorts each chunk into a run with its sort keys in front, and the runs
are merged on those keys.  An input of one chunk is sorted directly.
Chunks and runs are zstd files under the spill directory (--temp-dir): at
most about 1.5x the compressed input (360 MiB for a 238 MiB subset).

Usage:
    sort_jsonl.py <input.jsonl.zst> -o <sorted.jsonl.zst> \
        [--memory-limit 16GB] \
        [--threads 4] \
        [--temp-dir /path/to/scratch]
"""

import argparse
import heapq
import io
import os
import re
import shutil
import sys
import time
from collections.abc import Iterator
from typing import Any

import zstandard

# Allow importing from the same bin/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from duckdb_util import _sql_escape, init_duckdb, run_main


def eprint(*args: object, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)


# The sort keys, as columns of the inner SELECT; NULLs sort last.
KEYS_SQL = """
SELECT json,
    coalesce(json_extract_string(json, '$.entryType') LIKE '%Swiss-Prot%', false) AS reviewed,
    TRY_CAST(json_extract(json, '$.organism.taxonId') AS BIGINT) AS taxon_id,
    json_extract_string(json, '$.primaryAccession') AS accession
FROM read_ndjson_objects('{path}', maximum_object_size={max_object})
"""
ORDER_SQL = "ORDER BY reviewed DESC, taxon_id ASC NULLS LAST, accession ASC NULLS LAST"

# The output: the lines alone.
SORT_SQL = "SELECT json FROM (" + KEYS_SQL + ") " + ORDER_SQL

# A run: the keys in front of each line, as fields that sort the same way in
# merge_runs (bytes compare as DuckDB's VARCHARs do: byte by byte).
RUN_SQL = """
SELECT CAST(NOT reviewed AS INTEGER), CAST(taxon_id IS NULL AS INTEGER), coalesce(taxon_id, 0),
       CAST(accession IS NULL AS INTEGER), coalesce(accession, ''), json
FROM (""" + KEYS_SQL + ") " + ORDER_SQL

COPY_OPTIONS = ("(FORMAT CSV, HEADER false, QUOTE '', ESCAPE '', DELIMITER chr(1), "
                "COMPRESSION ZSTD)")

_SIZE_UNITS = {"": 1, "B": 1, "K": 10**3, "KB": 10**3, "M": 10**6, "MB": 10**6,
               "G": 10**9, "GB": 10**9, "T": 10**12, "TB": 10**12,
               "KIB": 2**10, "MIB": 2**20, "GIB": 2**30, "TIB": 2**40}


def parse_size(text: str) -> int:
    """Bytes in a DuckDB-style size ('6912MB', '16GB', '1.5GiB')."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*", text)
    if not m or m.group(2).upper() not in _SIZE_UNITS:
        raise ValueError(f"not a size: {text!r}")
    return int(float(m.group(1)) * _SIZE_UNITS[m.group(2).upper()])


def max_object_size(longest_line: int) -> int:
    """read_ndjson_objects' maximum_object_size for lines up to
    ``longest_line`` bytes: DuckDB's 16 MiB default, or more for the largest
    entries (titin).  Each reader thread buffers twice this much, so a fixed
    512 MiB would cost 1 GiB of the memory limit per thread."""
    return max(16 << 20, ((longest_line >> 20) + 1) << 20)


def read_lines(path: str) -> Iterator[bytes]:
    """The lines of a JSONL(.zst) file, as bytes."""
    with open(path, "rb") as f:
        if path.endswith(".zst"):
            reader = zstandard.ZstdDecompressor().stream_reader(f, read_across_frames=True)
            yield from io.BufferedReader(reader, buffer_size=16 << 20)
        else:
            yield from f


def split_chunks(path: str, chunk_dir: str, chunk_bytes: int, threads: int) -> list[tuple[str, int]]:
    """Cut ``path`` into zstd chunks of about ``chunk_bytes`` (uncompressed)
    under ``chunk_dir``; [(chunk path, longest line in it)]."""
    chunks: list[tuple[str, int]] = []
    cctx = zstandard.ZstdCompressor(level=1, threads=threads)
    lines = read_lines(path)
    line = next(lines, None)
    while line is not None:
        name = os.path.join(chunk_dir, f"chunk_{len(chunks):05d}.jsonl.zst")
        size = longest = 0
        with open(name, "wb") as f, cctx.stream_writer(f) as out:
            while line is not None and size < chunk_bytes:
                if not line.endswith(b"\n"):
                    line += b"\n"
                out.write(line)
                size += len(line)
                longest = max(longest, len(line))
                line = next(lines, None)
        chunks.append((name, longest))
    return chunks


def _run_rows(path: str) -> Iterator[tuple[tuple[int, int, int, int, bytes], bytes]]:
    """(sort key, line) for each row of a run."""
    for row in read_lines(path):
        reviewed, no_taxon, taxon_id, no_accession, accession, line = row.split(b"\x01", 5)
        yield (int(reviewed), int(no_taxon), int(taxon_id), int(no_accession), accession), line


def merge_runs(runs: list[str], output: str, threads: int) -> int:
    """Merge sorted runs into ``output`` (zstd JSONL); the number of lines.
    Equal keys keep run order, as one ORDER BY over the input would not
    promise either."""
    count = 0
    cctx = zstandard.ZstdCompressor(level=3, threads=threads)
    with open(output, "wb") as f, cctx.stream_writer(f) as out:
        for _, line in heapq.merge(*(_run_rows(r) for r in runs), key=lambda row: row[0]):
            out.write(line)
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sort UniProtKB JSONL by reviewed status then taxon ID"
    )
    parser.add_argument("input", help="Input JSONL(.zst) file")
    parser.add_argument(
        "-o", "--output", required=True,
        help="Output sorted JSONL(.zst) file",
    )
    parser.add_argument("--memory-limit", default="16GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    parser.add_argument(
        "--temp-dir", default=None,
        help="DuckDB spill directory, which also holds the chunks and runs. "
             "Defaults to $TMPDIR or /tmp/duckdb_temp.",
    )
    parser.add_argument(
        "--chunk-bytes", default=None,
        help="Uncompressed bytes per chunk (e.g. 2GB). Default: the memory limit / (2 + threads).",
    )
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    output_path = os.path.abspath(args.output)
    threads = args.threads or os.cpu_count() or 1
    chunk_bytes = (parse_size(args.chunk_bytes) if args.chunk_bytes
                   else parse_size(args.memory_limit) // (2 + threads))

    eprint("=" * 60)
    eprint("UniProtKB JSONL Sort")
    eprint("=" * 60)
    eprint(f"  Input:  {jsonl_path}")
    eprint(f"  Output: {output_path}")
    eprint(f"  Memory: {args.memory_limit}, chunks of {chunk_bytes / 2**20:,.0f} MiB")
    eprint()

    con = init_duckdb(args.memory_limit, args.threads, args.temp_dir)
    try:
        setting = con.sql("SELECT current_setting('temp_directory')").fetchone()
        assert setting is not None
        work_dir = os.path.join(setting[0], "sort_runs")
        os.makedirs(work_dir)
        t0 = time.time()

        eprint("Cutting the input into chunks...")
        chunks = split_chunks(jsonl_path, work_dir, chunk_bytes, threads)
        eprint(f"  {len(chunks)} chunk(s) ({time.time() - t0:.1f}s)")

        eprint("Sorting (reviewed DESC, organism_id ASC, accession ASC) and writing...")
        if len(chunks) <= 1:
            # One chunk: sort the input itself, no keys, no merge.
            longest = chunks[0][1] if chunks else 0
            sql = SORT_SQL.format(path=_sql_escape(jsonl_path), max_object=max_object_size(longest))
            con.sql(f"COPY ({sql}) TO '{_sql_escape(output_path)}' {COPY_OPTIONS}")
        else:
            runs = []
            for i, (chunk, longest) in enumerate(chunks):
                t1 = time.time()
                run = os.path.join(work_dir, f"run_{i:05d}.csv.zst")
                sql = RUN_SQL.format(path=_sql_escape(chunk), max_object=max_object_size(longest))
                con.sql(f"COPY ({sql}) TO '{_sql_escape(run)}' {COPY_OPTIONS}")
                os.remove(chunk)
                runs.append(run)
                eprint(f"  run {i + 1}/{len(chunks)} sorted ({time.time() - t1:.1f}s)")
            t1 = time.time()
            n = merge_runs(runs, output_path, threads)
            eprint(f"  {len(runs)} runs merged: {n:,} lines ({time.time() - t1:.1f}s)")
        shutil.rmtree(work_dir, ignore_errors=True)

        elapsed = time.time() - t0
        eprint(f"  Done in {elapsed:.1f}s")

        # Quick sanity: report output size
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        eprint(f"  Output: {size_mb:.1f} MB")
        eprint("=" * 60)
    finally:
        con.close()


if __name__ == "__main__":
    run_main(main)
