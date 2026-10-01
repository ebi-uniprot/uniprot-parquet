#!/usr/bin/env python3
"""
Sort a UniProtKB JSONL(.zst) file by reviewed status (Swiss-Prot first),
then organism taxon ID, then accession.

Uses DuckDB's out-of-core sorting — spills to disk when the dataset exceeds
memory, so it handles the full 160GB+ UniProtKB in bounded memory.

Output is a zstd-compressed JSONL file holding the input's lines, byte for
byte, reordered.  Each line is read as raw text (read_ndjson_objects) and
the sort keys are extracted from it; nothing is parsed into DuckDB's
inferred schema and serialised again, which would add a null for every key
an entry lacks, reorder keys and rewrite numbers.  The lines are written
back with a CSV COPY that neither quotes nor escapes: JSON text never holds
the \x01 delimiter or a raw line break, so each line is one field as is.

Usage:
    sort_jsonl.py <input.jsonl.zst> -o <sorted.jsonl.zst> \
        [--memory-limit 16GB] \
        [--threads 4] \
        [--temp-dir /path/to/scratch]
"""

import argparse
import os
import sys
import time
from typing import Any

# Allow importing from the same bin/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from duckdb_util import _sql_escape, init_duckdb, run_main


def eprint(*args: object, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)


# maximum_object_size=512 MB: the largest UniProtKB entries (titin) exceed
# DuckDB's 16 MB default (as in parquet_transform.build_read_clause).
SORT_SQL = """
SELECT json
FROM read_ndjson_objects('{path}', maximum_object_size=536870912)
ORDER BY
    coalesce(json_extract_string(json, '$.entryType') LIKE '%Swiss-Prot%', false) DESC,
    TRY_CAST(json_extract(json, '$.organism.taxonId') AS BIGINT),
    json_extract_string(json, '$.primaryAccession')
"""


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
        help="DuckDB spill directory. Defaults to $TMPDIR or /tmp/duckdb_temp.",
    )
    args = parser.parse_args()

    jsonl_path = os.path.abspath(args.input)
    output_path = os.path.abspath(args.output)

    eprint("=" * 60)
    eprint("UniProtKB JSONL Sort")
    eprint("=" * 60)
    eprint(f"  Input:  {jsonl_path}")
    eprint(f"  Output: {output_path}")
    eprint(f"  Memory: {args.memory_limit}")
    eprint()

    con = init_duckdb(args.memory_limit, args.threads, args.temp_dir)
    try:
        sql = SORT_SQL.format(path=_sql_escape(jsonl_path))
        safe_output = _sql_escape(output_path)

        eprint("Sorting (reviewed DESC, organism_id ASC, accession ASC) and writing...")
        eprint("  DuckDB will spill to disk if the dataset exceeds memory.")
        t0 = time.time()

        con.sql(f"""
            COPY ({sql}) TO '{safe_output}'
            (FORMAT CSV, HEADER false, QUOTE '', ESCAPE '', DELIMITER chr(1),
             COMPRESSION ZSTD)
        """)

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
