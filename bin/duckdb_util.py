#!/usr/bin/env python3
"""
duckdb_util.py — the DuckDB connection every pipeline script uses.

Kept apart from parquet_transform.py so that sort_jsonl.py imports only this:
the workflow re-runs a process when a module its script imports changes
(bin_digest in upjson2lake.nf), and a change to the transform must not redo
the sort.
"""

import atexit
import os
import shutil
import sys
import tempfile

import duckdb

# Exit status of a DuckDB out-of-memory error: EX_TEMPFAIL (sysexits.h), a
# temporary failure worth retrying.  The workflow's oom_retry retries it with
# twice the memory; every other failure exits 1 and is terminal.
EX_TEMPFAIL = 75


def _sql_escape(path: str) -> str:
    """Escape a file path for use inside a DuckDB SQL string literal."""
    return path.replace("'", "''")


def init_duckdb(memory_limit: str, threads: int | None,
                temp_dir: str | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql(f"SET memory_limit='{_sql_escape(memory_limit)}'")
    if threads:
        con.sql(f"SET threads={threads}")
    # Priority: explicit --temp-dir > $TMPDIR (SLURM local scratch) > /tmp
    if temp_dir is None:
        temp_dir = os.environ.get("TMPDIR", "/tmp/duckdb_temp")
    # Unique per process: concurrent jobs may share --temp-dir (e.g. the
    # documented /scratch/$USER/duckdb_tmp), and a shared spill path risks
    # collisions and cross-job cleanup.
    os.makedirs(temp_dir, exist_ok=True)
    spill_dir = tempfile.mkdtemp(prefix="duckdb_spill_", dir=temp_dir)
    atexit.register(shutil.rmtree, spill_dir, ignore_errors=True)
    con.sql(f"SET temp_directory='{_sql_escape(spill_dir)}'")
    print(f"  DuckDB temp directory: {spill_dir}", file=sys.stderr)
    # 64-bit Arrow offsets: a batch of long rows (publications, comments)
    # can hold more than 2 GiB of strings in one column, which the regular
    # 32-bit offsets cannot address.  stream_to_parquet casts each batch back
    # to the regular types (_to_regular_types), so the files are unchanged.
    con.sql("SET arrow_large_buffer_size=true")
    return con


def _is_out_of_memory(e: BaseException) -> bool:
    """True for a DuckDB out-of-memory error in any of its guises: the typed
    exception, or, once a query streams through PyArrow's RecordBatchReader
    (every batch after the first), an OSError or ArrowException carrying
    DuckDB's message."""
    if isinstance(e, duckdb.OutOfMemoryException):
        return True
    import pyarrow
    return (isinstance(e, (OSError, pyarrow.ArrowException))
            and str(e).startswith("Out of Memory Error"))


def run_main(main) -> None:
    """Run a script's main(); a DuckDB out-of-memory error, wherever it is
    raised, exits EX_TEMPFAIL with a clear message instead of a traceback
    and exit 1."""
    try:
        main()
    except Exception as e:
        if not _is_out_of_memory(e):
            raise
        print(f"FATAL — DuckDB ran out of memory: {e}\n"
              f"  Exit {EX_TEMPFAIL} (EX_TEMPFAIL): the workflow retries with twice the memory; "
              f"by hand, raise --memory-limit or point --temp-dir at more spill space.",
              file=sys.stderr)
        sys.exit(EX_TEMPFAIL)
