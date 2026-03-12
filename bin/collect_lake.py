#!/usr/bin/env python3
"""
Collect per-chunk Hive-partitioned Parquet files into a consolidated table.

Reads all chunk parquet files for a given table, and re-writes them into
a single Hive-partitioned output with optimal file sizes.

Usage:
    collect_lake.py <table_name> <chunk_dir1> <chunk_dir2> ... -o <outdir>
        --memory-limit 16GB --max-parquet-mb 512
"""

import os
import sys
import glob
import argparse
import duckdb


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main():
    parser = argparse.ArgumentParser(description='Collect chunk parquets into final lake table')
    parser.add_argument('table', help='Table name (core, seqs, or features)')
    parser.add_argument('chunk_dirs', nargs='+', help='Chunk output directories')
    parser.add_argument('-o', '--outdir', default='.', help='Output directory')
    parser.add_argument('--memory-limit', default='16GB', help='DuckDB memory limit')
    parser.add_argument('--max-parquet-mb', type=int, default=512, help='Max parquet file size')
    args = parser.parse_args()

    con = duckdb.connect()
    con.sql(f"SET memory_limit='{args.memory_limit}'")

    # Find all parquet files for this table across chunk directories
    files = []
    for d in args.chunk_dirs:
        pattern = os.path.join(d, args.table, '**', '*.parquet')
        files.extend(glob.glob(pattern, recursive=True))
    files.sort()

    if not files:
        eprint(f"WARNING: No parquet files found for {args.table}")
        sys.exit(0)

    eprint(f"Collecting {len(files)} files for {args.table}")

    out = os.path.join(args.outdir, args.table)
    os.makedirs(out, exist_ok=True)

    con.sql(f"""
        COPY (
            SELECT * FROM read_parquet(
                {files},
                hive_partitioning=true,
                union_by_name=true
            )
        ) TO '{out}'
        (FORMAT PARQUET, PARTITION_BY (review_status, tax_division),
         COMPRESSION 'zstd', ROW_GROUP_SIZE 500000)
    """)

    # Report file sizes
    for root, dirs, fnames in os.walk(out):
        for fname in fnames:
            if fname.endswith('.parquet'):
                path = os.path.join(root, fname)
                size_mb = os.path.getsize(path) / (1024 * 1024)
                eprint(f"  {path}: {size_mb:.1f} MB")
                if size_mb > args.max_parquet_mb:
                    eprint(f"  WARNING: exceeds {args.max_parquet_mb}MB limit!")


if __name__ == '__main__':
    main()
