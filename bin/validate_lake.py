#!/usr/bin/env python3
"""
Validate the final UniProtKB Parquet datalake.

Checks:
  - Row counts per table and partition
  - core/seqs row count consistency
  - File sizes against maximum threshold

Usage:
    validate_lake.py <lake_dir> [--max-parquet-mb 512]

Expects lake_dir to contain core/, seqs/, features/ subdirectories
with Hive-partitioned parquet files.
"""

import os
import sys
import argparse
import duckdb


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main():
    parser = argparse.ArgumentParser(description='Validate UniProtKB Parquet datalake')
    parser.add_argument('lake_dirs', nargs='+', help='Directories containing table subdirs (core/, seqs/, features/)')
    parser.add_argument('--max-parquet-mb', type=int, default=512, help='Max parquet file size in MB')
    parser.add_argument('-o', '--output', default='validation_report.txt', help='Output report file')
    args = parser.parse_args()

    con = duckdb.connect()
    lines = []

    def log(msg):
        print(msg)
        lines.append(msg)

    log("=" * 60)
    log("UNIPROT DATALAKE VALIDATION REPORT")
    log("=" * 60)

    # Build glob patterns — table dirs may be spread across multiple input dirs
    def find_parquet(table):
        patterns = []
        for d in args.lake_dirs:
            candidate = os.path.join(d, table)
            if os.path.isdir(candidate):
                patterns.append(os.path.join(candidate, '**', '*.parquet'))
            # Also check if d itself IS the table dir (e.g. d="core")
            elif os.path.basename(d) == table and os.path.isdir(d):
                patterns.append(os.path.join(d, '**', '*.parquet'))
        return patterns

    table_counts = {}
    for table in ['core', 'seqs', 'features']:
        log(f"\n--- {table.upper()} ---")
        patterns = find_parquet(table)
        if not patterns:
            log(f"  NOT FOUND")
            continue

        try:
            # Collect all matching files
            import glob as globmod
            files = []
            for p in patterns:
                files.extend(globmod.glob(p, recursive=True))

            if not files:
                log(f"  NO PARQUET FILES")
                continue

            count = con.sql(f"""
                SELECT count(*) FROM read_parquet({files}, hive_partitioning=true)
            """).fetchone()[0]
            table_counts[table] = count
            log(f"  Total rows: {count:,}")

            partitions = con.sql(f"""
                SELECT review_status, tax_division, count(*) as cnt
                FROM read_parquet({files}, hive_partitioning=true)
                GROUP BY 1, 2 ORDER BY 1, 2
            """).fetchall()
            for rs, td, cnt in partitions:
                log(f"  {rs}/{td}: {cnt:,}")
        except Exception as e:
            log(f"  ERROR: {e}")

    # Consistency check
    if 'core' in table_counts and 'seqs' in table_counts:
        log(f"\n--- CONSISTENCY ---")
        log(f"  core rows: {table_counts['core']:,}, seqs rows: {table_counts['seqs']:,}")
        match = table_counts['core'] == table_counts['seqs']
        log(f"  match: {'YES' if match else 'MISMATCH!'}")

    # File sizes
    log(f"\n--- FILE SIZES ---")
    total_size = 0
    max_size = 0
    max_file = ""
    for d in args.lake_dirs:
        for root, dirs, files in os.walk(d):
            for f in files:
                if f.endswith('.parquet'):
                    fp = os.path.join(root, f)
                    sz = os.path.getsize(fp)
                    total_size += sz
                    if sz > max_size:
                        max_size = sz
                        max_file = fp
                    if sz > args.max_parquet_mb * 1024 * 1024:
                        log(f"  WARNING: {fp} is {sz/(1024*1024):.1f}MB (>{args.max_parquet_mb}MB)")

    log(f"  Total: {total_size/(1024*1024):.1f} MB")
    log(f"  Largest: {max_size/(1024*1024):.1f} MB ({max_file})")
    log(f"  All <{args.max_parquet_mb}MB: {'YES' if max_size < args.max_parquet_mb*1024*1024 else 'NO'}")
    log("\n" + "=" * 60)

    with open(args.output, 'w') as f:
        f.write("\n".join(lines) + "\n")


if __name__ == '__main__':
    main()
