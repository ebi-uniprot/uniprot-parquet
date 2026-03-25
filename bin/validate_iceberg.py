#!/usr/bin/env python3
"""
Validate the UniProtKB Iceberg data lake.

Checks:
  - Both tables exist and are readable
  - Row counts for entries and features
  - Snapshot metadata (snapshot ID, timestamp, summary)
  - entries.feature_count sum ≈ features row count
  - Data files: count, total size, largest file
  - Sample queries to verify data integrity

Usage:
    validate_iceberg.py --catalog-uri sqlite:///catalog.db \
        [--namespace uniprot] [-o validation_report.txt]
"""

import sys
import argparse
import time
from datetime import datetime, timezone

from pyiceberg.catalog.sql import SqlCatalog


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main():
    parser = argparse.ArgumentParser(description="Validate UniProtKB Iceberg data lake")
    parser.add_argument(
        "--catalog-uri", required=True,
        help="SQLite catalog URI (e.g. sqlite:///catalog.db)",
    )
    parser.add_argument("--namespace", default="uniprot", help="Iceberg namespace")
    parser.add_argument("-o", "--output", default="validation_report.txt", help="Output report file")
    args = parser.parse_args()

    lines = []

    def log(msg):
        print(msg)
        lines.append(msg)

    log("=" * 60)
    log("UNIPROT ICEBERG DATALAKE VALIDATION REPORT")
    log(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    log("=" * 60)

    # Connect to catalog
    try:
        catalog = SqlCatalog(
            "uniprot",
            **{"uri": args.catalog_uri, "warehouse": "."},
        )
        log(f"\nCatalog: {args.catalog_uri}")
    except Exception as e:
        log(f"\nERROR: Cannot connect to catalog: {e}")
        with open(args.output, "w") as f:
            f.write("\n".join(lines) + "\n")
        sys.exit(1)

    table_counts = {}

    for table_name in ["entries", "features"]:
        log(f"\n--- {table_name.upper()} ---")
        full_name = f"{args.namespace}.{table_name}"

        try:
            table = catalog.load_table(full_name)
        except Exception as e:
            log(f"  ERROR: Cannot load table '{full_name}': {e}")
            continue

        # Snapshot info
        snapshot = table.current_snapshot()
        if snapshot:
            log(f"  Snapshot ID:  {snapshot.snapshot_id}")
            ts = datetime.fromtimestamp(
                snapshot.timestamp_ms / 1000, tz=timezone.utc
            )
            log(f"  Timestamp:    {ts.isoformat()}")
            if snapshot.summary:
                for k, v in snapshot.summary.items():
                    log(f"  {k}: {v}")
        else:
            log("  No snapshots (empty table)")

        # Row count via scan
        try:
            t0 = time.time()
            scan = table.scan()
            arrow_table = scan.to_arrow()
            count = arrow_table.num_rows
            table_counts[table_name] = count
            log(f"  Row count:    {count:,} (scanned in {time.time()-t0:.1f}s)")
        except Exception as e:
            log(f"  ERROR scanning: {e}")

        # Data file stats
        try:
            data_files = list(table.inspect.data_files())
            if data_files:
                total_size = sum(f["file_size_in_bytes"] for f in data_files)
                max_size = max(f["file_size_in_bytes"] for f in data_files)
                log(f"  Data files:   {len(data_files)}")
                log(f"  Total size:   {total_size / (1024*1024):.1f} MB")
                log(f"  Largest file: {max_size / (1024*1024):.1f} MB")
        except Exception as e:
            log(f"  (Could not inspect data files: {e})")

        # Schema summary
        log(f"  Columns:      {len(table.schema().fields)}")

        # Table properties
        props = table.properties
        if props:
            release = props.get("uniprot.release", "unknown")
            log(f"  Release:      {release}")

    # Consistency check
    if "entries" in table_counts and "features" in table_counts:
        log(f"\n--- CONSISTENCY ---")
        log(f"  entries rows:  {table_counts['entries']:,}")
        log(f"  features rows: {table_counts['features']:,}")
        ratio = table_counts["features"] / max(table_counts["entries"], 1)
        log(f"  features/entry ratio: {ratio:.1f}")

        # Check feature_count sum vs features table rows
        try:
            entries_table = catalog.load_table(f"{args.namespace}.entries")
            fc_scan = entries_table.scan(selected_fields=("feature_count",))
            fc_arrow = fc_scan.to_arrow()
            fc_col = fc_arrow.column("feature_count")
            import pyarrow.compute as pc
            fc_sum = pc.sum(fc_col).as_py()
            if fc_sum is not None:
                log(f"  sum(entries.feature_count): {fc_sum:,}")
                match = fc_sum == table_counts["features"]
                log(f"  matches features rows: {'YES' if match else 'MISMATCH!'}")
        except Exception as e:
            log(f"  (Could not verify feature_count: {e})")

    # Sample data check
    log(f"\n--- SAMPLE DATA ---")
    for table_name in ["entries", "features"]:
        full_name = f"{args.namespace}.{table_name}"
        try:
            table = catalog.load_table(full_name)
            scan = table.scan(limit=3)
            arrow = scan.to_arrow()
            log(f"  {table_name} first 3 accessions: {arrow.column('acc').to_pylist()[:3]}")
        except Exception as e:
            log(f"  {table_name}: ERROR sampling: {e}")

    # Reviewed/unreviewed split
    log(f"\n--- REVIEWED SPLIT ---")
    try:
        entries_table = catalog.load_table(f"{args.namespace}.entries")
        arrow = entries_table.scan(selected_fields=("reviewed",)).to_arrow()
        import pyarrow.compute as pc
        reviewed_count = pc.sum(pc.cast(arrow.column("reviewed"), "int64")).as_py()
        total = arrow.num_rows
        log(f"  Reviewed (Swiss-Prot): {reviewed_count:,}")
        log(f"  Unreviewed (TrEMBL):  {total - reviewed_count:,}")
    except Exception as e:
        log(f"  ERROR: {e}")

    log("\n" + "=" * 60)

    with open(args.output, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
