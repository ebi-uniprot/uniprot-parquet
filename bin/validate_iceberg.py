#!/usr/bin/env python3
"""
Validate the UniProtKB Iceberg data lake.

Checks:
  - Both tables exist and are readable
  - Row counts from snapshot metadata (no full scan needed)
  - Snapshot metadata (snapshot ID, timestamp, summary)
  - entries.feature_count sum ≈ features row count
  - Data files: count, total size, largest file
  - Sample queries to verify data integrity
  - Reviewed/unreviewed split

Usage:
    validate_iceberg.py --catalog-uri sqlite:///catalog.db \
        --warehouse /path/to/warehouse \
        [--namespace uniprotkb] [-o validation_report.txt]
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
    parser.add_argument(
        "--warehouse", required=True,
        help="Iceberg warehouse directory",
    )
    parser.add_argument("--namespace", default="uniprotkb", help="Iceberg namespace")
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
            **{"uri": args.catalog_uri, "warehouse": args.warehouse},
        )
        log(f"\nCatalog:   {args.catalog_uri}")
        log(f"Warehouse: {args.warehouse}")
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

        # Snapshot info + row count from metadata (no scan needed)
        snapshot = table.current_snapshot()
        if snapshot:
            log(f"  Snapshot ID:  {snapshot.snapshot_id}")
            ts = datetime.fromtimestamp(
                snapshot.timestamp_ms / 1000, tz=timezone.utc
            )
            log(f"  Timestamp:    {ts.isoformat()}")
            if snapshot.summary:
                summary_dict = snapshot.summary.ser_model()
                for k, v in summary_dict.items():
                    log(f"  {k}: {v}")
                # Use total-records from snapshot summary instead of full scan
                total_records = summary_dict.get("total-records")
                if total_records is not None:
                    count = int(total_records)
                    table_counts[table_name] = count
                    log(f"  Row count (from metadata): {count:,}")
        else:
            log("  No snapshots (empty table)")

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
        # Only scan the single integer column — bounded memory
        try:
            entries_table = catalog.load_table(f"{args.namespace}.entries")
            import pyarrow.compute as pc

            fc_sum = 0
            for batch in entries_table.scan(
                selected_fields=("feature_count",)
            ).to_arrow_batch_reader():
                col = batch.column("feature_count")
                batch_sum = pc.sum(col).as_py()
                if batch_sum is not None:
                    fc_sum += batch_sum

            log(f"  sum(entries.feature_count): {fc_sum:,}")
            match = fc_sum == table_counts["features"]
            log(f"  matches features rows: {'YES' if match else 'MISMATCH!'}")
        except Exception as e:
            log(f"  (Could not verify feature_count: {e})")

    # Sample data check (bounded — limit=3)
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

    # Reviewed/unreviewed split — stream in batches to avoid OOM
    log(f"\n--- REVIEWED SPLIT ---")
    try:
        entries_table = catalog.load_table(f"{args.namespace}.entries")
        import pyarrow.compute as pc

        reviewed_count = 0
        total = 0
        for batch in entries_table.scan(
            selected_fields=("reviewed",)
        ).to_arrow_batch_reader():
            col = batch.column("reviewed")
            total += len(col)
            reviewed_count += pc.sum(pc.cast(col, "int64")).as_py()

        log(f"  Reviewed (Swiss-Prot): {reviewed_count:,}")
        log(f"  Unreviewed (TrEMBL):  {total - reviewed_count:,}")
    except Exception as e:
        log(f"  ERROR: {e}")

    log("\n" + "=" * 60)

    with open(args.output, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
