#!/usr/bin/env python3
"""
Production validation for the UniProtKB Iceberg data lake.

Designed for pharma and bioinformatics consumers who need absolute confidence
that no data was lost, corrupted, or malformed during the transform.

Uses the source JSONL(.zst) as ground truth and verifies the Iceberg tables
against it.  Exits 1 on ANY failure — Nextflow gates the provenance manifest
behind this.

Checks (in order):

  1. COMPLETENESS
     - JSONL line count == entries row count
     - sum(entries.feature_count) == features row count
     - sum(entries.xref_count) == xrefs row count
     - sum(entries.comment_count) == comments row count

  2. UNIQUENESS
     - entries.acc has zero duplicates

  3. NULL KEYS
     - acc, reviewed, taxid never null in any table

  4. REFERENTIAL INTEGRITY
     - Every acc in features exists in entries
     - Every acc in xrefs exists in entries
     - Every acc in comments exists in entries

  5. SORT ORDER
     - All tables sorted by (reviewed DESC, taxid ASC)

  6. ROUND-TRIP SPOT CHECK
     - Sample N accessions from the JSONL
     - For each, verify acc, taxid, seq_length, feature_count match Iceberg

  7. PARQUET FILE INTEGRITY
     - Every data file in the warehouse is a readable Parquet file

  8. SNAPSHOT SANITY
     - Each table has exactly 1 APPEND snapshot (no accidental double-writes)

Usage:
    validate_iceberg.py \
        --catalog-uri sqlite:///catalog.db \
        --warehouse /path/to/warehouse \
        --jsonl /path/to/uniprot.jsonl.zst \
        [--namespace uniprotkb] \
        [--spot-check-n 1000] \
        [-o validation_report.txt]

Exit codes:
    0  All checks passed
    1  One or more checks FAILED — do not publish this lake
"""

import os
import sys
import json
import random
import argparse
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table.snapshots import Operation


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# ─── JSONL ground truth helpers ────────────────────────────────────────

def count_jsonl_lines(jsonl_path: str) -> int:
    """Count lines in a JSONL(.zst) file without loading into memory."""
    count = 0
    if jsonl_path.endswith(".zst"):
        import zstandard as zstd
        dctx = zstd.ZstdDecompressor()
        with open(jsonl_path, "rb") as f:
            with dctx.stream_reader(f) as reader:
                buf = b""
                while True:
                    chunk = reader.read(16 * 1024 * 1024)  # 16 MB chunks
                    if not chunk:
                        if buf:
                            count += 1  # last line without trailing newline
                        break
                    buf += chunk
                    lines = buf.split(b"\n")
                    # Last element is incomplete (or empty if chunk ended on \n)
                    count += len(lines) - 1
                    buf = lines[-1]
    else:
        with open(jsonl_path, "rb") as f:
            for _ in f:
                count += 1
    return count


def _open_jsonl_lines(jsonl_path: str):
    """Yield text lines from a JSONL(.zst) file, handling decompression."""
    if jsonl_path.endswith(".zst"):
        import zstandard as zstd
        import io
        dctx = zstd.ZstdDecompressor()
        with open(jsonl_path, "rb") as f:
            with dctx.stream_reader(f) as reader:
                yield from io.TextIOWrapper(reader, encoding="utf-8")
    else:
        with open(jsonl_path) as f:
            yield from f


def sample_jsonl_entries(jsonl_path: str, n: int,
                         seed: int = 42) -> list[dict]:
    """Reservoir-sample N parsed entries from a JSONL(.zst) file.

    Uses reservoir sampling so we only stream the file once, and each entry
    has equal probability of being selected regardless of file order.
    """
    rng = random.Random(seed)
    reservoir = []
    idx = 0

    for line in _open_jsonl_lines(jsonl_path):
        line = line.strip()
        if not line:
            continue
        if idx < n:
            reservoir.append(json.loads(line))
        else:
            j = rng.randint(0, idx)
            if j < n:
                reservoir[j] = json.loads(line)
        idx += 1

    return reservoir


# ─── Validation framework ─────────────────────────────────────────────

class ValidationReport:
    """Accumulates pass/fail checks and produces a report."""

    def __init__(self):
        self.checks = []
        self.failures = 0

    def check(self, name: str, passed: bool, detail: str = ""):
        status = "PASS" if passed else "FAIL"
        if not passed:
            self.failures += 1
        entry = f"  [{status}] {name}"
        if detail:
            entry += f"  —  {detail}"
        self.checks.append(entry)
        # Also print to stderr for live monitoring on SLURM
        eprint(entry)

    def passed(self) -> bool:
        return self.failures == 0

    def summary(self) -> str:
        total = len(self.checks)
        passed = total - self.failures
        return (
            f"{passed}/{total} checks passed, "
            f"{self.failures} failed"
        )

    def full_report(self) -> str:
        lines = [
            "=" * 70,
            "UNIPROT ICEBERG DATA LAKE — PRODUCTION VALIDATION REPORT",
            f"Generated: {datetime.now(timezone.utc).isoformat()}",
            "=" * 70,
        ]
        for entry in self.checks:
            lines.append(entry)
        lines.append("")
        lines.append("─" * 70)
        verdict = "ALL CHECKS PASSED" if self.passed() else "VALIDATION FAILED"
        lines.append(f"VERDICT: {verdict}  ({self.summary()})")
        lines.append("─" * 70)
        return "\n".join(lines)


# ─── Individual check implementations ─────────────────────────────────

def _build_entry_acc_set(catalog, namespace):
    """Build a Python set of all entry accessions, streaming in batches.

    At production scale (~250M entries, ~110 bytes per acc string + set overhead),
    this uses ~27 GB — fits comfortably in 96 GB process memory.
    """
    entries = catalog.load_table(f"{namespace}.entries")
    acc_set = set()
    for batch in entries.scan(
        selected_fields=("acc",)
    ).to_arrow_batch_reader():
        acc_set.update(batch.column("acc").to_pylist())
    return acc_set


def check_completeness(report, catalog, namespace, jsonl_count):
    """Verify row counts match the JSONL ground truth."""
    report.checks.append("\n--- 1. COMPLETENESS ---")
    eprint("\n--- 1. COMPLETENESS ---")

    # entries count == JSONL lines
    entries = catalog.load_table(f"{namespace}.entries")
    snap = entries.current_snapshot()
    entries_count = int(snap.summary["total-records"]) if snap else 0
    report.check(
        "entries count == JSONL line count",
        entries_count == jsonl_count,
        f"entries={entries_count:,}, jsonl={jsonl_count:,}"
    )

    # Child table count consistency via entries aggregate columns
    for child_name, count_col in [
        ("features", "feature_count"),
        ("xrefs", "xref_count"),
        ("comments", "comment_count"),
    ]:
        # Sum the count column from entries
        agg_sum = 0
        for batch in entries.scan(
            selected_fields=(count_col,)
        ).to_arrow_batch_reader():
            s = pc.sum(batch.column(count_col)).as_py()
            if s is not None:
                agg_sum += s

        # Get child table row count
        child = catalog.load_table(f"{namespace}.{child_name}")
        child_snap = child.current_snapshot()
        child_count = int(child_snap.summary["total-records"]) if child_snap else 0

        report.check(
            f"sum(entries.{count_col}) == {child_name} rows",
            agg_sum == child_count,
            f"sum={agg_sum:,}, {child_name}={child_count:,}"
        )


def check_uniqueness(report, catalog, namespace):
    """Verify entries.acc has no duplicates.

    Streams batches and maintains a running set — bounded by O(unique accs)
    which is ~27 GB for 250M entries.  Never materialises full columns.
    """
    report.checks.append("\n--- 2. UNIQUENESS ---")
    eprint("\n--- 2. UNIQUENESS ---")

    entries = catalog.load_table(f"{namespace}.entries")
    seen = set()
    total = 0
    dupes = 0

    for batch in entries.scan(
        selected_fields=("acc",)
    ).to_arrow_batch_reader():
        accs = batch.column("acc").to_pylist()
        total += len(accs)
        for acc in accs:
            if acc in seen:
                dupes += 1
            else:
                seen.add(acc)

    report.check(
        "entries.acc is unique (no duplicates)",
        dupes == 0,
        f"total={total:,}, unique={len(seen):,}, dupes={dupes:,}"
    )


def check_null_keys(report, catalog, namespace):
    """Verify critical columns are never null.

    Streams batches and accumulates null counts — never materialises
    full columns.  Memory cost is O(1) per table.
    """
    report.checks.append("\n--- 3. NULL KEYS ---")
    eprint("\n--- 3. NULL KEYS ---")

    key_checks = [
        ("entries",  ["acc", "reviewed", "taxid"]),
        ("features", ["acc", "reviewed", "taxid", "type"]),
        ("xrefs",    ["acc", "reviewed", "taxid", "database", "id"]),
        ("comments", ["acc", "reviewed", "taxid", "comment_type"]),
    ]

    for table_name, columns in key_checks:
        table = catalog.load_table(f"{namespace}.{table_name}")
        null_counts = {col: 0 for col in columns}

        for batch in table.scan(
            selected_fields=tuple(columns)
        ).to_arrow_batch_reader():
            for col_name in columns:
                null_counts[col_name] += batch.column(col_name).null_count

        for col_name in columns:
            nc = null_counts[col_name]
            report.check(
                f"{table_name}.{col_name} has no nulls",
                nc == 0,
                f"null_count={nc:,}" if nc > 0 else ""
            )


def check_referential_integrity(report, catalog, namespace):
    """Verify every acc in child tables exists in entries.

    Builds the entry acc set once (~27 GB for 250M entries), then streams
    each child table batch-by-batch checking membership.  Child table accs
    are never fully materialised — only one batch at a time.
    """
    report.checks.append("\n--- 4. REFERENTIAL INTEGRITY ---")
    eprint("\n--- 4. REFERENTIAL INTEGRITY ---")

    eprint("  Building entry accession set...")
    entries_accs = _build_entry_acc_set(catalog, namespace)
    eprint(f"  Entry acc set: {len(entries_accs):,} accessions")

    for child_name in ["features", "xrefs", "comments"]:
        child = catalog.load_table(f"{namespace}.{child_name}")
        orphans = set()

        for batch in child.scan(
            selected_fields=("acc",)
        ).to_arrow_batch_reader():
            for acc in batch.column("acc").to_pylist():
                if acc not in entries_accs:
                    orphans.add(acc)

        report.check(
            f"all {child_name}.acc exist in entries",
            len(orphans) == 0,
            f"{len(orphans):,} orphan accessions" if orphans else ""
        )
        if orphans:
            examples = sorted(orphans)[:5]
            report.checks.append(f"    orphan examples: {examples}")


def check_sort_order(report, catalog, namespace):
    """Verify all tables are sorted by (reviewed DESC, taxid ASC).

    Swiss-Prot (reviewed=True) entries come first, then TrEMBL (reviewed=False),
    each group sorted by taxid ascending.

    Uses vectorised Arrow compute within each batch and scalar boundary
    checks between batches.  Memory is bounded to one batch at a time.
    """
    report.checks.append("\n--- 5. SORT ORDER ---")
    eprint("\n--- 5. SORT ORDER ---")

    for table_name in ["entries", "features", "xrefs", "comments"]:
        table = catalog.load_table(f"{namespace}.{table_name}")
        is_sorted = True
        disorder_detail = ""
        row_offset = 0
        prev_last_reviewed = None
        prev_last_taxid = None

        for batch in table.scan(
            selected_fields=("reviewed", "taxid")
        ).to_arrow_batch_reader():
            n = batch.num_rows
            if n == 0:
                continue

            reviewed = batch.column("reviewed")
            taxids = batch.column("taxid")

            # Check boundary between previous batch and this batch
            if prev_last_reviewed is not None:
                first_reviewed = reviewed[0].as_py()
                first_taxid = taxids[0].as_py()
                prev_key = (not prev_last_reviewed, prev_last_taxid)
                curr_key = (not first_reviewed, first_taxid)
                if prev_key > curr_key:
                    is_sorted = False
                    disorder_detail = (
                        f"disorder at row {row_offset}: "
                        f"reviewed={prev_last_reviewed}→{first_reviewed}, "
                        f"taxid={prev_last_taxid}→{first_taxid}"
                    )
                    break

            # Vectorised within-batch check:
            # Sort key is (NOT reviewed, taxid).  The sequence is sorted iff
            # for every consecutive pair (i, i+1):
            #   (NOT reviewed[i], taxid[i]) <= (NOT reviewed[i+1], taxid[i+1])
            #
            # This is equivalent to: there is NO i where
            #   reviewed[i] < reviewed[i+1]  (True→False is OK, False→True is not)
            #   OR (reviewed[i] == reviewed[i+1] AND taxid[i] > taxid[i+1])
            if n > 1:
                rev_prev = reviewed.slice(0, n - 1)
                rev_next = reviewed.slice(1, n - 1)
                tax_prev = taxids.slice(0, n - 1)
                tax_next = taxids.slice(1, n - 1)

                # reviewed went from False to True (disorder in DESC)
                rev_disorder = pc.and_(
                    pc.invert(rev_prev),
                    rev_next,
                )
                # Same reviewed, but taxid decreased (disorder in ASC)
                tax_disorder = pc.and_(
                    pc.equal(rev_prev, rev_next),
                    pc.greater(tax_prev, tax_next),
                )
                any_disorder = pc.or_(rev_disorder, tax_disorder)

                if pc.any(any_disorder).as_py():
                    # Find first disorder index for reporting
                    idx = pc.index(any_disorder, True).as_py()
                    r0 = reviewed[idx].as_py()
                    r1 = reviewed[idx + 1].as_py()
                    t0 = taxids[idx].as_py()
                    t1 = taxids[idx + 1].as_py()
                    is_sorted = False
                    disorder_detail = (
                        f"disorder at row {row_offset + idx + 1}: "
                        f"reviewed={r0}→{r1}, taxid={t0}→{t1}"
                    )
                    break

            prev_last_reviewed = reviewed[n - 1].as_py()
            prev_last_taxid = taxids[n - 1].as_py()
            row_offset += n

        report.check(
            f"{table_name} sorted by (reviewed DESC, taxid ASC)",
            is_sorted,
            disorder_detail
        )


def check_round_trip(report, catalog, namespace, jsonl_path, n):
    """Spot-check N entries against the JSONL ground truth."""
    report.checks.append(f"\n--- 6. ROUND-TRIP SPOT CHECK (n={n}) ---")
    eprint(f"\n--- 6. ROUND-TRIP SPOT CHECK (n={n}) ---")

    eprint(f"  Sampling {n} entries from JSONL (reservoir sampling)...")
    t0 = time.time()
    sampled = sample_jsonl_entries(jsonl_path, n)
    eprint(f"  Sampled {len(sampled)} entries in {time.time()-t0:.1f}s")

    if not sampled:
        report.check("round-trip sample non-empty", False, "no entries sampled")
        return

    # Build lookup from JSONL: acc → {taxid, seq_length, feature_count}
    jsonl_lookup = {}
    for entry in sampled:
        acc = entry.get("primaryAccession")
        if not acc:
            continue
        taxid = entry.get("organism", {}).get("taxonId")
        seq_len = entry.get("sequence", {}).get("length")
        if seq_len is not None:
            seq_len = int(seq_len)
        features = entry.get("features", [])
        feature_count = len(features) if features else 0
        xrefs = entry.get("uniProtKBCrossReferences", [])
        xref_count = len(xrefs) if xrefs else 0
        comments = entry.get("comments", [])
        comment_count = len(comments) if comments else 0

        jsonl_lookup[acc] = {
            "taxid": taxid,
            "seq_length": seq_len,
            "feature_count": feature_count,
            "xref_count": xref_count,
            "comment_count": comment_count,
        }

    # Read matching entries from Iceberg
    entries_table = catalog.load_table(f"{namespace}.entries")
    fields = ("acc", "taxid", "seq_length", "feature_count",
              "xref_count", "comment_count")
    iceberg_lookup = {}
    for batch in entries_table.scan(selected_fields=fields).to_arrow_batch_reader():
        accs = batch.column("acc").to_pylist()
        taxids = batch.column("taxid").to_pylist()
        seq_lens = batch.column("seq_length").to_pylist()
        fc = batch.column("feature_count").to_pylist()
        xc = batch.column("xref_count").to_pylist()
        cc = batch.column("comment_count").to_pylist()
        for i, acc in enumerate(accs):
            if acc in jsonl_lookup:
                iceberg_lookup[acc] = {
                    "taxid": taxids[i],
                    "seq_length": seq_lens[i],
                    "feature_count": fc[i],
                    "xref_count": xc[i],
                    "comment_count": cc[i],
                }

    # Check that all sampled accessions were found
    missing = set(jsonl_lookup.keys()) - set(iceberg_lookup.keys())
    report.check(
        f"all {len(jsonl_lookup)} sampled accessions found in Iceberg",
        len(missing) == 0,
        f"{len(missing)} missing" if missing else ""
    )
    if missing:
        report.checks.append(f"    missing examples: {sorted(missing)[:5]}")

    # Field-level comparison
    mismatches = {"taxid": 0, "seq_length": 0, "feature_count": 0,
                  "xref_count": 0, "comment_count": 0}
    mismatch_examples = {}

    for acc, expected in jsonl_lookup.items():
        if acc not in iceberg_lookup:
            continue
        actual = iceberg_lookup[acc]
        for field in mismatches:
            if expected[field] != actual[field]:
                mismatches[field] += 1
                if field not in mismatch_examples:
                    mismatch_examples[field] = (
                        acc, expected[field], actual[field]
                    )

    for field, count in mismatches.items():
        detail = ""
        if count > 0 and field in mismatch_examples:
            acc, exp, act = mismatch_examples[field]
            detail = f"{count} mismatches, e.g. {acc}: expected={exp}, got={act}"
        report.check(
            f"round-trip {field} matches JSONL",
            count == 0,
            detail
        )


def check_parquet_integrity(report, warehouse):
    """Verify every Parquet file in the warehouse is readable."""
    report.checks.append("\n--- 7. PARQUET FILE INTEGRITY ---")
    eprint("\n--- 7. PARQUET FILE INTEGRITY ---")

    total_files = 0
    corrupt_files = []

    for root, dirs, files in os.walk(warehouse):
        for fname in files:
            if not fname.endswith(".parquet"):
                continue
            total_files += 1
            fpath = os.path.join(root, fname)
            try:
                meta = pq.read_metadata(fpath)
                # Also verify we can read the schema
                pq.read_schema(fpath)
            except Exception as e:
                corrupt_files.append((fpath, str(e)))

    report.check(
        f"all {total_files} Parquet files readable",
        len(corrupt_files) == 0,
        f"{len(corrupt_files)} corrupt" if corrupt_files else ""
    )
    for fpath, err in corrupt_files[:5]:
        report.checks.append(f"    CORRUPT: {fpath} — {err}")


def check_snapshots(report, catalog, namespace):
    """Verify each table has exactly 1 APPEND snapshot."""
    report.checks.append("\n--- 8. SNAPSHOT SANITY ---")
    eprint("\n--- 8. SNAPSHOT SANITY ---")

    for table_name in ["entries", "features", "xrefs", "comments"]:
        table = catalog.load_table(f"{namespace}.{table_name}")
        snapshots = list(table.metadata.snapshots)
        append_snapshots = [
            s for s in snapshots
            if s.summary and s.summary.operation == Operation.APPEND
        ]
        report.check(
            f"{table_name} has exactly 1 APPEND snapshot",
            len(append_snapshots) == 1,
            f"got {len(append_snapshots)}" if len(append_snapshots) != 1 else ""
        )


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Production validation for UniProtKB Iceberg data lake"
    )
    parser.add_argument(
        "--catalog-uri", required=True,
        help="SQLite catalog URI (e.g. sqlite:///catalog.db)",
    )
    parser.add_argument(
        "--warehouse", required=True,
        help="Iceberg warehouse directory",
    )
    parser.add_argument(
        "--jsonl", required=True,
        help="Source JSONL(.zst) file — ground truth for validation",
    )
    parser.add_argument("--namespace", default="uniprotkb")
    parser.add_argument(
        "--spot-check-n", type=int, default=1000,
        help="Number of entries to spot-check against JSONL (default: 1000)",
    )
    parser.add_argument("-o", "--output", default="validation_report.txt")
    args = parser.parse_args()

    t_start = time.time()
    eprint("=" * 70)
    eprint("PRODUCTION VALIDATION — UniProtKB Iceberg Data Lake")
    eprint("=" * 70)

    # Connect to catalog
    catalog = SqlCatalog(
        "uniprot",
        **{"uri": args.catalog_uri, "warehouse": args.warehouse},
    )

    report = ValidationReport()

    # ── 1. Count JSONL lines (ground truth) ──
    eprint("\nCounting JSONL lines (ground truth)...")
    t0 = time.time()
    jsonl_count = count_jsonl_lines(args.jsonl)
    eprint(f"  JSONL: {jsonl_count:,} lines ({time.time()-t0:.1f}s)")

    # ── Run all checks ──
    check_completeness(report, catalog, args.namespace, jsonl_count)
    check_uniqueness(report, catalog, args.namespace)
    check_null_keys(report, catalog, args.namespace)
    check_referential_integrity(report, catalog, args.namespace)
    check_sort_order(report, catalog, args.namespace)
    check_round_trip(
        report, catalog, args.namespace,
        args.jsonl, args.spot_check_n,
    )
    check_parquet_integrity(report, args.warehouse)
    check_snapshots(report, catalog, args.namespace)

    # ── Write report ──
    elapsed = time.time() - t_start
    report_text = report.full_report()
    report_text += f"\n\nValidation completed in {elapsed:.1f}s"

    with open(args.output, "w") as f:
        f.write(report_text + "\n")

    eprint(f"\n{'=' * 70}")
    eprint(f"VERDICT: {'ALL CHECKS PASSED' if report.passed() else 'VALIDATION FAILED'}")
    eprint(f"  {report.summary()}")
    eprint(f"  Report: {args.output}")
    eprint(f"  Elapsed: {elapsed:.1f}s")
    eprint(f"{'=' * 70}")

    sys.exit(0 if report.passed() else 1)


if __name__ == "__main__":
    main()
