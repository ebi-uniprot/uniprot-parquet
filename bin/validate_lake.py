#!/usr/bin/env python3
"""
Production validation for the UniProtKB Parquet data lake.

Designed for pharma and bioinformatics consumers who need absolute confidence
that no data was lost, corrupted, or malformed during the transform.

Uses the source JSONL(.zst) as ground truth and verifies the Parquet tables
against it.  Exits 1 on ANY failure — Nextflow gates the provenance manifest
behind this.

Checks (in order):

  1. COMPLETENESS
     - JSONL line count == entries row count
     - sum(entries.feature_count) == features row count
     - sum(entries.xref_count) == xrefs row count
     - sum(entries.comment_count) == comments row count
     - sum(entries.reference_count) == references row count

  2. UNIQUENESS
     - entries.acc has zero duplicates

  3. NULL KEYS
     - acc, reviewed, taxid never null in any table

  4. REFERENTIAL INTEGRITY
     - Every acc in features exists in entries
     - Every acc in xrefs exists in entries
     - Every acc in comments exists in entries
     - Every acc in references exists in entries

  5. SORT ORDER
     - All tables sorted by (reviewed DESC, taxid ASC, acc ASC)

  6. ROUND-TRIP SPOT CHECK
     - Sample N accessions from the JSONL
     - For each, verify acc, taxid, seq_length, feature_count match lake

  7. PARQUET FILE INTEGRITY
     - Every data file in the lake is a readable Parquet file

Usage:
    validate_lake.py \
        --lake /path/to/lake \
        --jsonl /path/to/uniprot.jsonl.zst \
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
import pyarrow.dataset as ds


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# ─── Parquet reading helpers ──────────────────────────────────────────

def open_table(lake_dir, table_name):
    """Open a Parquet dataset from the lake directory."""
    table_dir = os.path.join(lake_dir, table_name)
    if not os.path.isdir(table_dir):
        raise FileNotFoundError(f"Table directory not found: {table_dir}")
    return ds.dataset(table_dir, format="parquet")


def count_rows(dataset):
    """Count total rows in a Parquet dataset."""
    return dataset.count_rows()


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
    """Reservoir-sample N parsed entries from a JSONL(.zst) file."""
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
            "UNIPROT PARQUET DATA LAKE — PRODUCTION VALIDATION REPORT",
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


def check_completeness(report, lake_dir, jsonl_count):
    """Verify row counts match the JSONL ground truth."""
    report.checks.append("\n--- 1. COMPLETENESS ---")
    eprint("\n--- 1. COMPLETENESS ---")

    entries_ds = open_table(lake_dir, "entries")
    entries_count = count_rows(entries_ds)
    report.check(
        "entries count == JSONL line count",
        entries_count == jsonl_count,
        f"entries={entries_count:,}, jsonl={jsonl_count:,}"
    )

    # Sum all four count columns in a single scan of entries
    count_cols = ["feature_count", "xref_count", "comment_count", "reference_count"]
    agg_sums = {col: 0 for col in count_cols}
    for batch in entries_ds.to_batches(columns=count_cols):
        for col in count_cols:
            s = pc.sum(batch.column(col)).as_py()
            if s is not None:
                agg_sums[col] += s

    for child_name, count_col in [
        ("features", "feature_count"),
        ("xrefs", "xref_count"),
        ("comments", "comment_count"),
        ("references", "reference_count"),
    ]:
        child_ds = open_table(lake_dir, child_name)
        child_count = count_rows(child_ds)

        report.check(
            f"sum(entries.{count_col}) == {child_name} rows",
            agg_sums[count_col] == child_count,
            f"sum={agg_sums[count_col]:,}, {child_name}={child_count:,}"
        )


def check_uniqueness(report, lake_dir):
    """Verify entries.acc has no duplicates.  Returns the acc set."""
    report.checks.append("\n--- 2. UNIQUENESS ---")
    eprint("\n--- 2. UNIQUENESS ---")

    entries_ds = open_table(lake_dir, "entries")
    seen = set()
    total = 0
    dupes = 0

    for batch in entries_ds.to_batches(columns=["acc"]):
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
    return seen


def check_null_keys(report, lake_dir):
    """Verify critical columns are never null."""
    report.checks.append("\n--- 3. NULL KEYS ---")
    eprint("\n--- 3. NULL KEYS ---")

    key_checks = [
        ("entries",    ["acc", "reviewed", "taxid", "entry_type"]),
        ("features",   ["acc", "from_reviewed", "taxid", "type"]),
        ("xrefs",      ["acc", "from_reviewed", "taxid", "database", "id"]),
        ("comments",   ["acc", "from_reviewed", "taxid", "comment_type"]),
        ("references", ["acc", "from_reviewed", "taxid", "citation_type", "reference_number"]),
    ]

    for table_name, columns in key_checks:
        dataset = open_table(lake_dir, table_name)
        null_counts = {col: 0 for col in columns}

        for batch in dataset.to_batches(columns=columns):
            for col_name in columns:
                null_counts[col_name] += batch.column(col_name).null_count

        for col_name in columns:
            nc = null_counts[col_name]
            report.check(
                f"{table_name}.{col_name} has no nulls",
                nc == 0,
                f"null_count={nc:,}" if nc > 0 else ""
            )


def check_referential_integrity(report, lake_dir, entries_accs):
    """Verify every acc in child tables exists in entries."""
    report.checks.append("\n--- 4. REFERENTIAL INTEGRITY ---")
    eprint("\n--- 4. REFERENTIAL INTEGRITY ---")
    eprint(f"  Using entry acc set: {len(entries_accs):,} accessions")

    for child_name in ["features", "xrefs", "comments", "references"]:
        child_ds = open_table(lake_dir, child_name)
        orphans = set()

        for batch in child_ds.to_batches(columns=["acc"]):
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


def check_sort_order(report, lake_dir):
    """Verify all tables are sorted by (reviewed/from_reviewed DESC, taxid ASC, acc ASC)."""
    report.checks.append("\n--- 5. SORT ORDER ---")
    eprint("\n--- 5. SORT ORDER ---")

    sort_check_tables = [
        ("entries",    "reviewed"),
        ("features",   "from_reviewed"),
        ("xrefs",      "from_reviewed"),
        ("comments",   "from_reviewed"),
        ("references", "from_reviewed"),
    ]

    for table_name, rev_col in sort_check_tables:
        dataset = open_table(lake_dir, table_name)
        is_sorted = True
        disorder_detail = ""
        row_offset = 0
        prev_last_reviewed = None
        prev_last_taxid = None
        prev_last_acc = None

        for batch in dataset.to_batches(columns=[rev_col, "taxid", "acc"]):
            n = batch.num_rows
            if n == 0:
                continue

            reviewed = batch.column(rev_col)
            taxids = batch.column("taxid")
            accs = batch.column("acc")

            # Check boundary between previous batch and this batch
            if prev_last_reviewed is not None:
                first_reviewed = reviewed[0].as_py()
                first_taxid = taxids[0].as_py()
                first_acc = accs[0].as_py()
                prev_key = (not prev_last_reviewed, prev_last_taxid, prev_last_acc)
                curr_key = (not first_reviewed, first_taxid, first_acc)
                if prev_key > curr_key:
                    is_sorted = False
                    disorder_detail = (
                        f"disorder at row {row_offset}: "
                        f"{rev_col}={prev_last_reviewed}→{first_reviewed}, "
                        f"taxid={prev_last_taxid}→{first_taxid}, "
                        f"acc={prev_last_acc}→{first_acc}"
                    )
                    break

            # Vectorised within-batch check
            if n > 1:
                rev_prev = reviewed.slice(0, n - 1)
                rev_next = reviewed.slice(1, n - 1)
                tax_prev = taxids.slice(0, n - 1)
                tax_next = taxids.slice(1, n - 1)
                acc_prev = accs.slice(0, n - 1)
                acc_next = accs.slice(1, n - 1)

                rev_equal = pc.equal(rev_prev, rev_next)
                tax_equal = pc.equal(tax_prev, tax_next)

                rev_disorder = pc.and_(
                    pc.invert(rev_prev),
                    rev_next,
                )
                tax_disorder = pc.and_(
                    rev_equal,
                    pc.greater(tax_prev, tax_next),
                )
                acc_disorder = pc.and_(
                    pc.and_(rev_equal, tax_equal),
                    pc.greater(acc_prev, acc_next),
                )
                any_disorder = pc.or_(pc.or_(rev_disorder, tax_disorder), acc_disorder)

                if pc.any(any_disorder).as_py():
                    idx = pc.index(any_disorder, True).as_py()
                    r0 = reviewed[idx].as_py()
                    r1 = reviewed[idx + 1].as_py()
                    t0 = taxids[idx].as_py()
                    t1 = taxids[idx + 1].as_py()
                    a0 = accs[idx].as_py()
                    a1 = accs[idx + 1].as_py()
                    is_sorted = False
                    disorder_detail = (
                        f"disorder at row {row_offset + idx + 1}: "
                        f"{rev_col}={r0}→{r1}, taxid={t0}→{t1}, "
                        f"acc={a0}→{a1}"
                    )
                    break

            prev_last_reviewed = reviewed[n - 1].as_py()
            prev_last_taxid = taxids[n - 1].as_py()
            prev_last_acc = accs[n - 1].as_py()
            row_offset += n

        report.check(
            f"{table_name} sorted by ({rev_col} DESC, taxid ASC, acc ASC)",
            is_sorted,
            disorder_detail
        )


def check_round_trip(report, lake_dir, jsonl_path, n):
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

    # Build lookup from JSONL
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
        references = entry.get("references", [])
        reference_count = len(references) if references else 0

        jsonl_lookup[acc] = {
            "taxid": taxid,
            "seq_length": seq_len,
            "feature_count": feature_count,
            "xref_count": xref_count,
            "comment_count": comment_count,
            "reference_count": reference_count,
        }

    # Read matching entries from lake
    entries_ds = open_table(lake_dir, "entries")
    fields = ["acc", "taxid", "seq_length", "feature_count",
              "xref_count", "comment_count", "reference_count"]
    lake_lookup = {}
    for batch in entries_ds.to_batches(columns=fields):
        accs = batch.column("acc").to_pylist()
        taxids = batch.column("taxid").to_pylist()
        seq_lens = batch.column("seq_length").to_pylist()
        fc = batch.column("feature_count").to_pylist()
        xc = batch.column("xref_count").to_pylist()
        cc = batch.column("comment_count").to_pylist()
        rc = batch.column("reference_count").to_pylist()
        for i, acc in enumerate(accs):
            if acc in jsonl_lookup:
                lake_lookup[acc] = {
                    "taxid": taxids[i],
                    "seq_length": seq_lens[i],
                    "feature_count": fc[i],
                    "xref_count": xc[i],
                    "comment_count": cc[i],
                    "reference_count": rc[i],
                }

    missing = set(jsonl_lookup.keys()) - set(lake_lookup.keys())
    report.check(
        f"all {len(jsonl_lookup)} sampled accessions found in lake",
        len(missing) == 0,
        f"{len(missing)} missing" if missing else ""
    )
    if missing:
        report.checks.append(f"    missing examples: {sorted(missing)[:5]}")

    mismatches = {"taxid": 0, "seq_length": 0, "feature_count": 0,
                  "xref_count": 0, "comment_count": 0, "reference_count": 0}
    mismatch_examples = {}

    for acc, expected in jsonl_lookup.items():
        if acc not in lake_lookup:
            continue
        actual = lake_lookup[acc]
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


def check_parquet_integrity(report, lake_dir):
    """Verify every Parquet file in the lake is readable."""
    report.checks.append("\n--- 7. PARQUET FILE INTEGRITY ---")
    eprint("\n--- 7. PARQUET FILE INTEGRITY ---")

    total_files = 0
    corrupt_files = []

    for table_name in ["entries", "features", "xrefs", "comments", "references"]:
        table_dir = os.path.join(lake_dir, table_name)
        if not os.path.isdir(table_dir):
            continue
        for fname in os.listdir(table_dir):
            if not fname.endswith(".parquet"):
                continue
            total_files += 1
            fpath = os.path.join(table_dir, fname)
            try:
                pq.read_metadata(fpath)
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


def check_manifest(report, lake_dir):
    """Verify manifest.json exists and is consistent with actual files."""
    report.checks.append("\n--- 8. MANIFEST CONSISTENCY ---")
    eprint("\n--- 8. MANIFEST CONSISTENCY ---")

    manifest_path = os.path.join(lake_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        report.check("manifest.json exists", False, "file not found")
        return

    with open(manifest_path) as f:
        manifest = json.load(f)

    report.check("manifest.json exists", True)

    # Check each table's files match what's on disk
    for table_name in ["entries", "features", "xrefs", "comments", "references"]:
        table_info = manifest.get("tables", {}).get(table_name, {})
        manifest_files = set(table_info.get("files", []))
        table_dir = os.path.join(lake_dir, table_name)

        if os.path.isdir(table_dir):
            actual_files = {f for f in os.listdir(table_dir) if f.endswith(".parquet")}
        else:
            actual_files = set()

        report.check(
            f"{table_name} manifest files match disk",
            manifest_files == actual_files,
            f"manifest={len(manifest_files)}, disk={len(actual_files)}"
            if manifest_files != actual_files else ""
        )


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Production validation for UniProtKB Parquet data lake"
    )
    parser.add_argument(
        "--lake", required=True,
        help="Lake directory (contains entries/, features/, etc.)",
    )
    parser.add_argument(
        "--jsonl", required=True,
        help="Source JSONL(.zst) file — ground truth for validation",
    )
    parser.add_argument(
        "--spot-check-n", type=int, default=1000,
        help="Number of entries to spot-check against JSONL (default: 1000)",
    )
    parser.add_argument("-o", "--output", default="validation_report.txt")
    args = parser.parse_args()

    t_start = time.time()
    eprint("=" * 70)
    eprint("PRODUCTION VALIDATION — UniProtKB Parquet Data Lake")
    eprint("=" * 70)

    report = ValidationReport()

    # ── 1. Count JSONL lines (ground truth) ──
    eprint("\nCounting JSONL lines (ground truth)...")
    t0 = time.time()
    jsonl_count = count_jsonl_lines(args.jsonl)
    eprint(f"  JSONL: {jsonl_count:,} lines ({time.time()-t0:.1f}s)")

    # ── Run all checks ──
    check_completeness(report, args.lake, jsonl_count)
    entry_accs = check_uniqueness(report, args.lake)
    check_null_keys(report, args.lake)
    check_referential_integrity(report, args.lake, entry_accs)
    check_sort_order(report, args.lake)
    check_round_trip(report, args.lake, args.jsonl, args.spot_check_n)
    check_parquet_integrity(report, args.lake)
    check_manifest(report, args.lake)

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
