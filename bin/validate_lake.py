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
     - sum(entries.reference_count) == publications row count

  2. UNIQUENESS
     - entries.acc has zero duplicates

  3. NULL KEYS AND EMPTY STRINGS
     - acc, reviewed, taxid never null in any table
     - Identity columns (acc, id, sequence) never empty strings

  4. REFERENTIAL INTEGRITY
     - Every acc in features exists in entries
     - Every acc in xrefs exists in entries
     - Every acc in comments exists in entries
     - Every acc in publications exists in entries

  5. SORT ORDER
     - All tables sorted by (reviewed DESC, taxid ASC, acc ASC)

  6. ROUND-TRIP SPOT CHECK
     - Sample N accessions from the JSONL
     - For each, verify acc, taxid, seq_length, feature_count match lake

  7. PARQUET FILE INTEGRITY
     - Every data file in the lake is a readable Parquet file

  8. MANIFEST CONSISTENCY
     - manifest.json file lists match actual files on disk

  9. DENORMALIZED COLUMN SYNC
     - taxid and from_reviewed in child tables match entries

  10. SEQUENCE INTEGRITY
      - len(sequence) == seq_length for every entry
      - No entries with seq_length == 0

  11. FEATURE COORDINATE BOUNDARIES
      - start_pos <= end_pos where both are non-null

  12. SCHEMA TYPE PROTECTION
      - Critical columns have expected Arrow types (not silently cast)

  13. SCHEMA EVOLUTION GUARD
      - Inferred Parquet schema matches committed baseline
      - Detects renamed/dropped/new fields from upstream JSON changes

Usage:
    validate_lake.py \
        --lake /path/to/lake \
        --jsonl /path/to/uniprot.jsonl.zst \
        [--spot-check-n 1000] \
        [--schema-baseline /path/to/schema_baseline.json] \
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
    """Count lines in a JSONL(.zst) file without loading into memory.

    Counts newline characters in the stream.  A trailing newline (the
    normal case) does not produce an extra count because the empty
    string after it is never counted as a line.
    """
    count = 0
    if jsonl_path.endswith(".zst"):
        import zstandard as zstd
        dctx = zstd.ZstdDecompressor()
        with open(jsonl_path, "rb") as f:
            with dctx.stream_reader(f) as reader:
                while True:
                    chunk = reader.read(16 * 1024 * 1024)  # 16 MB chunks
                    if not chunk:
                        break
                    count += chunk.count(b"\n")
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
        ("publications", "reference_count"),
    ]:
        child_ds = open_table(lake_dir, child_name)
        child_count = count_rows(child_ds)

        report.check(
            f"sum(entries.{count_col}) == {child_name} rows",
            agg_sums[count_col] == child_count,
            f"sum={agg_sums[count_col]:,}, {child_name}={child_count:,}"
        )


def check_uniqueness(report, lake_dir):
    """Verify entries.acc has no duplicates.  Returns (total, unique_count).

    Uses DuckDB aggregation instead of materializing 250M accessions into a
    Python set (~2.5 GB heap), which would pressure the GC and starve
    downstream DuckDB queries of memory.
    """
    report.checks.append("\n--- 2. UNIQUENESS ---")
    eprint("\n--- 2. UNIQUENESS ---")

    import duckdb
    entries_path = os.path.join(lake_dir, "entries", "*.parquet")
    row = duckdb.sql(f"""
        SELECT count(*) AS total, count(DISTINCT acc) AS unique_count
        FROM read_parquet('{entries_path}')
    """).fetchone()
    total, unique_count = row
    dupes = total - unique_count

    report.check(
        "entries.acc is unique (no duplicates)",
        dupes == 0,
        f"total={total:,}, unique={unique_count:,}, dupes={dupes:,}"
    )
    return total, unique_count


def check_null_keys(report, lake_dir):
    """Verify critical columns are never null and identity columns have no empty strings."""
    report.checks.append("\n--- 3. NULL KEYS AND EMPTY STRINGS ---")
    eprint("\n--- 3. NULL KEYS AND EMPTY STRINGS ---")

    key_checks = [
        ("entries",    ["acc", "reviewed", "taxid", "entry_type"]),
        ("features",   ["acc", "from_reviewed", "taxid", "type"]),
        ("xrefs",      ["acc", "from_reviewed", "taxid", "database", "id"]),
        ("comments",   ["acc", "from_reviewed", "taxid", "comment_type"]),
        ("publications", ["acc", "from_reviewed", "taxid", "citation_type", "reference_number"]),
    ]

    # Identity columns that must never be empty strings
    empty_string_checks = [
        ("entries",    ["acc", "id", "sequence"]),
        ("features",   ["acc"]),
        ("xrefs",      ["acc", "id"]),
        ("comments",   ["acc"]),
        ("publications", ["acc"]),
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

    for table_name, columns in empty_string_checks:
        dataset = open_table(lake_dir, table_name)
        empty_counts = {col: 0 for col in columns}

        for batch in dataset.to_batches(columns=columns):
            for col_name in columns:
                empty_counts[col_name] += pc.sum(
                    pc.equal(batch.column(col_name), "")
                ).as_py()

        for col_name in columns:
            ec = empty_counts[col_name]
            report.check(
                f"{table_name}.{col_name} has no empty strings",
                ec == 0,
                f"empty_count={ec:,}" if ec > 0 else ""
            )


def check_referential_integrity(report, lake_dir, entry_count):
    """Verify every acc in child tables exists in entries.

    Uses DuckDB anti-joins instead of Python iteration for scalability
    (the xrefs table can exceed 16B rows at production scale).
    """
    report.checks.append("\n--- 4. REFERENTIAL INTEGRITY ---")
    eprint("\n--- 4. REFERENTIAL INTEGRITY ---")
    eprint(f"  Entries: {entry_count:,} unique accessions")

    import duckdb
    entries_path = os.path.join(lake_dir, "entries", "*.parquet")

    for child_name in ["features", "xrefs", "comments", "publications"]:
        child_path = os.path.join(lake_dir, child_name, "*.parquet")
        t0 = time.time()
        try:
            result = duckdb.sql(f"""
                SELECT DISTINCT c.acc
                FROM read_parquet('{child_path}') c
                LEFT JOIN read_parquet('{entries_path}') e ON c.acc = e.acc
                WHERE e.acc IS NULL
                LIMIT 100
            """).fetchall()
            orphans = [row[0] for row in result]
            elapsed = time.time() - t0
            report.check(
                f"all {child_name}.acc exist in entries",
                len(orphans) == 0,
                f"{len(orphans):,}+ orphan accessions ({elapsed:.1f}s)" if orphans else f"({elapsed:.1f}s)"
            )
            if orphans:
                report.checks.append(f"    orphan examples: {orphans[:5]}")
        except Exception as e:
            report.check(
                f"all {child_name}.acc exist in entries",
                False,
                f"DuckDB error: {e}"
            )


def check_sort_order(report, lake_dir):
    """Verify all tables are sorted by (reviewed/from_reviewed DESC, taxid ASC, acc ASC)."""
    report.checks.append("\n--- 5. SORT ORDER ---")
    eprint("\n--- 5. SORT ORDER ---")

    sort_check_tables = [
        ("entries",    "reviewed"),
        ("features",   "from_reviewed"),
        ("xrefs",      "from_reviewed"),
        ("comments",   "from_reviewed"),
        ("publications", "from_reviewed"),
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

    # Read matching entries from lake via DuckDB (avoids scanning all 250M rows in Python)
    import duckdb
    entries_path = os.path.join(lake_dir, "entries", "*.parquet")
    acc_list = list(jsonl_lookup.keys())
    lake_lookup = {}
    try:
        rows = duckdb.sql(f"""
            SELECT acc, taxid, seq_length, feature_count,
                   xref_count, comment_count, reference_count
            FROM read_parquet('{entries_path}')
            WHERE acc IN ({','.join("'" + a.replace("'", "''") + "'" for a in acc_list)})
        """).fetchall()
        for row in rows:
            lake_lookup[row[0]] = {
                "taxid": row[1],
                "seq_length": row[2],
                "feature_count": row[3],
                "xref_count": row[4],
                "comment_count": row[5],
                "reference_count": row[6],
            }
    except Exception as e:
        report.check("round-trip lake lookup", False, f"DuckDB error: {e}")
        return

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

    for table_name in ["entries", "features", "xrefs", "comments", "publications"]:
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
    for table_name in ["entries", "features", "xrefs", "comments", "publications"]:
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


def check_denormalized_sync(report, lake_dir):
    """Verify that denormalized parent columns in child tables match entries.

    Uses DuckDB joins instead of Python dicts for scalability (entries alone
    is ~250M rows; materializing a Python dict would need ~25 GB heap).
    """
    report.checks.append("\n--- 9. DENORMALIZED COLUMN SYNC ---")
    eprint("\n--- 9. DENORMALIZED COLUMN SYNC ---")

    import duckdb
    entries_path = os.path.join(lake_dir, "entries", "*.parquet")

    for child_name in ["features", "xrefs", "comments", "publications"]:
        child_path = os.path.join(lake_dir, child_name, "*.parquet")
        t0 = time.time()
        try:
            result = duckdb.sql(f"""
                SELECT
                    count(*) FILTER (WHERE c.taxid != e.taxid) AS taxid_mismatches,
                    count(*) FILTER (WHERE c.from_reviewed != e.reviewed) AS reviewed_mismatches
                FROM read_parquet('{child_path}') c
                JOIN read_parquet('{entries_path}') e ON c.acc = e.acc
            """).fetchone()
            taxid_mismatches = result[0]
            reviewed_mismatches = result[1]
            elapsed = time.time() - t0

            report.check(
                f"{child_name}.taxid matches entries.taxid",
                taxid_mismatches == 0,
                f"{taxid_mismatches:,} mismatches ({elapsed:.1f}s)" if taxid_mismatches else f"({elapsed:.1f}s)"
            )
            report.check(
                f"{child_name}.from_reviewed matches entries.reviewed",
                reviewed_mismatches == 0,
                f"{reviewed_mismatches:,} mismatches" if reviewed_mismatches else ""
            )
        except Exception as e:
            report.check(
                f"{child_name} denormalized sync",
                False,
                f"DuckDB error: {e}"
            )


def check_sequence_integrity(report, lake_dir):
    """Verify sequence string length equals seq_length integer."""
    report.checks.append("\n--- 10. SEQUENCE INTEGRITY ---")
    eprint("\n--- 10. SEQUENCE INTEGRITY ---")

    entries_ds = open_table(lake_dir, "entries")
    mismatches = 0
    zero_length = 0
    mismatch_examples = []

    for batch in entries_ds.to_batches(columns=["acc", "sequence", "seq_length"]):
        seq_lens = batch.column("seq_length")
        str_lens = pc.utf8_length(batch.column("sequence"))
        is_equal = pc.equal(seq_lens, str_lens)
        n_bad = pc.sum(pc.invert(is_equal)).as_py()
        if n_bad > 0:
            mismatches += n_bad
            if len(mismatch_examples) < 3:
                accs = batch.column("acc").to_pylist()
                sls = seq_lens.to_pylist()
                stls = str_lens.to_pylist()
                for i, eq in enumerate(is_equal.to_pylist()):
                    if not eq and len(mismatch_examples) < 3:
                        mismatch_examples.append(
                            f"{accs[i]}: seq_length={sls[i]}, "
                            f"len(sequence)={stls[i]}"
                        )

        zero_length += pc.sum(pc.equal(seq_lens, 0)).as_py()

    report.check(
        "len(sequence) == seq_length for all entries",
        mismatches == 0,
        f"{mismatches:,} mismatches" if mismatches else ""
    )
    for ex in mismatch_examples:
        report.checks.append(f"    {ex}")

    report.check(
        "no entries with seq_length == 0",
        zero_length == 0,
        f"{zero_length:,} entries with seq_length=0" if zero_length else ""
    )


def check_feature_coordinates(report, lake_dir):
    """Verify feature start_pos <= end_pos where both are non-null."""
    report.checks.append("\n--- 11. FEATURE COORDINATE BOUNDARIES ---")
    eprint("\n--- 11. FEATURE COORDINATE BOUNDARIES ---")

    features_ds = open_table(lake_dir, "features")
    inverted = 0
    inversion_examples = []

    for batch in features_ds.to_batches(
        columns=["acc", "type", "start_pos", "end_pos"]
    ):
        starts = batch.column("start_pos")
        ends = batch.column("end_pos")
        # Only check where both are non-null
        both_present = pc.and_(
            pc.is_valid(starts),
            pc.is_valid(ends),
        )
        start_gt_end = pc.and_(
            both_present,
            pc.greater(starts, ends),
        )
        n_bad = pc.sum(start_gt_end).as_py()
        if n_bad > 0:
            inverted += n_bad
            if len(inversion_examples) < 3:
                accs = batch.column("acc").to_pylist()
                types = batch.column("type").to_pylist()
                ss = starts.to_pylist()
                es = ends.to_pylist()
                for i, bad in enumerate(start_gt_end.to_pylist()):
                    if bad and len(inversion_examples) < 3:
                        inversion_examples.append(
                            f"{accs[i]} ({types[i]}): "
                            f"start={ss[i]} > end={es[i]}"
                        )

    report.check(
        "features: start_pos <= end_pos (where both non-null)",
        inverted == 0,
        f"{inverted:,} inverted coordinates" if inverted else ""
    )
    for ex in inversion_examples:
        report.checks.append(f"    {ex}")


def check_schema_types(report, lake_dir):
    """Verify critical columns have expected Arrow types after schema inference."""
    report.checks.append("\n--- 12. SCHEMA TYPE PROTECTION ---")
    eprint("\n--- 12. SCHEMA TYPE PROTECTION ---")

    # Expected types for critical columns (Arrow type string prefixes)
    # Using startswith() to allow int32/int64 flexibility
    expected_types = {
        "entries": {
            "acc":              "string",
            "reviewed":         "bool",
            "taxid":            "int",
            "seq_length":       "int",
            "sequence":         "string",
            "annotation_score": ("double", "float"),
        },
        "features": {
            "acc":              "string",
            "from_reviewed":    "bool",
            "taxid":            "int",
            "type":             "string",
            "start_pos":        "int",
            "end_pos":          "int",
        },
        "xrefs": {
            "acc":              "string",
            "from_reviewed":    "bool",
            "taxid":            "int",
            "database":         "string",
            "id":               "string",
        },
    }

    for table_name, columns in expected_types.items():
        dataset = open_table(lake_dir, table_name)
        schema = dataset.schema
        for col_name, expected in columns.items():
            idx = schema.get_field_index(col_name)
            if idx == -1:
                report.check(
                    f"{table_name}.{col_name} exists in schema",
                    False, "column missing"
                )
                continue
            actual_type = str(schema.field(idx).type)
            if isinstance(expected, tuple):
                matches = any(actual_type.startswith(e) for e in expected)
            else:
                matches = actual_type.startswith(expected)
            report.check(
                f"{table_name}.{col_name} type is {expected}",
                matches,
                f"actual={actual_type}" if not matches else ""
            )


def check_schema_evolution(report, lake_dir, baseline_path):
    """
    Detect upstream UniProtKB JSON schema changes by comparing inferred Parquet
    schema against a committed baseline.  Reports missing columns (ERROR) and
    new columns (WARNING) to alert on schema drift.
    """
    report.checks.append("\n--- 13. SCHEMA EVOLUTION GUARD ---")
    eprint("\n--- 13. SCHEMA EVOLUTION GUARD ---")

    if not os.path.exists(baseline_path):
        report.check(
            "schema baseline file exists",
            False,
            f"baseline not found at {baseline_path}"
        )
        return

    # Load baseline schema
    try:
        with open(baseline_path) as f:
            baseline = json.load(f)
    except Exception as e:
        report.check(
            "schema baseline file is valid JSON",
            False,
            f"error reading {baseline_path}: {e}"
        )
        return

    report.check("schema baseline file exists", True)

    # Compare each table's columns
    for table_name in ["entries", "features", "xrefs", "comments", "publications"]:
        if table_name not in baseline:
            eprint(f"  warning: {table_name} not in baseline, skipping")
            continue

        baseline_cols = set(baseline[table_name])
        dataset = open_table(lake_dir, table_name)
        schema = dataset.schema
        actual_cols = {field.name for field in schema}

        missing_cols = baseline_cols - actual_cols
        new_cols = actual_cols - baseline_cols

        # Missing columns are ERRORS (upstream field dropped or renamed)
        if missing_cols:
            missing_detail = ", ".join(sorted(missing_cols)[:5])
            report.check(
                f"{table_name}: no missing columns",
                False,
                f"{len(missing_cols)} expected but not found: {missing_detail}"
            )
        else:
            report.check(
                f"{table_name}: no missing columns",
                True
            )

        # New columns are WARNINGS (upstream added new field)
        if new_cols:
            report.checks.append(
                f"  [WARN] NEW in {table_name}: {sorted(new_cols)}"
            )
            new_detail = ", ".join(sorted(new_cols)[:5])
            report.checks.append(
                f"  [{len(new_cols)} new column(s): {new_detail}]"
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
    parser.add_argument(
        "--schema-baseline", default=None,
        help="Path to schema baseline JSON (enables schema evolution check)",
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
    entry_total, entry_unique = check_uniqueness(report, args.lake)
    check_null_keys(report, args.lake)
    check_referential_integrity(report, args.lake, entry_unique)
    check_sort_order(report, args.lake)
    check_round_trip(report, args.lake, args.jsonl, args.spot_check_n)
    check_parquet_integrity(report, args.lake)
    check_manifest(report, args.lake)
    check_denormalized_sync(report, args.lake)
    check_sequence_integrity(report, args.lake)
    check_feature_coordinates(report, args.lake)
    check_schema_types(report, args.lake)
    if args.schema_baseline:
        check_schema_evolution(report, args.lake, args.schema_baseline)

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
