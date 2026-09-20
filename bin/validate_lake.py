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
     - every file's size and SHA-256 match manifest file_details

  9. DENORMALIZED COLUMN SYNC
     - taxid and reviewed in child tables match entries

  10. SEQUENCE INTEGRITY
      - len(sequence) == seq_length for every entry
      - No entries with seq_length == 0

  11. FEATURE COORDINATE BOUNDARIES
      - start_pos <= end_pos where both are non-null

  12. SCHEMA TYPE PROTECTION
      - Critical columns have expected Arrow types (not silently cast)

  13. FIELD COMPLETENESS
      - Every top-level JSON field is captured in entries or a child table
      - Catches silent data loss when UniProt adds new top-level fields

  14. SCHEMA EVOLUTION GUARD
      - Inferred Parquet schema matches committed baseline
      - Detects renamed/dropped/new fields from upstream JSON changes

  15. COMMENT TEXT
      - Every text-bearing comment type has a populated text_value

  16. RECONSTRUCTION
      - Sampled entries rebuilt from the five tables (bin/reconstruct.py)
        equal the source JSONL, order-independent inside arrays

  18. ACCESSION MAP
      - Primary rows == entries, secondary rows == sum(len(secondary_accs)),
        no duplicate (acc, primary_acc), every primary_acc exists,
        (reviewed, taxid) match entries  (17 is reserved for bloom filters,
        deferred: PyArrow 23 cannot write them)

  19. PARTITIONS
      - review_status=swissprot|trembl directories agree with the stored
        `reviewed` column (row-group statistics), per-partition row counts
        match the manifest and sum to row_count, swissprot files first

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
import hashlib
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

# Every table in the lake (kept in sync with parquet_transform.TABLE_DEFS).
ALL_TABLES = ["entries", "features", "xrefs", "comments", "publications", "accession_map"]


def _table_files(lake_dir, table_name):
    """Every Parquet file of a table, recursively (Hive partition dirs), sorted
    so review_status=swissprot files precede review_status=trembl files.

    os.walk rather than glob: it also descends into hidden directories such
    as a writer's leftover ``.tmp/``, exactly as DuckDB's ``**`` glob does,
    so check_manifest sees the same files a raw reader would and fails on
    any file the manifest does not list."""
    table_dir = os.path.join(lake_dir, table_name)
    return sorted(os.path.join(root, f)
                  for root, _, files in os.walk(table_dir)
                  for f in files if f.endswith(".parquet"))


def _table_glob(lake_dir, table_name):
    """DuckDB glob for a table: `**` crosses the partition directories (the
    form the README recommends, so the validator reads what users read)."""
    return os.path.join(lake_dir, table_name, "**", "*.parquet")


def open_table(lake_dir, table_name):
    """Open a table as a PyArrow dataset over its explicit, sorted file list
    (lazy, bounded memory).  No Hive columns are added: the validator sees
    exactly the stored schema."""
    table_dir = os.path.join(lake_dir, table_name)
    if not os.path.isdir(table_dir):
        raise FileNotFoundError(f"Table directory not found: {table_dir}")
    return ds.dataset(_table_files(lake_dir, table_name), format="parquet")


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
        self.checks = []          # formatted lines, including section headers
        self.records = []         # one dict per check, for validation_report.json
        self.failures = 0
        self.elapsed_s = None

    def check(self, name: str, passed: bool, detail: str = ""):
        status = "PASS" if passed else "FAIL"
        if not passed:
            self.failures += 1
        entry = f"  [{status}] {name}"
        if detail:
            entry += f"  —  {detail}"
        self.checks.append(entry)
        self.records.append({"name": name, "passed": bool(passed), "detail": detail})
        eprint(entry)

    def passed(self) -> bool:
        return self.failures == 0

    def summary(self) -> str:
        total = len(self.records)
        passed = total - self.failures
        return (
            f"{passed}/{total} checks passed, "
            f"{self.failures} failed"
        )

    def to_dict(self) -> dict:
        """Machine-readable form of the report (validation_report.json, plan F.2.1)."""
        return {
            "passed": self.passed(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": self.summary(),
            "elapsed_s": self.elapsed_s,
            "checks": list(self.records),
        }

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


def check_completeness(report, lake_dir, jsonl_count, expected_count=None):
    """Verify row counts match the JSONL ground truth.

    ``expected_count`` is the entry count recorded upstream of the sort
    (STREAM_JSONL's entry_count.txt): the sorted JSONL is the ground truth for
    every other check, so this is the one anchor that catches rows lost
    between the raw stream and the file being validated against."""
    report.checks.append("\n--- 1. COMPLETENESS ---")
    eprint("\n--- 1. COMPLETENESS ---")

    entries_ds = open_table(lake_dir, "entries")
    entries_count = count_rows(entries_ds)
    report.check(
        "entries count == JSONL line count",
        entries_count == jsonl_count,
        f"entries={entries_count:,}, jsonl={jsonl_count:,}"
    )
    if expected_count is not None:
        report.check(
            "JSONL line count and entries count == expected count",
            jsonl_count == expected_count == entries_count,
            f"expected={expected_count:,}, jsonl={jsonl_count:,}, entries={entries_count:,}"
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
    entries_path = _table_glob(lake_dir, "entries")
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
        ("features",   ["acc", "reviewed", "taxid", "type"]),
        ("xrefs",      ["acc", "reviewed", "taxid", "database", "id"]),
        ("comments",   ["acc", "reviewed", "taxid", "comment_type"]),
        ("publications", ["acc", "reviewed", "taxid", "citation_type", "reference_number"]),
        ("accession_map", ["acc", "primary_acc", "is_primary", "reviewed", "taxid"]),
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
                # pc.sum returns null on an empty batch (zero-row tables)
                empty_counts[col_name] += pc.sum(
                    pc.equal(batch.column(col_name), "")
                ).as_py() or 0

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
    entries_path = _table_glob(lake_dir, "entries")

    for child_name in ["features", "xrefs", "comments", "publications"]:
        child_path = _table_glob(lake_dir, child_name)
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


def _check_sorted(dataset, columns, key_fn):
    """Streaming sort check for (bool DESC, k2 ASC, k3 ASC) keys.

    ``columns`` names the three key columns (the first a boolean sorted DESC);
    ``key_fn(*values) -> tuple`` builds the comparable key used at batch
    boundaries.  Returns (is_sorted, detail)."""
    is_sorted = True
    disorder_detail = ""
    row_offset = 0
    prev_last = None

    for batch in dataset.to_batches(columns=list(columns)):
        n = batch.num_rows
        if n == 0:
            continue

        flags = batch.column(columns[0])
        k2 = batch.column(columns[1])
        k3 = batch.column(columns[2])

        # Check boundary between previous batch and this batch
        if prev_last is not None:
            first = (flags[0].as_py(), k2[0].as_py(), k3[0].as_py())
            if key_fn(*prev_last) > key_fn(*first):
                is_sorted = False
                disorder_detail = (
                    f"disorder at row {row_offset}: "
                    + ", ".join(f"{c}={a}→{b}" for c, a, b in zip(columns, prev_last, first))
                )
                break

        # Vectorised within-batch check
        if n > 1:
            f_prev, f_next = flags.slice(0, n - 1), flags.slice(1, n - 1)
            k2_prev, k2_next = k2.slice(0, n - 1), k2.slice(1, n - 1)
            k3_prev, k3_next = k3.slice(0, n - 1), k3.slice(1, n - 1)

            f_equal = pc.equal(f_prev, f_next)
            k2_equal = pc.equal(k2_prev, k2_next)

            f_disorder = pc.and_(pc.invert(f_prev), f_next)          # false → true
            k2_disorder = pc.and_(f_equal, pc.greater(k2_prev, k2_next))
            k3_disorder = pc.and_(pc.and_(f_equal, k2_equal), pc.greater(k3_prev, k3_next))
            any_disorder = pc.or_(pc.or_(f_disorder, k2_disorder), k3_disorder)

            if pc.any(any_disorder).as_py():
                idx = pc.index(any_disorder, True).as_py()
                is_sorted = False
                disorder_detail = (
                    f"disorder at row {row_offset + idx + 1}: "
                    + ", ".join(f"{c}={col[idx].as_py()}→{col[idx + 1].as_py()}"
                                for c, col in zip(columns, (flags, k2, k3)))
                )
                break

        prev_last = (flags[n - 1].as_py(), k2[n - 1].as_py(), k3[n - 1].as_py())
        row_offset += n

    return is_sorted, disorder_detail


def check_sort_order(report, lake_dir):
    """Verify every table is sorted by its declared order: (reviewed DESC,
    taxid ASC, acc ASC) for the five data tables, (reviewed DESC, acc ASC,
    primary_acc ASC) for accession_map."""
    report.checks.append("\n--- 5. SORT ORDER ---")
    eprint("\n--- 5. SORT ORDER ---")

    three_key = lambda r, t, a: (not r, t, a)  # noqa: E731
    sort_check_tables = [
        ("entries",       ["reviewed", "taxid", "acc"], three_key),
        ("features",      ["reviewed", "taxid", "acc"], three_key),
        ("xrefs",         ["reviewed", "taxid", "acc"], three_key),
        ("comments",      ["reviewed", "taxid", "acc"], three_key),
        ("publications",  ["reviewed", "taxid", "acc"], three_key),
        ("accession_map", ["reviewed", "acc", "primary_acc"], lambda r, a, p: (not r, a, p)),
    ]

    for table_name, columns, key_fn in sort_check_tables:
        dataset = open_table(lake_dir, table_name)
        is_sorted, disorder_detail = _check_sorted(dataset, columns, key_fn)
        report.check(
            f"{table_name} sorted by ({columns[0]} DESC, {columns[1]} ASC, {columns[2]} ASC)",
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
    entries_path = _table_glob(lake_dir, "entries")
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

    for table_name in ALL_TABLES:
        table_dir = os.path.join(lake_dir, table_name)
        if not os.path.isdir(table_dir):
            continue
        for fpath in _table_files(lake_dir, table_name):
            total_files += 1
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
    for table_name in ALL_TABLES:
        table_info = manifest.get("tables", {}).get(table_name, {})
        manifest_files = set(table_info.get("files", []))
        table_dir = os.path.join(lake_dir, table_name)

        if os.path.isdir(table_dir):
            actual_files = {os.path.relpath(f, table_dir) for f in _table_files(lake_dir, table_name)}
        else:
            actual_files = set()

        report.check(
            f"{table_name} manifest files match disk",
            manifest_files == actual_files,
            f"manifest={len(manifest_files)}, disk={len(actual_files)}"
            if manifest_files != actual_files else ""
        )

        # Per-file SHA-256 and size (plan F.2.1): "what we published is what we built".
        details = table_info.get("file_details") or {}
        bad = []
        for rel in sorted(manifest_files & actual_files):
            d = details.get(rel)
            path = os.path.join(table_dir, rel)
            if not d:
                bad.append(f"{rel}: no file_details")
                continue
            if os.path.getsize(path) != d.get("size_bytes"):
                bad.append(f"{rel}: size")
                continue
            h = hashlib.sha256()
            with open(path, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
            if h.hexdigest() != d.get("sha256"):
                bad.append(f"{rel}: sha256")
        n = len(manifest_files & actual_files)
        report.check(f"{table_name}: {n - len(bad)}/{n} file hashes match", not bad, "; ".join(bad[:3]))


def check_denormalized_sync(report, lake_dir):
    """Verify that denormalized parent columns in child tables match entries.

    Uses DuckDB joins instead of Python dicts for scalability (entries alone
    is ~250M rows; materializing a Python dict would need ~25 GB heap).
    """
    report.checks.append("\n--- 9. DENORMALIZED COLUMN SYNC ---")
    eprint("\n--- 9. DENORMALIZED COLUMN SYNC ---")

    import duckdb
    entries_path = _table_glob(lake_dir, "entries")

    for child_name in ["features", "xrefs", "comments", "publications"]:
        child_path = _table_glob(lake_dir, child_name)
        t0 = time.time()
        try:
            result = duckdb.sql(f"""
                SELECT
                    count(*) FILTER (WHERE c.taxid != e.taxid) AS taxid_mismatches,
                    count(*) FILTER (WHERE c.reviewed != e.reviewed) AS reviewed_mismatches
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
                f"{child_name}.reviewed matches entries.reviewed",
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
        n_bad = pc.sum(pc.invert(is_equal)).as_py() or 0
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

        zero_length += pc.sum(pc.equal(seq_lens, 0)).as_py() or 0

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
        n_bad = pc.sum(start_gt_end).as_py() or 0
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
            "reviewed":    "bool",
            "taxid":            "int",
            "type":             "string",
            "start_pos":        "int",
            "end_pos":          "int",
        },
        "xrefs": {
            "acc":              "string",
            "reviewed":    "bool",
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

    # ── Every declared column type (plan G.2) ──
    # COLUMN_TYPES holds the DuckDB type of every column whose source path is
    # optional; a build that lacked the field must still emit the declared
    # type, never an int32 from an untyped NULL.
    import duckdb
    from parquet_transform import COLUMN_TYPES
    by_table = {}
    for (table_name, col_name), expected in COLUMN_TYPES.items():
        by_table.setdefault(table_name, {})[col_name] = expected
    for table_name, columns in sorted(by_table.items()):
        path = _table_glob(lake_dir, table_name)
        actual = {r[0]: r[1] for r in
                  duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
        mismatches = [f"{c}: expected {e}, actual {actual.get(c)}"
                      for c, e in columns.items() if actual.get(c) != e]
        report.check(
            f"{table_name}: {len(columns)} declared column types match",
            not mismatches,
            "; ".join(mismatches)
        )


def check_field_completeness(report, lake_dir, jsonl_path):
    """Verify the lake captures every top-level field from the source JSON.

    Samples one entry from the JSONL and checks that every top-level key is
    either (a) a column or nested struct field in the entries table, or
    (b) the source array for a child table (features, xrefs, comments,
    publications).  Catches silent data loss when UniProt adds a new
    top-level field that the pipeline's explicit SELECT list doesn't cover.

    Uses a union of keys across a sample (not just one entry) because some
    fields are rare (e.g. organismHosts only appears on virus entries,
    geneLocations is uncommon).
    """
    report.checks.append("\n--- 13. FIELD COMPLETENESS ---")
    eprint("\n--- 13. FIELD COMPLETENESS ---")

    # ── Collect all top-level keys from a sample of source entries ──
    eprint("  Sampling source JSONL for top-level field names...")
    source_keys = set()
    # Use the existing sample (up to 1000 entries) to cover rare fields.
    sampled = sample_jsonl_entries(jsonl_path, 1000)
    if not sampled:
        report.check("field completeness sample non-empty", False, "no entries sampled")
        return
    for entry in sampled:
        source_keys.update(entry.keys())
    eprint(f"  Source has {len(source_keys)} unique top-level fields: {sorted(source_keys)}")

    # ── Map source fields to where they land in the lake ──
    # Entries table: columns and nested structs cover most fields.
    # Child tables: the four array fields are unnested into their own tables.
    child_array_fields = {
        "features":                    "features",
        "uniProtKBCrossReferences":    "xrefs",
        "comments":                    "comments",
        "references":                  "publications",
    }

    # Get entries column names from the Parquet schema
    entries_ds = open_table(lake_dir, "entries")
    entries_columns = {field.name for field in entries_ds.schema}

    # Known mappings: source field name → entries column name.
    # Only needed when the column name differs from the source field name.
    source_to_entries = {
        "primaryAccession":   "acc",
        "uniProtkbId":        "id",
        "entryType":          "entry_type",
        "secondaryAccessions": "secondary_accs",
        "organism":           "organism_residual",
        "proteinDescription": "protein_desc_residual",
        "genes":              "genes_full",
        "keywords":           "keywords_full",
        "sequence":           "sequence",
        "proteinExistence":   "protein_existence",
        "annotationScore":    "annotation_score",
        "entryAudit":         "first_public",       # split across multiple columns
        "extraAttributes":    "extra_attributes",
        "organismHosts":      "organism_hosts",
        "geneLocations":      "gene_locations",
    }

    uncaptured = []
    for key in sorted(source_keys):
        # Is it a child table array?
        if key in child_array_fields:
            continue
        # Does it map to a known entries column?
        mapped_col = source_to_entries.get(key)
        if mapped_col and mapped_col in entries_columns:
            continue
        # Is the source field name itself a column? (fallback for future fields
        # that might be added with matching names)
        if key in entries_columns:
            continue
        uncaptured.append(key)

    if uncaptured:
        report.check(
            "all source JSON fields captured in lake",
            False,
            f"{len(uncaptured)} uncaptured field(s): {', '.join(uncaptured)}"
        )
        for f in uncaptured:
            report.checks.append(f"    ✗ '{f}' — not in entries columns or child tables")
    else:
        report.check(
            "all source JSON fields captured in lake",
            True,
            f"all {len(source_keys)} source fields accounted for"
        )


# Comment types whose prose lives in a top-level ``texts`` key (plan G.1).
# DISEASE and SUBCELLULAR LOCATION keep theirs under ``note.texts`` and are
# deliberately absent: ``text_value`` is NULL for them by design.
TEXT_COMMENT_TYPES = {
    "FUNCTION", "SUBUNIT", "TISSUE SPECIFICITY", "DOMAIN", "PTM",
    "SIMILARITY", "CAUTION", "MISCELLANEOUS", "ACTIVITY REGULATION",
    "ALLERGEN", "BIOTECHNOLOGY", "DEVELOPMENTAL STAGE",
    "DISRUPTION PHENOTYPE", "INDUCTION", "PATHWAY", "POLYMORPHISM",
    "TOXIC DOSE",
}


def check_text_value(report, lake_dir):
    """Text-bearing comment types must have a populated text_value."""
    report.checks.append("\n--- 15. COMMENT TEXT ---")
    eprint("\n--- 15. COMMENT TEXT ---")
    import duckdb
    path = _table_glob(lake_dir, "comments")
    rows = duckdb.sql(f"""
        SELECT comment_type, count(*) AS n, count(text_value) AS with_text
        FROM read_parquet('{path}') GROUP BY 1
    """).fetchall()
    present = {r[0]: (r[1], r[2]) for r in rows}
    for ctype in sorted(TEXT_COMMENT_TYPES & set(present)):
        n, with_text = present[ctype]
        report.check(f"comments.text_value populated for {ctype}", with_text > 0,
                     f"{with_text:,}/{n:,} rows have text")


def check_reconstruction(report, lake_dir, jsonl_path, n):
    """g(f(x)) == x on a sample: rebuild sampled entries from the five tables
    with bin/reconstruct.py and compare with the JSONL (plan A11, the release
    gate for the residual trim A13)."""
    report.checks.append(f"\n--- 16. RECONSTRUCTION (n={n}) ---")
    eprint(f"\n--- 16. RECONSTRUCTION (n={n}) ---")
    import duckdb
    from reconstruct import reconstruct_entry, entries_match

    sampled = sample_jsonl_entries(jsonl_path, n)
    originals = {e["primaryAccession"]: e for e in sampled if e.get("primaryAccession")}
    if not originals:
        report.check("reconstruction sample non-empty", False, "no entries sampled")
        return
    in_list = ",".join("'" + a.replace("'", "''") + "'" for a in originals)

    def fetch(table):
        path = _table_glob(lake_dir, table)
        tbl = duckdb.sql(f"SELECT * FROM read_parquet('{path}') WHERE acc IN ({in_list})").arrow().read_all()
        grouped = {}
        # MAP columns come back as dicts (an empty MAP as {}, not []), so
        # reconstruct.py never has to guess a column's type from its values.
        for row in tbl.to_pylist(maps_as_pydicts="strict"):
            grouped.setdefault(row["acc"], []).append(row)
        return grouped

    t0 = time.time()
    rows = {t: fetch(t) for t in ("entries", "features", "xrefs", "comments", "publications")}
    eprint(f"  Fetched rows for {len(originals)} accessions in {time.time()-t0:.1f}s")

    matched, failures = 0, []
    for acc, orig in originals.items():
        entry_rows = rows["entries"].get(acc)
        if not entry_rows:
            failures.append(f"{acc}: not in entries")
            continue
        rebuilt = reconstruct_entry(entry_rows[0], rows["features"].get(acc, []),
                                    rows["xrefs"].get(acc, []), rows["comments"].get(acc, []),
                                    rows["publications"].get(acc, []))
        ok, diff = entries_match(rebuilt, orig)
        if ok:
            matched += 1
        else:
            failures.append(f"{acc}: {diff}")
    report.check(
        f"reconstruction matches JSONL for {matched}/{len(originals)} sampled entries",
        not failures,
        "; ".join(failures[:3]) + (f" (+{len(failures)-3} more)" if len(failures) > 3 else ""),
    )


def check_accession_map(report, lake_dir):
    """The six assertions of plan §5.5 on accession_map (sort order is
    covered by check_sort_order)."""
    report.checks.append("\n--- 18. ACCESSION MAP ---")
    eprint("\n--- 18. ACCESSION MAP ---")
    import duckdb
    amap = _table_glob(lake_dir, "accession_map")
    entries = _table_glob(lake_dir, "entries")

    n_primary, n_secondary, n_dup = duckdb.sql(f"""
        SELECT count(*) FILTER (WHERE is_primary), count(*) FILTER (WHERE NOT is_primary),
               count(*) - count(DISTINCT (acc, primary_acc))
        FROM read_parquet('{amap}')
    """).fetchone()
    n_entries, n_secs = duckdb.sql(f"""
        SELECT count(*), coalesce(sum(len(secondary_accs)), 0) FROM read_parquet('{entries}')
    """).fetchone()
    report.check("accession_map primary rows == entries rows", n_primary == n_entries,
                 f"{n_primary:,} vs {n_entries:,}")
    report.check("accession_map secondary rows == sum(len(entries.secondary_accs))",
                 n_secondary == n_secs, f"{n_secondary:,} vs {n_secs:,}")
    report.check("accession_map has no duplicate (acc, primary_acc)", n_dup == 0, f"{n_dup:,} duplicates")

    orphans = duckdb.sql(f"""
        SELECT count(*) FROM read_parquet('{amap}') m
        ANTI JOIN read_parquet('{entries}') e ON m.primary_acc = e.acc
    """).fetchone()[0]
    report.check("every accession_map.primary_acc exists in entries", orphans == 0, f"{orphans:,} orphans")

    mism = duckdb.sql(f"""
        SELECT count(*) FILTER (WHERE m.reviewed != e.reviewed OR m.taxid != e.taxid)
        FROM read_parquet('{amap}') m JOIN read_parquet('{entries}') e ON m.acc = e.acc
        WHERE m.is_primary
    """).fetchone()[0]
    report.check("accession_map (reviewed, taxid) match entries for primary rows", mism == 0,
                 f"{mism:,} mismatches")


def check_partitions(report, lake_dir):
    """Hive partitions agree with the stored column they mirror (plan D.3).
    Uses stats_min_value / stats_max_value: DuckDB's stats_min / stats_max are
    the legacy Parquet fields, which PyArrow omits for string columns.
    every row group under review_status=swissprot has reviewed min = max =
    true (trembl: false), per-partition row counts match the manifest, the
    two sides sum to the table count, and swissprot files precede trembl
    files in the manifest's file list."""
    report.checks.append("\n--- 19. PARTITIONS ---")
    eprint("\n--- 19. PARTITIONS ---")
    import duckdb
    with open(os.path.join(lake_dir, "manifest.json")) as f:
        manifest = json.load(f)
    expected_flag = {"swissprot": "true", "trembl": "false"}
    for table_name, info in manifest["tables"].items():
        part = info.get("partitioning") or {}
        keys = part.get("keys") or []
        if part.get("scheme") != "hive" or not keys:
            report.check(f"{table_name} manifest declares hive partitioning", False, "missing")
            continue
        key = keys[0]
        derived = key["derived_from"]
        side_total = 0
        for side in key["values"]:
            path = os.path.join(lake_dir, table_name, f"{key['name']}={side}", "*.parquet")
            bad, rows = duckdb.sql(f"""
                SELECT count(*) FILTER (WHERE lower(stats_min_value) != '{expected_flag[side]}'
                                           OR lower(stats_max_value) != '{expected_flag[side]}'),
                       coalesce(sum(row_group_num_rows), 0)
                FROM parquet_metadata('{path}') WHERE path_in_schema = '{derived}'
            """).fetchone()
            report.check(f"{table_name}/{key['name']}={side}: every row group has {derived} = {expected_flag[side]}",
                         bad == 0, f"{bad} row groups disagree")
            report.check(f"{table_name}/{key['name']}={side}: rows match manifest",
                         rows == key["row_counts"].get(side), f"disk {rows} vs manifest {key['row_counts'].get(side)}")
            side_total += rows
        report.check(f"{table_name}: partition rows sum to row_count", side_total == info["row_count"],
                     f"{side_total} vs {info['row_count']}")
        sides = [f.split("/")[0] for f in info["files"]]
        report.check(f"{table_name}: swissprot files precede trembl files", sides == sorted(sides), str(sides[:4]))


def check_schema_evolution(report, lake_dir, baseline_path):
    """
    Detect upstream UniProtKB JSON schema changes by comparing inferred Parquet
    schema against a committed baseline.  Reports missing columns (ERROR) and
    new columns (WARNING) to alert on schema drift.
    """
    report.checks.append("\n--- 14. SCHEMA EVOLUTION GUARD ---")
    eprint("\n--- 14. SCHEMA EVOLUTION GUARD ---")

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
    for table_name in ALL_TABLES:
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
    parser.add_argument(
        "--expected-count", type=int, default=None,
        help="Entry count recorded upstream of the sort (STREAM_JSONL's "
             "entry_count.txt); both the JSONL and the entries table must match it",
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
    check_completeness(report, args.lake, jsonl_count, args.expected_count)
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
    check_field_completeness(report, args.lake, args.jsonl)
    check_text_value(report, args.lake)
    check_reconstruction(report, args.lake, args.jsonl, args.spot_check_n)
    check_accession_map(report, args.lake)
    check_partitions(report, args.lake)
    if args.schema_baseline:
        check_schema_evolution(report, args.lake, args.schema_baseline)

    # ── Write report ──
    elapsed = time.time() - t_start
    report_text = report.full_report()
    report_text += f"\n\nValidation completed in {elapsed:.1f}s"

    with open(args.output, "w") as f:
        f.write(report_text + "\n")
    report.elapsed_s = round(elapsed, 1)
    json_output = os.path.splitext(args.output)[0] + ".json"
    with open(json_output, "w") as f:
        json.dump(report.to_dict(), f, indent=2)

    eprint(f"\n{'=' * 70}")
    eprint(f"VERDICT: {'ALL CHECKS PASSED' if report.passed() else 'VALIDATION FAILED'}")
    eprint(f"  {report.summary()}")
    eprint(f"  Report: {args.output} (+ {json_output})")
    eprint(f"  Elapsed: {elapsed:.1f}s")
    eprint(f"{'=' * 70}")

    sys.exit(0 if report.passed() else 1)


if __name__ == "__main__":
    main()
