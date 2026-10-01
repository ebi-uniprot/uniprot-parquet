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
     - entries.accession has zero duplicates

  3. NULL KEYS AND EMPTY STRINGS
     - accession, reviewed, organism_id never null in any table
     - Identity columns (accession, id) never empty strings
     - entries.entryType holds only the values the transform knows
       (parquet_transform.ENTRY_TYPES): a reworded value would be filed
       as TrEMBL

  4. REFERENTIAL INTEGRITY
     - Every accession in features exists in entries
     - Every accession in xrefs exists in entries
     - Every accession in comments exists in entries
     - Every accession in publications exists in entries

  5. SORT ORDER AND ARRAY POSITIONS
     - entries sorted by (reviewed DESC, organism_id ASC, accession ASC); child tables by
       the same keys then their *_index; accession_map by
       (reviewed DESC, accession ASC, primary_accession ASC)
     - Each child table's *_index runs 0..n-1 per accession, with n equal to
       the entry's *_count (per-accession completeness); one streaming pass
       per child table, merged with entries in their shared sort order

  6. ROUND-TRIP SPOT CHECK
     - Sample N TrEMBL and N Swiss-Prot accessions from the JSONL (two
       reservoirs, one pass: a uniform sample would hold ~0.23% Swiss-Prot)
     - For each, verify accession, organism_id, length, feature_count match lake

  7. PARQUET FILE INTEGRITY
     - Every data file in the lake is a readable Parquet file

  8. MANIFEST CONSISTENCY
     - manifest.json file lists match actual files on disk
     - every file's size, SHA-256 and MD5 match manifest file_details

  9. DENORMALIZED COLUMN SYNC
     - organism_id and reviewed in child tables match entries

  10. SEQUENCE INTEGRITY
      - len(sequence.value) == length for every entry
      - No entries with length == 0

  11. FEATURE COORDINATE BOUNDARIES
      - start_pos <= end_pos where both are non-null

  12. SHORTCUT COLUMN TYPES
      - Every shortcut column (parquet_transform.COLUMN_TYPES) has its
        declared type

  13. FIELD COMPLETENESS
      - Every top-level key of a sampled entry, and of its features,
        cross-references, comments and references, is a source column of
        its table; source columns without a description (fields UniProt
        added) are listed as warnings

  14. SCHEMA DIFF (informational, never fails)
      - With --previous-manifest: tables and columns added, removed or
        retyped since the previous release, as warnings; shortcut changes
        without a schema_version bump are flagged

  15. COMMENT TEXT
      - Every text-bearing comment type has a populated text_value

  16. RECONSTRUCTION
      - Sampled entries rebuilt from the five tables (bin/reconstruct.py)
        equal the source JSONL, including array order

  18. ACCESSION MAP
      - Primary rows == entries, secondary rows == sum(len(secondaryAccessions)),
        no duplicate (accession, primary_accession), every primary_accession exists
      - Secondary rows == the (secondary, entry) pairs of entries.secondaryAccessions
        (EXCEPT ALL both ways), primary rows map to themselves, every row's
        (reviewed, organism_id) is its entry's  (17 is reserved for bloom
        filters, deferred: PyArrow 23 cannot write them)

  19. PARTITIONS
      - review_status=swissprot|trembl directories agree with the stored
        `reviewed` column (row-group statistics), per-partition row counts
        match the manifest and sum to row_count, swissprot files first

  20. SHORTCUT COLUMN COVERAGE (warnings; failures with --release)
      - A shortcut column with no value in any row (footer statistics) is
        listed: its source field may be absent or renamed upstream.  A
        warning for a subset; with --release (a full release, as VALIDATE
        runs it) a failure, as is a table with no rows

  21. SHORTCUT VALUES
      - For the entries sampled for check 16, every shortcut column of every
        table equals an independent Python computation from the JSON entry
        (bin/shortcut_reference.py): reconstruction reads only source
        columns, so this is what catches a wrong shortcut

Usage:
    validate_lake.py \
        --lake /path/to/lake \
        --jsonl /path/to/uniprot.jsonl.zst \
        [--spot-check-n 1000] [--release] \
        [--previous-manifest /path/to/<previous release>/lake/manifest.json] \
        [--memory-limit 16GB] [--threads N] [--temp-dir /scratch/duckdb_tmp] \
        [-o validation_report.txt]

Exit codes:
    0  All checks passed (warnings do not fail)
    1  One or more checks FAILED — do not publish this lake
"""

import os
import sys
import json
import random
import re
import argparse
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from concurrent.futures import ThreadPoolExecutor

from parquet_transform import CHILD_TABLES, TABLE_DEFS, hash_file, init_duckdb


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# ─── DuckDB ────────────────────────────────────────────────────────────
# One connection serves every check.  main() sizes it from --memory-limit /
# --threads / --temp-dir (VALIDATE passes its task's share): DuckDB's default
# connection would take 80% of the node's memory and every core, and spill
# wherever it likes.

_DUCKDB = {"con": None, "settings": ("16GB", None, None)}


def _duckdb():
    """The validator's DuckDB connection (init_duckdb), opened on first use."""
    if _DUCKDB["con"] is None:
        _DUCKDB["con"] = init_duckdb(*_DUCKDB["settings"])
    return _DUCKDB["con"]


# ─── Parquet reading helpers ──────────────────────────────────────────

# Every table in the lake.
ALL_TABLES = [name for name, _, _ in TABLE_DEFS]


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


def _sql_in_list(values):
    """The inside of a SQL ``IN (...)``: each value as a quoted string literal."""
    return ",".join("'" + v.replace("'", "''") + "'" for v in values)


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
    """Yield the lines of a JSONL(.zst) file as bytes, handling
    decompression.  Bytes, not text: nearly every line of a ~1 TB stream is
    only counted, so decoding it to str would be wasted work (json.loads
    takes the bytes of a sampled line directly)."""
    if jsonl_path.endswith(".zst"):
        import zstandard as zstd
        import io
        dctx = zstd.ZstdDecompressor()
        with open(jsonl_path, "rb") as f:
            with dctx.stream_reader(f) as reader:
                yield from io.BufferedReader(reader, buffer_size=16 << 20)
    else:
        with open(jsonl_path, "rb") as f:
            yield from f


# entryType of a JSONL line, found without parsing the line: one C-speed
# search per line of a ~250M-line stream.  UniProt writes the key once, at
# the top level of the entry.
_ENTRY_TYPE_RE = re.compile(rb'"entryType"\s*:\s*"([^"]*)"')


def _is_reviewed(entry: dict) -> bool:
    """Swiss-Prot (True) or TrEMBL (False), as the sampler classifies a line."""
    return "Swiss-Prot" in (entry.get("entryType") or "")


def sample_and_count_jsonl(jsonl_path: str, n: int, seed: int = 42,
                           n_reviewed: int | None = None) -> tuple[list[dict], int, int]:
    """Reservoir-sample parsed entries from a JSONL(.zst) file, stratified,
    and count its non-empty lines, in a single decompression pass.

    Two reservoirs: ``n_reviewed`` (default ``n``) Swiss-Prot entries and
    ``n`` TrEMBL entries.  A uniform sample of a full release would hold
    about 2 Swiss-Prot entries in 1,000 (0.23%), so the structures only
    Swiss-Prot has would almost never be checked.  Each line is classified
    by a bytes regex on its entryType; only lines entering a reservoir are
    parsed.  The sorted JSONL puts Swiss-Prot first, but nothing here relies
    on that.

    Returns (Swiss-Prot sample + TrEMBL sample, line count, lines without an
    entryType).  The full stream (~1 TB uncompressed at production scale) is
    expensive to decompress, so main() does this once and shares the sample
    across every check that needs source entries."""
    rng = random.Random(seed)
    sizes = {True: n if n_reviewed is None else n_reviewed, False: n}
    reservoirs: dict[bool, list[bytes]] = {True: [], False: []}
    seen = {True: 0, False: 0}
    idx = unclassified = 0

    for line in _open_jsonl_lines(jsonl_path):
        line = line.strip()
        if not line:
            continue
        idx += 1
        m = _ENTRY_TYPE_RE.search(line)
        if m is None:
            unclassified += 1
            continue
        stratum = b"Swiss-Prot" in m.group(1)
        k, reservoir = sizes[stratum], reservoirs[stratum]
        if seen[stratum] < k:
            reservoir.append(line)
        else:
            j = rng.randint(0, seen[stratum])
            if j < k:
                reservoir[j] = line
        seen[stratum] += 1

    return [json.loads(line) for line in reservoirs[True] + reservoirs[False]], idx, unclassified


def sample_jsonl_entries(jsonl_path: str, n: int,
                         seed: int = 42) -> list[dict]:
    """Reservoir-sample N Swiss-Prot and N TrEMBL parsed entries from a JSONL(.zst) file."""
    return sample_and_count_jsonl(jsonl_path, n, seed)[0]


def _spot_subsample(sampled: list[dict], n: int, seed: int = 42,
                    n_reviewed: int | None = None) -> list[dict]:
    """A deterministic subsample of the shared JSONL sample, for the checks
    that honour --spot-check-n: up to ``n_reviewed`` (default ``n``)
    Swiss-Prot and ``n`` TrEMBL entries (the shared sample holds
    max(spot_check_n, 1000) of each)."""
    out = []
    for stratum, k in ((True, n if n_reviewed is None else n_reviewed), (False, n)):
        part = [e for e in sampled if _is_reviewed(e) == stratum]
        out += part if k >= len(part) else random.Random(seed).sample(part, k)
    return out


def _strata(entries) -> str:
    """'x Swiss-Prot + y TrEMBL' for a list of entries."""
    entries = list(entries)
    reviewed = sum(map(_is_reviewed, entries))
    return f"{reviewed:,} Swiss-Prot + {len(entries) - reviewed:,} TrEMBL"


# ─── Validation framework ─────────────────────────────────────────────

class ValidationReport:
    """Accumulates pass/fail checks and produces a report."""

    def __init__(self):
        self.checks = []          # formatted lines, including section headers
        self.records = []         # one dict per check, for validation_report.json
        self.warnings = []        # informational findings; never fail the lake
        self.failures = 0
        self.elapsed_s = None

    def warn(self, name: str, detail: str = ""):
        entry = f"  [WARN] {name}"
        if detail:
            entry += f"  —  {detail}"
        self.checks.append(entry)
        self.warnings.append({"name": name, "detail": detail})
        eprint(entry)

    def note(self, text: str):
        self.checks.append(f"  [INFO] {text}")
        eprint(f"  [INFO] {text}")

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
            f"{self.failures} failed, {len(self.warnings)} warnings"
        )

    def to_dict(self) -> dict:
        """Machine-readable form of the report (validation_report.json)."""
        return {
            "passed": self.passed(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": self.summary(),
            "elapsed_s": self.elapsed_s,
            "checks": list(self.records),
            "warnings": list(self.warnings),
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
    """Verify entries.accession has no duplicates.  Returns (total, unique_count).

    Uses DuckDB aggregation instead of materializing 250M accessions into a
    Python set (~2.5 GB heap), which would pressure the GC and starve
    downstream DuckDB queries of memory.
    """
    report.checks.append("\n--- 2. UNIQUENESS ---")
    eprint("\n--- 2. UNIQUENESS ---")

    entries_path = _table_glob(lake_dir, "entries")
    row = _duckdb().sql(f"""
        SELECT count(*) AS total, count(DISTINCT accession) AS unique_count
        FROM read_parquet('{entries_path}')
    """).fetchone()
    total, unique_count = row
    dupes = total - unique_count

    report.check(
        "entries.accession is unique (no duplicates)",
        dupes == 0,
        f"total={total:,}, unique={unique_count:,}, dupes={dupes:,}"
    )
    return total, unique_count


def check_null_keys(report, lake_dir, jsonl_unclassified=None):
    """Verify critical columns are never null and identity columns have no
    empty strings, that entryType holds only known values, and (given the
    sampler's count) that every JSONL line has an entryType."""
    report.checks.append("\n--- 3. NULL KEYS AND EMPTY STRINGS ---")
    eprint("\n--- 3. NULL KEYS AND EMPTY STRINGS ---")

    key_checks = [
        ("entries",    ["accession", "reviewed", "organism_id", "entryType"]),
        ("features",   ["accession", "reviewed", "organism_id", "type"]),
        ("xrefs",      ["accession", "reviewed", "organism_id", "database", "id"]),
        ("comments",   ["accession", "reviewed", "organism_id", "commentType"]),
        ("publications", ["accession", "reviewed", "organism_id", "citation", "referenceNumber"]),
        ("accession_map", ["accession", "primary_accession", "is_primary", "reviewed", "organism_id"]),
    ]

    # Identity columns that must never be empty strings
    empty_string_checks = [
        ("entries",    ["accession", "id"]),
        ("features",   ["accession"]),
        ("xrefs",      ["accession", "id"]),
        ("comments",   ["accession"]),
        ("publications", ["accession"]),
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

    from parquet_transform import ENTRY_TYPES
    entry_types = [v for (v,) in _duckdb().sql(
        f"SELECT DISTINCT entryType FROM read_parquet('{_table_glob(lake_dir, 'entries')}')").fetchall()]
    unknown = sorted((v for v in entry_types if v not in ENTRY_TYPES), key=str)
    report.check("entries.entryType values are known", not unknown,
                 f"unknown: {unknown}" if unknown else "")
    if jsonl_unclassified is not None:
        report.check("every JSONL line has an entryType", jsonl_unclassified == 0,
                     f"{jsonl_unclassified:,} lines without one" if jsonl_unclassified else "")

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
    """Verify every accession in child tables exists in entries.

    Uses DuckDB anti-joins instead of Python iteration for scalability
    (the xrefs table can exceed 16B rows at production scale).
    """
    report.checks.append("\n--- 4. REFERENTIAL INTEGRITY ---")
    eprint("\n--- 4. REFERENTIAL INTEGRITY ---")
    eprint(f"  Entries: {entry_count:,} unique accessions")

    entries_path = _table_glob(lake_dir, "entries")

    for child_name in CHILD_TABLES:
        child_path = _table_glob(lake_dir, child_name)
        t0 = time.time()
        try:
            result = _duckdb().sql(f"""
                SELECT DISTINCT c.accession
                FROM read_parquet('{child_path}') c
                LEFT JOIN read_parquet('{entries_path}') e ON c.accession = e.accession
                WHERE e.accession IS NULL
                LIMIT 100
            """).fetchall()
            orphans = [row[0] for row in result]
            elapsed = time.time() - t0
            report.check(
                f"all {child_name}.accession exist in entries",
                len(orphans) == 0,
                f"{len(orphans):,}+ orphan accessions ({elapsed:.1f}s)" if orphans else f"({elapsed:.1f}s)"
            )
            if orphans:
                report.checks.append(f"    orphan examples: {orphans[:5]}")
        except Exception as e:
            report.check(
                f"all {child_name}.accession exist in entries",
                False,
                f"DuckDB error: {e}"
            )


class _SortChecker:
    """Streaming sort check for (bool DESC, k2 ASC, ..., kn ASC) keys, fed
    one record batch at a time.

    ``columns`` names the key columns (the first a boolean sorted DESC);
    ``key_fn(*values) -> tuple`` builds the comparable key used at batch
    boundaries.  ``detail`` is empty until the first row out of order."""

    def __init__(self, columns, key_fn):
        self.columns, self.key_fn = list(columns), key_fn
        self.detail = ""
        self._row_offset = 0
        self._prev_last = None

    def feed(self, batch):
        n = batch.num_rows
        if self.detail or n == 0:
            return
        cols = [batch.column(c) for c in self.columns]

        # Boundary between the previous batch and this one
        if self._prev_last is not None:
            first = tuple(c[0].as_py() for c in cols)
            if self.key_fn(*self._prev_last) > self.key_fn(*first):
                self.detail = (
                    f"disorder at row {self._row_offset}: "
                    + ", ".join(f"{c}={a}→{b}" for c, a, b in zip(self.columns, self._prev_last, first))
                )
                return

        # Vectorised within-batch check: row i+1 is out of order when every
        # earlier key is equal and this key goes the wrong way.
        if n > 1:
            prev = [c.slice(0, n - 1) for c in cols]
            nxt = [c.slice(1, n - 1) for c in cols]
            any_disorder = pc.and_(pc.invert(prev[0]), nxt[0])             # false → true
            prefix_equal = pc.equal(prev[0], nxt[0])
            for p, q in zip(prev[1:], nxt[1:]):
                any_disorder = pc.or_(any_disorder, pc.and_(prefix_equal, pc.greater(p, q)))
                prefix_equal = pc.and_(prefix_equal, pc.equal(p, q))

            if pc.any(any_disorder).as_py():
                idx = pc.index(any_disorder, True).as_py()
                self.detail = (
                    f"disorder at row {self._row_offset + idx + 1}: "
                    + ", ".join(f"{c}={col[idx].as_py()}→{col[idx + 1].as_py()}"
                                for c, col in zip(self.columns, cols))
                )
                return

        self._prev_last = tuple(c[n - 1].as_py() for c in cols)
        self._row_offset += n


def _check_sorted(dataset, columns, key_fn):
    """Stream ``dataset`` through a _SortChecker; (is_sorted, detail)."""
    checker = _SortChecker(columns, key_fn)
    for batch in dataset.to_batches(columns=list(columns)):
        checker.feed(batch)
        if checker.detail:
            break
    return not checker.detail, checker.detail


# Child table → (array position column, entries count column)
CHILD_INDEX = {table: (index, count) for table, (_, index, count) in CHILD_TABLES.items()}


def check_sort_order(report, lake_dir):
    """Verify every table is sorted by its declared order, and that each child
    table's array positions are exactly 0..n-1 per accession.  Each child
    table is read once: the same batches feed both checks."""
    report.checks.append("\n--- 5. SORT ORDER AND ARRAY POSITIONS ---")
    eprint("\n--- 5. SORT ORDER AND ARRAY POSITIONS ---")

    orders = {name: order for name, order, _ in TABLE_DEFS}
    # Every table sorts by reviewed DESC, then ascending columns.
    assert all(o[0] == "reviewed DESC" and all(c.endswith(" ASC") for c in o[1:])
               for o in orders.values()), orders

    def columns_of(table_name):
        return [c.split()[0] for c in orders[table_name]]

    def key(reviewed, *rest):
        return (not reviewed, *rest)

    def label(table_name):
        return f"{table_name} sorted by ({', '.join(orders[table_name])})"

    is_sorted, detail = _check_sorted(open_table(lake_dir, "entries"), columns_of("entries"), key)
    report.check(label("entries"), is_sorted, detail)

    entries = open_table(lake_dir, "entries")
    positions = []
    for table_name, (idx, count_col) in CHILD_INDEX.items():
        columns = columns_of(table_name)
        checker = _SortChecker(columns, key)

        def fed(dataset, checker=checker, columns=columns):
            for batch in dataset.to_batches(columns=columns):
                checker.feed(batch)
                yield batch

        batches = fed(open_table(lake_dir, table_name))
        ok, detail = _check_array_positions(batches, idx, entries, count_col)
        for _ in batches:           # positions failed early: finish the sort check
            if checker.detail:
                break
        report.check(label(table_name), not checker.detail, checker.detail)
        positions.append((f"{table_name}.{idx} is 0..{count_col}-1 for every accession", ok, detail))

    is_sorted, detail = _check_sorted(open_table(lake_dir, "accession_map"),
                                      columns_of("accession_map"), key)
    report.check(label("accession_map"), is_sorted, detail)
    for name, ok, detail in positions:
        report.check(name, ok, detail)


def _index_runs(batches, idx, problem):
    """Stream a child table's record ``batches`` (holding ``accession`` and ``idx``)
    in stored order; yield (accession, n) array pairs, one per accession in order
    (n = its row count).  Stops at the first row whose ``idx`` is not 0 on a
    new accession or the previous row's + 1, recording it in ``problem``.
    With indices contiguous from 0, n = last index + 1."""
    carry = None                                    # (accession, idx) of the last row so far
    for batch in batches:
        if batch.num_rows == 0:
            continue
        accession, pos = batch.column("accession"), batch.column(idx)
        n = len(accession)
        # Row 0 is compared with the carried row; rows 1.. with the row before
        # (zero-copy slices).
        continues = carry is not None and accession[0].as_py() == carry[0]
        first_expected = carry[1] + 1 if continues else 0
        if pos[0].as_py() != first_expected:
            problem.append(f"{accession[0].as_py()}: {idx} {pos[0].as_py()} where "
                           f"{first_expected} was expected")
            return
        same = pc.fill_null(pc.equal(accession.slice(1), accession.slice(0, n - 1)), False)
        expected = pc.if_else(same, pc.add(pos.slice(0, n - 1), 1), 0)
        bad = pc.fill_null(pc.not_equal(pos.slice(1), expected), True)
        if pc.any(bad).as_py():
            i = pc.index(bad, True).as_py()
            problem.append(f"{accession[i + 1].as_py()}: {idx} {pos[i + 1].as_py()} where "
                           f"{expected[i].as_py()} was expected")
            return
        # Rows followed by a new accession end a run; the batch's last row
        # may continue into the next batch, so it is carried.
        ends = pc.indices_nonzero(pc.invert(same))
        run_acc, run_n = accession.take(ends), pc.add(pos.take(ends), 1)
        if carry and not continues:
            run_acc = pa.concat_arrays([pa.array([carry[0]], accession.type), run_acc])
            run_n = pa.concat_arrays([pa.array([carry[1] + 1], run_n.type), run_n])
        yield run_acc, run_n
        carry = (accession[-1].as_py(), pos[-1].as_py())
    if carry:
        yield pa.array([carry[0]]), pa.array([carry[1] + 1])


def _expected_runs(entries, count_col):
    """(accession, count) array pairs of every entry with count > 0, in stored order."""
    for batch in entries.to_batches(columns=["accession", count_col]):
        keep = pc.greater(batch.column(1), 0)
        yield batch.column(0).filter(keep), batch.column(1).filter(keep)


def _aligned(left, right):
    """Pair two streams of (accession, n) arrays as equal-length slices; the last
    pair has None on the side that ran out first (both None when both did)."""
    left, right = iter(left), iter(right)
    x = y = None
    while True:
        while x is None or len(x[0]) == 0:
            x = next(left, None)
            if x is None:
                break
        while y is None or len(y[0]) == 0:
            y = next(right, None)
            if y is None:
                break
        if x is None or y is None:
            yield x, y
            return
        k = min(len(x[0]), len(y[0]))
        yield (x[0].slice(0, k), x[1].slice(0, k)), (y[0].slice(0, k), y[1].slice(0, k))
        x, y = (x[0].slice(k), x[1].slice(k)), (y[0].slice(k), y[1].slice(k))


def _check_array_positions(child_batches, idx, entries, count_col):
    """Single streaming pass over a child table's record batches: every
    accession's ``idx`` runs 0..n-1 in stored order, and the accessions with
    rows, and their n, are exactly the entries with ``count_col`` > 0, in the
    same (reviewed, organism_id, accession) order.  Returns (ok, detail)."""
    problem = []
    for got, want in _aligned(_index_runs(child_batches, idx, problem),
                              _expected_runs(entries, count_col)):
        if problem:
            return False, problem[0]
        if got is None and want is None:
            break
        if want is None:
            return False, f"{got[0][0].as_py()}: {got[1][0].as_py()} rows, no entry with {count_col} > 0"
        if got is None:
            return False, f"{want[0][0].as_py()}: no rows, {count_col} = {want[1][0].as_py()}"
        same = pc.and_(pc.equal(got[0], want[0]), pc.equal(pc.cast(got[1], pa.int64()),
                                                            pc.cast(want[1], pa.int64())))
        if not pc.all(same).as_py():
            i = pc.index(same, False).as_py()
            return False, (f"expected {want[1][i].as_py()} rows for {want[0][i].as_py()} "
                           f"({count_col}), found {got[1][i].as_py()} rows for {got[0][i].as_py()}")
    if problem:
        return False, problem[0]
    return True, ""


def check_round_trip(report, lake_dir, sampled_entries, n, n_reviewed=None):
    """Spot-check n TrEMBL and n_reviewed Swiss-Prot entries from the shared
    JSONL sample against the lake."""
    report.checks.append(f"\n--- 6. ROUND-TRIP SPOT CHECK (n={n}) ---")
    eprint(f"\n--- 6. ROUND-TRIP SPOT CHECK (n={n}) ---")

    sampled = _spot_subsample(sampled_entries, n, n_reviewed=n_reviewed)

    if not sampled:
        report.check("round-trip sample non-empty", False, "no entries sampled")
        return

    # Build lookup from JSONL
    jsonl_lookup = {}
    for entry in sampled:
        accession = entry.get("primaryAccession")
        if not accession:
            continue
        organism_id = entry.get("organism", {}).get("taxonId")
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

        jsonl_lookup[accession] = {
            "organism_id": organism_id,
            "length": seq_len,
            "feature_count": feature_count,
            "xref_count": xref_count,
            "comment_count": comment_count,
            "reference_count": reference_count,
        }

    # Read matching entries from lake via DuckDB (avoids scanning all 250M rows in Python)
    entries_path = _table_glob(lake_dir, "entries")
    acc_list = list(jsonl_lookup.keys())
    lake_lookup = {}
    try:
        rows = _duckdb().sql(f"""
            SELECT accession, organism_id, length, feature_count,
                   xref_count, comment_count, reference_count
            FROM read_parquet('{entries_path}')
            WHERE accession IN ({_sql_in_list(acc_list)})
        """).fetchall()
        for row in rows:
            lake_lookup[row[0]] = {
                "organism_id": row[1],
                "length": row[2],
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
        f"all {len(jsonl_lookup)} sampled accessions found in lake ({_strata(sampled)})",
        len(missing) == 0,
        f"{len(missing)} missing" if missing else ""
    )
    if missing:
        report.checks.append(f"    missing examples: {sorted(missing)[:5]}")

    mismatches = {"organism_id": 0, "length": 0, "feature_count": 0,
                  "xref_count": 0, "comment_count": 0, "reference_count": 0}
    mismatch_examples = {}

    for accession, expected in jsonl_lookup.items():
        if accession not in lake_lookup:
            continue
        actual = lake_lookup[accession]
        for field in mismatches:
            if expected[field] != actual[field]:
                mismatches[field] += 1
                if field not in mismatch_examples:
                    mismatch_examples[field] = (
                        accession, expected[field], actual[field]
                    )

    for field, count in mismatches.items():
        detail = ""
        if count > 0 and field in mismatch_examples:
            accession, exp, act = mismatch_examples[field]
            detail = f"{count} mismatches, e.g. {accession}: expected={exp}, got={act}"
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


def check_manifest(report, lake_dir, workers=None):
    """Verify manifest.json exists and is consistent with actual files.
    ``workers`` files are hashed at a time (hashlib releases the GIL)."""
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

        # Per-file size, SHA-256 and MD5: "what we published is what we built".
        details = table_info.get("file_details") or {}

        def problem(rel):
            d = details.get(rel)
            path = os.path.join(table_dir, rel)
            if not d:
                return f"{rel}: no file_details"
            if os.path.getsize(path) != d.get("size_bytes"):
                return f"{rel}: size"
            sha, md5 = hash_file(path)
            wrong = [name for name, got in (("sha256", sha), ("md5", md5)) if got != d.get(name)]
            return f"{rel}: {', '.join(wrong)}" if wrong else None

        with ThreadPoolExecutor(max_workers=workers) as pool:
            bad = [p for p in pool.map(problem, sorted(manifest_files & actual_files)) if p]
        n = len(manifest_files & actual_files)
        report.check(f"{table_name}: {n - len(bad)}/{n} file hashes match", not bad, "; ".join(bad[:3]))


def check_denormalized_sync(report, lake_dir):
    """Verify that denormalized parent columns in child tables match entries.

    Uses DuckDB joins instead of Python dicts for scalability (entries alone
    is ~250M rows; materializing a Python dict would need ~25 GB heap).
    """
    report.checks.append("\n--- 9. DENORMALIZED COLUMN SYNC ---")
    eprint("\n--- 9. DENORMALIZED COLUMN SYNC ---")

    entries_path = _table_glob(lake_dir, "entries")

    for child_name in CHILD_TABLES:
        child_path = _table_glob(lake_dir, child_name)
        t0 = time.time()
        try:
            result = _duckdb().sql(f"""
                SELECT
                    count(*) FILTER (WHERE c.organism_id != e.organism_id) AS taxid_mismatches,
                    count(*) FILTER (WHERE c.reviewed != e.reviewed) AS reviewed_mismatches
                FROM read_parquet('{child_path}') c
                JOIN read_parquet('{entries_path}') e ON c.accession = e.accession
            """).fetchone()
            taxid_mismatches = result[0]
            reviewed_mismatches = result[1]
            elapsed = time.time() - t0

            report.check(
                f"{child_name}.organism_id matches entries.organism_id",
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
    """Verify the length of sequence.value equals length."""
    report.checks.append("\n--- 10. SEQUENCE INTEGRITY ---")
    eprint("\n--- 10. SEQUENCE INTEGRITY ---")

    entries_ds = open_table(lake_dir, "entries")
    mismatches = 0
    zero_length = 0
    mismatch_examples = []

    for batch in entries_ds.to_batches(columns=["accession", "sequence", "length"]):
        seq_lens = batch.column("length")
        str_lens = pc.utf8_length(pc.struct_field(batch.column("sequence"), "value"))
        is_equal = pc.equal(seq_lens, str_lens)
        n_bad = pc.sum(pc.invert(is_equal)).as_py() or 0
        if n_bad > 0:
            mismatches += n_bad
            if len(mismatch_examples) < 3:
                accs = batch.column("accession").to_pylist()
                sls = seq_lens.to_pylist()
                stls = str_lens.to_pylist()
                for i, eq in enumerate(is_equal.to_pylist()):
                    if not eq and len(mismatch_examples) < 3:
                        mismatch_examples.append(
                            f"{accs[i]}: length={sls[i]}, "
                            f"len(sequence.value)={stls[i]}"
                        )

        zero_length += pc.sum(pc.equal(seq_lens, 0)).as_py() or 0

    report.check(
        "len(sequence.value) == length for all entries",
        mismatches == 0,
        f"{mismatches:,} mismatches" if mismatches else ""
    )
    for ex in mismatch_examples:
        report.checks.append(f"    {ex}")

    report.check(
        "no entries with length == 0",
        zero_length == 0,
        f"{zero_length:,} entries with length=0" if zero_length else ""
    )


def check_feature_coordinates(report, lake_dir):
    """Verify feature start_pos <= end_pos where both are non-null."""
    report.checks.append("\n--- 11. FEATURE COORDINATE BOUNDARIES ---")
    eprint("\n--- 11. FEATURE COORDINATE BOUNDARIES ---")

    features_ds = open_table(lake_dir, "features")
    inverted = 0
    inversion_examples = []

    for batch in features_ds.to_batches(
        columns=["accession", "type", "start_pos", "end_pos"]
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
                accs = batch.column("accession").to_pylist()
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
    """Verify every shortcut column has its declared type (COLUMN_TYPES).
    Source columns are not checked: their type follows the input by design."""
    report.checks.append("\n--- 12. SHORTCUT COLUMN TYPES ---")
    eprint("\n--- 12. SHORTCUT COLUMN TYPES ---")

    # COLUMN_TYPES holds the DuckDB type of every shortcut column: whatever
    # DuckDB inferred, and whether or not a build's input had the source.
    from parquet_transform import COLUMN_TYPES
    by_table = {}
    for (table_name, col_name), expected in COLUMN_TYPES.items():
        by_table.setdefault(table_name, {})[col_name] = expected
    for table_name, columns in sorted(by_table.items()):
        path = _table_glob(lake_dir, table_name)
        rel = _duckdb().sql(f"SELECT * FROM read_parquet('{path}') LIMIT 0")
        actual = {c: str(t) for c, t in zip(rel.columns, rel.types)}
        mismatches = [f"{c}: expected {e}, actual {actual.get(c)}"
                      for c, e in columns.items() if actual.get(c) != e]
        report.check(
            f"{table_name}: {len(columns)} shortcut column types match",
            not mismatches,
            "; ".join(mismatches)
        )


def check_curated_coverage(report, lake_dir, release=False):
    """Report a shortcut column that holds no value in any row: a source
    field UniProt renamed or dropped leaves it a typed NULL (or empty list)
    throughout while its data moves to a new source column, so
    reconstruction still passes.  Footer statistics only: a column chunk
    whose null count equals its value count holds nothing (an empty list
    counts as a null leaf).

    A warning for a subset, which may lack a source.  With ``release`` (a
    full UniProtKB release) every shortcut column must have values and
    every table rows: each is a failure."""
    from parquet_transform import TABLE_META
    report.checks.append("\n--- 20. SHORTCUT COLUMN COVERAGE ---")
    eprint("\n--- 20. SHORTCUT COLUMN COVERAGE ---")

    for table_name in ALL_TABLES:
        curated = TABLE_META[table_name]["columns"]["shortcut"]
        has_value = dict.fromkeys(curated, False)
        rows = 0
        for path in _table_files(lake_dir, table_name):
            meta = pq.read_metadata(path)
            rows += meta.num_rows
            for i in range(meta.num_row_groups):
                group = meta.row_group(i)
                for j in range(group.num_columns):
                    chunk = group.column(j)
                    top = chunk.path_in_schema.split(".")[0]
                    if has_value.get(top) is False:
                        stats = chunk.statistics
                        # No null count recorded: assume the column has values.
                        if (stats is None or not stats.has_null_count
                                or stats.null_count < chunk.num_values):
                            has_value[top] = True
        if not rows:
            if release:
                report.check(f"{table_name} has rows", False, "a full release has rows in every table")
            else:
                report.note(f"{table_name}: no rows, coverage not checked")
            continue
        empty = [column for column, seen in has_value.items() if not seen]
        why = ("NULL or empty in every row: is its source field absent or renamed "
               "upstream? (the transform log lists absent shortcut sources)")
        for column in empty:
            if release:
                report.check(f"{table_name}.{column} has values", False, why)
            else:
                report.warn(f"{table_name}.{column} has no values", why)
        if not empty:
            if release:
                report.check(f"every {table_name} shortcut column has values", True)
            else:
                report.note(f"{table_name}: every shortcut column has values")


def check_field_completeness(report, lake_dir, sampled_entries):
    """Every source field is a source column.

    The transform makes one column per top-level key of each table's JSON
    element, so every key of a sampled entry, and of its features,
    cross-references, comments and references, must be a column of its
    table: one that is not was dropped.  Source columns without a
    description are listed as warnings: fields UniProt added since this code
    was written.  Reconstruction (check 16) proves the full round trip.
    """
    report.checks.append("\n--- 13. FIELD COMPLETENESS ---")
    eprint("\n--- 13. FIELD COMPLETENESS ---")
    from parquet_transform import undescribed_source_columns

    sampled = [e for e in sampled_entries if e.get("primaryAccession")]
    if not sampled:
        report.check("field completeness sample non-empty", False, "no entries sampled")
        return
    columns = {t: _duckdb().sql(f"SELECT * FROM read_parquet('{_table_glob(lake_dir, t)}') "
                                f"LIMIT 0").columns
               for t in ("entries", *CHILD_TABLES)}
    arrays = {array: table for table, (array, _, _) in CHILD_TABLES.items()}
    missing = set()
    for entry in sampled:
        for key, value in entry.items():
            if key in arrays:
                for element in value or []:
                    missing.update(f"{arrays[key]}.{k}" for k in element
                                   if k not in columns[arrays[key]])
            elif key not in columns["entries"]:
                missing.add(f"entries.{key}")
    report.check(
        f"every key of {len(sampled)} sampled entries ({_strata(sampled)}) and their "
        f"elements is a source column",
        not missing,
        f"{len(missing)} missing: {', '.join(sorted(missing)[:10])}" if missing else "",
    )
    for table, names in columns.items():
        new = undescribed_source_columns(table, names)
        if new:
            report.warn(f"{table}: new UniProt fields (source columns without a description)",
                        ", ".join(new))


# Comment types whose prose lives in a top-level ``texts`` key.  DISEASE and
# SUBCELLULAR LOCATION keep theirs under ``note.texts`` and are deliberately
# absent: ``text_value`` is NULL for them by design.
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
    path = _table_glob(lake_dir, "comments")
    rows = _duckdb().sql(f"""
        SELECT commentType, count(*) AS n, count(text_value) AS with_text,
               list(DISTINCT accession ORDER BY accession) FILTER (WHERE text_value IS NULL)[:5] AS without
        FROM read_parquet('{path}') GROUP BY 1
    """).fetchall()
    present = {r[0]: r[1:] for r in rows}
    for ctype in sorted(TEXT_COMMENT_TYPES & set(present)):
        n, with_text, without = present[ctype]
        detail = f"{with_text:,}/{n:,} rows have text"
        if without:
            detail += f"; without: {', '.join(without)}"
        report.check(f"comments.text_value populated for {ctype}", with_text == n, detail)


def _sampled_originals(sampled_entries, n, n_reviewed=None):
    """{accession: entry} for the subsample checks 16 and 21 share."""
    return {e["primaryAccession"]: e for e in _spot_subsample(sampled_entries, n, n_reviewed=n_reviewed)
            if e.get("primaryAccession")}


def fetch_sample_rows(lake_dir, accessions):
    """{table: {accession: [row, ...]}}: every table's rows for the sampled
    entries, as rows_from_arrow dicts (JSON decoded, MAPs as dicts).
    accession_map rows are grouped by primary_accession, the entry they
    belong to.  One query per table, shared by checks 16 and 21."""
    from reconstruct import rows_from_arrow
    in_list = _sql_in_list(accessions)

    def fetch(table, key):
        path = _table_glob(lake_dir, table)
        tbl = _duckdb().sql(f"SELECT * FROM read_parquet('{path}') WHERE {key} IN ({in_list})").arrow().read_all()
        # DuckDB hands a JSON column over as plain strings: which columns are
        # JSON comes from the files' own schema.
        files = _table_files(lake_dir, table)
        json_columns = ({f.name for f in pq.read_schema(files[0]) if isinstance(f.type, pa.JsonType)}
                        if files else set())
        grouped = {}
        # MAP columns come back as dicts (an empty MAP as {}, not []), so
        # reconstruct.py never has to guess a column's type from its values.
        for row in rows_from_arrow(tbl, json_columns):
            grouped.setdefault(row[key], []).append(row)
        return grouped

    t0 = time.time()
    rows = {t: fetch(t, "accession") for t in ("entries", *CHILD_TABLES)}
    rows["accession_map"] = fetch("accession_map", "primary_accession")
    eprint(f"  Fetched rows for {len(accessions)} accessions in {time.time()-t0:.1f}s")
    return rows


def check_reconstruction(report, lake_dir, sampled_entries, n, rows=None, n_reviewed=None):
    """g(f(x)) == x on a sample: rebuild sampled entries from the five tables
    with bin/reconstruct.py and compare with the JSONL, array order included.
    ``rows`` is fetch_sample_rows' result for the same sample, if main()
    already fetched it."""
    report.checks.append(f"\n--- 16. RECONSTRUCTION (n={n}) ---")
    eprint(f"\n--- 16. RECONSTRUCTION (n={n}) ---")
    from reconstruct import reconstruct_entry, entries_match

    originals = _sampled_originals(sampled_entries, n, n_reviewed)
    if not originals:
        report.check("reconstruction sample non-empty", False, "no entries sampled")
        return
    if rows is None:
        rows = fetch_sample_rows(lake_dir, originals)

    matched, failures = {True: 0, False: 0}, []
    for accession, orig in originals.items():
        entry_rows = rows["entries"].get(accession)
        if not entry_rows:
            failures.append(f"{accession}: not in entries")
            continue
        try:
            rebuilt = reconstruct_entry(entry_rows[0], rows["features"].get(accession, []),
                                        rows["xrefs"].get(accession, []), rows["comments"].get(accession, []),
                                        rows["publications"].get(accession, []))
            ok, diff = entries_match(rebuilt, orig)
        except Exception as e:      # a rebuild error is a failed entry, not a lost report
            ok, diff = False, f"rebuild raised {type(e).__name__}: {e}"
        if ok:
            matched[_is_reviewed(orig)] += 1
        else:
            failures.append(f"{accession}: {diff}")
    totals = {s: sum(_is_reviewed(e) == s for e in originals.values()) for s in (True, False)}
    report.check(
        f"reconstruction matches JSONL for {matched[True]}/{totals[True]} Swiss-Prot and "
        f"{matched[False]}/{totals[False]} TrEMBL entries",
        not failures,
        "; ".join(failures[:3]) + (f" (+{len(failures)-3} more)" if len(failures) > 3 else ""),
    )


def check_shortcut_values(report, lake_dir, sampled_entries, n, rows=None, n_reviewed=None):
    """Every shortcut column of the sampled entries' rows, in every table,
    equals bin/shortcut_reference.py's independent computation from the
    JSON entry.  Lists compare in order (those built by list_distinct as
    sets), dates as ISO strings."""
    report.checks.append(f"\n--- 21. SHORTCUT VALUES (n={n}) ---")
    eprint(f"\n--- 21. SHORTCUT VALUES (n={n}) ---")
    from shortcut_reference import compare_rows, expected_shortcuts, lake_order

    originals = _sampled_originals(sampled_entries, n, n_reviewed)
    if not originals:
        report.check("shortcut values sample non-empty", False, "no entries sampled")
        return
    if rows is None:
        rows = fetch_sample_rows(lake_dir, originals)
    problems = {table: [] for table in ALL_TABLES}
    compared = dict.fromkeys(ALL_TABLES, 0)
    for accession, orig in originals.items():
        for table, expected in expected_shortcuts(orig).items():
            actual = lake_order(table, rows[table].get(accession, []))
            problems[table] += compare_rows(table, accession, expected, actual)
            compared[table] += len(expected)
    for table in ALL_TABLES:
        found = problems[table]
        report.check(
            f"{table} shortcut values match the JSONL ({compared[table]:,} rows of "
            f"{_strata(originals.values())} entries)",
            not found,
            "; ".join(found[:3]) + (f" (+{len(found)-3} more)" if len(found) > 3 else ""),
        )


def check_accession_map(report, lake_dir):
    """accession_map: primary rows == entries, secondary rows == sum of
    secondaryAccessions lengths, no duplicate (accession, primary_accession),
    every primary_accession exists, the secondary rows are exactly the
    (secondary, entry) pairs of entries.secondaryAccessions, primary rows map
    to themselves, and every row's (reviewed, organism_id) is its entry's
    (sort order is covered by check_sort_order)."""
    report.checks.append("\n--- 18. ACCESSION MAP ---")
    eprint("\n--- 18. ACCESSION MAP ---")
    amap = _table_glob(lake_dir, "accession_map")
    entries = _table_glob(lake_dir, "entries")

    n_primary, n_secondary, n_dup = _duckdb().sql(f"""
        SELECT count(*) FILTER (WHERE is_primary), count(*) FILTER (WHERE NOT is_primary),
               count(*) - count(DISTINCT (accession, primary_accession))
        FROM read_parquet('{amap}')
    """).fetchone()
    # secondaryAccessions is a source column: absent when no entry has one.
    has_secondaries = "secondaryAccessions" in _duckdb().sql(
        f"SELECT * FROM read_parquet('{entries}') LIMIT 0").columns
    secondaries = "coalesce(sum(len(secondaryAccessions)), 0)" if has_secondaries else "0"
    n_entries, n_secs = _duckdb().sql(f"""
        SELECT count(*), {secondaries} FROM read_parquet('{entries}')
    """).fetchone()
    report.check("accession_map primary rows == entries rows", n_primary == n_entries,
                 f"{n_primary:,} vs {n_entries:,}")
    report.check("accession_map secondary rows == sum(len(entries.secondaryAccessions))",
                 n_secondary == n_secs, f"{n_secondary:,} vs {n_secs:,}")
    report.check("accession_map has no duplicate (accession, primary_accession)", n_dup == 0, f"{n_dup:,} duplicates")

    orphans = _duckdb().sql(f"""
        SELECT count(*) FROM read_parquet('{amap}') m
        ANTI JOIN read_parquet('{entries}') e ON m.primary_accession = e.accession
    """).fetchone()[0]
    report.check("every accession_map.primary_accession exists in entries", orphans == 0, f"{orphans:,} orphans")

    # The mappings themselves, as set comparisons DuckDB can spill: each
    # secondary row is one (secondary accession, entry) pair of entries'
    # secondaryAccessions, in both directions.
    if has_secondaries:
        extra, lost = _duckdb().sql(f"""
            WITH m AS (SELECT accession, primary_accession FROM read_parquet('{amap}') WHERE NOT is_primary),
                 e AS (SELECT unnest(secondaryAccessions) AS accession, accession AS primary_accession
                       FROM read_parquet('{entries}'))
            SELECT (SELECT count(*) FROM (SELECT * FROM m EXCEPT ALL SELECT * FROM e)),
                   (SELECT count(*) FROM (SELECT * FROM e EXCEPT ALL SELECT * FROM m))
        """).fetchone()
    else:
        extra, lost = n_secondary, 0
    report.check("accession_map secondary rows map each secondary accession to its entry",
                 extra == 0 and lost == 0,
                 f"{extra:,} rows no entry lists, {lost:,} listed secondaries without their row"
                 if extra or lost else "")

    n_self = _duckdb().sql(f"""
        SELECT count(*) FILTER (WHERE is_primary AND accession IS DISTINCT FROM primary_accession)
        FROM read_parquet('{amap}')
    """).fetchone()[0]
    report.check("accession_map primary rows have accession == primary_accession", n_self == 0,
                 f"{n_self:,} rows" if n_self else "")

    mism = _duckdb().sql(f"""
        SELECT count(*) FILTER (WHERE m.reviewed IS DISTINCT FROM e.reviewed
                                   OR m.organism_id IS DISTINCT FROM e.organism_id)
        FROM read_parquet('{amap}') m JOIN read_parquet('{entries}') e ON m.primary_accession = e.accession
    """).fetchone()[0]
    report.check("accession_map (reviewed, organism_id) match the entry of primary_accession", mism == 0,
                 f"{mism:,} mismatches")


def check_partitions(report, lake_dir):
    """Hive partitions agree with the stored column they mirror.
    Uses stats_min_value / stats_max_value: DuckDB's stats_min / stats_max are
    the legacy Parquet fields, which PyArrow omits for string columns.
    every row group under review_status=swissprot has reviewed min = max =
    true (trembl: false), per-partition row counts match the manifest, the
    two sides sum to the table count, and swissprot files precede trembl
    files in the manifest's file list."""
    report.checks.append("\n--- 19. PARTITIONS ---")
    eprint("\n--- 19. PARTITIONS ---")
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
            bad, rows = _duckdb().sql(f"""
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


def _arrow_type_paths(t: str, prefix: str = "") -> set[str]:
    """Dotted field paths inside an Arrow type string (manifest "type"), list
    items adding no segment and MAP values sitting under ``*``.

    Arrow does not quote field names in a type string, so a key containing
    ", " or an unbalanced bracket is misread.  The previous release's
    manifest holds only these strings; the diff is informational."""
    from parquet_transform import _split_top
    t = t.strip()
    if t.startswith("struct<") and t.endswith(">"):
        paths = set()
        for part in _split_top(t[len("struct<"):-1]):
            name, _, sub = part.partition(": ")
            path = f"{prefix}.{name}" if prefix else name
            paths |= {path} | _arrow_type_paths(sub, path)
        return paths
    if t.split("<", 1)[0] in ("list", "large_list") and t.endswith(">"):
        return _arrow_type_paths(t[t.index("<") + 1:-1].partition(": ")[2], prefix)
    if t.startswith("map<") and t.endswith(">"):
        return _arrow_type_paths(_split_top(t[len("map<"):-1])[-1], f"{prefix}.*" if prefix else "*")
    return set()


def _type_change(before: str, after: str) -> str:
    """Readable summary of a column type change: the nested fields added and
    removed, or the two types when only a leaf type changed."""
    old, new = _arrow_type_paths(before), _arrow_type_paths(after)
    # Only the topmost changed path: a removed struct implies its fields.
    top = lambda ps: sorted(p for p in ps if not any(p.startswith(q + ".") for q in ps))  # noqa: E731
    added, removed = top(new - old), top(old - new)
    if added or removed:
        return "; ".join(p for p in (f"fields added: {', '.join(added)}" if added else "",
                                     f"fields removed: {', '.join(removed)}" if removed else "") if p)
    short = lambda t: t if len(t) <= 80 else t[:77] + "..."  # noqa: E731
    return f"{short(before)} → {short(after)}"


def check_schema_diff(report, lake_dir, previous_manifest_path):
    """Informational: how this build's schema differs from the previous release.

    Source columns follow the input, so one added, removed or retyped is
    expected whenever UniProt changes its JSON; it is reported, never
    failed.  A change to a shortcut column (TABLE_META "shortcut") is a
    contract change and is tagged SHORTCUT; if schema_version did not change
    with it, or with a table added or removed, that is flagged too.  Without
    a previous manifest there is nothing to compare.
    """
    report.checks.append("\n--- 14. SCHEMA DIFF VS PREVIOUS RELEASE (informational) ---")
    eprint("\n--- 14. SCHEMA DIFF VS PREVIOUS RELEASE (informational) ---")
    if not previous_manifest_path:
        report.note("no --previous-manifest given; schema diff skipped")
        return
    from parquet_transform import TABLE_META

    try:
        with open(previous_manifest_path) as f:
            previous = json.load(f)
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            current = json.load(f)
    except (OSError, ValueError) as e:
        report.warn("schema diff skipped: cannot read a manifest", str(e))
        return
    report.note(f"previous: release {previous.get('release')}, schema_version "
                f"{previous.get('schema_version')}; this build: release {current.get('release')}, "
                f"schema_version {current.get('schema_version')}")

    prev_tables, cur_tables = previous.get("tables", {}), current.get("tables", {})

    def curated(table):
        # A shortcut now or in the previous release: a removed shortcut is no
        # longer in TABLE_META, only in the previous manifest.
        return (set(TABLE_META.get(table, {}).get("columns", {}).get("shortcut", []))
                | set(prev_tables.get(table, {}).get("column_categories", {}).get("shortcut", [])))
    curated_changed, any_diff = False, False
    for table in sorted(set(prev_tables) - set(cur_tables)):
        report.warn(f"table removed: {table}")
        curated_changed = any_diff = True
    for table in sorted(set(cur_tables) - set(prev_tables)):
        report.warn(f"table added: {table}")
        curated_changed = any_diff = True
    for table in sorted(set(prev_tables) & set(cur_tables)):
        curated_columns = curated(table)
        before = {c["name"]: c["type"] for c in prev_tables[table].get("columns", [])}
        after = {c["name"]: c["type"] for c in cur_tables[table].get("columns", [])}
        changes = ([(c, "removed", f"was {before[c]}") for c in sorted(set(before) - set(after))]
                   + [(c, "added", after[c]) for c in sorted(set(after) - set(before))]
                   + [(c, "retyped", _type_change(before[c], after[c]))
                      for c in sorted(set(before) & set(after)) if before[c] != after[c]])
        for column, what, detail in changes:
            is_curated = column in curated_columns
            tag = "SHORTCUT column" if is_curated else "column"
            curated_changed |= is_curated
            any_diff = True
            report.warn(f"{table}: {tag} {what}: {column}", detail)
    if not any_diff:
        report.note("no table or column differences")
    if previous.get("schema_version") == current.get("schema_version"):
        if curated_changed:
            report.warn("shortcut schema changed but schema_version did not",
                        f"both {current.get('schema_version')}")


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
        help="TrEMBL entries, and Swiss-Prot entries unless --spot-check-n-reviewed "
             "says otherwise, to spot-check against the JSONL (default: 1000 each)",
    )
    parser.add_argument(
        "--spot-check-n-reviewed", type=int, default=None,
        help="Swiss-Prot entries to spot-check (default: --spot-check-n).  The "
             "sample is stratified: --spot-check-n TrEMBL plus this many Swiss-Prot",
    )
    parser.add_argument(
        "--release", action="store_true",
        help="The lake is a full UniProtKB release (VALIDATE passes this): a "
             "shortcut column with no values, or an empty table, fails check 20 "
             "instead of warning",
    )
    parser.add_argument(
        "--previous-manifest", default=None,
        help="manifest.json of the previous release: report schema differences "
             "(informational, never fails)",
    )
    parser.add_argument(
        "--expected-count", type=int, default=None,
        help="Entry count recorded upstream of the sort (STREAM_JSONL's "
             "entry_count.txt); both the JSONL and the entries table must match it",
    )
    parser.add_argument("--memory-limit", default="16GB", help="DuckDB memory limit")
    parser.add_argument("--threads", type=int, default=None,
                        help="DuckDB threads, and files hashed at a time (default: all cores)")
    parser.add_argument(
        "--temp-dir", default=None,
        help="DuckDB spill directory. Defaults to $TMPDIR or /tmp/duckdb_temp.",
    )
    parser.add_argument("-o", "--output", default="validation_report.txt")
    args = parser.parse_args()
    _DUCKDB["settings"] = (args.memory_limit, args.threads, args.temp_dir)
    _DUCKDB["con"] = init_duckdb(*_DUCKDB["settings"])

    t_start = time.time()
    eprint("=" * 70)
    eprint("PRODUCTION VALIDATION — UniProtKB Parquet Data Lake")
    eprint("=" * 70)

    report = ValidationReport()
    report.note(f"DuckDB: memory_limit {args.memory_limit}, threads {args.threads or 'all'}")

    # ── 1. Count JSONL lines + sample entries (ground truth, one pass) ──
    # A single decompression pass serves both the line count and every check
    # that needs source entries (round-trip, field completeness,
    # reconstruction) — decompressing the stream per check is the dominant
    # cost at production scale.
    # Swiss-Prot and TrEMBL are sampled separately (sample_and_count_jsonl).
    n_reviewed = args.spot_check_n if args.spot_check_n_reviewed is None else args.spot_check_n_reviewed
    sample_n, sample_n_reviewed = max(args.spot_check_n, 1000), max(n_reviewed, 1000)
    eprint(f"\nCounting JSONL lines and sampling {sample_n_reviewed:,} Swiss-Prot + "
           f"{sample_n:,} TrEMBL entries (one pass)...")
    t0 = time.time()
    sampled_entries, jsonl_count, unclassified = sample_and_count_jsonl(
        args.jsonl, sample_n, n_reviewed=sample_n_reviewed)
    eprint(f"  JSONL: {jsonl_count:,} lines, {_strata(sampled_entries)} sampled "
           f"({time.time()-t0:.1f}s)")

    # ── Run all checks ──
    check_completeness(report, args.lake, jsonl_count, args.expected_count)
    entry_total, entry_unique = check_uniqueness(report, args.lake)
    check_null_keys(report, args.lake, unclassified)
    check_referential_integrity(report, args.lake, entry_unique)
    check_sort_order(report, args.lake)
    check_round_trip(report, args.lake, sampled_entries, args.spot_check_n, n_reviewed)
    check_parquet_integrity(report, args.lake)
    check_manifest(report, args.lake, args.threads)
    check_denormalized_sync(report, args.lake)
    check_sequence_integrity(report, args.lake)
    check_feature_coordinates(report, args.lake)
    check_schema_types(report, args.lake)
    check_curated_coverage(report, args.lake, args.release)
    check_field_completeness(report, args.lake, sampled_entries)
    check_schema_diff(report, args.lake, args.previous_manifest)
    check_text_value(report, args.lake)
    spot = _sampled_originals(sampled_entries, args.spot_check_n, n_reviewed)
    sample_rows = fetch_sample_rows(args.lake, spot) if spot else None
    check_reconstruction(report, args.lake, sampled_entries, args.spot_check_n, sample_rows, n_reviewed)
    check_accession_map(report, args.lake)
    check_partitions(report, args.lake)
    check_shortcut_values(report, args.lake, sampled_entries, args.spot_check_n, sample_rows, n_reviewed)

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
