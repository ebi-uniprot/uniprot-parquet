# Plan: schema for the first public release — accession lookup, `entries` columns, column order, Hive partitioning by review status, `entries` tier packaging

**Status:** proposed 2026-09-11; open decisions settled 2026-09-16. Implementation started 2026-09-16 on `static-lake` (one commit per step of `PLAN_SCHEMA_V2_STEPS.md`).
**Compatibility:** none required. Nothing in this repository has been published; there are no external readers, no back-catalogue and no v1 to migrate from. "Manifest version 2" below is an internal marker for the pipeline and its tests, not a user-facing schema version. Any decision elsewhere in the repo that was justified by "this would break users" (`AUDIT.md` O3/S10 naming freeze, single-release flips, deprecation windows) is open again; see "Decisions reopened by pre-public status" at the end.
**Motivation:** `DEMAND_REVIEW.md` (R1, R2, R5–R9, S1/S5/S11 from `AUDIT.md`); the size/packaging note in the review of this plan (Part F).
**Audience:** an implementer who has not read the rest of the repo. This file holds the design and the rationale. The ordered, file-by-file work order with verification commands is **`PLAN_SCHEMA_V2_STEPS.md`**; implement from that file and come back here only for the "why".
**Decisions taken 2026-09-16** (each is also recorded in its section): child-table `from_reviewed` is renamed to `reviewed`, no `is_*` booleans, `go_terms.evidence_type` (G.3); partition values are spelled out, `review_status=swissprot|trembl` (D.1); if the pinned PyArrow cannot write bloom filters, `accession_map` ships alone and the DuckDB-writer path is withdrawn (§4.3); `pubmed_ids` is sorted numerically (B.2); dedup Phases 1 and 2 (A11, A13) ship before the first public release (F.3); the launch set is fixed as a list (Release bundling); `sorted.jsonl.zst` is published beside the lake (F.7).

| Part | Change | Review ref |
| --- | --- | --- |
| **A** | New `accession_map` table (the primary lookup path, local and remote); bloom filters on `acc` as a local optimisation, with the false-positive rate set per table from row-group count | R1, S5 |
| **B** | Four new `entries` columns: `go_terms`, `pubmed_ids`, `proteome_ids`, `gene_name` | R5, R6, R7, S1 |
| **C** | Reorder `entries` so the nine hot columns are first and contiguous | R9 |
| **D** | Every table Hive-partitioned as `review_status=swissprot|trembl`; `reviewed` stays stored in every file (one name in every table after G.3); no organism level | R2 |
| **E** | `xrefs` deliberately unchanged, with the conditions for revisiting | R4 |
| **F** | Size and packaging: measure per-table bytes (A12), make `entries` a standalone downloadable tier (per-file hashes, partial-lake client, README), dedup-before-child-bytes rule, footer-cost rule for the file-size target | Plan review; AUDIT §6, A5, A12, A13 |
| **G** | Schema hygiene found in the 2026-09-11 review: `comments.text_value` is always NULL (bug), untyped `NULL` fallbacks make column types depend on the input, and the `reviewed`/`from_reviewed` naming split | Review of this plan |
| **H** | Cheap conventions the field expects: zstd level chosen by measurement, column descriptions in Arrow field metadata, enumerations in the manifest, semver schema version with a written policy, a completion marker, `LICENSE` and `CITATION.cff` | `DISTRIBUTION_PRACTICES_SURVEY.md`; review of this plan |

The launch set is Parts A–D, G, the dedup phases A11/A13, F.1, F.2.1 (sizes, `SHA256SUMS.txt`, `RELEASE.metalink`), F.2.2, `validation_report.json`, F.4, H.1, H.2, H.4, H.5 and H.6. Deferred to a minor schema bump after launch: `croissant.json`, `releases.json`, Hugging Face, H.3 enums, bloom filters on child tables, Phase 4 macros (see "Release bundling" for the rule behind the split). `manifest.json` `"version": 2` is the manifest *format* version; the public schema version is `schema_version: "1.0.0"` (H.4). F.1 (measurement) runs after G.1 and before Parts B and F.3/F.4; F.3/F.4 are rules and a measured decision, not code.

## Results log

Every measurement and spike outcome the plan asks for is recorded here, in one place, by the step that produces it (`PLAN_SCHEMA_V2_STEPS.md` names the step). Tables that are too wide for this list stay in their sections (F.1, F.4, H.1) and are linked.

| Item | Produced by | Result |
| --- | --- | --- |
| Phase 0.1: PyArrow bloom-filter parameter name (or "not supported") | Step 1 | **Not supported.** PyArrow 23.0.1 `ParquetWriter.__init__` has no bloom-related parameter (full list checked; none in `pyarrow._parquet` either). **B1 deferred** per §4.3; Step 12 skipped; `accession_map` ships alone. (2026-09-16, sandbox venv: pyarrow 23.0.1, duckdb 1.5.5) |
| Phase 0.2: DuckDB writes/reads bloom filters; row groups scanned for one lookup | Step 1 | DuckDB 1.5.5 writes them: `COPY … (BLOOM_FILTER_FALSE_POSITIVE_RATIO 0.001, DICTIONARY_SIZE_LIMIT 100000)` gives every one of 10 row groups a filter (65,553 bytes each). Reads them: single-accession lookup 1.26 ms with filters vs 3.55 ms on the same data rewritten without (`DICTIONARY_SIZE_LIMIT 0`). **`EXPLAIN ANALYZE` and the JSON profile in 1.5.5 do not print row groups scanned** (`operator_rows_scanned` reports 10,000,000 in both cases); the Step 20 benchmark must count candidate row groups from `parquet_metadata` statistics and say so. |
| Phase 0.3: `candidate_groups / row_groups` on the largest available lake | Step 1 | Demo lake (`demo/lake/2026_01/lake/entries/`, one file, one row group): `row_groups = 1`, `candidate_groups = 0` (P04637 is not in the demo). Meaningless as a ratio; re-run on the slice is **deferred** (Step 3 not runnable in the sandbox: `rest.uniprot.org` blocked, no full dump). |
| G.1: which map-access form works on the installed DuckDB | Step 2 | Form A (`unnest.texts`, the guarded expression made unconditional) — all three forms (A, B `unnest['texts']`, C `map_extract(unnest,'texts')[1]`) run on DuckDB 1.5.5 and return the same count (315/531 comments with `texts` on `small`); a `COFACTOR` comment yields NULL, not an error. Wrapped in `NULLIF(…, '')`. **Work-order correction:** `DISEASE` and `SUBCELLULAR LOCATION` keep their prose under `note.texts`, not top-level `texts`, so they are NULL by design and are excluded from the validator's `TEXT_COMMENT_TYPES` (Step 2 listed them). `text_value` is `string` on the fixture lake; the `comments` staged type is `MAP(VARCHAR, JSON)[]`, which `discover_schema_paths` never descends, confirming why the guard was always false. |
| F.1 table (see F.1) filled; `entries` share of lake; footer bytes | Step 3 | **Deferred (2026-09-16).** The slice needs `rest.uniprot.org` (blocked from the implementation sandbox) and the full `UniProtKB.json.gz`. `bin/sample_jsonl.py` is written and tested (`tests/test_sample_jsonl.py`); the build commands in Step 3 of the work order are ready to run as written. Fill the F.1 table and this row from that run. |
| B size gates (`go_terms`, `pubmed_ids`, `proteome_ids`, B.5) | Step 6 | **Fixture proxy only (stress fixture, 8,416 SP / 5,977 TrEMBL; slice deferred).** `go_terms`: mean `len(go_ids)` 12.95 (SP) / 22.63 (TrEMBL), empty-rate 0.2% / 0.5%; `go_terms` column chunks = 1.44 MB of 12.2 MB `entries` (~12%; `term` 0.75 MB, `id` 0.51 MB, `evidence_type` 0.13 MB, `aspect` 0.05 MB). `pubmed_ids`: share with ≥1 id 0.981 (SP) / 0.62 (TrEMBL), p99 length 45 / 12, max 224 / 15; 0.34 MB. PubMed ids failing `TRY_CAST(… AS BIGINT)`: 0 of 84,029 (stress). `proteome_ids`: 730 (SP) / 309 (TrEMBL) distinct ids, share with none 0.206 / 0.172; 9 KB. `gene_name` 60 KB, `division` 1 KB. B.5: FUNCTION text = 5.5 MB (SP, 8,165 entries) + 2.5 MB (TrEMBL, 2,806 entries) of `text_value` bytes vs 12.2 MB compressed `entries` — far above the ~5% rule on this fixture; SUBCELLULAR LOCATION `text_value` is NULL by design (prose under `note.texts`). `function_text` stays deferred; re-run on the slice. |
| D.5: per-division counts vs FTP; human-files-to-human-rows byte ratio | Step 6 / Step 3 | **Deferred** with the slice (ratio) and the first full build (division counts vs `taxonomic_divisions/`); see Step 3 item 5 and Step 21 item 5 of the work order. |
| D.3 spike: does DuckDB prune files through `REPLACE ((review_status = 'swissprot') AS reviewed)`? Option chosen | Step 14 | ______ |
| F.4 table (see F.4) filled; chosen `entries` file-size target; page-index footer delta | Step 18 | ______ |
| H.1 table (see H.1) filled; chosen zstd level | Step 18 | ______ |
| A11 (Step 10): `g()` limitation — array order is not reproduced | Step 10 | `bin/reconstruct.py::reconstruct_entry` rebuilds every fixture entry (default and stress) and the validator's check 16 passes; comparison is order-independent inside arrays (`deep_sort`) because the child tables carry no position index. Whether to add position columns is the plan owner's call; not done here. |
| A13: bytes before/after the residual trim on the slice ("after v2" column of F.1) | Step 11 | ______ |
| Phase 3: point-lookup benchmark table; B1 kept on child tables? | Step 20 | ______ |
| C.3: HTTP request count for the default-column organism query before/after the reorder | Step 20 | ______ |

---
---

# Part A — accession point-lookup
## 1. Problem statement

All five tables are sorted `(reviewed DESC, taxid ASC, acc ASC)` (`bin/parquet_transform.py`, `TABLE_DEFS`). Row-group min/max statistics prune well on `reviewed` and `taxid`. They do not prune on `acc`: a 100,000-row TrEMBL row group spans many organisms, so its `acc` range is effectively `[A0A…, Z…]` and an equality predicate on `acc` cannot exclude any row group. A point lookup therefore scans the entire `acc` column of the table (~250M values in `entries`, ~5B in `xrefs`).

The demand evidence (`handoff-8x1w-v2/`) shows accession is the single broadest predicate: 10.4M distinct queries from 16,374 clients on the search endpoint, plus 64.6M entries fetched by accession list on the accessions endpoint (36.8 per request programmatically), plus an unmeasured population of single-entry GETs.

## 2. Decision

Two additions. **B2 is the primary mechanism; B1 is a local optimisation for `entries`** (see §2.1 for why the order is this way round). Part A itself changes no sort order or column set; the file layout changes in Part D and Part A's paths follow it. If Phase 0.1 shows the pinned PyArrow cannot write bloom filters, ship B2 alone (§4.3).

| ID | Change | Serves |
| --- | --- | --- |
| **B1** | Parquet **bloom filters on `acc`**, written into the files: required on `entries`, optional on the child tables (decided by the Phase 3 false-positive numbers), false-positive rate per table from §2.1 | Local single-step lookups on `entries` in any engine that reads Parquet bloom filters (DuckDB ≥ 1.2, Spark/parquet-mr ≥ 1.12). Zero user friction: same files, same queries. |
| **B2** | A sixth table **`accession_map`** (one row per primary *and* secondary accession → primary accession, `reviewed`, `taxid`), sorted by `acc` | **The primary lookup path** on every table, local and remote: one row group of `accession_map` per partition, then one `(reviewed, taxid)`-pruned row group of the target table. Also the only path for legacy-accession resolution (a bloom filter cannot serve `list_contains(secondary_accs, …)`) and for engines that do not read bloom filters. |

### 2.1 False-positive rate: set by row-group count, not by taste

A bloom filter is consulted for every row group the min/max statistics cannot exclude, which on `acc` is every row group. Expected false-positive row groups per lookup = `row_groups × fpp`, and every false positive costs a read of the requested columns for that whole row group (several MB for the seven default `entries` columns; more with `sequence`).

| Table | Row groups at 100,000 rows (full build) | False-positive groups per lookup at fpp 0.05 | at 0.001 | at 0.0001 |
| --- | --- | --- | --- | --- |
| `entries` (~250M rows) | ~2,500 | ~125 | ~2.5 | ~0.25 |
| `features` / `comments` / `publications` (~1–3B rows) | ~10,000–30,000 | ~500–1,500 | ~10–30 | ~1–3 |
| `xrefs` (~5B rows) | ~50,000 | ~2,500 | ~50 | ~5 |

An earlier draft used 0.05 everywhere. That would have made a single-accession `entries` lookup read on the order of 125 row groups (hundreds of MB) and would still have passed the fixture test, which has one row group. Rules:

- `entries`: fpp ≤ 0.001. Filter bytes per distinct value grow from ~7 bits at 0.05 to ~15 bits at 0.001 (Parquet split-block formula, `bits = -8 / ln(1 - fpp^(1/8))` per distinct value), so `entries` carries ~0.5 GB of filters in total, read once per single-step lookup.
- Child tables: even at 0.0001 a lookup still touches several to tens of false-positive row groups because there are so many; each filter is small (~5,000 distinct `acc` per `xrefs` row group). B1 on child tables is therefore **optional**: keep it only if Phase 3 shows the single-step form is useful locally on them. The two-step form (B2, pruned by `(reviewed, taxid)`) is the documented path for child tables regardless.
- Record the chosen fpp per table in `manifest.json` (§4.4) so a reader can compute the expected candidate count.
- Any benchmark or pass criterion about bloom filters must run on a build with many row groups (subset or full), never on the fixture.

### Explicit non-goals

- No change to `(reviewed DESC, taxid ASC, acc ASC)` on any table.
- No Hive partitioning beyond Part D's `review_status` level (D.5 explains why there is no second level).
- No new client macros are *required*. Existing macros may be updated to use `accession_map` (Phase 4, optional).
- No bloom filters on columns other than `acc` in this plan. (`xrefs.id` is a candidate for a later plan; see `DEMAND_REVIEW.md` §2.3.)

### Known limitation to document, not fix

Because the sort cannot exclude any row group before its filter is tested, a single-step lookup reads **every** row group's bloom filter. Locally that is one pass over ~0.5 GB of filter bytes for `entries` (§2.1) and is fine. Over httpfs it is one range request per row group (~2,500 for `entries`, ~50,000 for `xrefs`); DuckDB does not coalesce ranges that far apart, so the cost is request count, not bytes, and at typical latency it is minutes. The single-step form is therefore **not** the remote path. The README documents the two-step form (B2) as the way to look up an accession remotely and on child tables, and the one-step form as a local convenience on `entries`. Phase 3 records request counts for both forms to keep this claim honest.

---

## 3. Phase 0 — feasibility spikes (must pass before Phase 1)

Run all three in the project environment (`environment.yml`: `pyarrow>=23.0,<24`, `duckdb>=1.5,<2`). Record results in the Results log at the top of this file.

### 0.1 Can the pinned PyArrow write bloom filters?

```python
import inspect, pyarrow.parquet as pq
print(pq.__version__ if hasattr(pq, "__version__") else "")
sig = inspect.signature(pq.ParquetWriter.__init__)
print([p for p in sig.parameters if "bloom" in p.lower()])
```

- **Pass:** a bloom-related parameter exists (e.g. something like `write_bloom_filter` / `bloom_filter_columns` / `bloom_filter_fpp`; the exact spelling is version-dependent, read the docstring). Phase 1 proceeds as in §4.2.
- **Fail:** no such parameter. **B1 is deferred** (decision 2026-09-16, §4.3): skip Phase 1, proceed to Phase 2 (`accession_map`), and record the deferral here and in the release notes.

### 0.2 Does DuckDB write and read them, and skip row groups?

```python
import duckdb
con = duckdb.connect()
con.sql("""
  COPY (SELECT 'P' || lpad((range*7919 % 1000000)::VARCHAR, 5, '0') AS acc, range AS v
        FROM range(1_000_000))
  TO '/tmp/bf_test.parquet'
  (FORMAT PARQUET, ROW_GROUP_SIZE 100000,
   DICTIONARY_SIZE_LIMIT 100000,
   BLOOM_FILTER_FALSE_POSITIVE_RATIO 0.001)   -- the entries value from §2.1, not 0.05
""")
print(con.sql("""
  SELECT row_group_id, bloom_filter_offset IS NOT NULL AS has_bloom, bloom_filter_length
  FROM parquet_metadata('/tmp/bf_test.parquet') WHERE path_in_schema = 'acc'
""").fetchall())
print(con.sql("EXPLAIN ANALYZE SELECT * FROM '/tmp/bf_test.parquet' WHERE acc = 'P00007'").fetchall()[0][1])
```

- **Pass:** every row group shows `has_bloom = true`, and `EXPLAIN ANALYZE` reports fewer row groups scanned than 10 (DuckDB prints the skipped/scanned row-group counts in the Parquet scan node).
- **Note:** DuckDB only writes bloom filters for columns it dictionary-encodes. For a unique-per-row `acc` column that requires `DICTIONARY_SIZE_LIMIT` ≥ row-group size. Confirm the option name against the installed DuckDB docs if the COPY fails.

### 0.3 Baseline: how bad is pruning today?

Against the largest lake you have (the demo lake at `demo/lake/2026_01/lake/` is acceptable for the *mechanics*; the numbers only mean something on a full or subset build):

```sql
SELECT count(*) AS row_groups,
       count(*) FILTER (WHERE stats_min <= 'P04637' AND stats_max >= 'P04637') AS candidate_groups
FROM parquet_metadata('<lake>/entries/*/*.parquet') WHERE path_in_schema = 'acc';
```

Record `candidate_groups / row_groups`. On production TrEMBL files this is expected to be ~1.0; that is the number Phase 1 has to move.

---

## 4. Phase 1 — B1: bloom filters on `acc`

### 4.1 Where the writer lives

`bin/parquet_transform.py`, `stream_to_parquet(con, sql, table_dir, batch_size, label, sort_order)` (around line 837). It:

1. runs the table SQL via `con.sql(sql).to_arrow_reader(batch_size)`,
2. opens a `pq.ParquetWriter(path, schema, compression="zstd", sorting_columns=...)` per output file,
3. calls `writer.write_table(tbl, row_group_size=100_000)`,
4. rolls to a new file at `TARGET_FILE_BYTES` (256 MiB today; F.4 decides the release value),
5. writes into `<table_dir>/.tmp/` and renames on success.

Keep 1, 4 and 5 exactly as they are. Only 2 and 3 change.

### 4.2 Implementation (PyArrow writer supports bloom filters; minimal diff)

- Add a module constant:
  ```python
  BLOOM_FILTER_COLUMNS = {"entries": ["acc"], "features": ["acc"], "xrefs": ["acc"],
                          "comments": ["acc"], "publications": ["acc"]}
  # Per table (§2.1): expected false-positive row groups per lookup = row_groups × fpp.
  # Child-table values are placeholders until Phase 3 decides whether B1 stays on them.
  BLOOM_FILTER_FPP = {"entries": 0.001, "features": 0.0001, "xrefs": 0.0001,
                      "comments": 0.0001, "publications": 0.0001}
  ```
- In `stream_to_parquet`, extend `writer_kwargs` with the bloom option(s) discovered in 0.1, keyed by `label`. Do not hard-code the parameter name from memory; take it from the 0.1 output.
- Add a CLI flag `--no-bloom-filters` (default: on) in `main()` next to the other `add_argument` calls, so a build can be reproduced without them if a reader regresses.

### 4.3 If PyArrow cannot write bloom filters: ship `accession_map` alone (decided 2026-09-16)

B2 is the primary path (§2), so deferring B1 loses only the local single-step convenience on `entries`. Adding a bloom filter later changes no column and no file name, so it is a minor schema bump under H.4 once the pinned PyArrow supports it. The alternative, moving the writer to DuckDB `COPY … (FORMAT PARQUET, BLOOM_FILTER_FALSE_POSITIVE_RATIO …, PARTITION_BY …)`, is **withdrawn and must not be re-proposed** for this reason alone. It was examined and found to put four properties the lake depends on at risk: row order inside each file (a partitioned `COPY` goes through a partition sink and does not guarantee that the `ORDER BY` order survives), `sorting_columns` footer metadata (DuckDB's writer may not emit it), Arrow field metadata (only the PyArrow writer emits `ARROW:schema`, which H.2 needs), and the five-digit file numbering (`FILENAME_PATTERN` supports `{i}` and `{uuid}` only). If bloom filters are ever worth a writer change, that change is a separate plan with its own tests for those four properties.

### 4.4 Manifest and data package

- `manifest.json` (`main()`, the `manifest_tables[name] = {...}` dicts, two places): add `"bloom_filter_columns": [...]` and `"bloom_filter_fpp": <the table's value from BLOOM_FILTER_FPP>` per table (empty list / `null` when `--no-bloom-filters` or when the table has no filter).
- `_build_datapackage()` : surface the same two keys on each resource (alongside `sortOrder`).
- Bump `manifest["version"]` from 1 to 2. This is the manifest *format* version (the structure gains new keys); the public schema version is H.4's `schema_version`. Add a line to the README "Metadata" paragraph.

### 4.5 Validator

`bin/validate_lake.py`: add `check_bloom_filters(report, lake_dir)` after `check_parquet_integrity` (line ~596) and wire it into `main()`.

- Using DuckDB (the validator already uses it for joins), for each table in the manifest with a non-empty `bloom_filter_columns`, query `parquet_metadata('<table>/*/*.parquet')` and assert every row group for every listed column has `bloom_filter_offset IS NOT NULL AND bloom_filter_length > 0`.
- Functional check on `entries` only: pick 20 accessions from the round-trip sample, run `SELECT count(*) FROM read_parquet(...) WHERE acc = ?`, assert count = 1. This does not prove skipping; presence + the Phase 0.2 spike does. Do **not** parse `EXPLAIN ANALYZE` text in the validator; it is not a stable interface.
- Add it to `validation_report.txt` and to the README validation list (do not hard-code check numbers in this plan; the validator already has more checks than the README's "12").

### 4.6 Tests

`tests/test_parquet_transform.py`:

- `TestBloomFilters.test_every_row_group_has_acc_bloom` for every table with a non-empty `BLOOM_FILTER_COLUMNS` entry (same DuckDB `parquet_metadata` query as the validator).
- `TestBloomFilters.test_point_lookup_returns_one_row` on `entries` for three accessions from the fixture.
- `TestBloomFilters.test_no_bloom_flag` builds with `--no-bloom-filters` and asserts offsets are null (mark `slow` if it doubles suite time).

`tests/test_validate.py`: `test_validate_passes_on_good_lake` already runs the full validator; add a negative test that strips bloom filters (rewrite one file with `pq.write_table` without the option) and asserts the new check fails.

---

## 5. Phase 2 — B2: `accession_map` table

### 5.1 Schema

```
accession_map/review_status=swissprot/accession_map_00001.parquet …   (Part D layout; partition = the primary entry's side)
accession_map/review_status=trembl/accession_map_00001.parquet …
  sorted by reviewed DESC, acc ASC, primary_acc ASC   (reviewed first so the Part D writer sees one flip; acc-sorted within each partition)
  acc          string   NOT NULL   primary or secondary accession (the lookup key)
  primary_acc  string   NOT NULL   current primary accession (FK → entries.acc)
  is_primary   bool     NOT NULL   true when acc == primary_acc
  reviewed     bool     NOT NULL   copied from entries
  taxid        int64    NOT NULL   copied from entries
```

Notes for the implementer:

- A secondary accession can map to **more than one** primary (entries that were split). Uniqueness is on `(acc, primary_acc)`, not on `acc`. Do not add a uniqueness check on `acc` alone.
- `reviewed` and `taxid` are included deliberately: with them, a reader that does not use bloom filters can prune the big tables with the existing sort:
  ```sql
  -- step 1: one row group per partition of accession_map (two row groups; both partitions are acc-sorted)
  SELECT primary_acc, reviewed, taxid FROM accession_map WHERE acc = 'P04637';
  -- step 2: pruned by (reviewed, taxid) statistics, then acc
  SELECT * FROM entries WHERE reviewed = true AND taxid = 9606 AND acc = 'P04637';
  ```
- No file/row-group pointers. They would change every release and tie the table to the physical layout.

### 5.2 SQL

Add `_build_accession_map_sql(schema_paths)` next to the other builders. It reads from the staged Parquet (same `{read_clause}` as the others), not from the written `entries` files, so it works under `--skip-existing`:

```sql
SELECT acc, primary_acc, is_primary, reviewed, taxid FROM (
    SELECT e.primaryAccession AS acc, e.primaryAccession AS primary_acc, true AS is_primary,
           CASE WHEN e.entryType LIKE '%Swiss-Prot%' THEN true ELSE false END AS reviewed,
           e.organism.taxonId AS taxid
    FROM {read_clause} e
  UNION ALL
    SELECT s AS acc, e.primaryAccession AS primary_acc, false AS is_primary,
           CASE WHEN e.entryType LIKE '%Swiss-Prot%' THEN true ELSE false END AS reviewed,
           e.organism.taxonId AS taxid
    FROM {read_clause} e, LATERAL unnest(COALESCE(e.secondaryAccessions, [])) AS t(s)
)
ORDER BY reviewed DESC, acc, primary_acc
```

Guard `secondaryAccessions` with `has("secondaryAccessions")` like the other optional paths (`organismHosts`, `geneLocations`); if absent, the second branch is dropped.

The `reviewed` CASE expression is duplicated from the child-table builders; factor it into a small helper string constant used by all of them rather than pasting a fourth copy.

### 5.3 Registration

- `TABLE_DEFS`: append `("accession_map", None, ["reviewed DESC", "acc ASC", "primary_acc ASC"])` with `partition_column="reviewed"` (Part D). The `_SQL_BUILDERS` dict in `main()` gets the new builder. `--skip-existing` then works unchanged.
- `TABLE_META["accession_map"]`: description, `primary_key: ["acc", "primary_acc"]`, `foreign_keys: {"primary_acc": "entries.acc"}`, `columns.convenience` = all five, `nested` = `[]`.
- `COLUMN_DESCRIPTIONS`: five entries.
- `BLOOM_FILTER_COLUMNS["accession_map"] = ["acc"]` and `BLOOM_FILTER_FPP["accession_map"] = 0.001` (cheap; the sort already prunes, the filter helps the rare multi-primary case and keeps behaviour uniform).
- Sort spill: this table is ~250M + secondaries rows of five narrow columns. The `ORDER BY reviewed DESC, acc` is a real sort (input is not in acc order). Expect low tens of GB of spill, not the 1.2 TB the child tables would need; note it in `upjson2lake.nf` `PARQUET_TRANSFORM` comments.

### 5.4 Views and docs

- `setup_views.sql` and `uniprot_parquet.py` `_SETUP_SQL`: add `CREATE OR REPLACE VIEW accession_map AS SELECT * FROM read_parquet('${BASE}/accession_map/*/*.parquet', …);` in the same form as the other views (Part D layout; the `hive_partitioning` option follows the D.3 spike's choice). (These two files are meant to be identical modulo placeholder syntax; `AUDIT.md` A23 wants one generated from the other. Do not fix A23 here, just keep them in sync.)
- `README.md`:
  - Tables table: add the row (grain "one row per primary or secondary accession", rows ≈ entries + secondaries).
  - Column reference: new block.
  - A short "Looking up by accession" subsection showing (a) the one-line raw-Parquet form that relies on bloom filters, (b) the two-step form for legacy accessions and for remote reads, with the httpfs cost caveat from §2 of this plan.
  - Validation list: the new checks.
- `datapackage.json` follows from `TABLE_META` automatically; verify the FK renders.

### 5.5 Validator

Add `check_accession_map(report, lake_dir)`:

1. `count(*) FILTER (is_primary)` == `entries` row count.
2. `count(*) FILTER (NOT is_primary)` == `sum(len(entries.secondary_accs))` (one scan of `entries`, same pattern as `check_completeness`).
3. Every `primary_acc` exists in `entries.acc` (anti-join, same pattern as `check_referential_integrity`).
4. For primary rows, `(reviewed, taxid)` match `entries` (join, same pattern as `check_denormalized_sync`).
5. Sort order `reviewed DESC, acc ASC, primary_acc ASC` — extend `check_sort_order` to read the order from the manifest instead of assuming the three-key order, so the new table is covered without a special case.
6. No duplicate `(acc, primary_acc)` pairs.

### 5.6 Tests

- `tests/test_parquet_transform.py`: `EXPECTED_TABLES` gains `"accession_map"`; `TestRowCounts` gets the two count identities; `TestSortOrder` covers the new order; `TestDataPackage` asserts the FK.
- `tests/test_roundtrip.py`: for every fixture entry, assert each of its secondary accessions resolves to it via `accession_map`.
- Idempotency (`tests/test_idempotency.py`): `--skip-existing` skips the new table like the others; no special handling needed, but the test's table list must include it.

---

## 6. Phase 3 — benchmark (evidence that it worked)

Add `benchmarks/bench_point_lookup.py` (mirror the structure of `benchmarks/bench_baseline.py`):

- Workloads: 1 accession, 37 accessions (programmatic batch mean), 1,000 accessions; each against `entries` (7 default columns), `features`, `xrefs`; each in both forms, single-step (bloom filter only) and two-step (through `accession_map`); local path and, when a URL is given, httpfs.
- Report per workload: wall time, row groups scanned vs total (from `EXPLAIN ANALYZE`, parsed only in the benchmark, never in the validator; if the installed DuckDB does not expose row-group counts there, derive candidates from `parquet_metadata` statistics plus timing), bytes read and HTTP request count (httpfs).
- Run on the subset or full build only. The fixture has one row group per table and cannot show false positives (§2.1).
- Run once on a lake built with `--no-bloom-filters` and once with them; write both to `benchmarks/results/point_lookup_<label>.json` and a short table into the Results log.
- Pass criteria (subset or full build): the two-step `entries` lookup reads ≤ 3 row groups in total (up to two of `accession_map`, one of `entries`); the single-step `entries` lookup passes no more than `2 × row_groups × fpp` false-positive row groups; the 37-accession batch through the two-step path is at least 10× faster than baseline; over httpfs the two-step form issues fewer than 20 range requests. If any fails, stop and reassess before Phase 4. Use the child-table single-step numbers to decide whether B1 stays on the child tables (§2.1).

---

## 7. Phase 4 — optional client conveniences

Only after Phases 1–3 pass:

- `protein_card(target_acc)` and `unnest_isoforms(target_acc)` in `setup_views.sql` / `uniprot_parquet.py`: resolve `target_acc` through `accession_map` first so legacy accessions work and remote reads take the two-step path. Keep the raw-Parquet form as the documented default; the macros are a convenience, not a requirement.
- Consider a `lookup(acc)` macro returning the primary row plus counts. Do not add more than that.

---

## 8. Acceptance checklist (Part A; the combined list is at the end of the file)

- [ ] Phase 0 results recorded in this file (0.1 outcome, 0.2 row-group skip evidence, 0.3 baseline ratio).
- [ ] Every row group of `entries` (and of any child table that kept B1 after Phase 3) has a bloom filter on `acc` at the table's recorded fpp; `--no-bloom-filters` disables it. If Phase 0.1 failed, B1 is recorded as deferred and B2 ships alone.
- [ ] `manifest.json` version 2 records `bloom_filter_columns` and `bloom_filter_fpp`; `datapackage.json` mirrors them.
- [ ] `accession_map` builds, validates (six checks), round-trips secondaries, and survives `--skip-existing`.
- [ ] Validator has two new checks; README validation list updated; `validation_report.txt` shows them passing on the stress fixture.
- [ ] Benchmark results recorded on a multi-row-group build; two-step lookup ≤ 3 row groups; single-step false positives within `2 × row_groups × fpp`; httpfs request counts recorded for both forms.
- [ ] README documents the two-step form as the primary and remote path, the single-step form as a local convenience on `entries`, and the httpfs request-count caveat.
- [ ] Full test suite green on default and `--stress` fixtures.

## 9. Out of scope, recorded so nobody re-derives them

- Bloom filters on `xrefs.id`, `features.feature_id`, `publications.citation_id`: plausible later, not here.
- Reordering `entries` columns (R9) and new `entries` columns (R5–R7): Parts C and B of this file. Organism partitioning (O4): rejected in D.5 in favour of a `division` column on `entries` and per-file taxid ranges in the manifest.
- Row-group size changes: keep 100,000 until the benchmark says otherwise.

---
---

# Part B — four new `entries` columns

**Motivation:** `DEMAND_REVIEW.md` §3.1 (R5, R6, R7, S1). All four are derivable from the source JSON already in the staged Parquet; no external data. Each is a list or scalar convenience column added to `_build_entries_sql` in `bin/parquet_transform.py`; none removes or changes an existing column.

## B.1 `go_terms :: list<struct{id: string, aspect: string, term: string, evidence_type: string}>`

**Source.** `uniProtKBCrossReferences[]` where `database = 'GO'`. Each has `id` (`GO:0005524`) and `properties` = list of `{key, value}` with keys `GoTerm` (value like `F:ATP binding`) and `GoEvidenceType` (value like `IEA:InterPro`). **Verify these two key strings against the staged Parquet on the fixture before writing SQL** (`SELECT DISTINCT p.key FROM … unnest(properties) p WHERE database='GO'`); do not trust this document for them.

**SQL sketch** (place directly after the existing `go_ids` expression, which stays):

```sql
[ struct_pack(
      id       := x.id,
      aspect   := left(gt.value, 1),          -- 'P' | 'F' | 'C'
      term     := substr(gt.value, 3),        -- strip the 'X:' prefix
      evidence_type := ge.value)
  FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
  IF x.database = 'GO' ]                       AS go_terms
```

DuckDB list comprehensions cannot bind `gt`/`ge` inline; implement the two property lookups as a small SQL macro or as `list_filter(x.properties, p -> p.key = 'GoTerm')[1].value` written out twice. Guard `properties` with `COALESCE(x.properties, [])` and with `has("uniProtKBCrossReferences.properties")` like the xrefs builder does. When `GoTerm` is absent for an entry, `aspect` and `term` are NULL, never the empty string (the validator's empty-string check must not be extended to these).

**Decisions recorded.** Keep `go_ids` because it is the cheap membership-test column (`list_contains` on a flat list); `go_terms` is the display/aspect column. Not a compatibility decision. `aspect` is a one-character VARCHAR, not an enum. `evidence_type` is included because it is free and the GO evidence string is part of what the API's `go_p` column renders; it is named `evidence_type`, not `evidence`, because the value is a type plus source (`IEA:InterPro`), not an ECO code like `features.evidence_codes` (G.3, decided 2026-09-16).

**Validator / tests.** In `check_round_trip` (or `tests/test_roundtrip.py`), for the sampled entries assert `len(go_terms) == len(go_ids)` and that every `go_terms[i].id` is in `go_ids`. Extend `TestSchema.test_entries_required_columns` with `go_terms`. Add `COLUMN_DESCRIPTIONS[("entries","go_terms")]` and the `TABLE_META["entries"]["columns"]["convenience"]` entry.

**Size gate.** Run on the A12 slice (or the stress fixture as a proxy, noting it is Swiss-Prot-heavy). Fixture-proxy numbers are in the Results log (Step 6); the slice run is pending:

```sql
SELECT reviewed, avg(len(go_ids)) AS mean_go, avg(CASE WHEN len(go_ids)=0 THEN 1 ELSE 0 END) AS null_rate
FROM entries GROUP BY 1;
```

Record the numbers here. No threshold blocks this column; the check exists to size the column in the release notes.

## B.2 `pubmed_ids :: list<string>`

**Source.** `references[].citation.citationCrossReferences[]` where `database = 'PubMed'` → `id`. This is the same path the publications builder already guards with `has("references.citation.citationCrossReferences")` (line ~599); reuse the guard.

**SQL sketch:**

```sql
list_transform(
    list_sort(list_transform(
        list_distinct(flatten(list_transform(
            COALESCE(e."references", []),
            r -> [ c.id FOR c IN COALESCE(r.citation.citationCrossReferences, []) IF c.database = 'PubMed' ]
        ))),
        x -> CAST(x AS BIGINT))),
    x -> CAST(x AS VARCHAR))                     AS pubmed_ids
```

**Decisions recorded.** Deduplicated and **sorted numerically** (decided 2026-09-16: cast to BIGINT for the sort, stored back as strings) so the column is deterministic across runs and reads in the order people expect (`'9999'` before `'10000'`) — `list_distinct` alone has implementation-defined order and would break round-trip equality tests. If any PubMed id fails the BIGINT cast on the slice (`TRY_CAST` returns NULL), stop: the source is not what this assumes. Stored as strings to match the API and `publications.citation_xrefs`, not as integers. Empty list, not NULL, when there are no PubMed citations (consistent with `go_ids`, `keyword_ids`).

**Validator / tests.** Assert for sampled entries that `pubmed_ids` equals the sorted distinct set of PubMed ids in that entry's `publications.citation_xrefs` rows. Extend `TestSchema` and `COLUMN_DESCRIPTIONS`.

**Size gate.** Record share of entries with ≥1 PubMed id and p99 list length, by `reviewed`, on the slice:

```sql
SELECT reviewed, avg(CASE WHEN len(pubmed_ids)>0 THEN 1 ELSE 0 END) AS share_with_pubmed,
       quantile_cont(len(pubmed_ids), 0.99) AS p99_len, max(len(pubmed_ids)) AS max_len
FROM entries GROUP BY 1;
```

## B.3 `proteome_ids :: list<string>`

**Source.** `uniProtKBCrossReferences[]` where `database = 'Proteomes'` → `id` (`UP000005640`). The `Component` property (chromosome/plasmid) stays in `xrefs.properties`; it is not lifted (demand for `proteomecomponent` is small: 36k requests).

**SQL sketch** (next to `xref_dbs`):

```sql
list_sort(list_distinct([ x.id FOR x IN COALESCE(e.uniProtKBCrossReferences, []) IF x.database = 'Proteomes' ]))  AS proteome_ids
```

**Decisions recorded.** Deduplicated because one entry can carry the same proteome id once per component. Sorted for determinism. Empty list when none.

**Validator / tests.** Assert `len(proteome_ids) == count(DISTINCT xrefs.id WHERE database='Proteomes')` for sampled accessions. Extend `TestSchema`, `COLUMN_DESCRIPTIONS`, `TABLE_META`.

**Size gate.** Record distinct proteome count and share of entries with none, by `reviewed`.

## B.4 `gene_name :: string`

**Source.** `genes[1].geneName.value`, i.e. exactly `gene_names[1]`. Emit it immediately after `gene_names` in the SELECT.

**Consequential edits (all required, or the column appears twice):**

- `setup_views.sql` line 33 and `uniprot_parquet.py` line 42: the `entries` view becomes plain `SELECT * FROM read_parquet(...)`; remove `, gene_names[1] AS gene_name`.
- Every macro in both files that says `e.gene_names[1] AS gene_name` (`protein_card`, `entries_with_features`, `entries_with_xrefs`, `unnest_isoforms`) becomes `e.gene_name`.
- `README.md` "Column reference": add `gene_name` to the Gene/protein line. The README examples already use `gene_name` and keep working.
- `AUDIT.md` S1 / A-S1: mark shipped.

**Tests.** `TestSchema` adds `gene_name`; a round-trip assertion that `gene_name IS NOT DISTINCT FROM gene_names[1]` over the whole fixture lake (one DuckDB query).

## B.5 Explicitly deferred: `function_text`, `subcellular_locations` (R8)

Not in this plan. They are gated on a coverage-and-size check because TrEMBL entries carry ARBA/UniRule FUNCTION comments and the column could be a large share of `entries` bytes. Run this on the A12 slice and record the result here; adopt in a later plan only if coverage is material **and** the two columns add < ~5% to `entries` bytes:

```sql
SELECT comment_type, from_reviewed, count(DISTINCT acc) AS entries, sum(strlen(text_value)) AS bytes  -- from_reviewed: the slice predates the G.3 rename
FROM comments WHERE comment_type IN ('FUNCTION', 'SUBCELLULAR LOCATION') GROUP BY 1, 2;
-- compare `bytes` with the compressed size of entries/ from manifest.json
```

---

# Part C — reorder the `entries` columns

**Motivation:** `DEMAND_REVIEW.md` §4 (R9). The seven UniProt default columns plus `taxid` and `sequence` are the nine columns in every top return-field pair, are 98–100% of accessions-endpoint requests and 72–97% of stream requests. Parquet reads one column chunk per requested column per row group; DuckDB's httpfs reader coalesces *adjacent* byte ranges, so keeping the hot chunks physically adjacent reduces range requests on remote reads. Locally the change is neutral.

## C.1 Target order

The SELECT list in `_build_entries_sql` becomes, in this order:

1. `acc`, `id`, `reviewed`, `taxid`, `organism_name`, `gene_names`, `protein_name`, `seq_length`, `sequence` — the nine hot columns, contiguous, first.
2. `gene_name` (Part B.4).
3. The remaining current convenience columns in their current relative order: `secondary_accs`, `organism_common`, `lineage`, `division` (D.5), `gene_synonyms`, `alt_protein_names`, `protein_flag`, `ec_numbers`, `protein_existence`, `annotation_score`, `seq_mass`, `seq_md5`, `seq_crc64`, `go_ids`, `go_terms` (B.1), `xref_dbs`, `proteome_ids` (B.3), `keyword_ids`, `keyword_names`, `first_public`, `last_modified`, `last_seq_modified`, `entry_version`, `seq_version`, `feature_count`, `xref_count`, `comment_count`, `reference_count`, `pubmed_ids` (B.2), `uniparc_id`, `entry_type`, `extra_attributes`.
4. Nested columns last: `organism`, `protein_desc`, `genes`, `keywords`, `organism_hosts`, `gene_locations` when Part C lands (step 2). A13 (step 3) then replaces the first four in the same positions with the residual/full columns named in the `DEDUPLICATION_PROPOSAL.md` appendix (`organism_residual`, `protein_desc_residual`, `genes_full`, `keywords_full`) and adds the promoted `keyword_categories` after `keyword_names`.

`ORDER BY` is unchanged. `sorting_columns` in the footer are built by column *name* (`build_sorting_columns`), so they follow the reorder automatically.

## C.2 What this breaks, and the response

- **Positional readers** (`SELECT * … ` consumers comparing by index, `pd.read_parquet(...).iloc[:, 0:7]`): would change. There are none outside this repo; the in-repo ones are the tests and view files listed below. The reorder simply lands before the first public release.
- **Manifest comparisons across releases** (`AUDIT.md` A10 drift diff): a reorder shows as a diff. Acceptable; it is a real schema change.
- **Tests:** `TestSchema` uses sets of names, unaffected. `check_schema_types` in the validator matches by name, unaffected. `tests/test_roundtrip.py` compares by name, unaffected. Nothing in the repo reads `entries` positionally; grep for `.column(0)` / `[0]` on entries batches before claiming this.

## C.3 Verification (this is what the reorder exists for)

- From `parquet_metadata('lake/entries/*/*.parquet')`, for one row group, list `(path_in_schema, data_page_offset, total_compressed_size)` ordered by offset and confirm the nine hot columns are contiguous.
- In `benchmarks/bench_point_lookup.py` (Part A Phase 3), add a "default seven columns, one organism" workload and, when a URL is given, record the HTTP request count from DuckDB's `EXPLAIN ANALYZE` before and after the reorder. Record both numbers here. There is no pass threshold; the requirement is that the number does not go up.

---

# Part D — Hive-partitioned tables: `review_status=swissprot|trembl` directories, `reviewed` kept as a stored column

**Motivation:** `DEMAND_REVIEW.md` §2.1 (R2). `reviewed` is the most-requested filter (44.2M requests, 10,357 clients, top facet). The sort already places every Swiss-Prot row first. This part makes the split visible **to engines**, not only to readers of the README: a filter on the partition key prunes whole files before their footers are opened, a Swiss-Prot-only download is one directory, and the layout matches what every Parquet release in `benchmarks/DISTRIBUTION_PRACTICES_SURVEY.md` does (Overture `theme=/type=`, Common Crawl `crawl=/subset=`, Open Targets `sourceId=`, and this repository's own `main` branch, which uses `review_status=sp|tr`; this plan spells the values out, see D.1 rule 1).

**History of this decision.** An earlier draft rejected Hive directories and proposed flat directories with side-named files (`entries_sp_00001.parquet` / `entries_tr_00001.parquet`). The rejection rested on three costs: a flat glob `entries/*.parquet` returns nothing; the partition column lives only in the path with engine-dependent typing (`BOOLEAN` in one engine, the string `'true'` in another); and a single downloaded file no longer says which side it is. The second and third costs come from using the boolean `reviewed` column itself as the partition key, because engines refuse or mishandle a column present both in the path and in the file. Using a **separately named string key** in the path and keeping `reviewed` stored in every file removes both. The first cost is real for raw DuckDB users and is accepted: it is one documented glob, the client hides it (F.2.2), and every other engine takes the directory path. Side-named files are therefore withdrawn; do not re-propose them.

## D.1 Layout

Every table becomes a two-partition Hive directory:

```
lake/
  entries/
    review_status=swissprot/entries_00001.parquet …
    review_status=trembl/entries_00001.parquet …
  features/
    review_status=swissprot/features_00001.parquet …
    review_status=trembl/features_00001.parquet …
  xrefs/       review_status=swissprot/… review_status=trembl/…
  comments/    review_status=swissprot/… review_status=trembl/…
  publications/review_status=swissprot/… review_status=trembl/…
  accession_map/review_status=swissprot/… review_status=trembl/…     (Part A; partitioned by the *primary* entry's side)
```

Rules:

1. **Partition key** is `review_status` with values `swissprot` (Swiss-Prot, `reviewed = true`) and `trembl` (TrEMBL, `reviewed = false`). String values, not booleans, so every engine types the path column identically. The values are spelled out rather than the FTP's `sp`/`tr` (decided 2026-09-16): a path is the one place a value has to explain itself to a Spark or Polars user who has never seen the FTP, and `main`'s `sp|tr` has no external readers to keep. Two values and one level; D.5 records why no second level is adopted.
2. **`reviewed` stays as a stored BOOLEAN column** in every file of every table, exactly as today (the child tables' `from_reviewed` is renamed to `reviewed` in G.3, which lands before Part D). Each file is self-describing without its path, and a reader with `hive_partitioning=false` sees the same schema as before. The redundancy is one run-length-encoded bit per row.
3. **No file straddles the boundary** by construction: the writer opens a new partition directory when the side flips.
4. Numbering restarts at `00001` in each partition. The file name pattern is unchanged, `{label}_{n:05d}.parquet`.
5. Sort order within each partition is unchanged: `taxid ASC, acc ASC` (the first key is now constant within a partition). The global order across the table is still `(reviewed DESC, taxid ASC, acc ASC)` because `swissprot` precedes `trembl` lexically (a reason to keep these spellings if anyone proposes others); `sorting_columns` footer metadata is unchanged.

What each engine sees, all verified in D.4:

| Reader | Call | Result |
| --- | --- | --- |
| DuckDB | `read_parquet('lake/entries/*/*.parquet')` | all rows; `review_status` VARCHAR added automatically (auto-detected Hive layout); `WHERE review_status = 'swissprot'` opens no `trembl` file |
| DuckDB, flat glob | `read_parquet('lake/entries/*.parquet')` | **no files** — the one ergonomic cost; documented in README and the error is DuckDB's own "No files found" |
| DuckDB, recursive glob | `read_parquet('lake/entries/**/*.parquet')` | all rows, Hive auto-detected; the form the README recommends for raw DuckDB reads, because it is independent of partition depth |
| DuckDB, explicit | `read_parquet('lake/entries/*/*.parquet', hive_partitioning = false)` | all rows, no extra column, identical schema to today |
| Polars | `pl.scan_parquet('lake/entries/')` | directory scan, Hive auto-detected, `review_status` as `String`, file pruning on filter |
| pandas / PyArrow | `pd.read_parquet('lake/entries/')`, `pq.read_table('lake/entries/')` | `partitioning='hive'` is PyArrow's default for directories; `review_status` dictionary<string> |
| PyArrow dataset | `ds.dataset('lake/entries/', partitioning='hive')` | as above; must pass `partitioning` explicitly to `ds.dataset` |
| Spark | `spark.read.parquet('lake/entries/')` | native; `review_status` string |
| R arrow | `open_dataset('lake/entries/')` | Hive auto-detected |
| `uniprot_parquet.connect()` / `setup_views.sql` | unchanged call | views built over the explicit file list from `manifest.json` (F.2.2), so partitioning is invisible; a `review_status` column appears in the views only if the D.3 spike picks option 2 (users filter on `reviewed`, which is stored, either way) |

Because `reviewed` is stored and `review_status` is derived, the two never disagree by construction, and the validator checks it (D.3).

## D.2 Writer change

`stream_to_parquet` in `bin/parquet_transform.py` (around line 837) receives batches already sorted `reviewed DESC`, so all `true` rows precede all `false` rows.

- Add an argument `partition_column` (`"reviewed"` for every table after the G.3 rename; every table in `TABLE_DEFS` sets it explicitly rather than defaulting, so a future table cannot be written unpartitioned by accident). Maintain the current side and a per-side file counter. Map `true → "swissprot"`, `false → "trembl"`.
- Output directory for the current side is `os.path.join(table_dir, f"review_status={side}")`; the `.tmp/` staging directory and final rename happen per partition directory (`table_dir/review_status=swissprot/.tmp/` → `table_dir/review_status=swissprot/`), so the atomicity guarantee is unchanged.
- On each batch: if entirely on the current side, write as today. If its first row is on the other side, close the current file and switch partition. If the flip is inside the batch (the boolean is sorted, so `pc.index(col, False)` finds it), write the `true` slice, close, switch, write the `false` slice. A batch contains at most one flip.
- The file-size roll-over rule (F.4 target) applies within a partition; the boundary forces one possibly small final `swissprot` file per table.
- Return value gains the partition: `files` becomes a list of paths **relative to `table_dir`** (`review_status=swissprot/entries_00001.parquet`), which is what `manifest.json`, `SHA256SUMS.txt`, `RELEASE.metalink` and the client all need.
- Nothing about row groups, `sorting_columns`, compression, page index (F.4) or bloom filters (Part A) changes.
- `--skip-existing` (around line 1315): list `*/*.parquet` (or walk one level) instead of `*.parquet`; the row-count and schema reads are unchanged.

## D.3 Everything that references file paths

- `manifest.json`: `files` entries carry the partition prefix. Add per table `"partitioning": {"scheme": "hive", "keys": [{"name": "review_status", "type": "string", "values": ["swissprot", "trembl"], "derived_from": "reviewed"}]}`, so a tool can discover the layout and the stored column it mirrors without parsing paths. Add per-partition row counts under the same key so a client can size a Swiss-Prot-only download.
- `datapackage.json` (`_build_datapackage`): resource `path` lists carry the prefix; add the partitioning block to each resource's descriptor. `croissant.json` (F.2.1): the `FileSet` `includes` becomes `entries/*/*.parquet`.
- `bin/validate_lake.py`: `check_manifest` compares listings recursively. New `check_partitions`: for every file under `review_status=swissprot`, `min(reviewed) = max(reviewed) = true` from `parquet_metadata` statistics; the mirror for `trembl`; the two partitions' row counts sum to the table count and match the manifest's per-partition counts. One metadata query per table, no data read. `check_sort_order` runs per partition and additionally asserts every `swissprot` file sorts before every `trembl` file in the manifest's `files` order.
- `bin/release_manifest.py`: file-size walk becomes recursive (or reads `size_bytes` from the manifest, F.2.1).
- `setup_views.sql`: globs become `${BASE}/<table>/*/*.parquet`, with the `hive_partitioning` form the spike below chooses, and a comment explaining both. `uniprot_parquet.py` `_SETUP_SQL` the same until F.2.2 replaces the globs with manifest file lists; after F.2.2 the glob path is only the fallback when the manifest cannot be fetched.
- **Views and file pruning (spike required).** With `hive_partitioning = false` the views expose only the stored `reviewed` column, and a `WHERE reviewed = true` through a view still opens every TrEMBL footer to read its statistics, which is the remote cost Part D exists to remove. Two options; spike both on the fixture with `EXPLAIN ANALYZE` and record the outcome here:
  1. `SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed) FROM read_parquet('…/*/*.parquet', hive_partitioning = true)` — `reviewed` is then derived from the partition value, so a filter on it is an expression over a Hive column and DuckDB should prune files before opening them. Schema of the view is unchanged (no extra column). **Preferred if DuckDB prunes.**
  2. Leave `review_status` visible in the views (`hive_partitioning = true`, no `EXCLUDE`) and document "filter on `review_status = 'swissprot'` to skip TrEMBL files". Works for certain; costs one redundant column in `SELECT *`.
  Whichever is chosen applies to `setup_views.sql`, `_SETUP_SQL` and the manifest-driven views in F.2.2 (an explicit file list still carries the `review_status=` path segment, so Hive detection works on it).
- Optionally add a `swissprot` view over `review_status=swissprot` in both view files; cheap and matches the FTP mental model. Not required.
- `tests/test_parquet_transform.py`: `TestManifest.test_manifest_files_match_disk` walks one level; new `TestPartitions` — every table has exactly the two partition directories (or one, for a fixture with a single side), every file path matches `^review_status=(swissprot|trembl)/<table>_\d{5}\.parquet$`, the side statistics as above, `read_parquet('entries/*/*.parquet')` in DuckDB yields a `review_status` column with the expected counts, `read_parquet(…, hive_partitioning=false)` yields the pre-Part-D schema, `pl.scan_parquet('entries/')` and `pq.read_table('entries/')` return the fixture's row count, and `SELECT count(*) FROM read_parquet('entries/review_status=swissprot/*.parquet')` equals the fixture's Swiss-Prot count. Fixtures contain both sides (`tests/fetch_fixtures.py`), so the flip is exercised; add one synthetic mid-batch flip with a tiny `--batch-size` on `small.json.gz`.
- `README.md`: the "All tables are sorted Swiss-Prot first…" paragraph gains: *"Every table is Hive-partitioned by `review_status=swissprot|trembl` (Swiss-Prot / TrEMBL). `reviewed` is also stored in every file, so files are self-describing; the directory is what lets engines skip TrEMBL entirely. Raw DuckDB reads use `<table>/**/*.parquet`; Polars, pandas, PyArrow, Spark and R take the table directory."* The "Direct access" examples change from `entries/*.parquet` to `entries/**/*.parquet` and one Swiss-Prot-only example is added (`entries/review_status=swissprot/*.parquet`). The F.2.4 Download section's tier commands become directory copies (see F.2.4).
- `demo/lake/2026_01/` is regenerated.

## D.4 Verification

- `find lake -name '*.parquet' | sed 's#/[^/]*$##' | sort -u` lists exactly `<table>/review_status=swissprot` and `<table>/review_status=trembl` for each table.
- Validator `check_partitions` passes on the full build.
- Each row of the reader table in D.1 is executed against the fixture lake in `TestPartitions` (DuckDB, Polars, PyArrow/pandas; Spark and R are documented, not tested).
- DuckDB `EXPLAIN ANALYZE SELECT count(*) FROM read_parquet('lake/entries/*/*.parquet') WHERE review_status = 'swissprot'` reports only `swissprot` files opened; the Part A Phase 3 benchmark records files and footers touched for a reviewed-only count with and without the partition filter, locally and over httpfs.
- Full test suite green on default and `--stress` fixtures.

## D.5 No second partition level; `division` ships as a column, not a directory

**Candidates considered.** Four second-level keys were weighed. None is adopted as a partition; one becomes a column.

| Key | Verdict | Why |
| --- | --- | --- |
| **Taxonomic division** (`division=human|bacteria|…`, UniProt's own eleven FTP divisions) | **Rejected as a partition level; adopted as an `entries` column** | The measured organism demand (`DEMAND_REVIEW.md` §2) is per `taxid` (`organism_id`, `taxonomy_id`), not per division. An engine given `taxid = 9606` cannot use a `division=` key, so the level prunes nothing for the queries people actually run; only a query that literally names the division benefits, and the only division anyone is likely to name is `human`. Division membership moves when the taxonomy changes, so an entry's file path would not be stable across releases, which the per-file hashes and tier downloads (F.2) rely on. It is 132 leaf directories for a benefit concentrated in one of them. Applied to `accession_map`, which is sorted by `acc`, it would turn a two-row-group lookup into a twenty-two-row-group one. |
| **`human` alone** (`organism=human|other`) | Deferred; the only second-level key worth revisiting | A single, stable taxid, so none of the drift objection to top-N applies, and it is the most-requested organism. But it costs a path level on every table for one cell, and the per-file taxid ranges below give a one- or two-file human download without any layout change. Revisit only under the condition at the end of this section. |
| Top-N organisms + `_other` (`AUDIT.md` O4) | Rejected | A curated list, not a function of the data: users must know whether their organism is on it, the engine cannot prune inside `_other`, and the list drifts. |
| `xrefs` by `database` | Deferred to Part E's triggers | Real demand, but it works against the per-accession pattern Part A fixes, and the largest reason to touch `xrefs` by database (GO) moves to `entries.go_terms` in Part B. |
| `features` by `type`, `comments` by `comment_type` | Rejected for now | Evidence-neutral in `DEMAND_REVIEW.md` §2.4; per-organism access is already contiguous within a partition. |

**What ships instead.**

1. **`division` column on `entries`** (implemented 2026-09-16 with one deliberate deviation from the Step 6 sketch: every Eukaryota not matched by an earlier rule maps to `invertebrates`, because that is where the FTP files protists such as *Plasmodium*; `unclassified` is the remainder. Ten of the eleven values occur on the stress fixture. The gate below is still required.) (VARCHAR; the eleven FTP division names in lower case: `archaea`, `bacteria`, `fungi`, `human`, `invertebrates`, `mammals`, `plants`, `rodents`, `vertebrates`, `viruses`, `unclassified`), computed once per entry from `lineage` and `taxid` in `_build_entries_sql` (human = taxid 9606; rodents = Rodentia; mammals = Mammalia minus the two; and so on, reproducing UniProt's own rules). Not denormalised onto the child tables; join through `entries`, and add it there in a minor bump if demand appears. Enumerated per H.3. **Correctness gate, still required:** compare the per-division Swiss-Prot and TrEMBL entry counts with the entry counts of the same release's `taxonomic_divisions/uniprot_{sprot,trembl}_<division>.dat.gz` files on the UniProt FTP (line counts of `//` terminators, or the numbers in `relnotes.txt`). Exact match per division, or the lineage rules are fixed before the column ships. Users of the FTP will expect this column to mean what the FTP means.
2. **Per-file `taxid_min` / `taxid_max` in the manifest** (`file_details`, F.2.1), taken from the file's row-group statistics at write time, so they cost nothing. Because every partition is sorted `(taxid ASC, acc ASC)`, the files containing any one organism are contiguous and usually one or two per table per side. A client helper `files_for_taxid(taxid)` and a README example ("download human: these files") give an organism-level download for *every* organism without a layout change. This is what the division level was really for.
3. **`accession_map` is excluded from any second level**, whatever is decided later; say so in its manifest `partitioning` description so nobody re-derives it.

**Revisit condition** (before the first public release only; a level added after launch changes every path): if F.1 shows that the `entries` files containing human rows total more than ~10× the bytes of the human rows themselves, a two-file human download is a poor "human tier" and `organism=human|other` on `entries` only is reconsidered. Record the ratio here either way.

Result: ______ (date, build, division-count check outcome, human-files-to-human-rows byte ratio).

---

# Part E — `xrefs`: deliberately unchanged, with the trigger for revisiting

**Motivation:** `DEMAND_REVIEW.md` §2.3 (R4). Every measured way users ask for cross-references is by database (`xref_pdb` 1.3M requests / 1,433 clients; `xref:` filter 3.4M distinct queries, rising; GO 14.9M), and `xrefs` has no database key. Partitioning by `database=` would serve that, but it trades directly against the accession pattern Part A fixes: "all xrefs for one accession" would touch one directory per database instead of one row-group range.

**Decision:** no change to `xrefs` in this plan. Revisit only when all three hold:

1. Part A has shipped and `bench_point_lookup.py` has numbers for `xrefs` lookups with bloom filters (so the cost side of the trade-off is measured, not guessed).
2. The per-database skew check has been run on a full build and recorded here:
   ```sql
   SELECT database, count(*) AS n, count(DISTINCT acc) AS entries
   FROM xrefs GROUP BY 1 ORDER BY n DESC;
   ```
   This decides N for a top-N + `_other` layout and shows how many databases would become tiny files.
3. A one-time check of whether xrefs arrive grouped by database within each entry in the source (`SELECT acc, bool_and(database >= lag(database) OVER (PARTITION BY acc ORDER BY rowid)) …` on the staged Parquet). If they do, a within-accession `database` sort is free at build time and may capture most of the benefit without partitioning.

Until then, the documented pattern for per-database access is `WHERE taxid = ? AND database = ?` (row-group pruned on `taxid`, filtered on `database`), and the GO demand specifically is answered by `entries.go_terms` (Part B.1), which removes the largest single reason to touch `xrefs` by database.

---

# Part F — size and packaging: a standalone `entries` tier, measured before shrunk

**Motivation:** the review of this plan (commit `f1c9fb6`) noted that nothing in Parts A–E makes the lake smaller, that `AUDIT.md` §6 estimates 500 GB – 1.2 TB per release (larger than the ~186 GB of source XML.gz), and that most labs will not download that. It asked for `entries` to be publishable as a standalone tier, for the dedup phases (A11/A13) to land before more child-table bytes are added, and for fewer, larger files to cut per-file footer reads over httpfs.

**Framing.** The target is a **smaller download for the common case**, not a smaller lake. `DEMAND_REVIEW.md` §3 measures the `entries` convenience columns at 93.7% of non-default API demand (67.9% of all), and the lake is already laid out one directory per table. So the largest adoption gain costs zero bytes: make `lake/entries/` fetchable and verifiable on its own, and say so first in the README. Everything that *does* remove bytes is gated on a measurement, because the two in-repo size samples disagree by 14× (`AUDIT.md` §6.1: 150 GB vs 2.4 TB extrapolated) and no shrink decision can be sized until that is resolved.

Six sub-parts. F.1 is a prerequisite for F.3 and F.4; F.2 is independent and can ship with Parts A–D; F.5 and F.6 are policy statements that need a decision from the UniProt FTP team and are recorded here so the README can quote them. Peer evidence for each is in `benchmarks/DISTRIBUTION_PRACTICES_SURVEY.md` (2026-09-11).

| ID | Change | Depends on |
| --- | --- | --- |
| **F.1** | Measure per-table bytes on the A12 slice; record here and in `AUDIT.md` §6.1 | — |
| **F.2** | `entries` tier: per-file sizes and SHA-256 in the manifest, a root `SHA256SUMS.txt` and `RELEASE.metalink`, `croissant.json` beside `datapackage.json`, a top-level `releases.json` index, a client that works on a partial lake, a README "Download" section that leads with `entries` | Part D (file paths) |
| **F.3** | Ordering rule: no new child-table columns until dedup Phase 2 (A13) has shipped; F.1 decides whether `comments`/`xrefs` need their own shrink work | F.1 |
| **F.4** | File-size target: choose between 512 MB and 1 GB for `entries` from the F.1 footer measurement; 256 MB must be justified, not assumed. Add the Parquet page index. | F.1, Part A Phase 0 |
| **F.5** | Retention policy, written in the README in UniProt's own terms | A1/O1 decision |
| **F.6** | Cloud channel: a `parquet/` prefix in UniProt's existing AWS Open Data bucket, EBI FTP/HTTPS/rsync canonical | A3/O2 decision |

## F.1 Measure first (this is AUDIT A12, scheduled here because Parts B and F.3/F.4 all gate on it)

**Slice.** Human reviewed + unreviewed (~210K entries) plus a 1M random TrEMBL sample, built with the current pipeline (`bin/parquet_transform.py`) at the default 256 MB target and 100,000-row groups. The Swiss-Prot-heavy `--stress` fixture is *not* an acceptable proxy for this measurement; the whole point is TrEMBL weight.

**Record, per table and per side (`reviewed` / `from_reviewed`):**

```sql
-- compressed bytes per table, split by side, from the Parquet metadata
SELECT file_name, row_group_id,
       sum(total_compressed_size) AS bytes,
       max(row_group_num_rows)    AS rows
FROM parquet_metadata('<slice>/lake/<table>/*/*.parquet')
GROUP BY 1, 2;
-- join row groups to side via the partition directory in file_name (every file is single-sided after Part D;
-- for child tables use min(from_reviewed) = max(from_reviewed) within the row group)
```

Fill this table in this file (placeholders until run):

| Table | Bytes on slice | Bytes / entry (SP) | Bytes / entry (TrEMBL) | Extrapolated to 248M (TrEMBL-weighted) | Share of lake |
| --- | --- | --- | --- | --- | --- |
| `entries` | | | | | |
| `features` | | | | | |
| `xrefs` | | | | | |
| `comments` | | | | | |
| `publications` | | | | | |
| `accession_map` (Part A) | | | | | |
| **lake total** | | | | | |
| `sorted.jsonl.zst` (same slice) | | | | | n/a |

Also record, for `entries` only: number of files, mean file bytes, and total footer bytes (`pyarrow.parquet.read_metadata(f).serialized_size` summed over files), plus the bloom-filter bytes once Part A Phase 1 is in (`parquet_metadata()` exposes `bloom_filter_length` per column chunk in DuckDB ≥ 1.2; otherwise sum from PyArrow's column-chunk metadata). These feed F.4.

**Done when:** the table above is filled, `AUDIT.md` §6.1 cites it instead of the two biased fixtures, and the extrapolated `entries` bytes are quoted in the README "Download" section (F.2.4). Effort is hours of compute plus a day; nothing else in Part F needs code first.

## F.2 Standalone `entries` tier

The lake directory stays as Part D defines it (`lake/<table>/review_status=<side>/<table>_NNNNN.parquet`, `lake/manifest.json`, `lake/datapackage.json`). "Tier" is a packaging and client property, not a second layout: a user who copies `lake/entries/` plus the two metadata files, and nothing else, must be able to verify what they have and run every `entries`-only query and macro. `sorted.jsonl.zst` already lives beside `lake/`, not inside it, so an `entries` fetch never pulls it; keep that and document it as optional.

### F.2.1 Per-file sizes and hashes in the manifest

`bin/parquet_transform.py`, the manifest block after the table loop (the `manifest = {"format": "uniprot-parquet", …}` dict, around line 1425). Today `tables[<name>]["files"]` is a list of file names and nothing records size or hash; `bin/release_manifest.py` recomputes `total_size_bytes` per table after the fact, and only an MD5 of the input JSONL exists.

- Keep `files` as the list of names (the in-repo tests and the datapackage builder use that shape, and a plain name list stays easy to consume; nothing external depends on it, so restructure if a better shape emerges). Add, per table, `size_bytes` (sum of file sizes) and `file_details: {<name>: {"size_bytes": int, "sha256": str, "taxid_min": int, "taxid_max": int}}` (the taxid range from the file's row-group statistics, D.5 item 2). Compute SHA-256 with a streamed 1 MiB-chunk reader while the file is still in page cache, immediately after `stream_to_parquet` returns for that table; do not re-read the whole lake at the end.
- The `--skip-existing` branch (the `existing = sorted(…)` block around line 1370) must fill the same keys from disk so a resumed build produces an identical manifest.
- Write `lake/SHA256SUMS.txt` in `sha256sum -c` format with paths relative to `lake/` (`<hash>  entries/review_status=swissprot/entries_00001.parquet`), one line per Parquet file plus `manifest.json`, `datapackage.json` and `LICENSE` (H.6) hashed last. A partial-download user verifies with `grep '  entries/' SHA256SUMS.txt | sha256sum -c`, or `grep '  entries/review_status=swissprot/'` for the Swiss-Prot tier. This is the per-file hash list `AUDIT.md` A5 (atomic publish-swap) asks for; produce it here so A5 only has to move it.
- Write `RELEASE.metalink` in `lake/` (and one per table directory) in Metalink 4 XML (RFC 5854): one `<file name=…>` per Parquet file with `<size>`, `<hash type="sha-256">` and `<hash type="md5">`, and `<url>` relative to the directory. This is the convention on every other UniProt FTP folder (`help/metalink.md` in `ebi-uniprot/uniprot-manual`: "Every folder on our FTP server contains a file called RELEASE.metalink"), so `curl --metalink` and existing UniProt download scripts work on the lake unchanged. MD5 is included only for metalink compatibility with those scripts; SHA-256 is the hash the validator checks. Generating it costs one more pass over the same `file_details` dict; put it next to the `SHA256SUMS.txt` writer.
- `_build_datapackage()` (around line 1127): add Frictionless `bytes` (the table's `size_bytes`) to each resource. Frictionless `hash` is per resource, not per path, so leave it out and point `description` at `SHA256SUMS.txt`.
- **Deferred to a minor bump after launch.** New `_build_croissant(manifest, release)` next to `_build_datapackage()`, writing `lake/croissant.json` (MLCommons Croissant 1.1 JSON-LD). Croissant is what Open Targets ships per dataset since 25.06 and what Hugging Face generates for every dataset; no biological Parquet release was found using Frictionless. Keep `datapackage.json` (it costs nothing) and generate both from the same `TABLE_META` and `COLUMN_DESCRIPTIONS`. Mapping: one `FileSet` per table (`includes: "entries/*/*.parquet"`, `encodingFormat: "application/vnd.apache.parquet"`); one `FileObject` per Parquet file with `contentSize` and `sha256` from `file_details` (Hugging Face leaves `sha256` empty; filling it is cheap and is the point); one `RecordSet` per table with a `Field` per column carrying the description and a `dataType` mapped from the Arrow type (string, int, float, bool, date; nested columns as `sc:Text` with the Arrow type in `description`, since Croissant has no struct type). `license`, `citeAs`, `version` (`uniprot_release` + `schema_version`) and `datePublished` come from the same config `AUDIT.md` A6 introduces. Validate with the `mlcroissant` package in `tests/test_parquet_transform.py` (`TestCroissant.test_validates` behind an `importorskip`).
- Copy `validation_report.txt` into the release directory as data, not only as text: extend `bin/validate_lake.py` `main()` to write `validation_report.json` beside the `-o` text file (check name, pass/fail, detail, the row counts it computed) and have `upjson2lake.nf` publish both. Open Targets is the only peer that publishes release QC as data (a Hugging Face dataset plus dashboard); a per-release JSON is the minimum that lets a downstream tool assert "this release passed round-trip" without parsing prose. `releases.json` (F.2.5) links to it.
- `bin/release_manifest.py`: read `size_bytes` from the lake manifest instead of walking the directory; keep the walk as a cross-check that logs a warning on mismatch.

**Verification:** `tests/test_parquet_transform.py` `TestManifest` gains `test_file_details_match_disk` (every listed file exists, size matches `os.path.getsize`, SHA-256 matches a fresh computation for at least the first file of each table) and `test_sha256sums_verifies` (run `sha256sum -c --quiet SHA256SUMS.txt` in the lake directory via `subprocess`, expect exit 0). `bin/validate_lake.py` `check_manifest` verifies every hash on the full build; this is the release gate for "what we published is what we built".

### F.2.2 Client works on a partial lake

`uniprot_parquet.py` `connect()` (line 164) runs `_SETUP_SQL`, which creates all the views eagerly over `read_parquet('{BASE}/<table>/*.parquet')` (`<table>/*/*.parquet` after Part D). DuckDB binds a view at creation, so on an `entries`-only copy `connect()` raises at the `features` view and the user gets nothing. Two changes:

1. **Manifest-driven views.** Read `manifest.json` first (`manifest()` at line 217 is local-only; extend it to fetch over `http(s)://` with `urllib` and over `s3://` via DuckDB's `read_text`, or fall back to the glob when the manifest cannot be fetched). For each table in `manifest["tables"]`, create the view over the explicit file list (`read_parquet(['{BASE}/entries/review_status=swissprot/entries_00001.parquet', …], hive_partitioning = <the option the D.3 spike chose>)`; the explicit list still carries the `review_status=` path segment, so D.3's choice applies unchanged and the views must not hard-code `false`) **only if the table's first file is present** (local: `os.path.exists`; remote: a `HEAD` request, or simply attempt the `CREATE VIEW` and catch `duckdb.IOException`). A `DuckDBPyConnection` cannot carry a Python attribute recording which tables were mounted, so extend the existing `tables()` helper to report `present` / `missing` per table from the same manifest-plus-existence check, and replace each missing table's view with a stub that raises a clear error:
   ```sql
   CREATE OR REPLACE VIEW features AS
     SELECT error('table "features" is not in this lake copy; download lake/features/ (see README "Download")') AS _;
   ```
   This keeps `SELECT … FROM features` failing loudly with an actionable message instead of "no files found".
   Explicit file lists are also what makes the client work over plain HTTP, where DuckDB's httpfs cannot expand a glob (no directory listing). That is a pre-existing gap the README's remote example papers over (`AUDIT.md` A3 / A15); fixing it here is incidental but should be called out in the release notes.
2. **Macros stay lazy.** DuckDB table macros are bound at call time, so `protein_card`, `organism_features` etc. can still be created; the ones that join a missing table fail with the stub view's message when called. No change needed, but `tests/test_client.py` (A15) must prove it.

`setup_views.sql` is the pure-SQL twin (`AUDIT.md` Q-L10 notes the two already drift). It cannot read a manifest; leave it glob-based and add a comment block at the top: "requires all six tables; for an `entries`-only copy use `uniprot_parquet.connect()` or create only the `entries` view".

**Verification:** `tests/test_client.py` (create it if A15 has not yet) gains `test_connect_entries_only`: copy the fixture lake's `entries/`, `manifest.json`, `datapackage.json` (and `accession_map/` once Part A lands) to a temp dir, `connect()`, assert `SELECT count(*) FROM entries` equals the manifest row count, assert `SELECT * FROM protein_card('P12345')`'s `entries`-only fields work if that macro touches only `entries` (check; otherwise pick one that does), and assert `SELECT count(*) FROM features` raises with the text `not in this lake copy`. A second test runs `connect()` on the full fixture and asserts nothing observable changed (same views, same row counts).

### F.2.3 Sizing the tier for the download decision

After F.1, the `entries` tier's byte count is known. Part B adds four `entries` columns and Part A adds `accession_map` and bloom filters; all of those *grow* the tier, and that is the right direction: it is the table people will download, and each addition removes a reason to also download a child table (`go_terms` for `xrefs`, `pubmed_ids` for `publications`). Record the tier's size before and after Parts A/B on the slice in the F.1 table (add an "after v2" column) so the release notes can quote the delta honestly.

### F.2.4 README "Download" section

New `## Download` section in `README.md`, placed before `## Using the lake` (line 21). Content, in this order:

1. A four-row table: **`entries` only** (bytes from F.1; "answers ~94% of measured API demand: accession, organism, gene, names, sequence, GO ids, keywords, PubMed ids, proteomes"), **Swiss-Prot only, all tables** (`review_status=swissprot/` of every table; bytes), **full lake** (bytes), **`sorted.jsonl.zst`** (bytes; "optional; the same data in a language-neutral format"). Numbers come from F.1, not estimates. The client works on any of the three Parquet subsets; a Swiss-Prot-only copy simply has one partition per table.
2. The two commands, entries-only first:
   ```bash
   # entries tier (recommended starting point)
   rsync -av --include='entries/***' --include='*.json' --include='SHA256SUMS.txt' --include='RELEASE.metalink' --include='LICENSE' --exclude='*' \
         rsync://<host>/<release>/lake/ ./lake/
   grep '  entries/' lake/SHA256SUMS.txt | (cd lake && sha256sum -c --quiet)
   # Swiss-Prot only, all tables (one directory per table)
   rsync -avm --include='*/' --include='review_status=swissprot/***' --include='*.json' --include='SHA256SUMS.txt' --include='RELEASE.metalink' --include='LICENSE' --exclude='*' \
         rsync://<host>/<release>/lake/ ./lake/
   # full lake
   rsync -av rsync://<host>/<release>/lake/ ./lake/
   ```
   (Substitute the canonical URL once `AUDIT.md` A3 picks it; use `wget -r` equivalents if EBI does not expose rsync for this path.)
3. One sentence: "`connect()` works on any of these copies; querying a table you did not download fails with a message naming the directory to fetch."
4. A pointer to the remote (httpfs) path for users who do not want to download at all, with the honest caveat from Part A §2 ("Known limitation") about what a remote point lookup costs.

Add "each table's `size_bytes` and per-file SHA-256 are in `manifest.json`; `SHA256SUMS.txt` covers the whole lake" under `### Metadata` (line 173).

### F.2.5 Top-level release index (deferred to a minor bump after launch; format fixed here)

`AUDIT.md` A5/O1 plan `latest` and `previous` symlinks. Symlinks serve shell users on FTP and rsync but are not machine-readable over HTTPS or from an object store. Add a small index one level above the release directories, at `<parquet root>/releases.json`, rewritten atomically by the publish step (A5):

```json
{
  "format": "uniprot-parquet-releases",
  "latest": "2026_03",
  "previous": "2026_02",
  "lts": ["2026_01"],
  "releases": {
    "2026_03": {
      "path": "2026_03/lake",
      "uniprot_release": "2026_03",
      "schema_version": "1.0.0",
      "manifest_version": 2,
      "published": "2026-09-10T00:00:00Z",
      "size_bytes": 0,
      "entries_size_bytes": 0,
      "manifest": "2026_03/lake/manifest.json",
      "croissant": "2026_03/lake/croissant.json",
      "validation_report": "2026_03/validation_report.json",
      "jsonl": "2026_03/sorted.jsonl.zst"
    }
  }
}
```

This is the CELLxGENE Census `release.json` pattern (`stable`/`latest` aliases, per-build URIs, an LTS flag) and Overture's STAC `latest`, and it is what `uniprot_parquet.connect("latest")` should resolve through: extend `connect()` to accept a release alias when given the parquet root rather than a lake directory. `size_bytes` and `entries_size_bytes` let a client tell the user what a full or tier download costs before starting it. The index is produced by the publish step, not by `parquet_transform.py`, because it spans releases; until A5 exists, `bin/release_manifest.py` can emit a single-release version for the demo. Test: `tests/test_client.py::test_connect_by_alias` against a temp root holding the fixture lake and a hand-written `releases.json`.

### F.2.6 Discoverability: a Swiss-Prot `entries` tier on Hugging Face (optional, after the first public release)

An official `huggingface.co/uniprot` organisation appears to exist with no public datasets (unverified, see the survey). A Swiss-Prot-only copy of the `entries` tier (`entries/review_status=swissprot/`, a few GB) fits the Hub's free limits (≤100k files per repo, ≤10k per folder, 500 GB per file) and gets the dataset viewer, `hf://` paths in DuckDB/Polars/PyArrow and an auto-generated Croissant card for nothing. The full lake does not fit the free tier (Tahoe-100M at 429 GB needed a paid plan). Not in scope for the first public release; recorded so the entries-tier layout (Part D) and the row-group size (F.4) are chosen with the Hub's guidance in mind: ~500 MB shards, 100–300 MB uncompressed row groups, `write_page_index=True`.

## F.3 Ordering rule: dedup before more child-table bytes

**Rule, recorded here so later plans inherit it:** no plan adds a column to `features`, `xrefs`, `comments` or `publications` until `DEDUPLICATION_PROPOSAL.md` Phase 2 (`AUDIT.md` A13, residual trim) has shipped. This plan already complies: Part B touches only `entries`, and Part E leaves `xrefs` alone. Because nothing is public, there is no compatibility reason to stage A11/A13 into a later release; the only reason they are not in this plan is engineering risk (A13 is a schema flip that A11's `g()` gate must protect). **Decided 2026-09-16: A11 and A13 ship before the first public release**, so the child tables are never published in their duplicated form and the "single-release flip" language in the proposal becomes moot. They are step 3 of the implementation order; A11's `g()` round-trip test is the release gate for A13. The F.1 slice is measured before the trim, so the F.1 table records pre-trim bytes and the "after v2" column F.2.3 asks for is filled from a post-A13 rebuild of the same slice.

**Caveat the review did not state, and F.1 must settle.** The ~17% dedup shrink comes from the `features.feature`, `publications.reference` and `entries` nested columns (`DEDUPLICATION_PROPOSAL.md`, per-table duplication table). It does not touch `xrefs` (its nested column was already removed) and deliberately leaves `comments.comment` alone (1% duplication). At TrEMBL weight, ~20 xrefs and ~1 comment per entry, `xrefs` is plausibly the largest table by bytes and `comments.comment` is 97% of its table as VARCHAR JSON (`AUDIT.md` §6.2). So if F.1 shows `xrefs` + `comments` above roughly half the lake, the shrink path for the *full* lake is `AUDIT.md` S2 (typed or JSON-logical-type `comment` column) and an `xrefs` byte audit (dictionary-encoding effectiveness on `acc`/`database`/`id`, `properties` list cost), not dedup. Record which case holds in the F.1 table's "Share of lake" column and add the corresponding item to `AUDIT.md` Tier 3. If instead `entries` + `features` + `publications` dominate, A13 is the whole answer and nothing new is needed.

Either way the `entries` tier (F.2) is unaffected: it is small relative to the lake and its bytes are wanted.

## F.4 File-size target: measure the footer cost, then decide

**Where 256 MB sits.** The survey puts the current target at the low end of the modern band: Overture averages 591 MB over 987 files (612 GB release), Common Crawl ≈ 0.7 GB, Hugging Face shards at 500 MB, DuckDB's guidance is 100 MB – 10 GB per file and Athena's 128 MB – 1 GB. The 100,000-row groups are inside DuckDB's 100k – 1M range and near the Hub's ~100 MB-uncompressed recommendation, so row groups stay as they are. Part D already removes every TrEMBL file and footer from a reviewed-only query, and bloom-filter reads (Part A) are per row group and independent of file size. Larger files trade fewer footer round-trips for bigger footers, slower resumable downloads, and less file-level parallelism in engines that parallelise by file rather than row group (DuckDB does not, Spark partly does).

**Decision rule.** The candidates for `entries` are **512 MB and 1 GB**; 256 MB is kept only if the measurement below shows the footer cost is already negligible. Evaluate on the F.1 slice with Part A Phase 1 applied (bloom filters change footer size): build the slice at 256 MB, 512 MB and 1 GB (`TARGET_FILE_BYTES` in `stream_to_parquet`), run the Part A Phase 0.3 organism query (`WHERE taxid = 9606`, default columns, `entries` only) over httpfs against a local HTTP server serving each build, with `EXPLAIN ANALYZE`, and record (a) number of files opened, (b) footer bytes fetched (sum of `serialized_size` for opened files), (c) data bytes fetched, (d) wall time. Extrapolate (a) and (b) to 248M entries using the F.1 `entries` bytes. Pick the smallest target at which extrapolated footer bytes are below 10% of data bytes for that query; if none reaches it, take 1 GB. Do not change child-table file sizes in this plan; they are not on the `entries` tier's download path and their footer cost is dominated by row-group count, not file count.

**Page index.** Pass `write_page_index=True` to `pq.ParquetWriter` for every table. Hugging Face's streaming guidance recommends it for random access within row groups, DuckDB ≥ 1.2 reads it, and the cost is a few KB per column chunk. Record the footer-size delta on the slice in the same table so it is not mistaken for a file-size effect. Verify with `parquet_metadata()`: `column_index_offset` and `offset_index_offset` non-null for every column chunk.

Record the result:

| Target | Files (slice) | Files (extrapolated) | Footer bytes | Data bytes | Footer / data | Wall time | Page index Δ footer |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 256 MB | | | | | | | |
| 512 MB | | | | | | | |
| 1 GB | | | | | | | |

Chosen target: ______ (reason: ______).

## F.5 Retention policy, stated in UniProt's own terms

`AUDIT.md` D1 chose a latest-only model with a short overlap (O1). Nothing is published yet, so there is no back-catalogue to keep; the policy only has to be written before the first public release. The survey shows latest-only is a departure from UniProt's own FTP policy ("We archive previous releases on our ftp site for at least 2 years. Beyond that time, we archive the first release of a year, YYYY_01", `help/synchronization.md`) and from every biological peer except Overture (60 days, written) and the Census weekly builds (~1 month; LTS ≥ 5 years). Users of the rest of the UniProt FTP will assume the lake follows the FTP rule unless told otherwise.

**Recommendation to put to the FTP team (A1/O1):** keep `latest` and `previous`, and additionally keep each year's `YYYY_01` release indefinitely, mirroring the long-term half of the FTP policy at roughly one extra release per year of storage. With F.1's numbers that is a bounded cost the release notes can state. If storage rules this out, the fallback is to keep only the `entries` tier and `manifest.json`/`SHA256SUMS.txt` of retired releases, so a paper's accession list can still be resolved against the release it cited.

Whatever is decided: write it in the README `### Versioning` section in the same words the FTP help uses, record it in `releases.json` (`lts` list, F.2.5), and note that `sorted.jsonl.zst` follows the same rule. Nothing in the pipeline changes; this is a publish-step (A5) rule.

## F.6 Cloud channel

Every flagship dataset in the survey is mirrored to at least one free, anonymously listable object-store bucket (gnomAD on GCS, S3 and Azure; Open Targets on FTP, GCS, S3 and BigQuery; AlphaFold DB on GCS; Census on S3), and every third-party "lakehouse-ready" Parquet copy of a biological database on AWS has been deprecated. UniProt already owns `s3://aws-open-data-uniprot-rdf` (AWS Open Data, eu-west-3, SIB-managed, one prefix per release since 2021-01, RDF only, no checksums).

**Recommendation to put to the FTP team and SIB (A3/O2):** publish the lake under UniProt's own name as a `parquet/<release>/` prefix in that bucket or a sibling bucket in the same registry entry, mirrored from EBI by the publish step; EBI FTP/HTTPS/rsync stays canonical. The `releases.json`, `manifest.json`, `SHA256SUMS.txt` and `RELEASE.metalink` are copied verbatim so the S3 copy is verifiable the same way. Add the registry-supported SNS topic for new-object notifications if the registry entry is updated (Open Targets does this). `uniprot_parquet.connect()` already accepts `s3://` paths; the README's remote example should show both the EBI HTTPS URL and the S3 URI once they exist.

Not in this plan's code; recorded so the manifest and index formats (F.2.1, F.2.5) are designed to be copied to an object store unchanged (relative paths only, no symlinks required).

## F.7 Explicit non-goals

- **No dropping of the nested (lossless) layer to chase parity with XML.gz.** The round-trip guarantee is the one property no peer publishes (`AUDIT.md` §7, A11 rationale); the `entries` tier answers the download-size objection without giving it up. The fair size comparison for the release notes is *`entries` tier vs XML.gz*, not full lake vs XML.gz, because the full lake also carries five denormalised, sorted, individually queryable tables that the XML does not.
- **No second layout, no "lite" schema.** One lake, one manifest; the tier is what you choose to copy.
- **`sorted.jsonl.zst` is published beside the lake** (decided 2026-09-16): it is produced anyway and is the language-neutral fallback the README already documents. It stays outside `lake/` so it never rides along with an `entries` fetch; it is listed as optional in the F.2.4 download table, its SHA-256 is recorded in `provenance.json` next to the existing MD5 of the input JSONL, and it follows the F.5 retention rule. Where EBI hosts it is a question for the publish plan (A1/A5), not whether.
- **No sizing claims in the README until F.1 has run.** The `AUDIT.md` §6 range stays in the audit, not in user-facing text.

---

# Part G — schema hygiene found in the 2026-09-11 review

Three findings from reading the current schema (`demo/lake/2026_01/lake/manifest.json`) against the SQL builders in `bin/parquet_transform.py`. G.1 is a bug; G.2 is a latent one; G.3 is a naming decision that is free only before launch.

## G.1 `comments.text_value` is always NULL (bug)

**Evidence.** The demo manifest types `comments.text_value` as `int32`. The only way a column defined as text ends up `int32` is the `else: text_value_expr = "NULL"` branch in `_build_comments_sql` (DuckDB types a bare `NULL` as INTEGER). That branch is taken when `has("comments.texts")` is false, and it is **always** false: the staged schema types `comments` as `MAP(VARCHAR, JSON)` (see the docstring of `_build_comments_variant_sql`, "Comments are MAP(VARCHAR, JSON) in the JSONL schema"), and `discover_schema_paths` descends into structs and lists but a map's values are opaque JSON strings, so no `comments.texts` path can exist. Every build, demo or production, therefore ships a `text_value` column that is entirely NULL, while `DEMAND_REVIEW.md` R8 and the README both present it as the FUNCTION / SUBCELLULAR LOCATION text. No test or validator check would catch it: `TestSchema` checks the column exists, `check_null_keys` does not cover it, and the round-trip check compares counts.

**Fix** (in `_build_comments_sql`; a few lines):

- Drop the `has("comments.texts")` guard and compute the column unconditionally. Start from the expression the guarded branch already contains, because its `unnest.texts` access is the same form that demonstrably works for `unnest.commentType` on the installed DuckDB:
  ```sql
  NULLIF(array_to_string(
      from_json(unnest.texts->'$[*].value', '["VARCHAR"]'),
      chr(10) || chr(10)), '')                     AS text_value
  ```
  Verify on the fixture that a comment with no `texts` key yields NULL rather than an error (map-extract semantics on a missing key changed across DuckDB 1.x); if it errors, use `map_extract`/`element_at` with `COALESCE` instead. Comments with no `texts` (COFACTOR, INTERACTION, CATALYTIC ACTIVITY, ALTERNATIVE PRODUCTS, …) get NULL, not `''`.
- **Sequencing.** This fix lands before the F.1 slice is built: F.1's `comments` bytes and the B.5 gate both read `text_value`, and today the column is empty, so measuring first would understate `comments` and make the B.5 gate trivially pass.
- **Validator:** in `check_field_completeness` or a new check, assert `count(text_value) > 0` for every text-bearing `comment_type` (`FUNCTION`, `SUBUNIT`, `SUBCELLULAR LOCATION`, `TISSUE SPECIFICITY`, `DISEASE`, …) and, on the round-trip sample, that `text_value` equals the double-newline join of the JSONL comment's `texts[*].value`.
- **Tests:** `tests/test_roundtrip.py` gains the same per-entry assertion; `TestSchema` asserts `text_value` is `string`, not merely present.
- **Docs:** the `AUDIT.md` S12 note (per-fragment texts with evidence) stands as the future shape; this fix only makes the documented column true.

## G.2 Untyped `NULL` fallbacks make column types depend on the input

**Evidence.** 24 occurrences of `else "NULL"` in the SQL builders. When the optional source path is absent from a given input, the column is emitted as an untyped `NULL`, which DuckDB and then Parquet record as `int32`. The demo manifest shows it for `entries.organism_hosts`, `features.ligand_note` and `comments.text_value`; a full build would show the real types for the first two, so **the schema of a release depends on which optional fields the input happened to contain**. That defeats the manifest as a schema contract, makes `check_schema_evolution` (A10) report spurious drift between a subset build and a full build, and means `datapackage.json`/`croissant.json` document different types for the same column across releases.

**Fix.**

- Give every fallback its intended type: `"NULL::VARCHAR"`, `"NULL::VARCHAR[]"`, `"NULL::STRUCT(…)"` and so on. Put the 24 intended types in one dict next to `COLUMN_DESCRIPTIONS` (`COLUMN_TYPES[(table, column)] = "VARCHAR"`) so the fallback, the validator and the datapackage builder read the same source. For struct-typed nested columns whose full type is long (`organism_hosts`, `gene_locations`), it is acceptable to write the DuckDB type string once in that dict.
- Extend `check_schema_types` from the current handful of "critical" columns to **every** column in `COLUMN_TYPES`, comparing the manifest's Arrow type to the expected one. This is what turns the manifest into a contract.
- `tests/test_parquet_transform.py`: build from a fixture that lacks an optional path (`small.json.gz` lacks `geneLocations`; it does carry `organismHosts`) and assert the column's Arrow type is the declared one, not `int32`.
- **Found while implementing (2026-09-16):** the same bug exists for *present* columns. The demo lake's `gene_locations` struct has no `value` sub-field because no demo entry carries one, so the struct type of a present column also depended on the input. Fix applied in Step 4: every optional column is `CAST(expr AS <declared type>)` when present (`_typed()`); DuckDB widens a struct by name, filling missing sub-fields with NULL. Because that cast would also silently *drop* a sub-field the declared type does not list, `check_declared_types()` runs in `main()` after schema discovery and aborts the build when the input has any nested path under a declared column's source that the declared type does not cover. Extending `COLUMN_TYPES` is the only way past it.
- Regenerate `demo/lake/2026_01/` and check the manifest diff shows only type corrections.

## G.3 Naming decisions that are free now and expensive after launch

Decided 2026-09-16:

1. **`from_reviewed` becomes `reviewed` in all six tables.** Child tables called the denormalised flag `from_reviewed` while `entries` and `accession_map` call it `reviewed`; the equally denormalised `taxid` was never prefixed, and the `from_` prefix conveyed provenance that the manifest's `foreign_keys` and column descriptions already record. The rename lands in the step-2 PR with Part C. Mechanically: the four child-table builders in `bin/parquet_transform.py` (`AS from_reviewed`, the `ORDER BY`), `TABLE_DEFS` sort orders, `TABLE_META` convenience lists and descriptions, `COLUMN_DESCRIPTIONS`, every validator query that names it (`check_sort_order`, `check_denormalized_sync`, `check_field_completeness`, …), the `required` sets in `tests/test_parquet_transform.py`, both view files' macros, and the README column reference. `AUDIT.md` and `DEMAND_REVIEW.md` keep the old name as history. SQL in this plan that runs on the F.1 slice (F.1, B.5) still says `from_reviewed` because that slice is built before the rename.
2. **No `is_*` prefix** (`AUDIT.md` S10/O3 closed). `reviewed` reads fine, the survey consensus for `is_` was weak, and a rename buys nothing. `accession_map.is_primary` is the one exception, kept because `primary` alone would read as a noun.
3. **`go_terms.evidence_type`**, not `evidence`, because the value is an evidence type plus source (`IEA:InterPro`), while `features.evidence_codes` holds ECO codes. Applied in B.1.

Record the final names in `SCHEMA.md` (`AUDIT.md` A14) before the first public release and freeze them there.

---

# Part H — cheap conventions the field expects

Six small items, none of which changes a table. Each is what a data engineer or a peer resource would look for first and find missing today. All are writer, manifest or repo-file changes; together they are a few days.

## H.1 Compression level, chosen by measurement

**Today.** `stream_to_parquet` passes `compression="zstd"` and nothing else (around line 905), so PyArrow writes at zstd level 1. The lake is written once per release and read many times; a higher level costs build time once and saves bytes on every download and every remote range read.

**Change.** Add `compression_level` to `writer_kwargs`, driven by a module constant `ZSTD_LEVEL` and a `--zstd-level` CLI flag. Choose the value from a sweep on the F.1 slice, run in the same job as F.1 and F.4: build `entries` and `xrefs` (the two extremes: long strings vs. many small repeated values) at levels 1, 3, 9 and 15; record compressed bytes, write wall time and, for the F.4 organism query, read wall time. Pick the highest level whose write-time cost is within what the SLURM budget in `README.md` "Production notes" tolerates (the transform is already the long stage; a 2× write slowdown on the Parquet write alone is acceptable, a 2× slowdown of the whole stage is not). Expect the answer to be in the 9–12 range; record it here:

| Level | `entries` bytes | `xrefs` bytes | Write time | Read time (F.4 query) |
| --- | --- | --- | --- | --- |
| 1 (today) | | | | |
| 3 | | | | |
| 9 | | | | |
| 15 | | | | |

Chosen level: ______. Record it in `manifest.json` (`"compression": {"codec": "zstd", "level": N}`) and in the Parquet footer key-value metadata (`AUDIT.md` A7), so a rebuild can reproduce the bytes.

## H.2 Column descriptions in Arrow field metadata

**Today.** `COLUMN_DESCRIPTIONS` (around line 978) feeds `manifest.json` and `datapackage.json` only. A user who opens one file in pandas, Polars, PyArrow or R sees bare column names.

**Change.** In `stream_to_parquet`, before the first `ParquetWriter` is created, rebuild `arrow_schema` with each field's metadata set: `field.with_metadata({"description": COLUMN_DESCRIPTIONS[(label, field.name)], "category": "convenience" | "nested", "source_path": <JSON path, when known>})`. PyArrow serialises field metadata into the Parquet file's `ARROW:schema` entry and restores it on read, so `pq.read_schema(f).field("gene_names").metadata[b"description"]` works, `pl.read_parquet_schema` exposes it, and R `arrow::open_dataset()$schema` shows it. Cost: a few KB per file.

- Batches from `to_arrow_reader` carry the bare schema; cast each batch to the annotated schema (`tbl.cast(annotated_schema)` is a no-op on data, metadata only) or, simpler, pass the annotated schema to the writer and let `write_table` accept the batch because field metadata does not participate in schema equality for writing. Verify which the pinned PyArrow accepts; both are one line.
- `source_path`: the JSON path the column is derived from is what `AUDIT.md` A14 wants in `SCHEMA.md` (column → source path). Add a `COLUMN_SOURCES[(table, column)]` dict beside `COLUMN_DESCRIPTIONS` and fill it as part of the same change; `SCHEMA.md` is then generated from the two dicts.
- **Test:** `TestSchema.test_field_metadata` reads one file per table and asserts every field has a non-empty `description`; `check_schema_types` (Part G.2) additionally asserts the metadata description equals the dictionary entry, so files and manifest cannot drift.

## H.3 Enumerations recorded in the manifest (deferred to a minor bump after launch)

**Today.** Nothing lists the valid values of `comments.comment_type`, `features.type`, `entries.protein_existence`, `entries.entry_type`, `publications.citation_type`, `xrefs.database`, or the new `go_terms.aspect` (Part B.1). Users discover them by scanning, and a new UniProt category is invisible to the drift check.

**Change.** In `main()`, after each table is written, run one `SELECT col, count(*) FROM read_parquet(files) GROUP BY 1 ORDER BY 2 DESC` per enumerated column (a fixed list `ENUM_COLUMNS = {"comments": ["comment_type"], "features": ["type"], …}`; for `xrefs.database` this is a full scan of one column over ~5B rows, a few minutes in DuckDB, acceptable, but if the transform stage is tight compute it from the staged data in the same pass as the table SQL; cap at 1,000 distinct values so `xrefs.database` (~190) fits and a mis-listed high-cardinality column fails loudly rather than bloating the manifest). Write the result into `manifest.json` under `tables.<t>.columns[i].enum` as `[{"value": …, "count": …}]`, and into `datapackage.json` as the Frictionless `constraints.enum` list and into `croissant.json` as the `Field`'s `sc:Enumeration`-style description (Croissant 1.1 has no enum constraint; put the list in `description`). `SCHEMA.md` (A14) renders the lists.

- **Validator:** `check_schema_evolution` (A10) compares each enum list to the baseline and reports added or removed values as a warning line in the release notes. Not an error: a new comment type is UniProt's decision, not a pipeline fault.
- **Test:** the fixture's `comment_type` enum contains `FUNCTION`; `go_terms.aspect` enum is exactly `{P, F, C}`.

## H.4 Semantic schema version with a written policy

**Today.** `manifest["version"]` is the integer `1`, bumped to `2` by Part A; `datapackage.json` says `"version": "1.0.0"` (line ~1210) with no rule for when either changes.

**Change.** One `SCHEMA_VERSION = "1.0.0"` constant in `bin/parquet_transform.py`, surfaced identically as `manifest.json` `schema_version`, `datapackage.json` `version`, `croissant.json` `version`, the Parquet footer key-value metadata (A7) and `releases.json` (F.2.5). It is `1.0.0`, not `2.0.0`, because it is the first *published* schema; the "v2" in this plan's history is an internal label. The integer `version` field stays and means the manifest *format* version; it becomes `2` in this plan because the manifest structure gains `file_details`, `partitioning`, `bloom_filter_*` and `schema_version`. The two numbers are independent and both are surfaced in `releases.json`. The policy, written into `SCHEMA.md` and the README `### Versioning` section:

- **Major:** any column renamed, removed, retyped or reordered; any table renamed or removed; partition or sort-order change; a change in NULL/empty-list convention.
- **Minor:** a column or table added; an enum gaining a value; a new manifest key.
- **Patch:** a bug fix that changes values without changing the schema (Part G.1 would be a patch on a published schema), compression or file-size changes, documentation.
- The UniProt data release (`2026_03`) and the schema version are independent; a data release normally ships with an unchanged schema.

`check_schema_evolution` enforces it: comparing against the committed baseline, a major-class change with only a minor or patch bump is an **error**; the baseline is updated in the same commit as the bump. This is the Census schema-spec model from the survey and the rule the drift check has lacked.

## H.5 Completion marker

**Today.** A release directory is a set of files copied by `publishDir`; nothing tells a mirror, an rsync cron job or a downloader that the copy is finished and validated.

**Change.** The publish step (`AUDIT.md` A5; until it exists, the `PROVENANCE` process in `upjson2lake.nf`, which already runs only after `VALIDATE` passes) writes `RELEASE_COMPLETE` as its **last** action, after `provenance.json`, `SHA256SUMS.txt`, `RELEASE.metalink` and `releases.json` are in place. Content: the release name, `schema_version`, the SHA-256 of `SHA256SUMS.txt`, and a UTC timestamp, one key per line. Mirrors and scripts test for the file before reading anything else; the README "Download" section (F.2.4) says so in one sentence. The A5 atomic swap flips the `latest` alias only for a directory that has the marker. Same convention as `_SUCCESS` in the Hadoop/Spark world and the per-folder `RELEASE.metalink` on the UniProt FTP, which is also written last.

- **Test:** `tests/test_validate.py` (or a small Nextflow test) asserts the marker is absent when validation fails and present, with the right hash, when it passes.

## H.6 `LICENSE` and `CITATION.cff`

**Today.** The repository `LICENSE` is MIT for the code; `datapackage.json` records MIT and CC-BY-4.0. The lake directory itself carries no licence file and nothing says how to cite the data.

**Change.**

- Write `lake/LICENSE` (the CC-BY-4.0 text plus a two-line header naming UniProt as the source and pointing at the UniProt licence page) from `main()` alongside the manifest. Add it to `SHA256SUMS.txt` and `RELEASE.metalink`. Record `"license": "CC-BY-4.0"` in the Parquet footer key-value metadata (A7) so a single file separated from the release still carries its terms.
- Add `CITATION.cff` at the repository root: the UniProt Consortium's current NAR paper as `preferred-citation`, the pipeline as the software entry, `license: MIT` for the code. `croissant.json` `citeAs` and `datapackage.json` `sources` are generated from it rather than typed twice. GitHub renders the file; Zenodo and Hugging Face read it.
- README: a "Citing" line under `### Metadata`.

---

# Release bundling and consolidated checklist

**What blocks the first public release, and what does not.** The "free before launch" argument applies only to changes that would break a reader after launch: column names, types, order and NULL convention, partition layout, file names, sort order and the file-size target. Everything additive (a new manifest key, a new sidecar file, footer metadata, a new view) is a minor bump under H.4 after launch at no cost to anyone, so it must not delay the first release. The split:

**The launch set (decided 2026-09-16).** Everything below ships in the first public release:

- Part A: B2 (`accession_map`); B1 on `entries` if Phase 0.1 passes.
- Part B (four columns), Part C (order), Part D (partitioning; D.5's `division` column and per-file taxid ranges).
- Part G (G.1 bug, G.2 typed fallbacks, G.3 naming).
- `DEDUPLICATION_PROPOSAL.md` Phases 1 and 2 (`AUDIT.md` A11, A13), per F.3.
- F.1 (measurement); F.2.1 sizes, `SHA256SUMS.txt` and `RELEASE.metalink` (the UniProt FTP convention; one pass over `file_details`); F.2.2 partial-lake client; `validation_report.json` (a few lines, and what lets a mirror assert the release passed); F.4 file-size target and page index.
- H.1 zstd level; H.2 field metadata (a few lines, and it makes every file self-describing); H.4 policy and `schema_version`; H.5 completion marker (written by `PROVENANCE` until A5 exists; mirrors and rsync jobs need it from day one); H.6 `lake/LICENSE` and `CITATION.cff`.

**Deferred to a minor schema bump after launch**, with the format decided in this plan so the bump is mechanical: `croissant.json` (needs `mlcroissant` and the A6 config), F.2.5 `releases.json` (needs the publish step and is only meaningful with a second release), F.2.6 Hugging Face, H.3 enums (a `SCHEMA.md` nicety), B1 on child tables (Phase 3 decides), Phase 4 macros. F.5 and F.6 are decisions for the FTP team and block nothing in code.

The launch set ships as one release so that the Phase 3 benchmark measures the combined effect and the first thing anyone downloads is the intended layout. Suggested implementation order, each step green on the default and `--stress` suites before the next:

1. Part A Phase 0 spikes (0.1 decides whether B1 ships at all, see 4.3; record the result before anything else is designed), then Part G.1 (`text_value` fix, a few lines), then the Part F.1 slice measurement. G.1 precedes F.1 because F.1's `comments` bytes and the B.5 gate both read `text_value`, which is empty today. Part B's size gates use the same slice.
2. Part G.2 (typed fallbacks, full-schema type check) and G.3 (the `reviewed` rename, `evidence_type`) with Part B (new columns), Part C (reorder) and H.2 (field metadata) — one PR; they all touch the SELECT lists and the column dictionaries. H.4 (semver) and H.6 (licence, citation) can go in the same PR.
3. `DEDUPLICATION_PROPOSAL.md` Phase 1 (A11: `g()` reconstruction and the round-trip gate), then Phase 2 (A13: trim the nested columns of `entries`, `features` and `publications` to residuals). Own PRs. Lands here so that Part D and F.2 hash the final column shapes and the F.4/H.1 measurements run on the trimmed tables.
4. Part A Phases 1–2 (bloom filters if 0.1 passed, `accession_map`).
5. Part D (Hive partitioning) — writer plus every path consumer (manifest, validator, views, tests, demo); can go in the same PR as Part A Phase 1 since both touch `stream_to_parquet`.
6. Part F.2 (per-file sizes/hashes, `SHA256SUMS.txt`, `RELEASE.metalink`, `validation_report.json`, manifest-driven partial-lake client, README "Download"; `croissant.json` and `releases.json` are deferred) — after Part D so the hashed file names are final. Part F.4's footer and page-index measurement and the H.1 compression sweep run on this build and fix the `entries` file-size target and the zstd level before the release build. H.5 (completion marker) is written by the `PROVENANCE` process until A5 exists. Parts F.5 and F.6 are decisions to raise with the FTP team in parallel; they block nothing in code.
7. Part A Phase 3 benchmark on the combined build; record results in this file.
8. Part A Phase 4 (optional macros, deferred unless trivial), README pass, `AUDIT.md` updates (S1 shipped, S5 shipped, S11 shipped, A12 done with numbers in §6.1, A5's hash list produced, O4 closed by D.5, R4 deferred with triggers, A11/A13 shipped, F.3 rule recorded).

Checklist additions to §8:

- [ ] Part B: `go_terms`, `pubmed_ids`, `proteome_ids`, `gene_name` present, described, categorised, round-trip-checked; size gates recorded; view files no longer synthesise `gene_name`.
- [ ] Part C: nine hot columns contiguous (verified from `parquet_metadata`); HTTP request count recorded before/after.
- [ ] Part D: every table has exactly `review_status=swissprot` and `review_status=trembl` partitions; `reviewed` stored in every file of every table; validator `check_partitions` passes; manifest carries the `partitioning` block and per-partition counts; README recommends `<table>/**/*.parquet` for raw DuckDB and the view files use the D.3 form; D.1 reader table executed in tests for DuckDB, Polars and PyArrow; D.5: `division` column on `entries` matches the FTP per-division counts, per-file `taxid_min`/`taxid_max` in the manifest with a `files_for_taxid` helper, `accession_map` marked as excluded from any future level, human-tier byte ratio recorded.
- [ ] Part E: nothing changed in `xrefs`; skew query and source-order query results recorded when available.
- [ ] Part F.1: slice measurement table filled (bytes per table and side, extrapolation, `entries` share, footer bytes); `AUDIT.md` §6.1 updated.
- [ ] Part F.2: `size_bytes` and `file_details` (SHA-256) per table in `manifest.json`; `SHA256SUMS.txt` and `RELEASE.metalink` at lake root and verified by the validator; `validation_report.json` emitted (`croissant.json` and `releases.json` deferred to a minor bump; their formats stay in F.2.1 and F.2.5); `connect()` works on an `entries`-only copy and fails with an actionable message on missing tables; README "Download" section leads with the `entries` tier and quotes measured sizes.
- [ ] Part F.3: A11 (`g()` round-trip gate) and A13 (residual trim) shipped; dedup-before-child-bytes rule recorded in `AUDIT.md` next to A13; F.1 says whether `xrefs`/`comments` need S2 or an xrefs byte audit; post-trim bytes recorded in the F.1 "after v2" column.
- [ ] Part G: `comments.text_value` populated and round-trip-checked; every `NULL` fallback typed and `check_schema_types` covers every column; demo manifest regenerated with no `int32` artefacts; naming decisions (G.3) recorded in `SCHEMA.md`.
- [ ] Part H: zstd level chosen from the sweep and recorded in manifest and footer; every field carries `description` (and `source_path` where known) metadata; H.3 enums deferred to a minor bump; `schema_version` semver in all five places with the policy in `SCHEMA.md` and enforced by `check_schema_evolution`; `RELEASE_COMPLETE` written last and gated on validation; `lake/LICENSE` and `CITATION.cff` present and hashed.
- [ ] Part F.4: footer-vs-data bytes for the organism query recorded at 256 MB, 512 MB and 1 GB; `entries` target chosen with the reason; page index written for every table and verified from `parquet_metadata()`.
- [ ] Part F.5: retention policy decided with the FTP team and written in the README `### Versioning` section; `lts` list in `releases.json`.
- [ ] Part F.6: cloud channel proposal put to the FTP team and SIB; manifest and index files use relative paths only.
- [ ] Manifest format `version` 2 and `schema_version` `1.0.0`; the first public release notes describe the layout as it is (no "breaking change" list, since there is no prior public release). Items in the "additive" column of the release-bundling table that did not make the release are listed in the notes as planned minor bumps.

## Decisions reopened by pre-public status

These were settled elsewhere on the grounds that a change would break existing users. There are no existing users, so each is a free choice to make before the first public release and a costly one after it. None is decided here.

| Where | Settled as | Reason given | Now |
| --- | --- | --- | --- |
| `AUDIT.md` O3 / S10 | Freeze `reviewed`, `acc`, `taxid`; no `is_*` booleans | "rename breaks every user" | Decided 2026-09-16: `reviewed` in all six tables, no `is_*` prefix (G.3). |
| `DEDUPLICATION_PROPOSAL.md` | Phase 2 as a single-release flip after a v1 has shipped, JSONL as the fallback for legacy consumers | migration | Decided 2026-09-16: both ship before launch, step 3 of the implementation order (F.3). |
| `AUDIT.md` D1 / O1 | Latest-only with one-release overlap | storage; and there was an implicit v1 to overlap with | Write the policy for launch (F.5); nothing to overlap yet. |
| `AUDIT.md` O5 | Keep `comments.comment` as VARCHAR JSON now; typed later after a user survey | avoid two changes for users | If F.1 shows `comments` is a large share of bytes, typing it (or at least tagging it JSON) before launch avoids ever publishing the VARCHAR form. |
| This plan, Part C | Ship the reorder only bundled with other changes | positional readers | Moot; bundle for benchmark reasons only. |
| This plan, F.2.1 | Keep `manifest.json` `files` as a name list | in-repo readers | Free to restructure; kept for simplicity, not compatibility. |
| This plan, G.3 | `from_reviewed` on child tables, `reviewed` on `entries` | inherited from the first draft | Decided 2026-09-16: `reviewed` everywhere (G.3). |

Part D was reopened on its merits, not on compatibility, and now adopts Hive partitioning with a separately named string key; see its "History of this decision".
