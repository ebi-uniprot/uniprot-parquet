# Plan: schema version 2 — accession lookup, `entries` columns, column order, side-named `entries` files

**Status:** proposed, 2026-09-11. Not started.
**Motivation:** `DEMAND_REVIEW.md` (R1, R2, R5–R9, S1/S5/S11 from `AUDIT.md`).
**Audience:** an implementer who has not read the rest of the repo. Every step names the file and function it touches, what "done" means, and how it is verified.

| Part | Change | Review ref |
| --- | --- | --- |
| **A** | Bloom filters on `acc` in every table; new `accession_map` table | R1, S5 |
| **B** | Four new `entries` columns: `go_terms`, `pubmed_ids`, `proteome_ids`, `gene_name` | R5, R6, R7, S1 |
| **C** | Reorder `entries` so the nine hot columns are first and contiguous | R9 |
| **D** | `entries` files never straddle the Swiss-Prot/TrEMBL boundary and are named `entries_sp_*` / `entries_tr_*`; directory stays flat | R2 |
| **E** | `xrefs` deliberately unchanged, with the conditions for revisiting | R4 |

All of A–D ship together as manifest **version 2**; see "Release bundling" at the end.

---
---

# Part A — accession point-lookup
## 1. Problem statement

All five tables are sorted `(reviewed DESC, taxid ASC, acc ASC)` (`bin/parquet_transform.py`, `TABLE_DEFS`). Row-group min/max statistics prune well on `reviewed` and `taxid`. They do not prune on `acc`: a 100,000-row TrEMBL row group spans many organisms, so its `acc` range is effectively `[A0A…, Z…]` and an equality predicate on `acc` cannot exclude any row group. A point lookup therefore scans the entire `acc` column of the table (~250M values in `entries`, ~5B in `xrefs`).

The demand evidence (`handoff-8x1w-v2/`) shows accession is the single broadest predicate: 10.4M distinct queries from 16,374 clients on the search endpoint, plus 64.6M entries fetched by accession list on the accessions endpoint (36.8 per request programmatically), plus an unmeasured population of single-entry GETs.

## 2. Decision

Two additions, no changes to existing sort orders, file layout, or column sets:

| ID | Change | Serves |
| --- | --- | --- |
| **B1** | Parquet **bloom filters on `acc`** in all five tables (`entries`, `features`, `xrefs`, `comments`, `publications`), written into the files | Any engine that reads Parquet bloom filters (DuckDB ≥ 1.2, Spark/parquet-mr ≥ 1.12). Zero user friction: same files, same queries. |
| **B2** | A sixth table **`accession_map`** (one row per primary *and* secondary accession → primary accession, `reviewed`, `taxid`), sorted by `acc` | Legacy-accession resolution (a bloom filter cannot serve `list_contains(secondary_accs, …)`), and an engine-agnostic two-step lookup for readers that do not use bloom filters. |

### Explicit non-goals

- No change to `(reviewed DESC, taxid ASC, acc ASC)` on any table.
- No Hive partitioning.
- No new client macros are *required*. Existing macros may be updated to use `accession_map` (Phase 4, optional).
- No bloom filters on columns other than `acc` in this plan. (`xrefs.id` is a candidate for a later plan; see `DEMAND_REVIEW.md` §2.3.)

### Known limitation to document, not fix

Because the sort cannot exclude any row group before its filter is tested, a single remote (httpfs) lookup reads **every** row group's bloom filter. Filter bytes scale with total distinct values, not with row-group size: at 5% false-positive rate `entries` carries roughly 150 MB of filters in total, and each child table a similar order of magnitude. Locally this is negligible. Over HTTP it is far cheaper than a column scan but is not a true point lookup. `accession_map` (B2) gives remote users a two-step path that reads one row group; document both in the README.

---

## 3. Phase 0 — feasibility spikes (must pass before Phase 1)

Run all three in the project environment (`environment.yml`: `pyarrow>=23.0,<24`, `duckdb>=1.5,<2`). Record results at the top of this file under a "Phase 0 results" heading.

### 0.1 Can the pinned PyArrow write bloom filters?

```python
import inspect, pyarrow.parquet as pq
print(pq.__version__ if hasattr(pq, "__version__") else "")
sig = inspect.signature(pq.ParquetWriter.__init__)
print([p for p in sig.parameters if "bloom" in p.lower()])
```

- **Pass:** a bloom-related parameter exists (e.g. something like `write_bloom_filter` / `bloom_filter_columns` / `bloom_filter_fpp`; the exact spelling is version-dependent, read the docstring). Use **Path A** in Phase 1.
- **Fail:** no such parameter. Use **Path B** (DuckDB writer) in Phase 1.

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
   BLOOM_FILTER_FALSE_POSITIVE_RATIO 0.05)
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
FROM parquet_metadata('<lake>/entries/*.parquet') WHERE path_in_schema = 'acc';
```

Record `candidate_groups / row_groups`. On production TrEMBL files this is expected to be ~1.0; that is the number Phase 1 has to move.

---

## 4. Phase 1 — B1: bloom filters on `acc`

### 4.1 Where the writer lives

`bin/parquet_transform.py`, `stream_to_parquet(con, sql, table_dir, batch_size, label, sort_order)` (around line 837). It:

1. runs the table SQL via `con.sql(sql).to_arrow_reader(batch_size)`,
2. opens a `pq.ParquetWriter(path, schema, compression="zstd", sorting_columns=...)` per output file,
3. calls `writer.write_table(tbl, row_group_size=100_000)`,
4. rolls to a new file at `TARGET_FILE_BYTES = 256 MiB`,
5. writes into `<table_dir>/.tmp/` and renames on success.

Keep 1, 4 and 5 exactly as they are. Only 2 and 3 change.

### 4.2 Path A — PyArrow writer supports bloom filters (preferred; minimal diff)

- Add a module constant:
  ```python
  BLOOM_FILTER_COLUMNS = {"entries": ["acc"], "features": ["acc"], "xrefs": ["acc"],
                          "comments": ["acc"], "publications": ["acc"]}
  BLOOM_FILTER_FPP = 0.05
  ```
- In `stream_to_parquet`, extend `writer_kwargs` with the bloom option(s) discovered in 0.1, keyed by `label`. Do not hard-code the parameter name from memory; take it from the 0.1 output.
- Add a CLI flag `--no-bloom-filters` (default: on) in `main()` next to the other `add_argument` calls, so a build can be reproduced without them if a reader regresses.

### 4.3 Path B — PyArrow cannot; use DuckDB's Parquet writer for the data path

Keep DuckDB as the writer for *all* tables (do not mix writers across tables; the manifest and validator assume one behaviour).

- Replace steps 2–3 with a DuckDB `COPY (<sql>) TO '<tmp_dir>' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000, FILE_SIZE_BYTES '256MB', DICTIONARY_SIZE_LIMIT 100000, BLOOM_FILTER_FALSE_POSITIVE_RATIO 0.05, FILENAME_PATTERN '<label>_{i}')`. Check the exact option names against the installed DuckDB; `FILE_SIZE_BYTES` and `FILENAME_PATTERN` exist in 1.x, and `PER_THREAD_OUTPUT false` keeps one sequence.
- **Things Path B must preserve, each with a test:**
  - `sorting_columns` footer metadata (`build_sorting_columns` / `pq.SortingColumn`). DuckDB's writer may not emit it. If it does not, either accept the loss and remove `sort_order`-in-footer claims from `README.md`/`datapackage.json`, or post-process is not possible without rewriting — so decide explicitly and record the decision here.
  - Row count and Arrow schema returned by `stream_to_parquet` (read them back from the written files with `pq.read_metadata` / `pq.read_schema`, as the `skip_set` branch in `main()` already does).
  - Five-digit zero-padded file names `<label>_00001.parquet` (the manifest, `check_manifest`, and `TestManifest.test_manifest_files_match_disk` depend on the listing matching).
  - The `.tmp/` → final rename.
  - `DICTIONARY_SIZE_LIMIT` must be ≥ `ROW_GROUP_SIZE` or DuckDB will silently write no filter for `acc`. Assert this in code.
- Memory: DuckDB `COPY` is streaming; confirm on the stress fixture that peak RSS is not worse than the PyArrow path.

### 4.4 Manifest and data package

- `manifest.json` (`main()`, the `manifest_tables[name] = {...}` dicts, two places): add `"bloom_filter_columns": [...]` and `"bloom_filter_fpp": 0.05` per table (empty list / `null` when `--no-bloom-filters`).
- `_build_datapackage()` : surface the same two keys on each resource (alongside `sortOrder`).
- Bump `manifest["version"]` from 1 to 2. Add a line to the README "Metadata" paragraph.

### 4.5 Validator

`bin/validate_lake.py`: add `check_bloom_filters(report, lake_dir)` after `check_parquet_integrity` (line ~596) and wire it into `main()`.

- Using DuckDB (the validator already uses it for joins), for each table in the manifest with a non-empty `bloom_filter_columns`, query `parquet_metadata('<table>/*.parquet')` and assert every row group for every listed column has `bloom_filter_offset IS NOT NULL AND bloom_filter_length > 0`.
- Functional check on `entries` only: pick 20 accessions from the round-trip sample, run `SELECT count(*) FROM read_parquet(...) WHERE acc = ?`, assert count = 1. This does not prove skipping; presence + the Phase 0.2 spike does. Do **not** parse `EXPLAIN ANALYZE` text in the validator; it is not a stable interface.
- Report it as check 15 in `validation_report.txt` and in the README validation list.

### 4.6 Tests

`tests/test_parquet_transform.py`:

- `TestBloomFilters.test_every_row_group_has_acc_bloom` for all five tables (same DuckDB `parquet_metadata` query as the validator).
- `TestBloomFilters.test_point_lookup_returns_one_row` on `entries` for three accessions from the fixture.
- `TestBloomFilters.test_no_bloom_flag` builds with `--no-bloom-filters` and asserts offsets are null (mark `slow` if it doubles suite time).
- If Path B: extend `TestSortOrder` to assert the footer `sorting_columns` still exist, **or** remove that assertion together with the README claim (see 4.3).

`tests/test_validate.py`: `test_validate_passes_on_good_lake` already runs the full validator; add a negative test that strips bloom filters (rewrite one file with `pq.write_table` without the option) and asserts the new check fails.

---

## 5. Phase 2 — B2: `accession_map` table

### 5.1 Schema

```
accession_map/accession_map_00001.parquet …   sorted by acc ASC, primary_acc ASC
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
  -- step 1: one row group of accession_map
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
ORDER BY acc, primary_acc
```

Guard `secondaryAccessions` with `has("secondaryAccessions")` like the other optional paths (`organismHosts`, `geneLocations`); if absent, the second branch is dropped.

The `reviewed` CASE expression is duplicated from the child-table builders; factor it into a small helper string constant used by all of them rather than pasting a fourth copy.

### 5.3 Registration

- `TABLE_DEFS`: append `("accession_map", None, ["acc ASC", "primary_acc ASC"])`. The `_SQL_BUILDERS` dict in `main()` gets the new builder. `--skip-existing` then works unchanged.
- `TABLE_META["accession_map"]`: description, `primary_key: ["acc", "primary_acc"]`, `foreign_keys: {"primary_acc": "entries.acc"}`, `columns.convenience` = all five, `nested` = `[]`.
- `COLUMN_DESCRIPTIONS`: five entries.
- `BLOOM_FILTER_COLUMNS["accession_map"] = ["acc"]` (cheap; the sort already prunes, the filter helps the rare multi-primary case and keeps behaviour uniform).
- Sort spill: this table is ~250M + secondaries rows of five narrow columns. The `ORDER BY acc` is a real sort (input is not in acc order). Expect low tens of GB of spill, not the 1.2 TB the child tables would need; note it in `upjson2lake.nf` `PARQUET_TRANSFORM` comments.

### 5.4 Views and docs

- `setup_views.sql` and `uniprot_parquet.py` `_SETUP_SQL`: add `CREATE OR REPLACE VIEW accession_map AS SELECT * FROM read_parquet('${BASE}/accession_map/*.parquet');`. (These two files are meant to be identical modulo placeholder syntax; `AUDIT.md` A23 wants one generated from the other. Do not fix A23 here, just keep them in sync.)
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
5. Sort order `acc ASC, primary_acc ASC` — extend `check_sort_order` to read the order from the manifest instead of assuming the three-key order, so the new table is covered without a special case.
6. No duplicate `(acc, primary_acc)` pairs.

### 5.6 Tests

- `tests/test_parquet_transform.py`: `EXPECTED_TABLES` gains `"accession_map"`; `TestRowCounts` gets the two count identities; `TestSortOrder` covers the new order; `TestDataPackage` asserts the FK.
- `tests/test_roundtrip.py`: for every fixture entry, assert each of its secondary accessions resolves to it via `accession_map`.
- Idempotency (`tests/test_idempotency.py`): `--skip-existing` skips the new table like the others; no special handling needed, but the test's table list must include it.

---

## 6. Phase 3 — benchmark (evidence that it worked)

Add `benchmarks/bench_point_lookup.py` (mirror the structure of `benchmarks/bench_baseline.py`):

- Workloads: 1 accession, 37 accessions (programmatic batch mean), 1,000 accessions; each against `entries` (7 default columns), `features`, `xrefs`; local path and, when a URL is given, httpfs.
- Report per workload: wall time, row groups scanned vs total (from `EXPLAIN ANALYZE`, parsed only in the benchmark, never in the validator), bytes read (httpfs).
- Run once on a lake built with `--no-bloom-filters` and once with them; write both to `benchmarks/results/point_lookup_<label>.json` and a short table into this file under "Phase 3 results".
- Pass criterion (local, subset or full build): row groups scanned for a single-accession `entries` lookup ≤ 2, and the 37-accession batch at least 10× faster than baseline. If either fails, stop and reassess before Phase 4.

---

## 7. Phase 4 — optional client conveniences

Only after Phases 1–3 pass:

- `protein_card(target_acc)` and `unnest_isoforms(target_acc)` in `setup_views.sql` / `uniprot_parquet.py`: resolve `target_acc` through `accession_map` first so legacy accessions work and remote reads take the two-step path. Keep the raw-Parquet form as the documented default; the macros are a convenience, not a requirement.
- Consider a `lookup(acc)` macro returning the primary row plus counts. Do not add more than that.

---

## 8. Acceptance checklist (Part A; the combined list is at the end of the file)

- [ ] Phase 0 results recorded in this file (0.1 outcome, 0.2 row-group skip evidence, 0.3 baseline ratio).
- [ ] Every row group of every table (six tables) has a bloom filter on `acc`; `--no-bloom-filters` disables it.
- [ ] `manifest.json` version 2 records `bloom_filter_columns` and `bloom_filter_fpp`; `datapackage.json` mirrors them.
- [ ] `accession_map` builds, validates (six checks), round-trips secondaries, and survives `--skip-existing`.
- [ ] `sorting_columns` footer metadata decision recorded (kept, or claim removed) if Path B was taken.
- [ ] Validator has two new checks; README validation list updated; `validation_report.txt` shows them passing on the stress fixture.
- [ ] Benchmark results recorded; single-accession `entries` lookup scans ≤ 2 row groups locally.
- [ ] README documents both lookup forms and the httpfs cost caveat.
- [ ] Full test suite green on default and `--stress` fixtures.

## 9. Out of scope, recorded so nobody re-derives them

- Bloom filters on `xrefs.id`, `features.feature_id`, `publications.citation_id`: plausible later, not here.
- Reordering `entries` columns (R9) and new `entries` columns (R5–R7): Parts C and B of this file. Organism partitioning (O4): still open, needs taxid skew and organism values; not in this plan.
- Row-group size changes: keep 100,000 until the benchmark says otherwise.

---
---

# Part B — four new `entries` columns

**Motivation:** `DEMAND_REVIEW.md` §3.1 (R5, R6, R7, S1). All four are derivable from the source JSON already in the staged Parquet; no external data. Each is a list or scalar convenience column added to `_build_entries_sql` in `bin/parquet_transform.py`; none removes or changes an existing column.

## B.1 `go_terms :: list<struct{id: string, aspect: string, term: string, evidence: string}>`

**Source.** `uniProtKBCrossReferences[]` where `database = 'GO'`. Each has `id` (`GO:0005524`) and `properties` = list of `{key, value}` with keys `GoTerm` (value like `F:ATP binding`) and `GoEvidenceType` (value like `IEA:InterPro`). **Verify these two key strings against the staged Parquet on the fixture before writing SQL** (`SELECT DISTINCT p.key FROM … unnest(properties) p WHERE database='GO'`); do not trust this document for them.

**SQL sketch** (place directly after the existing `go_ids` expression, which stays):

```sql
[ struct_pack(
      id       := x.id,
      aspect   := left(gt.value, 1),          -- 'P' | 'F' | 'C'
      term     := substr(gt.value, 3),        -- strip the 'X:' prefix
      evidence := ge.value)
  FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
  IF x.database = 'GO' ]                       AS go_terms
```

DuckDB list comprehensions cannot bind `gt`/`ge` inline; implement the two property lookups as a small SQL macro or as `list_filter(x.properties, p -> p.key = 'GoTerm')[1].value` written out twice. Guard `properties` with `COALESCE(x.properties, [])` and with `has("uniProtKBCrossReferences.properties")` like the xrefs builder does. When `GoTerm` is absent for an entry, `aspect` and `term` are NULL, never the empty string (the validator's empty-string check must not be extended to these).

**Decisions recorded.** Keep `go_ids` for backward compatibility (it is the cheap membership-test column; `go_terms` is the display/aspect column). `aspect` is a one-character VARCHAR, not an enum. `evidence` is included because it is free and the GO evidence string is part of what the API's `go_p` column renders.

**Validator / tests.** In `check_round_trip` (or `tests/test_roundtrip.py`), for the sampled entries assert `len(go_terms) == len(go_ids)` and that every `go_terms[i].id` is in `go_ids`. Extend `TestSchema.test_entries_required_columns` with `go_terms`. Add `COLUMN_DESCRIPTIONS[("entries","go_terms")]` and the `TABLE_META["entries"]["columns"]["convenience"]` entry.

**Size gate.** Run on the A12 slice (or the stress fixture as a proxy, noting it is Swiss-Prot-heavy):

```sql
SELECT reviewed, avg(len(go_ids)) AS mean_go, avg(CASE WHEN len(go_ids)=0 THEN 1 ELSE 0 END) AS null_rate
FROM entries GROUP BY 1;
```

Record the numbers here. No threshold blocks this column; the check exists to size the column in the release notes.

## B.2 `pubmed_ids :: list<string>`

**Source.** `references[].citation.citationCrossReferences[]` where `database = 'PubMed'` → `id`. This is the same path the publications builder already guards with `has("references.citation.citationCrossReferences")` (line ~599); reuse the guard.

**SQL sketch:**

```sql
list_sort(list_distinct(flatten(list_transform(
    COALESCE(e."references", []),
    r -> [ c.id FOR c IN COALESCE(r.citation.citationCrossReferences, []) IF c.database = 'PubMed' ]
))))                                            AS pubmed_ids
```

**Decisions recorded.** Deduplicated and **sorted** (lexically) so the column is deterministic across runs — `list_distinct` alone has implementation-defined order and would break round-trip equality tests. Stored as strings to match the API and `publications.citation_xrefs`, not as integers. Empty list, not NULL, when there are no PubMed citations (consistent with `go_ids`, `keyword_ids`).

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
SELECT comment_type, from_reviewed, count(DISTINCT acc) AS entries, sum(strlen(text_value)) AS bytes
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
3. The remaining current convenience columns in their current relative order: `secondary_accs`, `organism_common`, `lineage`, `gene_synonyms`, `alt_protein_names`, `protein_flag`, `ec_numbers`, `protein_existence`, `annotation_score`, `seq_mass`, `seq_md5`, `seq_crc64`, `go_ids`, `go_terms` (B.1), `xref_dbs`, `proteome_ids` (B.3), `keyword_ids`, `keyword_names`, `first_public`, `last_modified`, `last_seq_modified`, `entry_version`, `seq_version`, `feature_count`, `xref_count`, `comment_count`, `reference_count`, `pubmed_ids` (B.2), `uniparc_id`, `entry_type`, `extra_attributes`.
4. Nested structs last, unchanged: `organism`, `protein_desc`, `genes`, `keywords`, `organism_hosts`, `gene_locations`.

`ORDER BY` is unchanged. `sorting_columns` in the footer are built by column *name* (`build_sorting_columns`), so they follow the reorder automatically.

## C.2 What this breaks, and the response

- **Positional readers** (`SELECT * … ` consumers comparing by index, `pd.read_parquet(...).iloc[:, 0:7]`): breaking. This is why Parts A–D ship together as **manifest `version: 2`** in one release, with a line in the release notes. Do not ship the reorder alone in a minor release.
- **Manifest comparisons across releases** (`AUDIT.md` A10 drift diff): a reorder shows as a diff. Acceptable; it is a real schema change.
- **Tests:** `TestSchema` uses sets of names, unaffected. `check_schema_types` in the validator matches by name, unaffected. `tests/test_roundtrip.py` compares by name, unaffected. Nothing in the repo reads `entries` positionally; grep for `.column(0)` / `[0]` on entries batches before claiming this.

## C.3 Verification (this is what the reorder exists for)

- From `parquet_metadata('lake/entries/*.parquet')`, for one row group, list `(path_in_schema, data_page_offset, total_compressed_size)` ordered by offset and confirm the nine hot columns are contiguous.
- In `benchmarks/bench_point_lookup.py` (Part A Phase 3), add a "default seven columns, one organism" workload and, when a URL is given, record the HTTP request count from DuckDB's `EXPLAIN ANALYZE` before and after the reorder. Record both numbers here. There is no pass threshold; the requirement is that the number does not go up.

---

# Part D — boundary-aligned, side-named `entries` files (flat directory)

**Motivation:** `DEMAND_REVIEW.md` §2.1 (R2). `reviewed` is the most-requested filter (44.2M requests, 10,357 clients, top facet). The sort already places every Swiss-Prot row in the first files. This part makes that visible **by file name** so a reader can opt in to file-level pruning with a glob, without changing how anyone reads all entries.

**Why not Hive `reviewed=true/` directories.** Considered and rejected: a flat glob (`entries/*.parquet`) would return nothing in DuckDB and Polars; the `reviewed` column would live only in the path, with engine-dependent type inference (`BOOLEAN` vs the string `'true'`); and a single downloaded file would no longer say which side it is. Those costs fall on every reader, including the majority who want all entries, to serve a narrow remote-read case. Recorded here so it is not re-proposed.

## D.1 Layout

`entries/` stays a flat directory. The `reviewed` column stays a stored BOOLEAN column in every file. Two rules are added:

1. **No file straddles the Swiss-Prot → TrEMBL boundary.** The last `reviewed = true` row closes its file; the first `reviewed = false` row starts a new one.
2. **File names carry the side:** `entries_sp_00001.parquet …` for `reviewed = true`, `entries_tr_00001.parquet …` for `reviewed = false`. Numbering restarts at 00001 on each side.

Consequences, all intended:

- `read_parquet('entries/*.parquet')`, `pl.scan_parquet('entries/*.parquet')`, `pd.read_parquet('entries/')`, `open_dataset('entries/')`, `spark.read.parquet('entries/')` are unchanged and return every row with a boolean `reviewed`.
- `read_parquet('entries/entries_sp_*.parquet')` reads Swiss-Prot only, opening no TrEMBL file and no TrEMBL footer. Same with `entries_tr_*` for TrEMBL.
- Every file is still self-describing.
- No engine prunes by name automatically; the convention must be documented (D.4). That is the accepted cost.

Child tables are unchanged: `from_reviewed` stays a stored column, file names stay `features_00001.parquet` etc. (They are sorted `from_reviewed DESC` too, so the same convention *could* be applied later; not in this plan.)

## D.2 Writer change

`stream_to_parquet` in `bin/parquet_transform.py` receives batches already sorted `reviewed DESC`, so all `true` rows precede all `false` rows.

- Add an optional argument, e.g. `side_column="reviewed"`, passed only for `entries`. Maintain the current side (`"sp"` / `"tr"`) and a per-side file counter.
- On each batch, if the batch is entirely on the current side, write it as today. If its first row is on the other side, close the current file first. If the flip is *inside* the batch, locate it (the boolean column is sorted, so a single scan for the first `false`, or `pc.index`, suffices), write the `true` slice to the current file, close it, open the first `tr` file and write the `false` slice. A batch can contain at most one flip.
- File naming: `f"{label}_{side}_{n:05d}.parquet"` when `side_column` is set, otherwise the current `f"{label}_{n:05d}.parquet"`. The `.tmp/` staging and rename are unchanged.
- The 256 MiB roll-over rule applies within a side as before; the boundary just forces one extra, possibly small, final `sp` file.
- Nothing about row groups, sort order, `sorting_columns` metadata, compression or bloom filters (Part A) changes.

## D.3 Everything that references `entries` file names

- `manifest.json`: `files` lists the new names; add `"file_sides": {"entries_sp_*": {"reviewed": true}, "entries_tr_*": {"reviewed": false}}` (or equivalent) so tools can discover the convention without parsing names. Also record, per file, `reviewed_min`/`reviewed_max` is unnecessary — the name is the contract.
- `bin/validate_lake.py`: `check_manifest` compares listings, unaffected by naming. Add to `check_sort_order` (or a new `check_file_sides`): for every `entries_sp_*` file `min(reviewed) = max(reviewed) = true`, for every `entries_tr_*` file both `= false`, via `parquet_metadata` statistics — one query, no data read. Also assert the two sides' row counts sum to the entries count.
- `bin/release_manifest.py`, `--skip-existing` in `main()`: both list `*.parquet`; unaffected.
- `tests/test_parquet_transform.py`: `TestManifest.test_manifest_files_match_disk` unaffected; add `TestFileSides` — every `entries` file name matches `^entries_(sp|tr)_\d{5}\.parquet$`, side statistics as above, and a test that reads `entries_sp_*.parquet` and gets exactly the fixture's Swiss-Prot count. The fixtures contain both sides (`tests/fetch_fixtures.py` pulls reviewed and unreviewed), so the boundary split is exercised; add one small synthetic case where the flip falls mid-batch (use a tiny `--batch-size` on `small.json.gz`).
- `README.md`: one sentence under "All tables are sorted Swiss-Prot first…": *"`entries` files are named `entries_sp_*` (Swiss-Prot) and `entries_tr_*` (TrEMBL) and never mix the two, so `entries/entries_sp_*.parquet` is a reviewed-only read with no footer access."* Add a Swiss-Prot-only example next to the existing "Direct access" examples. No existing example changes.
- `setup_views.sql`, `uniprot_parquet.py`: unchanged (`entries/*.parquet` still matches). Optionally add a `swissprot` view over `entries_sp_*.parquet`; not required.

## D.4 Verification

- `ls lake/entries/` shows only the two prefixes; counts per side match `SELECT reviewed, count(*) FROM entries GROUP BY 1`.
- `parquet_metadata` side check passes for every file (validator).
- DuckDB `EXPLAIN ANALYZE SELECT count(*) FROM 'lake/entries/entries_sp_*.parquet'` opens only `sp` files (the plan's benchmark records the number of files and footers touched for a reviewed-only count, flat glob vs side glob).
- Full test suite green on default and `--stress` fixtures.

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

# Release bundling and consolidated checklist

Parts A–D ship together as **schema version 2** (manifest `"version": 2`), in one release, so that the benchmark in Part A Phase 3 measures the combined effect and users see one breaking change rather than four. Suggested implementation order, each step green on the default and `--stress` suites before the next:

1. Part A Phase 0 spikes (read-only investigations; run them first, record results).
2. Part B (new columns) and Part C (reorder) — one PR; they touch the same SELECT list.
3. Part A Phases 1–2 (bloom filters, `accession_map`).
4. Part D (side-named `entries` files) — a writer-only change; can go in the same PR as Part A Phase 1 since both touch `stream_to_parquet`.
5. Part A Phase 3 benchmark on the combined build; record results in this file.
6. Part A Phase 4 (optional macros), README pass, `AUDIT.md` updates (S1 shipped, S5 shipped, S11 shipped, O4 still open, R4 deferred with triggers).

Checklist additions to §8:

- [ ] Part B: `go_terms`, `pubmed_ids`, `proteome_ids`, `gene_name` present, described, categorised, round-trip-checked; size gates recorded; view files no longer synthesise `gene_name`.
- [ ] Part C: nine hot columns contiguous (verified from `parquet_metadata`); HTTP request count recorded before/after.
- [ ] Part D: every `entries` file is single-sided and named by side; validator side check passes; README documents the `entries_sp_*` convention; existing globs unchanged.
- [ ] Part E: nothing changed in `xrefs`; skew query and source-order query results recorded when available.
- [ ] Manifest version 2; release notes list the breaking changes (column order, new columns, new table, `entries` file names).
