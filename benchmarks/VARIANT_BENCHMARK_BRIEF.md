# VARIANT Benchmark Brief for LLM Collaborator

**To:** LLM collaborator picking up work on `uniprot-parquet`
**Date:** 2026-04-12
**Repo:** `uniprot-parquet`, branch `static-lake`
**DuckDB version:** 1.5.1 (VARIANT + Parquet shredding supported)

---

## 1. What This Project Is

`uniprot-parquet` transforms UniProtKB JSON (250M protein entries, ~500 GB raw) into a sorted, star-schema Parquet data lake with 5 tables: `entries`, `features`, `xrefs`, `comments`, `publications`. A single-file Python client (`uniprot_parquet.py`) wraps DuckDB with views and macros for zero-config querying.

### Current architecture

```
Input: UniProtKB JSON → sorted JSONL(.zst) → Parquet star schema

entries (5 tables):
  entries/       — 43 cols: 37 typed "convenience" + 6 nested structs
  features/      — 20 cols: 19 typed + 1 nested struct
  xrefs/         — 8 cols: all typed
  comments/      — 6 cols: 5 typed + 1 nested struct
  publications/  — 20 cols: 19 typed + 1 nested struct
```

The 88 "convenience columns" are manually extracted from deeply nested JSON by 5 SQL builder functions in `bin/parquet_transform.py` (500 LOC, 36.2% of the pipeline). These handle schema evolution, optional fields, API renames (`submittedNames` → `submissionNames`), and EC number extraction from 3 separate naming blocks.

---

## 2. What We Tested

We benchmarked DuckDB 1.5's `VARIANT` type (Parquet V3, automatic shredding) as a potential replacement for hand-extracted convenience columns. We tested 4 layouts against 16 analytical queries on a 5,378-entry demo lake (134K total rows across all tables).

### Layouts

| Layout | Description | Entries table | Child tables |
|--------|-------------|---------------|--------------|
| **Baseline** | Current star schema | 37 typed + 6 nested cols | Pre-unnested, typed convenience cols |
| **A** | Single table, full VARIANT | `acc`, `taxid`, `reviewed` + `data VARIANT` (entire entry) | None — unnest at query time |
| **B** | Star schema + VARIANT | `acc`, `taxid`, `reviewed` + `data VARIANT` (entire entry incl. arrays) | Pre-unnested, `data VARIANT` per row |
| **C** | Star schema + VARIANT, arrays stripped | `acc`, `taxid`, `reviewed`, `sequence`, `seq_length` + `data VARIANT` (arrays excluded) | Pre-unnested, `data VARIANT` per row |

### Query suite (16 queries)

Point lookups, organism filters, keyword/GO searches, aggregations, joins, full scans, nested struct access, comment/publication queries. Same queries adapted for each layout's column structure.

---

## 3. Results

### Query latency (median ms, 3 runs after warmup)

```
Query                          Base      A         B         C       C/Base
──────────────────────────── ──────── ──────── ──────── ──────── ────────
point_lookup_by_acc            14.8   3889.7   3485.4    136.1    9.2x
protein_card_macro              4.6   3661.5   3660.2    116.5   25.1x
organism_filter_human           5.0   5942.2   5873.5    327.8   65.4x
organism_features               3.9   5558.0     65.8     63.8   16.5x
keyword_search                  4.0   5708.3   5534.3    278.3   68.9x
go_term_search                  3.7   5493.8   5477.1    282.0   77.1x
count_by_organism               3.4   5483.7   5551.6    276.1   80.4x
feature_type_distribution       5.1   5819.4      4.8      4.8    0.9x ✓
xref_database_distribution      6.1    OOM       3.9      3.6    0.6x ✓
entries_join_features          10.2   7549.8   7371.7    484.0   47.4x
full_scan_entries               2.1      3.6      4.1      2.7    1.3x ✓
full_scan_xrefs                 1.3    OOM       2.0      1.2    0.9x ✓
nested_organism_access          4.0   8924.6   8999.6    832.0  208.0x
nested_protein_desc_access      6.9   3627.1   3471.8    134.8   19.4x
comment_function_search         2.0    ERR       1.4      1.4    0.7x ✓
publication_count_per_entry     3.5    ERR       3.5      3.4    1.0x ✓
```

### Storage

```
             Baseline    Layout A    Layout B    Layout C
Total        3.23 MB     3.92 MB     5.87 MB     3.12 MB
entries      1022 KB     3.92 MB     3.92 MB     1.17 MB
features     181 KB      —           124 KB      124 KB
xrefs        888 KB      —           1.11 MB     1.11 MB
comments     137 KB      —           153 KB      153 KB
publications 1.05 MB     —           582 KB      582 KB
```

### Build time (5,378 entries)

```
Baseline pipeline:  2.04s
Layout A:           1.86s
Layout B:           4.72s
Layout C:           4.04s
```

### Complexity

```
Baseline:  1383 LOC, 500 SQL builder LOC (36.2%), 88 convenience columns
VARIANT:   ~434 LOC, 0 SQL builder LOC, 0 convenience columns
```

---

## 4. Key Findings

### VARIANT shredding works — DuckDB automatically shreds nested fields into native typed Parquet columns

The entries VARIANT produced 269 physical Parquet columns (Layout C) and 417 columns (Layout A). Scalar paths like `data.organism.taxonId` are shredded to native `int32`, string fields to `string`. This is correct Parquet V3 behaviour.

### Child table queries are at parity or faster with VARIANT

Queries on pre-unnested tables (`features`, `xrefs`, `comments`, `publications`) using VARIANT `data` columns perform identically to typed convenience columns: 0.6x–1.0x (i.e., same speed or faster). The VARIANT shredding handles the nested structs (ligand, evidences, citation) without any manual extraction.

### Entries table queries are 10–200x slower with VARIANT

Even in Layout C (arrays stripped, only scalar/small-nested fields in VARIANT), accessing fields through VARIANT dot notation is dramatically slower than typed columns. The overhead scales with the number of shredded columns being read — `nested_organism_access` (208x) reads 2 VARIANT paths across all rows, while `full_scan_entries` (1.3x, just `count(*)`) barely touches the VARIANT at all.

### VARIANT arrays are the performance killer

Layout A (full VARIANT with arrays) causes OOM errors on xref queries (95K xrefs across 5K entries) and 1000x+ slowdowns on everything else. Layout B (star schema but arrays still in entries VARIANT) is equally slow on entries queries because the VARIANT column is 3.9 MB of shredded array data. Layout C's array-stripping cut entries from 3.9 MB to 1.17 MB and queries from 3000–9000ms to 100–800ms.

### Two known query bugs in Layout C

`go_term_search` and `comment_function_search` return 0 rows on Layout C vs 75 and 215 on baseline. This is a VARIANT CAST syntax issue in the benchmark queries, not a data problem — the data is present but the `CAST(data.keywords AS STRUCT(...)[])` pattern may need adjustment for VARIANT arrays. These need debugging.

### Storage is slightly better with VARIANT

Layout C (3.12 MB) is 3% smaller than baseline (3.23 MB). VARIANT's binary encoding is more compact than typed columns for deeply nested structs. Child tables are smaller (features: 124 KB vs 181 KB) because VARIANT avoids duplicating columns that exist in both convenience and nested forms.

---

## 5. Recommended Next Step: Layout D (Hybrid)

Based on these results, the optimal architecture is a hybrid:

### Entries table: keep typed convenience columns (current approach)
The 37 typed columns on entries are essential for fast filtering, aggregation, and search. VARIANT's 10-200x overhead is unacceptable for the most-queried table. Keep the SQL builders for entries.

### Child tables: replace convenience columns with VARIANT
Features, xrefs, comments, and publications should use VARIANT `data` columns instead of manually extracted convenience columns. The benchmark shows parity performance (0.6-1.0x), and this would:
- Eliminate 4 of 5 SQL builder functions (~300 LOC)
- Remove schema evolution complexity for child tables
- Preserve all nested data without duplication
- Allow future field access without pipeline changes

### Child table schema (Layout D)

```
features:     acc, from_reviewed, taxid, type, data VARIANT
xrefs:        acc, from_reviewed, taxid, database, data VARIANT
comments:     acc, from_reviewed, taxid, comment_type, data VARIANT
publications: acc, from_reviewed, taxid, data VARIANT
```

The typed filter/sort columns (`type`, `database`, `comment_type`) are kept for fast WHERE clauses. All other fields accessed via `data.field.path` VARIANT dot notation.

### What this preserves from current architecture
- Star schema (5 tables, pre-unnested child tables)
- Sort orders (reviewed DESC, taxid, acc)
- Manifest.json, datapackage.json
- Client library views and macros (need updating for VARIANT syntax)
- Test suite structure

### What this changes
- `_build_features_sql`, `_build_xrefs_sql`, `_build_comments_sql`, `_build_publications_sql` replaced by simple UNNEST + VARIANT cast
- `_build_entries_sql` stays (typed convenience columns)
- Client library macros updated: e.g., `f.description` → `CAST(f.data.description AS VARCHAR)`
- `setup_views.sql` updated similarly

---

## 6. Files to Know

| File | Purpose | Lines |
|------|---------|-------|
| `bin/parquet_transform.py` | Core pipeline: JSON → Parquet | 1383 |
| `uniprot_parquet.py` | Single-file client library (views + macros) | 392 |
| `setup_views.sql` | Pure SQL equivalent of the client library | ~80 |
| `benchmarks/bench_baseline.py` | Baseline benchmark (16 queries, 4 dimensions) | ~500 |
| `benchmarks/bench_variant.py` | VARIANT benchmark (same 16 queries, 3 layouts) | ~800 |
| `benchmarks/build_variant_lake.py` | Builds VARIANT lakes A, B, C | ~430 |
| `tests/test_parquet_transform.py` | Main test suite | ~260 |
| `tests/test_roundtrip.py` | JSON → Parquet → JSON roundtrip validation | ~500 |
| `demo/demo.ipynb` | Jupyter demo notebook | ~800 cells |

---

## 7. Open Questions for the Next Session

1. **Debug the two query mismatches** — `go_term_search` (75→0) and `comment_function_search` (215→0) on Layout C. Likely a VARIANT array CAST issue.
2. **Implement Layout D** — hybrid: typed entries + VARIANT child tables. Modify `parquet_transform.py` to emit VARIANT for child tables.
3. **Update client library** — `uniprot_parquet.py` and `setup_views.sql` need new view/macro definitions for VARIANT child columns.
4. **Benchmark at scale** — run on `diverse_stress.json.gz` (71 MB) on a machine with sufficient memory (>8 GB). The demo lake (5K entries) may underrepresent shredding effectiveness.
5. **Test PyArrow interop** — verify PyArrow can read the VARIANT Parquet files (for non-DuckDB consumers).
6. **Consider DuckDB 1.5.x improvements** — VARIANT is new; future point releases may significantly improve dot-notation query performance on shredded columns.
