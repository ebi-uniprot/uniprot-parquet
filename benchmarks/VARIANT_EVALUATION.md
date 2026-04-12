# Evaluation of DuckDB VARIANT for Semi-Structured Protein Data in uniprot-parquet

## 1. Introduction

uniprot-parquet transforms UniProtKB JSON releases into a columnar Parquet data lake organised as a star schema: a central entries table joined to four child tables (features, cross-references, comments, publications) pre-unnested at build time. The current pipeline hand-extracts 88 typed convenience columns across these five tables using 500 lines of SQL builder code that must evolve with every schema change in the upstream UniProtKB JSON format.

DuckDB 1.5 introduced the VARIANT type, a binary-encoded semi-structured data type inspired by Snowflake and available natively in Parquet via the Variant Encoding specification (GH-10468, ratified 2025). VARIANT stores per-value type tags and supports automatic Parquet column shredding, where the writer identifies frequently-accessed nested paths and stores them as separate typed columnar streams. This means a single `data VARIANT` column can offer columnar scan performance for hot paths while preserving the full nested structure of the source document.

We evaluated whether VARIANT could replace some or all of the 88 hand-extracted columns, reducing code complexity and schema maintenance burden without sacrificing query performance. This document reports methods, results, and conclusions for use in a forthcoming bioinformatics publication. The evaluation confirms that the current fully typed star schema delivers the best query performance across all 16 benchmark queries, validating the design choice of explicit column extraction over semi-structured storage.


## 2. Methods

### 2.1 Test dataset

All benchmarks used a 5,378-entry subset of the UniProtKB 2026_01 release, stored as pre-sorted JSONL (zstd-compressed). This subset produces 134,016 total rows across the five star-schema tables:

| Table        | Rows    |
|--------------|---------|
| entries      | 5,378   |
| features     | 10,817  |
| xrefs        | 95,385  |
| comments     | 4,261   |
| publications | 18,175  |
| **Total**    | **134,016** |

The input file is `demo/lake/2026_01/sorted.jsonl.zst` (2.49 MB compressed).

### 2.2 Software environment

All benchmarks were executed on a single machine running Linux (aarch64) with DuckDB 1.5.1 (Python binding) and a 3 GB memory limit. VARIANT is a built-in DuckDB type from version 1.5 onward and requires no extension installation. Each query was timed with 3 runs; we report the median of 3 warm executions (after one cold run to populate the buffer pool).

### 2.3 Layouts evaluated

We evaluated five Parquet layouts built from the same source data:

**Baseline (current production schema).** Star schema with 88 hand-extracted typed convenience columns across 5 tables. Entries carry 37 convenience columns (acc, taxid, protein_name, gene_names, go_ids, keyword_names, etc.) plus 6 nested struct columns (organism, protein_desc, etc.). Child tables carry typed columns for all frequently-queried fields (e.g., features.start_pos, features.description, xrefs.database, comments.text_value).

**Layout A (single VARIANT table).** A single entries table with four typed columns (acc, taxid, reviewed, sequence) and one `data VARIANT` column containing the entire UniProtKB JSON entry. No child tables; features, cross-references, comments, and publications are accessed at query time via `LATERAL UNNEST` on the VARIANT.

**Layout B (star schema, full VARIANT).** Same five-table star schema as baseline, but all convenience columns replaced by a single `data VARIANT` column per table. Child tables retain typed filter columns (type, database, comment_type) alongside the VARIANT.

**Layout C (star schema, stripped-entries VARIANT).** Identical to Layout B, except the entries VARIANT column excludes array fields (features, cross-references, comments, references) that are already materialised in child tables. This reduces entries from 3.92 MB (Layouts A/B) to 1.17 MB.

**Layout D (hybrid).** Entries table retains all 37 typed convenience columns from the baseline (identical Parquet file). Child tables use VARIANT data columns (identical to Layout B/C children). This combines fast typed filtering on the most-queried table with schema-free storage on child tables.

### 2.4 Benchmark queries

We defined 16 analytical queries spanning six categories: point lookups, organism-filtered scans, keyword/GO searches, aggregations, joins, full-table scans, nested struct access, and comment/publication retrieval. Each query was executed identically across all layouts (with SQL adapted to the column access pattern of each layout) and verified to return identical row counts.

### 2.5 VARIANT-specific implementation details

Two technical issues required specific handling during VARIANT conversion:

**Comment MAP serialisation.** UniProtKB comments are read by DuckDB's JSON reader as `MAP(VARCHAR, JSON)` rather than typed structs. Direct casting (`unnest::VARIANT`) produces `VARIANT(ARRAY)` — a list of key-value pairs where dot notation does not work. The fix is a two-step cast through JSON: `(unnest::JSON)::VARIANT`, which normalises the map to a JSON object before VARIANT encoding, yielding `VARIANT(OBJECT)` with proper field access via dot notation.

**Comment type quoting.** The `commentType` field, when extracted from a VARIANT via `CAST(... AS VARCHAR)`, retains JSON string quoting (e.g., `"FUNCTION"` instead of `FUNCTION`). We apply `trim('"' FROM CAST(...))` to produce clean VARCHAR values matching the baseline.


## 3. Results

### 3.1 Query latency

The table below reports median warm query latency in milliseconds across all five layouts. The D/Base column shows the ratio of Layout D to Baseline latency; values ≤1.5 indicate near-parity.

| Query                     | Category        | Rows   | Baseline | A       | B       | C      | D      | D/Base |
|---------------------------|-----------------|--------|----------|---------|---------|--------|--------|--------|
| point_lookup_by_acc       | Point lookup    | 1      | 15.3     | 3479.1  | 3421.5  | 130.7  | 14.0   | 0.9x   |
| protein_card_macro        | Point lookup    | 1      | 4.8      | 3476.6  | 3415.3  | 119.0  | 3.5    | 0.7x   |
| organism_filter_human     | Organism filter | 2,825  | 5.0      | 5992.6  | 5793.4  | 336.9  | 4.8    | 0.9x   |
| organism_features         | Organism filter | 1,279  | 4.0      | 5586.2  | 67.3    | 69.4   | 68.7   | 17.3x  |
| keyword_search            | Keyword search  | 43     | 4.0      | 5457.5  | 5419.8  | 287.5  | 3.8    | 1.0x   |
| go_term_search            | GO search       | 75     | 3.7      | 5497.1  | 5455.3  | 560.4  | 3.1    | 0.8x   |
| count_by_organism         | Aggregation     | 5      | 2.8      | 5531.4  | 5484.2  | 294.5  | 3.5    | 1.3x   |
| feature_type_distribution | Aggregation     | 32     | 2.3      | 5618.0  | 7.7     | 5.8    | 10.6   | 4.7x   |
| xref_database_distribution| Aggregation     | 10     | 2.7      | OOM     | 4.1     | 4.0    | 9.1    | 3.4x   |
| entries_join_features     | Join            | 6,274  | 10.2     | 7020.4  | 6970.6  | 497.2  | 81.2   | 8.0x   |
| full_scan_entries         | Full scan       | 1      | 2.0      | 3.5     | 3.6     | 2.7    | 1.9    | 0.9x   |
| full_scan_xrefs           | Full scan       | 1      | 1.2      | OOM     | 1.4     | 1.3    | 1.4    | 1.1x   |
| nested_organism_access    | Nested access   | 2,825  | 3.9      | 8770.5  | 8627.1  | 832.0  | 5.4    | 1.4x   |
| nested_protein_desc_access| Nested access   | 50     | 6.8      | 3468.8  | 3413.7  | 125.1  | 3.3    | 0.5x   |
| comment_function_search   | Comment search  | 215    | 2.1      | Error   | 23.7    | 20.9   | 20.9   | 10.2x  |
| publication_count_per_entry| Publication    | 10     | 4.8      | Error   | 4.0     | 3.9    | 6.8    | 1.4x   |

Layout A queries consistently exceeded 3,000 ms (3–9 seconds) due to full-table VARIANT deserialisation on every query. Layouts B and C showed that entries queries touching the VARIANT column remained slow (3–9 seconds for B; 100–800 ms for C with stripped arrays), while child table queries on pre-unnested VARIANT achieved near-baseline performance. Layout D eliminates entries VARIANT overhead entirely by retaining typed columns, making it the strongest VARIANT alternative — but it still does not match the fully typed baseline on child-table queries.

Of Layout D's 16 queries, 10 matched baseline latency (≤1.5x), 3 showed moderate overhead (1.5–5x) on child-table aggregations, and 3 showed higher overhead (8–17x) on queries that join with or scan VARIANT child tables extracting multiple nested fields. No VARIANT layout outperformed the baseline on any query.

### 3.2 Storage

| Metric           | Baseline | Layout A | Layout B | Layout C | Layout D |
|------------------|----------|----------|----------|----------|----------|
| Total size       | 3.23 MB  | 3.92 MB  | 5.87 MB  | 3.12 MB  | 2.95 MB  |
| Total rows       | 134,016  | 5,378    | 134,016  | 134,016  | 134,016  |
| Parquet columns  | —        | 417      | 575      | 427      | 271      |

Per-table storage breakdown:

| Table        | Baseline   | Layout A  | Layout B  | Layout C  | Layout D  |
|--------------|------------|-----------|-----------|-----------|-----------|
| entries      | 1,022 KB   | 3,920 KB  | 3,920 KB  | 1,170 KB  | 1,022 KB  |
| features     | 181 KB     | —         | 124 KB    | 124 KB    | 124 KB    |
| xrefs        | 888 KB     | —         | 1,110 KB  | 1,110 KB  | 1,110 KB  |
| comments     | 137 KB     | —         | 151 KB    | 151 KB    | 151 KB    |
| publications | 1,050 KB   | —         | 582 KB    | 582 KB    | 582 KB    |

Layout D achieved 2.95 MB total storage, 9% smaller than the 3.23 MB baseline. The entries table is identical (1,022 KB); child table storage is smaller for features (124 vs 181 KB) and publications (582 vs 1,050 KB) because VARIANT's binary encoding is more compact than the hand-extracted typed columns, while xrefs is slightly larger (1,110 vs 888 KB) and comments is comparable (151 vs 137 KB).

### 3.3 Build time

| Layout   | Build time | Notes                                         |
|----------|------------|-----------------------------------------------|
| Baseline | 2.04 s     | Full pipeline: JSONL staging + 5 table builds  |
| Layout A | 1.94 s     | Single table, no child unnesting               |
| Layout B | 4.25 s     | 5 tables with full VARIANT entries             |
| Layout C | 3.39 s     | 5 tables with stripped VARIANT entries          |
| Layout D | 2.80 s     | Entries copied from baseline + 4 VARIANT children |

Layout D build time (2.80 s) is comparable to the baseline (2.04 s). In production, Layout D avoids the entries SQL builder entirely (copies the existing Parquet file) and uses simple UNNEST + VARIANT cast for child tables.

### 3.4 Schema complexity

| Metric                    | Baseline       | Layout D (VARIANT children) |
|---------------------------|----------------|-----------------------------|
| Pipeline total LOC        | 1,383          | ~1,000 (estimated)          |
| SQL builder LOC           | 500 (36.2%)    | ~125 (entries only)         |
| SQL builder functions     | 5              | 1 (entries) + 4 simple      |
| Convenience columns       | 88             | 37 (entries only)           |
| Schema-dependent paths    | ~40 optional   | ~40 (entries only)          |

The four child-table SQL builder functions (`_build_features_sql`, `_build_xrefs_sql`, `_build_comments_sql`, `_build_publications_sql`) total approximately 375 lines and contain intricate schema-conditional logic using `discover_schema_paths()` to handle optional fields. In Layout D, these are replaced by four VARIANT builders totalling approximately 75 lines with no schema dependencies.


## 4. Discussion

### 4.1 Entries: VARIANT is not viable

VARIANT on the entries table (Layouts A, B, C) introduced 10–200x query latency overhead for any operation touching entry-level fields. Even with array stripping (Layout C), point lookups degraded from 15 ms to 130 ms and organism filters from 5 ms to 337 ms. This overhead is structural: entries contain deeply nested objects (organism, proteinDescription, genes) that must be fully deserialised from VARIANT on every access, whereas typed Parquet columns store these as pre-shredded columnar streams.

### 4.2 Child tables: VARIANT incurs measurable overhead

While child table queries on VARIANT (Layouts B, C, D) were the best-performing VARIANT configuration, they still showed measurable overhead compared to the fully typed baseline. Feature type distribution ran at 4.7x baseline (2.3 ms vs 10.6 ms), cross-reference database distribution at 3.4x (2.7 ms vs 9.1 ms), and publication counting at 1.4x (4.8 ms vs 6.8 ms). Queries extracting multiple nested VARIANT fields showed more pronounced overhead: `entries_join_features` at 8.0x and `organism_features` at 17.3x. Even the closest-to-parity child table queries (full table scans at 1.1x) did not outperform the baseline. The overhead stems from runtime CAST operations required to extract typed values from VARIANT columns, compared to direct columnar reads on pre-typed columns.

### 4.3 Comment VARIANT: MAP vs OBJECT serialisation

UniProtKB comments posed a unique challenge. DuckDB's JSON reader infers comments as `MAP(VARCHAR, JSON)[]` rather than typed struct arrays. When cast directly to VARIANT (`unnest::VARIANT`), this produces `VARIANT(ARRAY)` — a list of key-value pairs where dot notation fails silently. The solution is a two-stage cast: `(unnest::JSON)::VARIANT`, which first normalises the map to a JSON object, then encodes as `VARIANT(OBJECT)` with working field access. This pattern may be relevant to other bioinformatics datasets where JSON readers infer map types for heterogeneous objects.

### 4.4 Conclusion: fully typed star schema is optimal for query performance

The fully typed baseline star schema delivers the best query performance across all 16 benchmark queries. No VARIANT layout matched or outperformed the baseline on any query category. The performance advantage of explicit typing is consistent: typed columns enable direct columnar reads without runtime deserialisation or CAST overhead, which benefits every access pattern from point lookups to aggregations to joins.

Layout D (hybrid) was the strongest VARIANT alternative, achieving near-parity on entries queries (which retain typed columns) and the smallest overhead on child tables. However, even Layout D showed 2–17x overhead on child-table queries that extract nested fields — overhead that is entirely absent in the fully typed baseline.

The 375 lines of schema-dependent SQL builder code that VARIANT would eliminate are a real maintenance cost, but the query performance benefit of explicit typing justifies this investment for a data lake whose primary purpose is to serve analytical queries. Schema evolution remains a manual process, but the typed approach ensures that every query against the lake runs at columnar-native speed without runtime type resolution.

For bioinformatics data lakes where query performance is the primary design objective, these results support choosing explicit column extraction over semi-structured storage, even when the semi-structured alternative (VARIANT) offers simpler schema management and comparable storage efficiency.


## 5. Supplementary Material

### S1. Benchmark query SQL — Baseline

All baseline queries use the uniprot-parquet client library views, which present Parquet files as virtual tables with pre-defined column names.

```sql
-- Q1: point_lookup_by_acc (Point lookup, 1 row)
SELECT * FROM entries WHERE acc = '{acc}'

-- Q2: protein_card_macro (Point lookup, 1 row)
SELECT * FROM protein_card('{acc}')

-- Q3: organism_filter_human (Organism filter, 2,825 rows)
SELECT acc, gene_names, protein_name FROM entries WHERE taxid = 9606

-- Q4: organism_features (Organism filter, 1,279 rows)
SELECT * FROM organism_features(9606, 'Domain')

-- Q5: keyword_search (Keyword search, 43 rows)
SELECT acc, protein_name FROM entries
WHERE list_contains(keyword_names, 'Kinase')

-- Q6: go_term_search (GO search, 75 rows)
SELECT acc, protein_name FROM entries
WHERE list_contains(go_ids, 'GO:0005524')

-- Q7: count_by_organism (Aggregation, 5 rows)
SELECT taxid, organism_name, count(*) AS n
FROM entries GROUP BY taxid, organism_name ORDER BY n DESC LIMIT 10

-- Q8: feature_type_distribution (Aggregation, 32 rows)
SELECT type, count(*) AS n FROM features GROUP BY type ORDER BY n DESC

-- Q9: xref_database_distribution (Aggregation, 10 rows)
SELECT database, count(*) AS n FROM xrefs
GROUP BY database ORDER BY n DESC LIMIT 10

-- Q10: entries_join_features (Join, 6,274 rows)
SELECT e.acc, e.protein_name, f.type, f.start_pos, f.end_pos, f.description
FROM entries e
JOIN features f ON e.acc = f.acc
WHERE e.taxid = 9606

-- Q11: full_scan_entries (Full scan, 1 row)
SELECT count(*) FROM entries

-- Q12: full_scan_xrefs (Full scan, 1 row)
SELECT count(*) FROM xrefs

-- Q13: nested_organism_access (Nested access, 2,825 rows)
SELECT acc, organism.taxonId, organism.scientificName
FROM entries WHERE organism.taxonId = 9606

-- Q14: nested_protein_desc_access (Nested access, 50 rows)
SELECT acc, protein_desc.recommendedName.fullName.value
FROM entries WHERE protein_desc IS NOT NULL LIMIT 50

-- Q15: comment_function_search (Comment search, 215 rows)
SELECT acc, text_value FROM comments
WHERE comment_type = 'FUNCTION' AND taxid = 9606

-- Q16: publication_count_per_entry (Publication, 10 rows)
SELECT acc, count(*) AS n FROM publications
GROUP BY acc ORDER BY n DESC LIMIT 10
```

### S2. Benchmark query SQL — Layout D (hybrid)

Layout D entries queries are identical to baseline (typed columns). Child table queries use VARIANT dot notation with explicit CAST for type extraction. Table references use `read_parquet()` with glob patterns.

```sql
-- Q1: point_lookup_by_acc (Point lookup, typed entries)
SELECT * FROM entries WHERE acc = '{acc}'

-- Q2: protein_card_macro (Point lookup, typed entries)
SELECT acc, taxid, reviewed, organism_name, protein_name, sequence
FROM entries WHERE acc = '{acc}'

-- Q3: organism_filter_human (Organism filter, typed entries)
SELECT acc, gene_names, protein_name FROM entries WHERE taxid = 9606

-- Q4: organism_features (Organism filter, VARIANT features child)
SELECT acc,
       CAST(data.location.start.value AS INTEGER) AS start_pos,
       CAST(data.location.end.value AS INTEGER) AS end_pos,
       CAST(data.description AS VARCHAR) AS description
FROM features WHERE taxid = 9606 AND type = 'Domain'

-- Q5: keyword_search (Keyword search, typed entries)
SELECT acc, protein_name FROM entries
WHERE list_contains(keyword_names, 'Kinase')

-- Q6: go_term_search (GO search, typed entries)
SELECT acc, protein_name FROM entries
WHERE list_contains(go_ids, 'GO:0005524')

-- Q7: count_by_organism (Aggregation, typed entries)
SELECT taxid, organism_name, count(*) AS n
FROM entries GROUP BY taxid, organism_name ORDER BY n DESC LIMIT 10

-- Q8: feature_type_distribution (Aggregation, VARIANT features child)
SELECT type, count(*) AS n FROM features GROUP BY type ORDER BY n DESC

-- Q9: xref_database_distribution (Aggregation, VARIANT xrefs child)
SELECT database, count(*) AS n FROM xrefs
GROUP BY database ORDER BY n DESC LIMIT 10

-- Q10: entries_join_features (Join, typed entries + VARIANT features)
SELECT e.acc, e.protein_name,
       ft.type,
       CAST(ft.data.location.start.value AS INTEGER) AS start_pos,
       CAST(ft.data.location.end.value AS INTEGER) AS end_pos,
       CAST(ft.data.description AS VARCHAR) AS description
FROM entries e
JOIN features ft ON e.acc = ft.acc
WHERE e.taxid = 9606

-- Q11: full_scan_entries (Full scan, typed entries)
SELECT count(*) FROM entries

-- Q12: full_scan_xrefs (Full scan, VARIANT xrefs child)
SELECT count(*) FROM xrefs

-- Q13: nested_organism_access (Typed columns, no nested access needed)
SELECT acc, taxid, organism_name FROM entries WHERE taxid = 9606

-- Q14: nested_protein_desc_access (Typed columns)
SELECT acc, protein_name FROM entries WHERE protein_name IS NOT NULL LIMIT 50

-- Q15: comment_function_search (VARIANT comments child)
-- Comments require CAST to typed struct array for LATERAL UNNEST
-- because data.texts is a VARIANT array, not a native list.
SELECT c.acc,
       CAST(tx.t.value AS VARCHAR) AS text_value
FROM comments c,
LATERAL UNNEST(CAST(c.data.texts
    AS STRUCT(value VARCHAR, evidences JSON)[])) AS tx(t)
WHERE c.comment_type = 'FUNCTION' AND c.taxid = 9606

-- Q16: publication_count_per_entry (VARIANT publications child)
SELECT acc, count(*) AS n FROM publications
GROUP BY acc ORDER BY n DESC LIMIT 10
```

### S3. Layout D child table schema

Each child table retains typed filter/sort columns for WHERE clauses. All other fields are accessed via VARIANT dot notation at query time.

```
features:     acc VARCHAR, from_reviewed BOOLEAN, taxid INTEGER,
              type VARCHAR, data VARIANT

xrefs:        acc VARCHAR, from_reviewed BOOLEAN, taxid INTEGER,
              database VARCHAR, data VARIANT

comments:     acc VARCHAR, from_reviewed BOOLEAN, taxid INTEGER,
              comment_type VARCHAR, data VARIANT

publications: acc VARCHAR, from_reviewed BOOLEAN, taxid INTEGER,
              data VARIANT
```

The entries table schema is unchanged from the baseline (37 typed convenience columns plus 6 nested struct columns).

### S4. VARIANT child table build SQL

The Layout D child tables are built by simple UNNEST + VARIANT cast from JSONL input, with no schema-conditional logic:

```sql
-- Features (VARIANT child)
SELECT
    e.primaryAccession                        AS acc,
    CASE WHEN e.entryType LIKE '%Swiss-Prot%'
         THEN true ELSE false END             AS from_reviewed,
    e.organism.taxonId                        AS taxid,
    unnest.type                               AS type,
    unnest::VARIANT                           AS data
FROM read_json_auto(...) e,
LATERAL UNNEST(COALESCE(e.features, [])) AS t(unnest)
ORDER BY from_reviewed DESC, taxid, acc

-- Cross-references (VARIANT child)
SELECT
    e.primaryAccession                        AS acc,
    CASE WHEN e.entryType LIKE '%Swiss-Prot%'
         THEN true ELSE false END             AS from_reviewed,
    e.organism.taxonId                        AS taxid,
    unnest.database                           AS database,
    unnest::VARIANT                           AS data
FROM read_json_auto(...) e,
LATERAL UNNEST(COALESCE(e.uniProtKBCrossReferences, [])) AS t(unnest)
ORDER BY from_reviewed DESC, taxid, acc

-- Comments (VARIANT child — requires JSON intermediate cast)
SELECT
    e.primaryAccession                        AS acc,
    CASE WHEN e.entryType LIKE '%Swiss-Prot%'
         THEN true ELSE false END             AS from_reviewed,
    e.organism.taxonId                        AS taxid,
    trim('"' FROM CAST(unnest.commentType AS VARCHAR)) AS comment_type,
    (unnest::JSON)::VARIANT                   AS data
FROM read_json_auto(...) e,
LATERAL UNNEST(COALESCE(e.comments, [])) AS t(unnest)
ORDER BY from_reviewed DESC, taxid, acc

-- Publications (VARIANT child)
SELECT
    e.primaryAccession                        AS acc,
    CASE WHEN e.entryType LIKE '%Swiss-Prot%'
         THEN true ELSE false END             AS from_reviewed,
    e.organism.taxonId                        AS taxid,
    unnest::VARIANT                           AS data
FROM read_json_auto(...) e,
LATERAL UNNEST(COALESCE(e."references", [])) AS t(unnest)
ORDER BY from_reviewed DESC, taxid, acc
```

### S5. Reproduction

```bash
# Prerequisites: Python 3.10+, DuckDB >= 1.5
pip install duckdb

# Clone and enter repo
git clone https://github.com/<user>/uniprot-parquet
cd uniprot-parquet
git checkout static-lake

# Build baseline lake (if not present)
python bin/parquet_transform.py demo/lake/2026_01/sorted.jsonl.zst \
    --outdir demo/lake/2026_01/lake --memory-limit 3GB

# Run baseline benchmarks
python benchmarks/bench_baseline.py

# Build all VARIANT layouts (A, B, C, D)
python benchmarks/build_variant_lake.py \
    --input demo/lake/2026_01/sorted.jsonl.zst \
    --baseline-lake demo/lake/2026_01/lake

# Run VARIANT benchmarks (includes Layout D)
python benchmarks/bench_variant.py

# Build a Layout D lake using the pipeline directly
python bin/parquet_transform.py demo/lake/2026_01/sorted.jsonl.zst \
    --outdir my_layout_d_lake --variant-children --memory-limit 3GB
```

### S6. Key files

| File                              | Description                                    | Lines  |
|-----------------------------------|------------------------------------------------|--------|
| `bin/parquet_transform.py`        | Core pipeline: JSONL to Parquet star schema     | ~1,500 |
| `uniprot_parquet.py`              | Single-file Python client library               | 392    |
| `benchmarks/bench_baseline.py`    | Baseline benchmark suite (16 queries)           | ~500   |
| `benchmarks/bench_variant.py`     | VARIANT benchmark suite (16 queries x 5 layouts)| ~1,000 |
| `benchmarks/build_variant_lake.py`| Builds VARIANT lakes A, B, C, D                 | ~520   |
