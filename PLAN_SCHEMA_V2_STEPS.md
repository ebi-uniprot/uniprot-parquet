# Work order: implementing `PLAN_SCHEMA_V2.md`

**Status:** written 2026-09-16 against commit `f1c9fb6` plus the uncommitted `PLAN_SCHEMA_V2.md`. Not started.

This file is the ordered, file-by-file set of edits that implements `PLAN_SCHEMA_V2.md` (the plan). The plan holds the design and the reasons; this file holds the work. If the two disagree, this file is wrong: fix it here and say so in the plan's Results log.

## How to use this file

1. Read the plan once, end to end, before starting. Do not reopen anything listed in the plan's "Decisions taken 2026-09-16" line.
2. Do the steps **in order**. Each step has **Files**, **Changes**, **Tests**, **Verify** and **Done when**. Do not start a step until the previous step's "Done when" holds.
3. Work on a branch. Commit at the end of every step with the message `Step N: <step title>`. Never commit a failing test suite.
4. Whenever a step says "verify against the installed DuckDB/PyArrow", run the snippet exactly as written, follow the branch that matches what you see, and record the outcome in the plan's **Results log** table. Do not guess API names from memory.
5. Line numbers below are as of commit `f1c9fb6`. They drift as you edit. Always relocate with `grep -n` before editing.
6. The test suite is the gate. Run it after every step:
   ```bash
   python -m pytest tests/ -q            # default fixture, ~2 min
   python -m pytest tests/ -q --stress   # before finishing steps 11, 14, 15 and 21
   ```
   Fixtures download automatically on the first run (`tests/fetch_fixtures.py`).

## Repository map (what you will touch)

| Path | What it is |
| --- | --- |
| `bin/parquet_transform.py` | The transform. SQL builders (`_build_entries_sql` …), `TABLE_DEFS`, `TABLE_META`, `COLUMN_DESCRIPTIONS`, the Parquet writer `stream_to_parquet`, `_build_datapackage`, `main()` (manifest). Almost every step edits it. |
| `bin/validate_lake.py` | The release validator. One `check_*` function per check, wired in `main()`. Exit 1 blocks publication. |
| `bin/release_manifest.py` | Writes `provenance.json` after validation. |
| `bin/stream_jsonl.py`, `bin/sort_jsonl.py` | JSON → JSONL, and the sort. Not changed by this plan except where noted. |
| `uniprot_parquet.py` | The single-file Python client (`connect`, `manifest`, `tables`, `schema`). Views and macros live in `_SETUP_SQL`. |
| `setup_views.sql` | The pure-SQL twin of `_SETUP_SQL`. Keep the two in sync by hand. |
| `upjson2lake.nf` | Nextflow pipeline: `STREAM_JSONL → SORT_JSONL → PARQUET_TRANSFORM → VALIDATE → PROVENANCE`. |
| `tests/conftest.py` | Builds one fixture lake per session (`parquet_lake`) by running the transform as a subprocess. |
| `tests/test_parquet_transform.py`, `tests/test_roundtrip.py`, `tests/test_validate.py`, `tests/test_idempotency.py` | The suites. |
| `demo/` | `run_demo.sh` builds `demo/lake/2026_01/` with the pipeline; the demo lake's `manifest.json` is committed and is what the plan's Part G was read from. |
| `benchmarks/DEDUPLICATION_PROPOSAL.md` | The dedup design; its appendix (lines 368–694) is the target schema for Step 11. |

## Gotchas that will cost you an afternoon

- **Doubled braces in the SQL builders.** Every `_build_*_sql` returns an f-string, and `main()` then calls `.format(read_clause=…)` on it. Inside the f-string the placeholder is written `{{read_clause}}` (doubled) and any literal `{` or `}` in SQL must also be doubled. A single `{` in new SQL raises `KeyError` at `.format` time.
- **DuckDB types a bare `NULL` as `INTEGER`.** That is the Part G.2 bug. Every fallback must be `NULL::<type>`.
- **`unnest` is the alias of the unnested element** in the child-table builders (`LATERAL unnest(sub.features)` puts the element in a column literally named `unnest`). `unnest.type` is a field of that element.
- **`TABLE_DEFS` tuples are unpacked in three loops in `main()`** (`for name, _, _ in TABLE_DEFS`) and one `for name, sql_template, sort_order in TABLE_DEFS`. When you add a fourth field (Step 14), update all four unpackings. `grep -n "in TABLE_DEFS" bin/parquet_transform.py`.
- **The fixture lake has one file and one row group per table.** Anything about row-group skipping, false positives or file pruning cannot be tested on it. Those measurements run on the slice (Step 3) or a full build.
- **`ds.dataset(dir)` in PyArrow discovers files recursively but does not add Hive columns unless `partitioning="hive"` is passed.** After Step 14 the validator's and tests' `open_table` keep working unchanged and do not see `review_status`; that is intended.
- **DuckDB globs:** `*` does not cross directories. After Step 14, every `read_parquet('<table>/*.parquet')` in the validator, the client and the docs must become `<table>/*/*.parquet` (views) or `<table>/**/*.parquet` (docs).

---

## Step 0 — Baseline

**Verify**
```bash
git status                       # clean tree, on your branch
python -m pytest tests/ -q       # note the passing count (README says 89)
```
**Done when** the suite is green and the count is recorded in your commit message for Step 1.

---

## Step 1 — Phase 0 spikes (plan Part A §3)

No code changes. Three snippets, three rows of the Results log.

**1.1 Can the pinned PyArrow write bloom filters?**
```python
import inspect, pyarrow, pyarrow.parquet as pq
print(pyarrow.__version__)
sig = inspect.signature(pq.ParquetWriter.__init__)
print([p for p in sig.parameters if "bloom" in p.lower()])
print(pq.ParquetWriter.__init__.__doc__)   # read the bloom paragraph, note the parameter names and their types
```
- Non-empty list → record the exact parameter name(s) and their expected value shape (a list of column names? a dict of column → fpp? a bool plus a column list?) in the Results log. Step 12 will use them verbatim.
- Empty list → record "not supported; B1 deferred". **Skip Step 12 entirely.** Everything else is unchanged.

**1.2 Does DuckDB write and read them?** Run the plan's §0.2 snippet exactly. If the `COPY` fails on an option name, record the error text and move on; this spike is informational (it tells you whether DuckDB, the reader users will use, honours the filters).

**1.3 Baseline pruning.** Run the plan's §0.3 query against `demo/lake/2026_01/lake/entries/*.parquet` (the demo lake is still flat at this point). Record the ratio. Re-run it in Step 3 on the slice, where it means something.

**Done when** the three Results-log rows are filled.

---

## Step 2 — G.1: make `comments.text_value` real

**Files:** `bin/parquet_transform.py` (`_build_comments_sql`, lines 539–545), `bin/validate_lake.py`, `tests/test_parquet_transform.py`, `tests/test_roundtrip.py`.

**Changes**

1. In `_build_comments_sql`, delete the `if has("comments.texts"): … else: text_value_expr = "NULL"` block and replace it with an unconditional expression. Find the form that works on the installed DuckDB with this ladder, run against the fixture's staged Parquet (build one with `python bin/parquet_transform.py <fixture jsonl> --outdir /tmp/g1` and keep `/tmp/g1/.staging/staged.parquet` by commenting out the cleanup in `main()`'s `finally` temporarily, or simply test against `read_json_auto` on a fixture JSONL):
   ```sql
   -- Form A (the existing guarded expression, made unconditional)
   SELECT typeof(unnest.texts), count(unnest.texts)
   FROM (SELECT comments FROM <staged> WHERE comments IS NOT NULL) s, LATERAL unnest(s.comments);
   -- Form B
   SELECT typeof(unnest['texts']), count(unnest['texts']) FROM … ;
   -- Form C
   SELECT typeof(map_extract(unnest, 'texts')[1]), count(map_extract(unnest, 'texts')[1]) FROM … ;
   ```
   Take the first form that runs without error **and** returns a non-zero count. Call the winning accessor `TEXTS`. Then the column expression is:
   ```sql
   NULLIF(array_to_string(
       from_json(TEXTS->'$[*].value', '["VARCHAR"]'),
       chr(10) || chr(10)), '')                     AS text_value
   ```
   Confirm on a `COFACTOR` comment (no `texts` key) that the expression yields NULL, not an error. Record the winning form in the Results log.
2. `bin/validate_lake.py`: add `check_text_value(report, lake_dir)` after `check_field_completeness` and call it from `main()` right after `check_field_completeness(...)`:
   ```python
   def check_text_value(report, lake_dir):
       """Text-bearing comment types must have a populated text_value."""
       report.checks.append("\n--- 15. COMMENT TEXT ---")
       eprint("\n--- 15. COMMENT TEXT ---")
       import duckdb
       path = os.path.join(lake_dir, "comments", "*.parquet")   # becomes **/*.parquet in Step 14
       rows = duckdb.sql(f"""
           SELECT comment_type, count(*) AS n, count(text_value) AS with_text
           FROM read_parquet('{path}') GROUP BY 1
       """).fetchall()
       text_types = {"FUNCTION", "SUBUNIT", "SUBCELLULAR LOCATION", "TISSUE SPECIFICITY",
                     "DISEASE", "DOMAIN", "PTM", "SIMILARITY", "CAUTION", "MISCELLANEOUS"}
       present = {r[0]: (r[1], r[2]) for r in rows}
       for ctype in sorted(text_types & set(present)):
           n, with_text = present[ctype]
           report.check(f"comments.text_value populated for {ctype}", with_text > 0,
                        f"{with_text:,}/{n:,} rows have text")
   ```
3. Update the module docstring's check list in `bin/validate_lake.py` (add item 15) and the README "Validation" list.

**Tests**

- `tests/test_parquet_transform.py::TestSchema`: add
  ```python
  def test_comments_text_value_is_string(self, comments_ds):
      assert str(comments_ds.schema.field("text_value").type) in ("string", "large_string")
  ```
- `tests/test_roundtrip.py::TestCommentContent`: add `test_text_value_matches_texts`. For every original entry and every original comment that has a `texts` key, compute `expected = "\n\n".join(t["value"] for t in c["texts"])` and assert `expected` is in the set of `row["text_value"]` for that accession's lake rows with the same `comment_type`. For comments without `texts`, assert the matching lake rows have `text_value is None`.

**Verify**
```bash
python -m pytest tests/ -q
```
**Done when** the suite is green, the validator's new check passes on the fixture (it runs inside `tests/test_validate.py`), and the Results-log G.1 row is filled.

---

## Step 3 — F.1: build the measurement slice and fill the F.1 table

**Purpose.** Every later size decision (F.3, F.4, H.1, D.5 ratio, B size gates) reads from this slice. Build it once, keep it.

**Files:** new `bin/sample_jsonl.py`; no changes to existing code.

**Changes**

1. Write `bin/sample_jsonl.py`: reservoir-sample `--n` lines from a JSONL(.zst) read on stdin or from a path, with `--where trembl|swissprot|all` (filter on `"entryType"` containing `TrEMBL` / `Swiss-Prot`, tested by a fast substring check on the raw line before parsing) and `--seed`. Stream; never hold more than `n` lines. Output JSONL to stdout.
2. Build the slice input:
   ```bash
   # human, reviewed + unreviewed, via the REST stream (the same source as demo/run_demo.sh)
   curl --globoff -o human.json.gz \
     "https://rest.uniprot.org/uniprotkb/stream?query=organism_id:9606&format=json&compressed=true"
   pigz -dc human.json.gz | python bin/stream_jsonl.py > human.jsonl
   # 1M random TrEMBL entries from the full dump (needs UniProtKB.json.gz, see run_lake.sh)
   pigz -dc UniProtKB.json.gz | python bin/stream_jsonl.py \
     | python bin/sample_jsonl.py --n 1000000 --where trembl --seed 1 > trembl_1m.jsonl
   cat human.jsonl trembl_1m.jsonl | zstd -3 -T0 -o slice.jsonl.zst
   python bin/sort_jsonl.py slice.jsonl.zst -o slice.sorted.jsonl.zst --memory-limit 16GB
   python bin/parquet_transform.py slice.sorted.jsonl.zst --outdir slice/lake --release slice --memory-limit 16GB
   python bin/validate_lake.py --lake slice/lake --jsonl slice.sorted.jsonl.zst -o slice/validation_report.txt
   ```
   Keep `slice.sorted.jsonl.zst`; every later measurement step rebuilds `slice/lake` from it.
3. Fill the plan's F.1 table with these queries (run in DuckDB against `slice/lake`):
   ```sql
   -- bytes and rows per table and side (entries uses reviewed; child tables still say from_reviewed at this step)
   SELECT 'entries' AS t, reviewed AS side, count(*) AS rows FROM read_parquet('slice/lake/entries/*.parquet') GROUP BY 1,2;
   -- compressed bytes per table from metadata
   SELECT sum(total_compressed_size) FROM parquet_metadata('slice/lake/entries/*.parquet');
   -- repeat for features, xrefs, comments, publications
   ```
   Bytes per entry per side = table bytes attributable to that side ÷ entries on that side. Because the slice's files are not yet single-sided, attribute row groups to a side by `min(reviewed) = max(reviewed)` per row group (`parquet_metadata` `stats_min`/`stats_max` on the `reviewed` / `from_reviewed` column); mixed row groups are at most one per table, ignore them. Extrapolate to 248M entries at the current Swiss-Prot/TrEMBL ratio (~570k / ~248M).
4. Footer bytes for `entries`: `sum(pq.read_metadata(f).serialized_size for f in files)`.
5. Also record now, for D.5's revisit condition: bytes of the `entries` files that contain any `taxid = 9606` row group (from `parquet_metadata` `stats_min <= 9606 <= stats_max` on `taxid`) versus the compressed bytes of the human row groups alone. The ratio goes in the Results log D.5 row.
6. Run the plan's Phase 0.3 query on the slice and record the ratio.

**Done when** the F.1 table in the plan is filled, `AUDIT.md` §6.1 cites it, and the Results-log rows for F.1, 0.3 and the D.5 ratio are filled.

---

## Step 4 — G.2: typed `NULL` fallbacks and a full schema-type check

**Files:** `bin/parquet_transform.py`, `bin/validate_lake.py` (`check_schema_types`, line 799), `tests/test_parquet_transform.py`, `tests/conftest.py`.

**Changes**

1. Add, next to `COLUMN_DESCRIPTIONS`, a dict of **DuckDB** type strings for every column that has a `NULL` fallback in a builder. The fallbacks today (`grep -n '"NULL"' bin/parquet_transform.py`):

   | Table | Columns with a fallback |
   | --- | --- |
   | entries | `organism_hosts`, `gene_locations` |
   | features | `feature_id`, `original_sequence`, `alternative_sequences`, `ligand_name`, `ligand_id`, `ligand_label`, `ligand_note` |
   | xrefs | `isoform_id`, `evidences`, `properties` |
   | publications | `title`, `authors`, `authoring_group`, `journal`, `volume`, `first_page`, `last_page`, `submission_database`, `citation_xrefs`, `reference_positions`, `reference_comments`, `evidences` |

   Get each type string from a build where the field is present. Build the stress fixture lake (`python tests/fetch_fixtures.py --scale stress`, then transform it) and run:
   ```sql
   DESCRIBE SELECT * FROM read_parquet('<stress lake>/features/*.parquet');   -- column_name, column_type
   ```
   Copy `column_type` verbatim for each column above into:
   ```python
   COLUMN_TYPES = {
       ("entries", "organism_hosts"): "STRUCT(...)[]",     # paste from DESCRIBE
       ("features", "feature_id"):    "VARCHAR",
       # … one line per column in the table above
   }
   ```
   If a column is absent from the stress build too, take its type from the slice lake (Step 3).
2. Add a helper and use it in every fallback:
   ```python
   def _null(table: str, column: str) -> str:
       return f"NULL::{COLUMN_TYPES[(table, column)]}"
   ```
   e.g. `feature_id = "unnest.featureId" if has("features.featureId") else _null("features", "feature_id")`. Do this for all 24. A `KeyError` here is the intended failure: every fallback must be typed.
3. `bin/validate_lake.py::check_schema_types`: keep the existing hand-written `expected_types` check, and add a second loop that imports `COLUMN_TYPES` from `parquet_transform` (the script directory is on `sys.path` when run as a script; in tests it is added by `conftest.py`) and, per table, runs `DESCRIBE SELECT * FROM read_parquet('<table glob>')` in DuckDB, then asserts `column_type == COLUMN_TYPES[(table, col)]` for every listed column. Report one check line per table ("`<table>`: N declared column types match").

**Tests**

- `tests/conftest.py`: add a session fixture `small_lake` that builds a lake from `tests/fixtures/small.json.gz` (it is a `{"results": [...]}` file like the others; reuse the JSON→JSONL code of `small_jsonl` by factoring it into a helper `_json_gz_to_jsonl_zst(src, dst)`).
- `tests/test_parquet_transform.py`: new `TestTypedFallbacks::test_absent_optional_field_keeps_declared_type` — on `small_lake` (which lacks `organismHosts`), assert `str(entries_ds.schema.field("organism_hosts").type) != "int32"` and that it starts with `list<`.

**Verify** `python -m pytest tests/ -q`; then `python bin/validate_lake.py --lake demo/lake/2026_01/lake --jsonl demo/lake/2026_01/sorted.jsonl.zst -o /tmp/r.txt` **must now fail** on the demo lake (it still has the `int32` columns); that is expected until Step 21 regenerates the demo.

**Done when** the suite is green and no `"NULL"` string literal remains in `bin/parquet_transform.py` except inside `_null`.

---

## Step 5 — G.3: rename `from_reviewed` → `reviewed` everywhere

**Files:** every file `grep -rln from_reviewed bin tests uniprot_parquet.py setup_views.sql README.md` returns.

**Changes** (mechanical; do them in this order and re-run the grep after each file)

1. `bin/parquet_transform.py`: the four child builders (`AS from_reviewed` → `AS reviewed`, `sub.from_reviewed` → `sub.reviewed`, `ORDER BY sub.from_reviewed DESC` → `ORDER BY sub.reviewed DESC`), the four `_build_*_variant_sql` builders, `TABLE_DEFS` sort orders and the comment above them, `TABLE_META` convenience lists and the `features` description string, `COLUMN_DESCRIPTIONS` keys (`("features", "from_reviewed")` → `("features", "reviewed")`, same text).
2. `bin/validate_lake.py`: `check_null_keys` lists, `check_sort_order` table list (all become `"reviewed"`), `check_denormalized_sync` SQL (`c.reviewed != e.reviewed`) and its check label, `check_schema_types` expected types, the module docstring.
3. `tests/test_parquet_transform.py`: `TestSchema` required sets, `TestSortOrder` (column names and test names), anything else the grep finds.
4. `README.md`: the "All tables are sorted…" line, "Schema design" paragraph (delete the sentence explaining the two names), the column reference blocks, the Validation list.
5. `uniprot_parquet.py` and `setup_views.sql`: nothing references `from_reviewed` today; confirm with grep.

**Verify**
```bash
grep -rn from_reviewed bin tests uniprot_parquet.py setup_views.sql README.md   # must print nothing
python -m pytest tests/ -q
```
**Done when** both hold. `AUDIT.md`, `DEMAND_REVIEW.md` and `benchmarks/*.md` keep the old name as history; do not edit them.

---

## Step 6 — Part B columns, `evidence_type`, and the D.5 `division` column

**Files:** `bin/parquet_transform.py` (`_build_entries_sql`, `TABLE_META`, `COLUMN_DESCRIPTIONS`, `COLUMN_TYPES` if a fallback is needed), `uniprot_parquet.py`, `setup_views.sql`, `tests/test_parquet_transform.py`, `tests/test_roundtrip.py`, `README.md`.

**Changes**

1. **Shared `reviewed` expression.** Add a module constant and use it in all six builders (entries, four children, and Step 13's `accession_map`):
   ```python
   REVIEWED_EXPR = "CASE WHEN e.entryType LIKE '%Swiss-Prot%' THEN true ELSE false END"
   ```
   In the f-strings write `{REVIEWED_EXPR} AS reviewed`.
2. **`gene_name`.** Directly after the `gene_names` expression in `_build_entries_sql` add:
   ```sql
   list_extract(list_transform(COALESCE(e.genes, []), g -> g.geneName.value), 1) AS gene_name,
   ```
3. **`go_terms`.** Directly after `go_ids`. First verify the property key strings on the fixture's staged Parquet:
   ```sql
   SELECT DISTINCT p.key FROM (SELECT unnest(uniProtKBCrossReferences) AS x FROM <staged>) , LATERAL unnest(x.properties) AS t(p) WHERE x.database = 'GO';
   ```
   Expect `GoTerm` and `GoEvidenceType`. Then, guarded by `has("uniProtKBCrossReferences.properties")` (else `_null("entries", "go_terms")` with the struct type added to `COLUMN_TYPES`):
   ```sql
   [ struct_pack(
         id            := x.id,
         aspect        := left(list_filter(COALESCE(x.properties, []), p -> p.key = 'GoTerm')[1].value, 1),
         term          := substr(list_filter(COALESCE(x.properties, []), p -> p.key = 'GoTerm')[1].value, 3),
         evidence_type := list_filter(COALESCE(x.properties, []), p -> p.key = 'GoEvidenceType')[1].value)
     FOR x IN COALESCE(e.uniProtKBCrossReferences, [])
     IF x.database = 'GO' ]                       AS go_terms,
   ```
   If DuckDB rejects `[1].value` on an empty list result, wrap in `COALESCE(list_filter(...), [NULL])`. NULL, never `''`, when `GoTerm` is absent.
4. **`proteome_ids`.** After `xref_dbs`:
   ```sql
   list_sort(list_distinct([ x.id FOR x IN COALESCE(e.uniProtKBCrossReferences, []) IF x.database = 'Proteomes' ])) AS proteome_ids,
   ```
5. **`pubmed_ids`.** After `reference_count`, guarded like the publications builder by `has("references.citation.citationCrossReferences")` (else `CAST([] AS VARCHAR[])`):
   ```sql
   list_transform(
       list_sort(list_transform(
           list_distinct(flatten(list_transform(
               COALESCE(e."references", []),
               r -> [ c.id FOR c IN COALESCE(r.citation.citationCrossReferences, []) IF c.database = 'PubMed' ]
           ))),
           x -> CAST(x AS BIGINT))),
       x -> CAST(x AS VARCHAR))                     AS pubmed_ids,
   ```
   Verify once on the slice that `SELECT count(*) FROM (…) WHERE TRY_CAST(id AS BIGINT) IS NULL` is 0 for PubMed ids; if not, stop and report.
6. **`division`.** After `lineage`. UniProt's rule set, most specific first; `e.organism.lineage` is a `VARCHAR[]`:
   ```sql
   CASE
     WHEN e.organism.taxonId = 9606                              THEN 'human'
     WHEN list_contains(e.organism.lineage, 'Rodentia')          THEN 'rodents'
     WHEN list_contains(e.organism.lineage, 'Mammalia')          THEN 'mammals'
     WHEN list_contains(e.organism.lineage, 'Vertebrata')        THEN 'vertebrates'
     WHEN list_contains(e.organism.lineage, 'Metazoa')           THEN 'invertebrates'
     WHEN list_contains(e.organism.lineage, 'Fungi')             THEN 'fungi'
     WHEN list_contains(e.organism.lineage, 'Viridiplantae')     THEN 'plants'
     WHEN list_contains(e.organism.lineage, 'Bacteria')          THEN 'bacteria'
     WHEN list_contains(e.organism.lineage, 'Archaea')           THEN 'archaea'
     WHEN list_contains(e.organism.lineage, 'Viruses')           THEN 'viruses'
     ELSE 'unclassified'
   END                                             AS division,
   ```
   This is a first approximation of UniProt's rules. The D.5 correctness gate (per-division counts equal to the FTP `taxonomic_divisions/` files for the same release) decides whether it is right; the likely adjustments are where non-fungal, non-plant, non-animal eukaryotes (protists) and `unclassified sequences` land. Run the gate on the first full build (Step 21) and fix the CASE until counts match exactly. Record in the Results log.
7. **Views and macros.** In both `uniprot_parquet.py::_SETUP_SQL` and `setup_views.sql`: the `entries` view becomes `SELECT * FROM read_parquet(...)` (drop `, gene_names[1] AS gene_name`); every `e.gene_names[1] AS gene_name` in `protein_card`, `entries_with_features`, `entries_with_xrefs`, `unnest_isoforms` becomes `e.gene_name`.
8. **Metadata dicts.** Add the five columns to `TABLE_META["entries"]["columns"]["convenience"]` (in the Step 7 order) and to `COLUMN_DESCRIPTIONS`. Descriptions: `gene_name` "Primary gene name (first of gene_names)."; `go_terms` "GO annotations as {id, aspect (P/F/C), term, evidence_type}; go_ids is the flat id list."; `proteome_ids` "Distinct UniProt proteome ids (UP…) from cross-references, sorted."; `pubmed_ids` "Distinct PubMed ids cited by the entry, sorted numerically, stored as strings."; `division` "UniProt taxonomic division (archaea, bacteria, fungi, human, invertebrates, mammals, plants, rodents, vertebrates, viruses, unclassified), derived from lineage and taxid."

**Tests**

- `TestSchema.test_entries_required_columns`: add the five names.
- `tests/test_roundtrip.py::TestConvenienceColumns`, one test each, following `test_go_ids_match`:
  - `test_gene_name_matches`: `lake["gene_name"] == (lake["gene_names"] or [None])[0]`.
  - `test_go_terms_match_go_ids`: `{t["id"] for t in go_terms} == set(go_ids)` and `len(go_terms) == len(go_ids)`; every `aspect in {"P","F","C", None}`.
  - `test_pubmed_ids_match`: expected = sorted distinct PubMed ids from `orig["references"][*]["citation"]["citationCrossReferences"]` with `database == "PubMed"`, sorted by `int`; assert equal to lake list.
  - `test_proteome_ids_match`: expected = sorted distinct ids of `uniProtKBCrossReferences` with `database == "Proteomes"`.
  - `test_division_values`: every value in the eleven-name set; every entry with `taxid == 9606` has `'human'`.
- `tests/test_parquet_transform.py`: assert the `entries` view in `uniprot_parquet.connect()` has exactly one `gene_name` column (`DESCRIBE entries` has no duplicate names) — put it in `tests/test_client.py` if that file exists by now (Step 16 creates it); otherwise in `TestSchema`.

**Verify** `python -m pytest tests/ -q`; then on the slice lake run the plan's three B size-gate queries and the B.5 query, and record in the Results log.

**Done when** green, README column reference updated for the five columns, and the size gates recorded.

---

## Step 7 — Part C: reorder the `entries` SELECT

**Files:** `bin/parquet_transform.py` (`_build_entries_sql`, `TABLE_META["entries"]`), `tests/test_parquet_transform.py`.

**Changes.** Reorder the SELECT list to exactly:

```
acc, id, reviewed, taxid, organism_name, gene_names, protein_name, seq_length, sequence,
gene_name,
secondary_accs, organism_common, lineage, division, gene_synonyms, alt_protein_names, protein_flag,
ec_numbers, protein_existence, annotation_score, seq_mass, seq_md5, seq_crc64,
go_ids, go_terms, xref_dbs, proteome_ids, keyword_ids, keyword_names,
first_public, last_modified, last_seq_modified, entry_version, seq_version,
feature_count, xref_count, comment_count, reference_count, pubmed_ids,
uniparc_id, entry_type, extra_attributes,
organism, protein_desc, genes, keywords, organism_hosts, gene_locations
```
Make `TABLE_META["entries"]["columns"]["convenience"]` list the same names in the same order (it is documentation, but a matching order avoids confusion). `ORDER BY` is unchanged.

**Tests.** `TestSchema.test_entries_hot_columns_first`: `entries_ds.schema.names[:9] == ["acc","id","reviewed","taxid","organism_name","gene_names","protein_name","seq_length","sequence"]`.

**Verify**
```bash
python -m pytest tests/ -q
grep -n '\.column(0)\|\[0\]' tests/*.py bin/validate_lake.py | grep -i entries    # must show nothing positional
```
and on the fixture lake:
```sql
SELECT path_in_schema, data_page_offset FROM parquet_metadata('<lake>/entries/*.parquet')
WHERE row_group_id = 0 ORDER BY data_page_offset LIMIT 9;   -- the nine hot columns, in order
```
**Done when** both hold.

---

## Step 8 — H.2: column descriptions (and source paths) in Arrow field metadata

**Files:** `bin/parquet_transform.py` (`stream_to_parquet`, new `COLUMN_SOURCES`), `tests/test_parquet_transform.py`.

**Changes**

1. Add `COLUMN_SOURCES = {("entries", "acc"): "primaryAccession", ("entries", "taxid"): "organism.taxonId", …}` beside `COLUMN_DESCRIPTIONS`, one entry per column whose source is a single JSON path (skip derived columns like `feature_count`). Fill it from the SELECT lists; it is what `SCHEMA.md` (Step 9) renders.
2. Add a helper:
   ```python
   def _annotate_schema(schema: pa.Schema, label: str, file_meta: dict[str, str]) -> pa.Schema:
       cats = TABLE_META.get(label, {}).get("columns", {})
       missing = [f.name for f in schema if (label, f.name) not in COLUMN_DESCRIPTIONS]
       if missing:
           raise RuntimeError(f"{label}: no COLUMN_DESCRIPTIONS entry for {missing}")
       fields = []
       for f in schema:
           md = {"description": COLUMN_DESCRIPTIONS[(label, f.name)]}
           if f.name in cats.get("convenience", []):
               md["category"] = "convenience"
           elif f.name in cats.get("nested", []):
               md["category"] = "nested"
           src = COLUMN_SOURCES.get((label, f.name))
           if src:
               md["source_path"] = src
           fields.append(f.with_metadata(md))
       return pa.schema(fields, metadata=file_meta)
   ```
   `file_meta` is the file-level key/value metadata (Step 9 fills it: `uniprot_release`, `schema_version`, `license`, `zstd_level`, `generator`); pass `{}` for now.
3. In `stream_to_parquet`, where `arrow_schema` is first set, replace it with the annotated schema, and re-wrap every batch so its schema carries the metadata before writing:
   ```python
   arrow_tbl = pa.Table.from_arrays(list(arrow_tbl.columns), schema=arrow_schema)
   ```
   If the pinned PyArrow rejects that call for chunked arrays, use `arrow_tbl = arrow_tbl.cast(arrow_schema)` instead. One of the two works; test on the fixture.
4. The `RuntimeError` in (2) makes a missing description a build failure. Add descriptions for every column that is missing one (run the transform on the fixture and fix until it builds).

**Tests.** `TestSchema.test_field_metadata`: for every table, `pq.read_schema(first file)`; every field has non-empty `metadata[b"description"]`.

**Verify** `python -m pytest tests/ -q`; `python -c "import pyarrow.parquet as pq; print(pq.read_schema('<lake>/entries/entries_00001.parquet').field('gene_names').metadata)"`.

**Done when** green and the metadata prints.

---

## Step 9 — H.4 schema version, H.6 licence and citation, `SCHEMA.md`

**Files:** `bin/parquet_transform.py` (`main()`, `_build_datapackage`, `_annotate_schema` call), new `bin/data/CC-BY-4.0.txt`, new `CITATION.cff`, new `bin/gen_schema_md.py`, `README.md`, `tests/test_parquet_transform.py`.

**Changes**

1. Constants at module top:
   ```python
   SCHEMA_VERSION = "1.0.0"          # public schema semver (plan H.4)
   MANIFEST_FORMAT_VERSION = 2       # structure of manifest.json (plan §4.4)
   DATA_LICENSE = "CC-BY-4.0"
   ```
   `manifest["version"] = MANIFEST_FORMAT_VERSION`, add `manifest["schema_version"] = SCHEMA_VERSION`; `_build_datapackage`: `"version": SCHEMA_VERSION`. In `stream_to_parquet`, pass `file_meta = {"uniprot_release": release or "", "schema_version": SCHEMA_VERSION, "license": DATA_LICENSE, "generator": "uniprot-parquet parquet_transform.py"}` (add a `release` argument to `stream_to_parquet`; `main()` passes `args.release`).
2. `lake/LICENSE`: commit the CC BY 4.0 legal code as `bin/data/CC-BY-4.0.txt` (download once from `https://creativecommons.org/licenses/by/4.0/legalcode.txt`). In `main()`, after the manifest is written, write `<outdir>/LICENSE` = two header lines ("This directory contains data derived from UniProtKB (https://www.uniprot.org), licensed under CC BY 4.0. Pipeline code: MIT, see the repository.") + a blank line + the legal code.
3. `CITATION.cff` at the repository root (`cff-version: 1.2.0`, `type: software`, `title: uniprot-parquet`, `license: MIT`, `preferred-citation` = the UniProt Consortium NAR paper; copy the current citation from https://www.uniprot.org/help/publications).
4. `bin/gen_schema_md.py`: reads a lake's `manifest.json` plus `COLUMN_DESCRIPTIONS`, `COLUMN_SOURCES`, `COLUMN_TYPES`, `TABLE_META` from `parquet_transform`, writes `SCHEMA.md` with one section per table: description, primary/foreign keys, sort order, partitioning (Step 14), then a table of `column | type | category | source path | description`, and a final "Versioning policy" section containing the H.4 major/minor/patch rules verbatim. Commit `SCHEMA.md` generated from the demo lake at Step 21.
5. `README.md` `### Versioning`: add the H.4 policy paragraph and "the schema version is independent of the UniProt release". `### Metadata`: add a "Citing" line pointing at `CITATION.cff`.

**Tests.** `TestManifest.test_versions_consistent`: `manifest["version"] == 2`, `manifest["schema_version"] == datapackage["version"]`, and `pq.read_schema(first entries file).metadata[b"schema_version"].decode() == manifest["schema_version"]`. `TestManifest.test_license_file`: `<lake>/LICENSE` exists and starts with the header line.

**Done when** green.

**Deferred inside this step (do not do now):** the `check_schema_evolution` semver enforcement in plan H.4 needs a committed baseline from a *published* release. There is none yet. Record in `AUDIT.md` A10 that it starts with the second release.

---

## Step 10 — A11: the reconstruction function `g()` and its gate

**Purpose.** Prove the lake is lossless *before* Step 11 changes what is stored. `g(f(x)) == x` for every fixture entry, and for a sample of every release.

**Files:** new `bin/reconstruct.py`, new `tests/test_reconstruct.py`, `bin/validate_lake.py`, `tests/test_roundtrip.py` (move two helpers).

**Changes**

1. Move `deep_sort` and `normalize_value` from `tests/test_roundtrip.py` into `bin/reconstruct.py` and import them back into the test module, so the validator and the tests normalise identically.
2. In `bin/reconstruct.py` write `reconstruct_entry(entry_row, feature_rows, xref_rows, comment_rows, publication_rows) -> dict`. Inputs are plain dicts (one `entries` row as `{column: python value}`, and lists of child rows for the same `acc`). Output is the UniProtKB JSON entry. The mapping, for the layout as it stands after Step 7:

   | JSON key | From |
   | --- | --- |
   | `entryType`, `primaryAccession`, `uniProtkbId`, `secondaryAccessions` | `entry_type`, `acc`, `id`, `secondary_accs` |
   | `organism`, `proteinDescription`, `genes`, `keywords`, `organismHosts`, `geneLocations`, `extraAttributes` | the nested columns `organism`, `protein_desc`, `genes`, `keywords`, `organism_hosts`, `gene_locations`, `extra_attributes` as they are |
   | `sequence` | `{"value": sequence, "length": seq_length, "molWeight": seq_mass, "md5": seq_md5, "crc64": seq_crc64}` |
   | `proteinExistence`, `annotationScore` | same-named columns |
   | `entryAudit` | `{"firstPublicDate": first_public, "lastAnnotationUpdateDate": last_modified, "lastSequenceUpdateDate": last_seq_modified, "entryVersion": entry_version, "sequenceVersion": seq_version}` (dates as `YYYY-MM-DD` strings) |
   | `features` | `[row["feature"] for row in feature_rows]` |
   | `uniProtKBCrossReferences` | `[{"database": r["database"], "id": r["id"], "properties": r["properties"], "isoformId": r["isoform_id"], "evidences": r["evidences"]} for r in xref_rows]` |
   | `comments` | `[json.loads(r["comment"]) for r in comment_rows]` |
   | `references` | `[row["reference"] for row in publication_rows]` |

   Drop keys whose value is `None`, and drop `None`-valued fields inside structs (that is what `normalize_value` does; apply it to the whole result). Do not try to reproduce array order: the child tables carry no position index, so **comparison is order-independent inside arrays** (`deep_sort`), exactly as the existing round-trip tests already are. This is a known limitation; it is recorded in the plan's Results log by this step, and whether to add position columns is the plan owner's call, not yours.
3. `tests/test_reconstruct.py`: for every entry in the fixture, build the five row groups from the `lake_*` fixtures in `tests/test_roundtrip.py` (reuse them: import the module or copy the fixtures), call `reconstruct_entry`, and assert `deep_sort(normalize_value(result)) == deep_sort(normalize_value(original))`. Print the first differing key path on failure (write a small `first_diff(a, b)` helper).
4. `bin/validate_lake.py`: add `check_reconstruction(report, lake_dir, jsonl_path, n)` (check 16), called after `check_text_value`. Sample `n` entries with `sample_jsonl_entries`, fetch their rows from all five tables with one DuckDB query per table (`WHERE acc IN (...)`, like `check_round_trip`), group by `acc`, run `reconstruct_entry`, compare as in (3). Report "reconstruction matches JSONL for N/N sampled entries". This is the release gate the plan's F.3 names; `VALIDATE` already runs the validator with `--spot-check-n 1000`, so it is wired automatically.

**Verify** `python -m pytest tests/ -q`.

**Done when** `tests/test_reconstruct.py` passes on the default and `--stress` fixtures and the validator's check 16 passes inside `tests/test_validate.py`.

---

## Step 11 — A13: trim the nested columns to residuals

**Purpose.** Stop storing what the convenience columns already hold. The target schema is the appendix of `benchmarks/DEDUPLICATION_PROPOSAL.md` (lines 368–694, "Appendix: Final schema after deduplication"). **Read the whole appendix and the section "Critical technical note: no positional-zip splitting" before touching code.**

Two things in the appendix are stale and must be ignored: its sort orders (`acc ASC, start_pos ASC` etc.; this plan keeps `(reviewed DESC, taxid ASC, acc ASC)`) and the column name `from_reviewed` (renamed in Step 5). Everything else in the appendix is the target.

**Files:** `bin/parquet_transform.py`, `bin/reconstruct.py`, `bin/validate_lake.py` (`check_field_completeness`), `tests/test_roundtrip.py`, `tests/test_reconstruct.py`, `tests/test_parquet_transform.py`, `README.md`.

**Changes**

1. `_build_entries_sql`: replace `e.organism AS organism` with the residual struct `organism_residual` (`struct_pack(synonyms := e.organism.synonyms, evidences := e.organism.evidences)`, guarded by `has(...)` for each field, typed fallback via `_null`), likewise `protein_desc_residual`; rename `genes` → `genes_full` and `keywords` → `keywords_full` (kept whole, per the appendix); add the promoted convenience column `keyword_categories` (`list_transform(COALESCE(e.keywords, []), x -> x.category)`) directly after `keyword_names`. `organism_hosts` and `gene_locations` are unchanged.
2. `_build_features_sql`: replace `unnest AS feature` with `feature_residual` (the appendix's field list: `location.sequence`, full `evidences`, `featureCrossReferences`, `ligandPart`) and add the promoted `location_sequence` convenience column. Keep the full `[{evidenceCode, source, id}]` list in the residual; `evidence_codes` stays as a pure projection (the no-zip rule).
3. `_build_publications_sql`: replace `unnest AS reference` with `reference_residual` per the appendix.
4. `xrefs` and `comments`: unchanged.
5. Update, for every renamed or new column: `TABLE_META` (`nested` lists become `["organism_residual", "protein_desc_residual", "genes_full", "keywords_full", "organism_hosts", "gene_locations"]`, `["feature_residual"]`, `["reference_residual"]`), `COLUMN_DESCRIPTIONS`, `COLUMN_SOURCES`, `COLUMN_TYPES` (every residual field that can be absent from an input needs a typed fallback; take the types from the stress build as in Step 4), the Step 7 column order (residuals stay last, in the same positions the full structs held).
6. `bin/validate_lake.py::check_field_completeness`: the `source_to_entries` map becomes `"organism": "organism_residual"`, `"proteinDescription": "protein_desc_residual"`, `"genes": "genes_full"`, `"keywords": "keywords_full"`.
7. `bin/reconstruct.py`: `reconstruct_entry` now rebuilds `organism` from `taxid`, `organism_name`, `organism_common`, `lineage` plus `organism_residual`; `proteinDescription` from `protein_name`, `alt_protein_names`, `protein_flag`, `ec_numbers` plus `protein_desc_residual` (follow the appendix field by field; where the appendix says a field is in the residual, read it from there, never from a convenience list); `features[i]` from the convenience columns plus `feature_residual`; `references[i]` likewise. `genes`, `keywords` come from `genes_full`, `keywords_full` directly.
8. `tests/test_roundtrip.py::TestNestedStructs`: delete the four tests that compare `organism`, `protein_desc`, `genes`, `keywords` directly; `tests/test_reconstruct.py` is now the lossless proof. `TestSchema` required sets: replace `feature` with `feature_residual`, `reference` with `reference_residual`; `TestDataIntegrity.test_features_have_feature_struct` and `test_publications_have_reference_struct` check the residual columns exist (they may be all-NULL for simple entries; do not assert non-null).
9. README column reference: the "Full nested" lines become "Residual" lines with the new names and one sentence: "Residual structs hold only what the convenience columns do not; `bin/reconstruct.py` rebuilds the original JSON from both."

**Verify**
```bash
python -m pytest tests/ -q && python -m pytest tests/ -q --stress
```
Then rebuild the slice (`slice/lake`) and record compressed bytes per table before (Step 3 numbers) and after in the plan's F.1 table "after v2" column and the Results log.

**Done when** both suites are green, `check_reconstruction` passes on the slice, and the byte delta is recorded.

---

## Step 12 — Part A Phase 1: bloom filters on `acc` (only if Step 1.1 passed)

**Files:** `bin/parquet_transform.py`, `bin/validate_lake.py`, `tests/test_parquet_transform.py`, `tests/test_validate.py`, `README.md`.

**Changes**

1. Constants (plan §4.2), plus a helper that turns them into the PyArrow keyword(s) Step 1.1 found. Write the helper with the real parameter name; the placeholder below is only the shape:
   ```python
   BLOOM_FILTER_COLUMNS = {"entries": ["acc"], "features": ["acc"], "xrefs": ["acc"],
                           "comments": ["acc"], "publications": ["acc"], "accession_map": ["acc"]}
   BLOOM_FILTER_FPP = {"entries": 0.001, "features": 0.0001, "xrefs": 0.0001,
                       "comments": 0.0001, "publications": 0.0001, "accession_map": 0.001}
   BLOOM_FILTERS_ENABLED = True   # set from --no-bloom-filters in main()

   def _bloom_kwargs(label: str) -> dict:
       cols = BLOOM_FILTER_COLUMNS.get(label) if BLOOM_FILTERS_ENABLED else None
       if not cols:
           return {}
       return {"<parameter name from Step 1.1>": <value shape from Step 1.1, built from cols and BLOOM_FILTER_FPP[label]>}
   ```
2. `stream_to_parquet`: `writer_kwargs.update(_bloom_kwargs(label))`.
3. `main()`: `parser.add_argument("--no-bloom-filters", action="store_true")`; set the module flag. Manifest per table: `"bloom_filter_columns": cols or []`, `"bloom_filter_fpp": BLOOM_FILTER_FPP.get(name) if cols else None` (both branches of the table loop, including the `--skip-existing` branch, which reads them back from `parquet_metadata` presence: if the first file's first row group has `bloom_filter_offset` for `acc`, report the constants; else empty).
4. `_build_datapackage`: copy the two keys onto each resource.
5. `bin/validate_lake.py`: `check_bloom_filters(report, lake_dir)` (check 17) after `check_parquet_integrity`: for each manifest table with non-empty `bloom_filter_columns`, DuckDB `SELECT count(*) FILTER (WHERE bloom_filter_offset IS NULL OR bloom_filter_length = 0) FROM parquet_metadata('<table>/*.parquet') WHERE path_in_schema = 'acc'` must be 0; plus the 20-accession functional check from the plan (`count(*) = 1` per accession on `entries`).

**Tests** (plan §4.6): `TestBloomFilters` with the three tests; `tests/test_validate.py::test_validate_fails_without_bloom_filters` rewrites one `entries` file with `pq.write_table(pq.read_table(f), f, compression="zstd")` in a copy of the lake and asserts the validator exits 1 with `bloom` in the report.

**Verify** `python -m pytest tests/ -q`.

**Done when** green, and the manifest of the fixture lake shows the two keys.

---

## Step 13 — Part A Phase 2: the `accession_map` table

**Files:** `bin/parquet_transform.py`, `bin/validate_lake.py`, `uniprot_parquet.py`, `setup_views.sql`, `tests/test_parquet_transform.py`, `tests/test_roundtrip.py`, `tests/test_idempotency.py`, `README.md`.

**Changes**

1. Builder, next to the others (uses `REVIEWED_EXPR` from Step 6; note the doubled braces rule does not apply here because there are no literal braces):
   ```python
   def _build_accession_map_sql(schema_paths: set[str]) -> str:
       has_secondary = "secondaryAccessions" in schema_paths
       primary = f"""
       SELECT e.primaryAccession AS acc, e.primaryAccession AS primary_acc, true AS is_primary,
              {REVIEWED_EXPR} AS reviewed, e.organism.taxonId AS taxid
       FROM {{read_clause}} e"""
       secondary = f"""
       UNION ALL
       SELECT s AS acc, e.primaryAccession AS primary_acc, false AS is_primary,
              {REVIEWED_EXPR} AS reviewed, e.organism.taxonId AS taxid
       FROM {{read_clause}} e, LATERAL unnest(COALESCE(e.secondaryAccessions, [])) AS t(s)""" if has_secondary else ""
       return f"""
   SELECT acc, primary_acc, is_primary, reviewed, taxid FROM ({primary}{secondary})
   ORDER BY reviewed DESC, acc, primary_acc
   """
   ```
2. `TABLE_DEFS`: append `("accession_map", None, ["reviewed DESC", "acc ASC", "primary_acc ASC"])`. `_SQL_BUILDERS` in `main()`: add `"accession_map": _build_accession_map_sql`. The `_VARIANT_BUILDERS` branch: `accession_map` is not a child table; make the `if getattr(args, "variant_children") and name != "entries"` condition `name not in ("entries", "accession_map")`.
3. `TABLE_META["accession_map"]` = description "One row per primary or secondary accession, mapping to the current primary accession. The lookup table for point queries and for resolving retired accessions.", `primary_key: ["acc", "primary_acc"]`, `foreign_keys: {"primary_acc": "entries.acc"}`, `convenience` = the five columns, `nested: []`. Five `COLUMN_DESCRIPTIONS` and `COLUMN_SOURCES` entries.
4. `bin/validate_lake.py`: `check_accession_map(report, lake_dir)` (check 18) with the six assertions of plan §5.5. For assertion 5 (sort), refactor the inner loop of `check_sort_order` into `_check_sorted(dataset, columns, key_fn)` where `key_fn(row_values) -> tuple` and call it for the five three-key tables with `lambda r, t, a: (not r, t, a)` and for `accession_map` with `columns=["reviewed", "acc", "primary_acc"]`, `key_fn=lambda r, a, p: (not r, a, p)`. Add `"accession_map"` to every hard-coded table list in the validator (`check_parquet_integrity`, `check_manifest`, `check_schema_evolution`, `check_null_keys` with `["acc", "primary_acc", "is_primary", "reviewed", "taxid"]`).
5. Views: `CREATE OR REPLACE VIEW accession_map AS SELECT * FROM read_parquet('{BASE}/accession_map/*.parquet');` in `_SETUP_SQL` and `setup_views.sql` (Step 14 changes the glob).
6. README: Tables table row; column reference block; new "Looking up by accession" subsection with the two forms from plan §5.1 and the plan's remote caveat (§2 "Known limitation").
7. `upjson2lake.nf` `PARQUET_TRANSFORM` comment: note the ~tens of GB sort spill for this table.

**Tests**

- `tests/test_parquet_transform.py`: `EXPECTED_TABLES.append("accession_map")`; a `accession_map_ds` fixture; `TestRowCounts.test_accession_map_primary_count` (`is_primary` rows == entries rows) and `test_accession_map_secondary_count` (== sum of `len(secondary_accs)`); `TestSortOrder.test_accession_map_sorted`; `TestDataPackage.test_accession_map_fk` (a foreign key `primary_acc → entries.acc` is present).
- `tests/test_roundtrip.py`: `TestAccessionMap.test_secondaries_resolve`: for each original entry and each of its `secondaryAccessions`, the map has a row `(acc=secondary, primary_acc=orig acc, is_primary=False)`.
- `tests/test_idempotency.py`: add `"accession_map"` to `TABLE_NAMES`.

**Verify** `python -m pytest tests/ -q`.

**Done when** green and `--skip-existing` skips the new table (the idempotency test proves it).

---

## Step 14 — Part D: Hive partitioning by `review_status`

This is the riskiest step. Do it in the sub-order below, running the suite after 14.1, 14.3 and 14.6.

**Files:** `bin/parquet_transform.py` (`stream_to_parquet`, `TABLE_DEFS`, `main()`), `bin/validate_lake.py`, `bin/release_manifest.py`, `uniprot_parquet.py`, `setup_views.sql`, `tests/test_parquet_transform.py`, `tests/test_idempotency.py`, `README.md`.

### 14.1 Writer

Add constants and a fourth `TABLE_DEFS` field. Two names the writer below uses may not exist yet: define `ZSTD_LEVEL = 1` now (Step 18 makes it measured and adds the flag), and if Step 12 was skipped define `def _bloom_kwargs(label): return {}` so the writer stays identical either way.
```python
ZSTD_LEVEL = 1
PARTITION_KEY = "review_status"
PARTITION_VALUES = {True: "swissprot", False: "trembl"}
TABLE_DEFS = [
    ("entries",       None, ["reviewed DESC", "taxid ASC", "acc ASC"],         "reviewed"),
    ("features",      None, ["reviewed DESC", "taxid ASC", "acc ASC"],         "reviewed"),
    ("xrefs",         None, ["reviewed DESC", "taxid ASC", "acc ASC"],         "reviewed"),
    ("comments",      None, ["reviewed DESC", "taxid ASC", "acc ASC"],         "reviewed"),
    ("publications",  None, ["reviewed DESC", "taxid ASC", "acc ASC"],         "reviewed"),
    ("accession_map", None, ["reviewed DESC", "acc ASC", "primary_acc ASC"],   "reviewed"),
]
```
Update every unpacking of `TABLE_DEFS` in `main()` (`grep -n "in TABLE_DEFS"`) to four names.

Replace the body of `stream_to_parquet` with the following structure (keep the docstring, update it). The invariants: batches arrive sorted `reviewed DESC`, so all `true` rows precede all `false` rows and a batch contains at most one flip; nothing is visible under the final directories until the whole table has been written (the current all-or-nothing guarantee, which `--skip-existing` relies on).

```python
def stream_to_parquet(con, sql, table_dir, batch_size, label="table", sort_order=None,
                      partition_column="reviewed", release=None):
    reader = con.sql(sql).to_arrow_reader(batch_size=batch_size)
    os.makedirs(table_dir, exist_ok=True)
    t0 = time.time()

    total_rows = 0
    arrow_schema = None
    sorting_columns = None
    pending = []                       # (tmp_path, final_path, relative_path)
    st = {"side": None, "writer": None, "file_num": 0, "path": None}

    def part_dir(side):
        return os.path.join(table_dir, f"{PARTITION_KEY}={side}")

    def open_writer():
        st["file_num"] += 1
        tmp = os.path.join(part_dir(st["side"]), ".tmp")
        os.makedirs(tmp, exist_ok=True)
        st["path"] = os.path.join(tmp, f"{label}_{st['file_num']:05d}.parquet")
        kwargs = {"compression": "zstd", "compression_level": ZSTD_LEVEL,
                  "write_page_index": True}
        if sorting_columns:
            kwargs["sorting_columns"] = sorting_columns
        kwargs.update(_bloom_kwargs(label))                                  # Step 12; returns {} if skipped
        st["writer"] = pq.ParquetWriter(st["path"], arrow_schema, **kwargs)

    def close_writer():
        if st["writer"] is None:
            return
        st["writer"].close()
        st["writer"] = None
        final = os.path.join(part_dir(st["side"]), os.path.basename(st["path"]))
        pending.append((st["path"], final, os.path.relpath(final, table_dir)))

    def write_part(tbl):
        if st["writer"] is None:
            open_writer()
        st["writer"].write_table(tbl, row_group_size=100_000)
        if os.path.getsize(st["path"]) >= TARGET_FILE_BYTES:
            close_writer()

    try:
        for record_batch in reader:
            tbl = pa.Table.from_batches([record_batch])
            if tbl.num_rows == 0:
                continue
            if arrow_schema is None:
                file_meta = {"uniprot_release": release or "", "schema_version": SCHEMA_VERSION,
                             "license": DATA_LICENSE, "generator": "uniprot-parquet parquet_transform.py"}
                arrow_schema = _annotate_schema(tbl.schema, label, file_meta)
                sorting_columns = build_sorting_columns(sort_order, arrow_schema) if sort_order else None
            tbl = pa.Table.from_arrays(list(tbl.columns), schema=arrow_schema)
            total_rows += tbl.num_rows

            flags = tbl.column(partition_column)
            if flags.null_count:
                raise RuntimeError(f"{label}: {partition_column} has NULLs; cannot partition")
            n_true = pc.sum(flags).as_py() or 0
            first = flags[0].as_py()
            if first and n_true < tbl.num_rows:            # the flip is inside this batch
                parts = [(True, tbl.slice(0, n_true)), (False, tbl.slice(n_true))]
            else:
                if not first and n_true:
                    raise RuntimeError(f"{label}: batch not sorted {partition_column} DESC")
                parts = [(first, tbl)]

            for flag, part in parts:
                side = PARTITION_VALUES[flag]
                if st["side"] != side:
                    if st["side"] is not None and flag:           # true after false: input is not sorted
                        raise RuntimeError(f"{label}: {partition_column} flipped back to true")
                    close_writer()
                    st["side"], st["file_num"] = side, 0
                write_part(part)
            eprint(f"    {label}: {total_rows:,} rows so far ({time.time()-t0:.0f}s)")

        close_writer()
        for tmp_path, final_path, _ in pending:            # all-or-nothing publish
            shutil.move(tmp_path, final_path)
        for side in PARTITION_VALUES.values():
            tmp = os.path.join(part_dir(side), ".tmp")
            if os.path.isdir(tmp) and not os.listdir(tmp):
                os.rmdir(tmp)
    except Exception:
        if st["writer"] is not None:
            st["writer"].close()
        raise

    files = [rel for _, _, rel in pending]
    eprint(f"  {label}: {total_rows:,} rows in {len(files)} files ({time.time()-t0:.1f}s)")
    return total_rows, files, arrow_schema
```
`import pyarrow.compute as pc` at the top of the module. `main()` passes `partition_column` and `release=args.release`.

`--skip-existing` in `main()`: both the detection loop and the skipped-table branch must list files recursively and record them relative to the table directory:
```python
from glob import glob
def _list_table_files(table_dir):
    return sorted(os.path.relpath(p, table_dir)
                  for p in glob(os.path.join(table_dir, f"{PARTITION_KEY}=*", "*.parquet")))
```
The summary loop at the end of `main()` already joins `table_dir` with each file name, so relative paths work unchanged.

### 14.2 Manifest

Per table, in both branches of the table loop, add:
```python
"partitioning": {
    "scheme": "hive",
    "keys": [{"name": PARTITION_KEY, "type": "string",
              "values": sorted({f.split("/")[0].split("=")[1] for f in files}),
              "derived_from": partition_column,
              "row_counts": _partition_row_counts(table_dir, files)}],
    "note": "accession_map is excluded from any future second partition level (plan D.5)" if name == "accession_map" else "",
},
```
with `_partition_row_counts` summing `pq.read_metadata(path).num_rows` per partition value. `_build_datapackage`: resource `path` entries already prefix the table name; they now read `entries/review_status=swissprot/entries_00001.parquet`; copy the `partitioning` block onto the resource as `"partitioning"`.

### 14.3 Validator and tests that touch paths

- `bin/validate_lake.py`: `open_table` becomes `ds.dataset(sorted(glob(os.path.join(table_dir, "**", "*.parquet"), recursive=True)), format="parquet")` (explicit sorted list, so `review_status=swissprot` files precede `trembl` files and the global sort check still holds). Every DuckDB path the validator builds (`grep -n '"\*.parquet"' bin/validate_lake.py`, including the ones added in Steps 2, 4, 12 and 13) becomes `os.path.join(lake_dir, X, "**", "*.parquet")` (DuckDB accepts `**`). `check_parquet_integrity` and `check_manifest` list files with the recursive glob and compare **relative** paths with the manifest.
- New `check_partitions(report, lake_dir)` (check 19): for each manifest table and each partition value present on disk, from `parquet_metadata('<table>/review_status=<side>/*.parquet')` restricted to `path_in_schema = '<derived_from>'`: every row group's `stats_min` and `stats_max` equal the expected value (`true` for `swissprot`, `false` for `trembl`; DuckDB returns statistics as VARCHAR, compare lower-cased and verify once what a boolean minimum prints as), and `sum(row_group_num_rows)` equals the manifest's `row_counts[side]`; the two sides sum to `row_count`; and in the manifest `files` list every `swissprot` path precedes every `trembl` path.
- `tests/test_parquet_transform.py`: `test_manifest_files_match_disk` uses the recursive relative listing; new `TestPartitions` with the assertions in plan D.3 (directory set, path regex `^review_status=(swissprot|trembl)/<table>_\d{5}\.parquet$`, DuckDB `read_parquet('entries/*/*.parquet')` has a `review_status` column whose counts match, `hive_partitioning=false` has no such column, `pl.scan_parquet('<lake>/entries/')` and `pq.read_table('<lake>/entries/')` return the fixture row count, `read_parquet('entries/review_status=swissprot/*.parquet')` count equals the fixture's Swiss-Prot count). Add `test_mid_batch_flip`: transform `tests/fixtures/small.json.gz` with `--batch-size 7` into a temp dir and assert both partitions exist and row counts sum correctly.
- `tests/test_idempotency.py`: the `os.listdir(table_dir)` in `test_skip_existing_preserves_tables` becomes the recursive glob.

Run the suite now.

### 14.4 The D.3 spike (views)

On the fixture lake just built, in DuckDB:
```python
import duckdb
con = duckdb.connect()
base = "<lake>"
q1 = f"""EXPLAIN ANALYZE SELECT count(*) FROM (
  SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
  FROM read_parquet('{base}/entries/*/*.parquet', hive_partitioning = true)) WHERE reviewed = true"""
q2 = f"""EXPLAIN ANALYZE SELECT count(*) FROM read_parquet('{base}/entries/*/*.parquet', hive_partitioning = true)
  WHERE review_status = 'swissprot'"""
q3 = f"""EXPLAIN ANALYZE SELECT count(*) FROM read_parquet('{base}/entries/*/*.parquet', hive_partitioning = false)
  WHERE reviewed = true"""
for q in (q1, q2, q3):
    print(con.sql(q).fetchall()[0][1])
```
Read the Parquet scan node of each plan. `q2` is the reference: it must show that only the `swissprot` file was read. If `q1` shows the same file count as `q2`, **option 1** works: the views use the `EXCLUDE … REPLACE` form. If `q1` reads as many files as `q3`, use **option 2**: views keep `review_status` visible with `hive_partitioning = true`. Record the outcome in the Results log.

### 14.5 Views, client, docs

- `uniprot_parquet.py::_SETUP_SQL` and `setup_views.sql`: every base view becomes `read_parquet('{BASE}/<table>/*/*.parquet', hive_partitioning = true)` wrapped in the option chosen in 14.4 (option 1: `SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed) FROM …`; option 2: `SELECT * FROM …`). The `comments` view keeps its `REPLACE (comment::JSON AS comment)`; combine the two `REPLACE` clauses in one list. Add the D.3 comment block at the top of both files.
- `bin/release_manifest.py`: the size walk uses `os.walk` (or Step 15's `size_bytes`).
- `README.md`: the D.3 paragraph after "All tables are sorted…"; the "Direct access" examples use `lake/entries/**/*.parquet` for DuckDB, `lake/entries/` for Polars, pandas, R and Spark; a Swiss-Prot-only example (`lake/entries/review_status=swissprot/*.parquet`).

### 14.6 Verify
```bash
python -m pytest tests/ -q && python -m pytest tests/ -q --stress
find <fixture lake> -name '*.parquet' | sed 's#/[^/]*$##' | sort -u    # exactly <table>/review_status=<side> lines
```
**Done when** both suites are green, the `find` shows only partition directories, and the D.3 Results-log row is filled.

---

## Step 15 — F.2.1: sizes, hashes, taxid ranges, `SHA256SUMS.txt`, `RELEASE.metalink`, `validation_report.json`

**Files:** `bin/parquet_transform.py` (`main()`, `_build_datapackage`), `bin/validate_lake.py` (`ValidationReport`, `check_manifest`, `main()`), `bin/release_manifest.py`, `upjson2lake.nf` (`VALIDATE`), `tests/test_parquet_transform.py`.

**Changes**

1. Helper in `bin/parquet_transform.py`:
   ```python
   import hashlib
   def _file_details(table_dir, rel_path):
       path = os.path.join(table_dir, rel_path)
       h = hashlib.sha256()
       with open(path, "rb") as f:
           for chunk in iter(lambda: f.read(1 << 20), b""):
               h.update(chunk)
       meta = pq.read_metadata(path)
       col = meta.schema.names.index("taxid")          # column index in the Parquet schema
       mins, maxs = [], []
       for i in range(meta.num_row_groups):
           stats = meta.row_group(i).column(col).statistics
           if stats and stats.has_min_max:
               mins.append(stats.min); maxs.append(stats.max)
       return {"size_bytes": os.path.getsize(path), "sha256": h.hexdigest(),
               "taxid_min": min(mins) if mins else None, "taxid_max": max(maxs) if maxs else None}
   ```
   `meta.schema.names` for nested columns lists leaf paths; `taxid` is a top-level scalar so its index is exact. Call it for every file immediately after `stream_to_parquet` returns (and in the `--skip-existing` branch), storing `manifest_tables[name]["file_details"] = {rel: details}` and `["size_bytes"] = sum(...)`.
2. After the manifest is written, write `<outdir>/SHA256SUMS.txt`: one line `f"{sha256}  {table}/{rel}"` per Parquet file in manifest order, then `manifest.json`, `datapackage.json`, `LICENSE` (hash them after they are written; `SHA256SUMS.txt` is not self-listed). Two spaces between hash and path (the `sha256sum -c` format).
3. Write `<outdir>/RELEASE.metalink` (Metalink 4, RFC 5854) and one `RELEASE.metalink` inside each table directory listing that table's files with relative URLs:
   ```xml
   <?xml version="1.0" encoding="UTF-8"?>
   <metalink xmlns="urn:ietf:params:xml:ns:metalink">
     <published>2026-…Z</published>
     <file name="entries/review_status=swissprot/entries_00001.parquet">
       <size>123456</size>
       <hash type="sha-256">…</hash>
       <hash type="md5">…</hash>
       <url>entries/review_status=swissprot/entries_00001.parquet</url>
     </file>
   </metalink>
   ```
   Compute MD5 in the same streaming pass as SHA-256 (add a second hasher to `_file_details`; store `md5` in `file_details` too). Build the XML with `xml.etree.ElementTree`.
4. `_build_datapackage`: add `"bytes": table_info["size_bytes"]` to each resource and append "Per-file SHA-256 hashes are in SHA256SUMS.txt and manifest.json file_details." to the package description.
5. `bin/validate_lake.py`: `check_manifest` also recomputes SHA-256 for every file in `file_details` and compares (one check line per table: "N/N file hashes match"); on the full build this reads the whole lake once, which is the point. `ValidationReport` gains `to_dict()` (`{"passed": bool, "checks": [{"name", "passed", "detail"}], "summary": str, "elapsed_s": float}`); `main()` writes `<output stem>.json` next to the text report (`validation_report.json` for the default `-o`). `upjson2lake.nf` `VALIDATE` adds `path "validation_report.json"` to its outputs.
6. `bin/release_manifest.py`: per-table `total_size_bytes` comes from `manifest["tables"][t]["size_bytes"]`; keep the `os.walk` sum as a cross-check that logs a warning on mismatch. Include `accession_map` in its table list (make the list `manifest["tables"].keys()`). Add the SHA-256 of `--input-jsonl` next to the existing MD5 (plan F.7: `sorted.jsonl.zst` is published, so its hash belongs in `provenance.json`).

**Tests** (plan F.2.1): `TestManifest.test_file_details_match_disk`, `test_sha256sums_verifies` (`subprocess.run(["sha256sum", "-c", "--quiet", "SHA256SUMS.txt"], cwd=lake_dir)` returns 0; skip on platforms without `sha256sum`), `test_metalink_parses` (`ElementTree.parse` succeeds and the file count equals the manifest's), and in `tests/test_validate.py` assert `validation_report.json` exists beside the text report and has `"passed": true`.

**Verify** `python -m pytest tests/ -q`.

**Done when** green.

---

## Step 16 — F.2.2: the client works on a partial lake, and `files_for_taxid`

**Files:** `uniprot_parquet.py`, `setup_views.sql`, new `tests/test_client.py`.

**Changes**

1. Split `_SETUP_SQL` into `_VIEW_SQL_TEMPLATE` (one view) and `_MACRO_SQL` (all macros, unchanged). Add:
   ```python
   def _read_manifest(base: str) -> dict | None:
       """manifest.json from a local dir, http(s) URL or s3 URI; None if unreachable."""
       try:
           if base.startswith(("http://", "https://")):
               import urllib.request
               with urllib.request.urlopen(f"{base}/manifest.json", timeout=30) as r:
                   return json.loads(r.read())
           if base.startswith("s3://"):
               con = duckdb.connect(); con.sql("INSTALL httpfs; LOAD httpfs;")
               return json.loads(con.sql(f"SELECT content FROM read_text('{base}/manifest.json')").fetchone()[0])
           with open(os.path.join(base, "manifest.json")) as f:
               return json.load(f)
       except Exception:
           return None
   ```
2. `connect()`: after the httpfs setup, `m = _read_manifest(base)`. If `m` is `None`, fall back to the glob views exactly as today (one `CREATE VIEW` per table over `{BASE}/<table>/*/*.parquet`) and continue. Otherwise, for each table in `m["tables"]`: build `files = [f"{base}/{table}/{rel}" for rel in info["files"]]`; test presence with `os.path.exists(files[0])` locally or, for URLs, by attempting the `CREATE VIEW` and catching `duckdb.IOException` / `duckdb.HTTPException`; on success create the view over the explicit list (`read_parquet([...], hive_partitioning = true)` inside the option-1 or option-2 wrapper chosen in Step 14.4; `comments` keeps its `comment::JSON` replace); on absence create the stub:
   ```sql
   CREATE OR REPLACE VIEW {table} AS
     SELECT error('table "{table}" is not in this lake copy; download lake/{table}/ (see README "Download")') AS _
   ```
   Then run `_MACRO_SQL` unchanged (macros bind lazily).
3. `manifest(lake_path)` uses `_read_manifest` and raises the same errors as today when it returns `None`. `tables(lake_path)` returns `{name: {"row_count": n, "present": bool}}` (update its docstring and the README line that shows its output). Add:
   ```python
   def files_for_taxid(lake_path: str, taxid: int, table: str = "entries") -> list[str]:
       """Relative paths of the files that can contain rows for one organism."""
       m = manifest(lake_path)
       fd = m["tables"][table]["file_details"]
       return [f"{table}/{rel}" for rel, d in fd.items()
               if d.get("taxid_min") is not None and d["taxid_min"] <= taxid <= d["taxid_max"]]
   ```
4. `setup_views.sql`: the comment block "requires all six tables; for an `entries`-only copy use `uniprot_parquet.connect()` or create only the views you have".

**Tests** — create `tests/test_client.py` (this is also `AUDIT.md` A15):
- `test_connect_full_lake`: `connect(lake)`; every table name in `manifest["tables"]` yields `SELECT count(*)` equal to the manifest `row_count`; every macro runs (`protein_card(<fixture acc>)`, `organism_features(<taxid>, 'Chain')`, `organism_xrefs(<taxid>, ['GO'])`, `organism_comments(<taxid>, 'FUNCTION')`, `entries_with_features(<taxid>)`, `entries_with_xrefs(<taxid>, ['GO'])`, `unnest_isoforms(<acc with isoforms, or assert zero rows>)`) without raising.
- `test_connect_entries_only`: copy `entries/`, `accession_map/`, `manifest.json`, `datapackage.json` to a temp dir; `connect()`; `SELECT count(*) FROM entries` matches; `SELECT * FROM protein_card('<acc>')` returns one row; `SELECT count(*) FROM features` raises with `not in this lake copy` in the message; `tables()` reports `features` as `present: False`.
- `test_connect_without_manifest`: delete `manifest.json` from a copy; `connect()` still works via the glob fallback.
- `test_files_for_taxid`: for a fixture taxid, the returned files are non-empty and each named file's `taxid_min <= taxid <= taxid_max`.
- `test_entries_view_has_single_gene_name`: `DESCRIBE entries` lists `gene_name` exactly once.
- `test_split_sql_respects_quotes`: the existing helper with a `;` inside a string literal.

**Verify** `python -m pytest tests/ -q`.

**Done when** green.

---

## Step 17 — README

**Files:** `README.md`.

**Changes** (all of these are already specified in the plan; this is the checklist of where they go)

1. New `## Download` section before `## Using the lake` (plan F.2.4): the four-row table with the F.1 numbers, the three `rsync` commands, the one-sentence client note, the remote pointer with the request-count caveat, and one sentence on `RELEASE_COMPLETE` (Step 19): "a release directory is complete when `RELEASE_COMPLETE` exists; mirrors should check it first."
2. `## Tables`: add `accession_map`; the D.3 partition paragraph after "All tables are sorted…"; `reviewed` everywhere (Step 5).
3. `### Direct access`: `**/*.parquet` and directory examples (Step 14.5), a Swiss-Prot-only example, a "Looking up by accession" subsection (Step 13) showing the two-step form first.
4. `### Metadata`: `size_bytes` and `file_details`, `SHA256SUMS.txt`, `RELEASE.metalink`, `validation_report.json`, `LICENSE`, "Citing" line.
5. `### Schema design` and the column reference: new columns (Step 6), residual columns (Step 11), the Step 7 order, `evidence_type`.
6. `### Versioning`: H.4 policy; leave the F.5 retention sentence as "to be decided with the UniProt FTP team" until it is.
7. `### Validation`: replace the hard-coded "12 checks" with "the checks below" and list all checks including 15–19.
8. `### Production notes`: one line on the `accession_map` sort spill and on the zstd level's write-time cost (Step 18 numbers).

**Verify** every code example in the README runs against the fixture lake (put the ones that are cheap into `tests/test_client.py` as `test_readme_examples`, with `{BASE}` substituted).

**Done when** no README example is stale.

---

## Step 18 — F.4 file-size target, H.1 zstd level, page index

**Files:** `bin/parquet_transform.py` (constants, `main()` flags), new `benchmarks/bench_file_size.py`, new `benchmarks/bench_zstd.py`.

**Changes**

1. Constants and flags: `TARGET_FILE_BYTES = 256 * 1024 * 1024` moves out of `stream_to_parquet` to module level beside the `ZSTD_LEVEL = 1` from Step 14; `--target-file-bytes` and `--zstd-level` CLI flags override them (set the module globals in `main()` before the table loop, or pass them into `stream_to_parquet`). `write_page_index=True` is already in the Step 14 writer. Record in the manifest: `"compression": {"codec": "zstd", "level": ZSTD_LEVEL}`, `"target_file_bytes": TARGET_FILE_BYTES`; and `zstd_level` in the footer key/value metadata (`file_meta` in Step 14).
2. `benchmarks/bench_file_size.py --jsonl slice.sorted.jsonl.zst --out benchmarks/results/`: for each target in (256, 512, 1024) MB, build `entries` only into a temp dir (add `--only entries` to the transform, a small addition to `main()` that filters `TABLE_DEFS`), serve the directory with `python -m http.server <port>` in a subprocess, run in DuckDB with httpfs the query `SELECT acc, id, reviewed, taxid, organism_name, gene_names, protein_name, seq_length FROM read_parquet('http://localhost:<port>/entries/*/*.parquet') WHERE taxid = 9606` under `EXPLAIN ANALYZE`, and record: files opened, footer bytes (`sum(pq.read_metadata(f).serialized_size)` for the opened files), data bytes and wall time; also the footer-size delta with and without `write_page_index`. Write JSON and print the plan's F.4 table.
3. `benchmarks/bench_zstd.py`: for levels 1, 3, 9, 15 build `entries` and `xrefs` from the slice with `--zstd-level`, record compressed bytes and write wall time, and re-run the F.4 query locally for read time. Print the plan's H.1 table.
4. Apply the decision rules in plan F.4 and H.1; set the two constants; record both tables and the choices in the plan and the Results log.

**Verify** `python -m pytest tests/ -q` after changing the constants (the fixture is unaffected by either).

**Done when** the constants are set from measurements, and the manifest of a fresh fixture lake shows `compression.level` and `target_file_bytes`.

---

## Step 19 — H.5: `RELEASE_COMPLETE`

**Files:** `bin/release_manifest.py`, `upjson2lake.nf` (`PROVENANCE`), `tests/test_validate.py`.

**Changes**

1. `bin/release_manifest.py`: after `provenance.json` is written, write `RELEASE_COMPLETE` in the same directory as its **last** action:
   ```
   release: 2026_03
   schema_version: 1.0.0
   sha256sums_sha256: <sha256 of lake/SHA256SUMS.txt>
   completed_at: 2026-…Z
   ```
   Flag `--complete-marker <path>` (default: `RELEASE_COMPLETE` next to `-o`).
2. `upjson2lake.nf` `PROVENANCE`: add `path "RELEASE_COMPLETE", emit: complete` to the outputs; `publishDir` copies it. Because `PROVENANCE` runs only after `VALIDATE` passes (the `validation_ok` gate), the marker cannot appear for a failed release.
3. README "Download": the one-sentence rule (Step 17.1).

**Tests.** In `tests/test_validate.py`, run `bin/release_manifest.py` against the fixture lake into a temp dir and assert the marker exists, is written after `provenance.json` (compare `os.path.getmtime`, or check the file order in the script), and its `sha256sums_sha256` equals a fresh hash of `SHA256SUMS.txt`.

**Done when** green.

---

## Step 20 — Part A Phase 3: the point-lookup benchmark

**Files:** new `benchmarks/bench_point_lookup.py`.

**Changes.** Mirror `benchmarks/bench_baseline.py` (`_time_query`, JSON + text output). Arguments: `--lake` (local), `--url` (optional httpfs base), `--accessions` (file with one accession per line; default: 1,000 sampled from `accession_map`), `--label`. Workloads: 1, 37 and 1,000 accessions, each against `entries` (the nine hot columns), `features` and `xrefs`, each in the single-step form (`WHERE acc IN (...)`) and the two-step form (`JOIN accession_map USING (acc)` then `WHERE reviewed = … AND taxid = … AND acc = …`, generated per accession from the map), local and, when `--url` is given, remote. For each: wall time (5 runs), row groups scanned vs total (parse the `EXPLAIN ANALYZE` text of the Parquet scan node; if the installed DuckDB does not print row-group counts, fall back to `parquet_metadata` statistics to count candidate row groups and say so in the output), and for remote runs the request count and bytes (DuckDB prints HTTP statistics in `EXPLAIN ANALYZE` when `SET enable_profiling = 'json'`; if not, wrap the query in a counting proxy: `python -m http.server` with a logging handler and count log lines). Also the C.3 workload: the seven default columns for `taxid = 9606`, request count before/after the reorder (run it on a pre-Step-7 build if one exists; otherwise record "after" only and say so).

Print the plan's Phase 3 pass criteria with PASS/FAIL per criterion. Run it on the slice (never the fixture) with and without `--no-bloom-filters`, save both JSON files under `benchmarks/results/`, and copy the table into the plan's Results log. Decide B1 on child tables from the child-table single-step numbers (plan §2.1) and record it.

**Done when** the Results-log Phase 3 and C.3 rows are filled.

---

## Step 21 — Finish: demo, `SCHEMA.md`, audit, full-build gates

1. Regenerate the demo: `cd demo && ./run_demo.sh --clean` (needs network and Nextflow). Commit `demo/lake/2026_01/` (the manifest should show no `int32` artefacts, partition paths, `file_details`, `schema_version`).
2. `python bin/gen_schema_md.py --lake demo/lake/2026_01/lake -o SCHEMA.md`; commit `SCHEMA.md`.
3. `AUDIT.md` updates listed in the plan's implementation-order item 8, plus: A10 (semver enforcement starts with the second release), A12 (done, numbers in §6.1), A15 (client tests exist), O4 (closed by D.5), the F.3 rule beside A13.
4. Plan bookkeeping: every Results-log row filled; the acceptance checklists in Part A §8 and "Checklist additions" ticked; the D.5 `Result:` line filled once the full build exists.
5. First full build gates (run on the cluster, `./run_lake.sh full …`): the validator passes (all 19 checks); the D.5 division counts match the FTP `taxonomic_divisions/` counts for the same release (fix the CASE in Step 6 until they do, then rebuild); the Phase 3 benchmark passes its criteria on the full build as well as the slice.
6. Final suites: `python -m pytest tests/ -q && python -m pytest tests/ -q --stress`.

**Done when** all six hold. The launch set in the plan is then implemented; the deferred items (Croissant, `releases.json`, Hugging Face, H.3 enums, child-table bloom filters if Phase 3 dropped them, Phase 4 macros) are minor bumps under the H.4 policy and are listed in the release notes as planned.

---

## Appendix — validator check numbering after this work order

| # | Check | Added by |
| --- | --- | --- |
| 1–14 | existing (see `bin/validate_lake.py` docstring) | — |
| 15 | `check_text_value` | Step 2 |
| 16 | `check_reconstruction` | Step 10 |
| 17 | `check_bloom_filters` | Step 12 (skipped if Step 1.1 failed) |
| 18 | `check_accession_map` | Step 13 |
| 19 | `check_partitions` | Step 14 |

`check_schema_types` (12) is extended in Step 4; `check_manifest` (8) verifies hashes from Step 15; `check_sort_order` (5) is refactored in Step 13.
