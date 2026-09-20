# UniProt Parquet — Project Review & Roadmap

**Last consolidated:** 2026-05-23
**Branch:** `static-lake`
**Replaces:** all prior audit/review notes in this repo, plus the `benchmarks/` design proposals (which remain as input artifacts).

This is the single working document. Everything you need to plan against should be here. The IDs (D#, O#, S#, Q-C#/H#/M#/L#, A#) are stable — cite them in PRs and issues.

---

## 1. Executive summary

The pipeline foundations are strong: bounded-memory streaming, DuckDB-as-engine end-to-end, FAIR metadata, 14-check validator, 89-test suite. The 5-table star schema with hybrid convenience + nested columns is the right shape and matches surveyed peer projects (Open Targets, gnomAD, ADAM).

What blocks a confident public release falls into four buckets:

1. **Strategic decisions** — latest-only publish model, deletion of VARIANT branch, dedup proposal acceptance — mostly resolved in principle but not yet executed in code (§2, §7 Tier 1).
2. **Archival/integrity primitives** — per-file SHA256, atomic manifest writes, atomic publish-swap. Quick to build, mandatory before publish (§7 Tier 2).
3. **Schema improvements** — a dozen specific changes to make the Parquet better for actual users, of which 5-6 are no-regrets and the rest depend on user feedback (§4).
4. **The meta-gap** — nobody has asked actual UniProt users what queries they run. Designing in a vacuum is the single biggest risk to v1 (§4.4, A4).

---

## 2. Locked decisions

| ID | Decision | Rationale | Status |
|---|---|---|---|
| **D1** | **Latest-only publishing model** | UniProt FTP archives the source-of-record (XML/.dat/FASTA). Parquet here is a derived analytical view; archival belongs upstream. Old releases re-derivable from FTP XML on demand via the pipeline. | Direction confirmed; needs execution in A1+A5 |
| **D2** | **No VARIANT type. Typed star schema only.** | `benchmarks/VARIANT_EVALUATION.md:148-156` (rigorous evaluation, supersedes `VARIANT_BENCHMARK_BRIEF.md`) concludes no VARIANT layout matched baseline on any of 16 queries. Even Layout D shows 2-17× overhead on child-table queries. | Decision made for the published schema. The deletion planned in A2 was later reversed: `NEXT_TASKS.md` 3.6 keeps `--variant-children` as documented evaluation-only code |
| **D3** | **Adopt dedup proposal Phases 1+2** | `benchmarks/DEDUPLICATION_PROPOSAL.md` v2 — ~17% storage shrink, lossless reconstruction via `g()`, round-trip test as release gate. Phase 1 = build `g` + test (no schema change). Phase 2 = trim nested columns to residuals. | Approved; execution in A11+A13 |
| **D4** | **5-table star schema with hybrid convenience + nested** | Surveyed peer-project consensus (`BIOINFORMATICS_PARQUET_SURVEY.md:450-457`). Star matches data shape; convenience columns serve 90% of queries; nested preserves losslessness. Denormalized join keys (`acc`, `taxid`, `from_reviewed`) on child tables avoid most joins. | Already shipped, validated |
| **D5** | **Sort order `(reviewed/from_reviewed DESC, taxid ASC, acc ASC)`** | Swiss-Prot first; per-organism locality drives the most common query pattern. Within-protein sorts (start_pos for features) deliberately omitted to avoid ~1.2 TB sort spill — but see S4 below. | Already shipped, validated |

---

## 3. Open decisions

These block downstream work. Resolve in this order.

| ID | Decision | Options | Recommendation | Blocks |
|---|---|---|---|---|
| **O1** | Transition window for latest-only | (a) zero overlap; (b) `latest` + `previous` symlinks, ~1 release overlap; (c) keep N releases | (b) — minimal cost, handles "mid-paper" case | A5 |
| **O2** | Canonical EBI URL | `ftp.ebi.ac.uk/pub/databases/uniprot/parquet/latest/lake` (recommended) vs alternatives | Decide with UniProt FTP team | A3, A15, docs |
| **O3** | Naming convention | (a) freeze current (`reviewed`/`acc`/`taxid`); (b) rename to survey consensus (`is_reviewed`/`acc_id` etc.) | (a) — rename breaks every user; document current names in SCHEMA.md | A14 |
| **O4** | **Closed 2026-09-16 by `PLAN_SCHEMA_V2.md` D.5:** no organism partition level; `entries.division` column plus per-file `taxid_min`/`taxid_max` in the manifest and `uniprot_parquet.files_for_taxid()` instead. — Top-N organisms to partition `xrefs`/`features` by | 0 (no partitioning); top-20 model organisms; top-50; all | top-20 + `_other` partition (covers ~80% of query volume) | S7 |
| **O5** | Comment column representation | (a) keep VARCHAR JSON; (b) tag as JSON `logical_type`; (c) typed STRUCT per comment_type | (b) immediately, (c) post-user-survey | S2 |
| **O6** | User survey before v1 lock-in | Ship survey alongside v0 / wait for organic feedback / never | Ship survey — this is the highest-leverage action available | A4 |

---

## 4. Schema assessment

### 4.1 What's right (do not change)

- 5-table star schema (entries, features, xrefs, comments, publications) — D4.
- Sort order `(reviewed DESC, taxid ASC, acc ASC)` — D5.
- Denormalized `acc`/`taxid`/`from_reviewed` on child tables — eliminates joins for ~90% of queries.
- Sequence on `entries` (every query needs it; correct denormalization).
- Convenience + nested hybrid (after dedup trim — D3).
- Flat list columns for `go_ids`, `keyword_names`, `gene_names` — exactly what ML/aggregation workflows want.
- Schema inferred via DuckDB `read_json_auto` with `sample_size=-1` — robust to schema drift in either direction.
- `--skip-existing` flag enables resume after OOM (`parquet_transform.py:1274`).
- One Parquet file per ~256 MB, atomic write via `.tmp/` rename (`parquet_transform.py:864-944`).

### 4.2 Schema improvements — no-regrets (ship regardless of user feedback)

| ID | Change | Location | Effort | Why |
|---|---|---|---|---|
| **S1** | **Shipped 2026-09-16** (`PLAN_SCHEMA_V2_STEPS.md` Step 6). — Add `gene_name` (singular, = `gene_names[1]`) as real column on `entries` | `parquet_transform.py:_build_entries_sql` | hours | Both client views already do this transform (`uniprot_parquet.py:42`, `setup_views.sql:33`). Non-DuckDB users (Polars/pandas/PySpark) currently have to know the trick. |
| **S2** | Tag `comments.comment` Parquet field with `logical_type=JSON` | `parquet_transform.py:562` + Arrow schema metadata | 1 day | Stored as VARCHAR; engines that recognize JSON logical type (Spark 3.5+, recent DuckDB) auto-cast. Removes the `comment::JSON` hack in views. |
| **S3** | First-class `isoforms` table | New table builder in `parquet_transform.py`, new view in `setup_views.sql`/`uniprot_parquet.py` | 2-3 days | Isoforms currently buried inside `comments` of type `ALTERNATIVE_PRODUCTS`. The `unnest_isoforms()` macro (`uniprot_parquet.py:107-129`) is an admission the schema is wrong. Proteomics/mass-spec users need this constantly. Columns: `acc`, `isoform_id`, `name`, `sequence_status`, `variant_sequence_ids`. |
| **S4** | Verify whether `features` is already in `start_pos` order within an `acc` | One-time measurement | hours | UniProt JSON emits features in source order, usually start_pos. If true (likely), document as guaranteed and users skip `ORDER BY start_pos`. README's claim that this is unsorted may be wrong. |
| **S5** | **Shipped 2026-09-17** as the `accession_map` table (Step 13). — Reverse-accession lookup table (`accession_map`: `secondary` → `primary`) | New table builder | 1 day | `secondary_accs` is `list[string]` on entries — finding current primary for legacy accession requires `list_contains` over 250M rows. New table is <1 GB and saves every legacy-data user a full scan. |
| **S6** | Add `annotation_quality :: tinyint` and `is_high_confidence :: bool` to `entries` | `parquet_transform.py:_build_entries_sql` | hours | `annotation_score` is a double (currently 1-5 integer). Tinyint compresses much better. Boolean (`>= 4`) matches the most common filter pattern. |
| **S7** | Sequence hash *verification*, not just propagation | `validate_lake.py` new `check_sequence_hashes()` | 1 day | `seq_md5`/`seq_crc64` are written from upstream (`parquet_transform.py:350-351`) but never re-verified. Cheap streaming check. Catches upstream-data corruption. (Also = Q-H3.) |

### 4.3 Schema improvements — pending O5/O6 (need decision or user feedback)

| ID | Change | Why hold | Decide via |
|---|---|---|---|
| **S8** | Partition `xrefs` and `features` Hive-style by top-N organisms (`taxid=9606/...`) + `_other` | Need O4 decision on N; need to confirm the query-pattern assumption | O4 + user survey |
| **S9** | Rethink `xrefs.properties` (currently opaque `list[struct{key,value}]`) | Three options: per-database sub-tables / typed convenience columns for top DBs / keep generic + publish schema doc. Need to know which databases users actually filter on. | User survey |
| **S10** | Rename booleans to `is_*` prefix universally | Current naming consistent + already in production demo. Rename breaks every user. | O3 |
| **S11** | **Shipped 2026-09-16** as `entries.proteome_ids` (Step 6); a `proteomes` table stays pending the user survey. — First-class `proteomes` table or `proteome_id` column | UniProt reference proteomes are first-class concept; can be approximated via `taxid + reviewed` but not exactly. Worth doing if users actually want it. | User survey |
| **S12** | Per-fragment `comment_texts :: list[struct{value, evidence_codes}]` instead of newline-concatenated `text_value` | Current `text_value` (`parquet_transform.py:539-545`) drops per-text evidence and is lossy. Affects users who care about evidence (per surveys, this is a UniProt differentiator). | Wait for S2 outcome and user survey |

### 4.4 The meta-finding: no user research

Every choice above is an educated guess informed by peer projects and codebase introspection. **Nobody has asked actual UniProt users what queries they run.** This is the single highest-leverage action available — a 10-question survey to the UniProt mailing list before v1 lock-in would beat any of these recommendations.

Specifically ask:
- "Which of these query patterns do you run most?" (list 10 patterns)
- "Have you used the REST API for batch / pipeline work?" (yes → prime audience)
- "Which engine do you use?" (validates S2 — if 80% answer DuckDB, JSON-as-VARCHAR is fine; if 40% answer Polars/Spark, S2 is mandatory)
- "Would you use isoforms / FASTA export / cross-release diffs?"
- "Which xref databases do you filter on?" (validates S9 partition list)
- "Do you query by organism most of the time?" (validates S8 partitioning)

See A4 in the roadmap.

---

## 5. Code quality findings

Items resolved by schema work (§4) or roadmap actions (§7) are cross-referenced. Remaining items are pure code/operations concerns.

### 5.1 Critical (blocking public release)

| ID | Finding | Location | Resolved by |
|---|---|---|---|
| **Q-C1** | Output Parquet files have no checksums anywhere | `release_manifest.py:91-95` only hashes input JSONL; `parquet_transform.py:1429-1440` writes no per-file SHA256 | A5 (atomic publish bundles SHA256SUMS.txt) |
| **Q-C2** | ~~No top-level release index~~ | — | Obsoleted by D1 (latest-only); a single `latest.json` is enough |
| **Q-C3** | Manifest writes are not atomic — kill mid-`json.dump` truncates the file | `parquet_transform.py:1437-1447` | A5 (atomic publish-swap writes to temp + fsync + rename) |
| **Q-C4** | Round-trip validator only compares 6 scalar fields | `validate_lake.py:509-554` skips all nested struct columns | A8 (extend to nested) + A11 (dedup `g()` provides stronger guarantee) |
| **Q-C5** | Schema-drift guard intentionally disabled | `upjson2lake.nf:198-227` drops `--schema-baseline` flag; `validate_lake.py:948-1016` implementation is wired up but not invoked | A10 (re-enable as warning-with-diff, blocker on type changes) |

### 5.2 High

| ID | Finding | Location | Resolved by |
|---|---|---|---|
| **Q-H1** | Personal branding in published `datapackage.json` | `parquet_transform.py:1209` (`homepage: github.com/dlrice/...`), `:1229-1232` (contributor) | A6 |
| **Q-H2** | ~~Half-shipped VARIANT~~ | — | Superseded: kept as evaluation-only code per `NEXT_TASKS.md` 3.6 |
| **Q-H3** | `seq_md5`/`seq_crc64` propagated, not verified | `parquet_transform.py:350-351` write; `validate_lake.py:707-750` only checks length | S7 |
| **Q-H4** | Provenance missing reproducibility fields | `release_manifest.py:81-137` no Python/DuckDB/PyArrow versions, no hostname, no duration, no validator outcome embed | A7 (also covers Parquet footer metadata) |
| **Q-H5** | No partitioning | All tables are flat sorted directories | S8 (pending O4) |
| **Q-H6** | `feature_count`/`xref_count` only checked per-table, not per-acc | `validate_lake.py:227-263` global sum only | A18 |
| **Q-H7** | Comment text concatenation silently lossy/fragile | `parquet_transform.py:539-545` drops per-text evidence | S12 |
| **Q-H8** | SQL via Python `.format()` / `.replace()` | `parquet_transform.py:1413`, `uniprot_parquet.py:211`, `sort_jsonl.py:76` | A19 (parameterize + tighten `_sql_escape` usage; reject metachar in `--release`) |

### 5.3 Medium

| ID | Finding | Location | Resolved by |
|---|---|---|---|
| **Q-M1** | CI is single Python/OS/DuckDB version | `.github/workflows/ci.yml:27` | A16 (matrix) |
| **Q-M2** | Idempotency tests don't cover the failure case `--skip-existing` exists for | `tests/test_idempotency.py:87-132` only tests clean-rerun, not kill-and-resume | A17 |
| **Q-M3** | Validator round-trip uses unparameterized IN-list of 1000 accs | `validate_lake.py:544` | A19 |
| **Q-M4** | `sort_jsonl.py` re-parses JSON through DuckDB, not bit-identical to input | `sort_jsonl.py:33-40, 84-86` | Caveat README; or external sort on key prefix |
| **Q-M5** | No per-column profile / data quality report | — | A20 |
| **Q-M6** | `validation_report.txt` human-readable only | `upjson2lake.nf:222` | A21 (also emit `.json`) |
| **Q-M7** | Client silently mis-serves a corrupt lake | `uniprot_parquet.py:164-215` doesn't cross-check manifest vs disk | A15 (cross-check + optional SHA verify) |
| **Q-M8** | `comments.comment` is VARCHAR JSON | `parquet_transform.py:562` | S2 |
| **Q-M9** | Local Nextflow profile has no memory/OOM-retry directives | `nextflow.config:22-24` | A22 |
| **Q-M10** | No tests for the client (`uniprot_parquet.py`), `setup_views.sql`, or `manifest()`/`schema()`/`tables()` helpers | — | A23 |

### 5.4 Low (cleanup)

All resolved by A24 (repo hygiene sweep).

| ID | Finding | Location |
|---|---|---|
| **Q-L1** | `.vscode/settings.json` committed | repo root |
| **Q-L2** | Empty committed `lake/reports/` confuses with runtime output dir (`nextflow.config:13-19`) | repo root |
| **Q-L3** | `__pycache__/` committed in `bin/`, `benchmarks/`, `tests/` | (multiple) |
| **Q-L4** | `.DS_Store` files committed | (multiple) |
| **Q-L5** | `benchmarks/results/*.json` committed despite `.gitignore:59-60` claiming otherwise | `benchmarks/results/` |
| **Q-L6** | Format string mismatch: code writes `"uniprot-parquet"`, old demo manifest says `"uniprot-parquet-lake"` | `parquet_transform.py:1430` vs `demo/lake/2026_01/lake/manifest.json` |
| **Q-L7** | Unconditional `from frictionless import Package` at module top breaks pip-light installs | `parquet_transform.py:53` |
| **Q-L8** | `stream_jsonl.py` silently logs JSON errors without `--max-errors` threshold | `stream_jsonl.py:78` |
| **Q-L9** | Missing optional fields logged to stderr, not manifest | `parquet_transform.py:1346-1351` |
| **Q-L10** | `setup_views.sql` and `uniprot_parquet.py` duplicate the same SQL with different placeholder syntax (`${BASE}` vs `{BASE}`); drift independently | generate one from the other |

---

## 6. Release size & storage

### 6.1 Estimate

Two in-repo measurements disagree by 14×:

| Sample | Entries | Total Parquet | Per entry | Linear → 248 M |
|---|---|---|---|---|
| `demo/lake/2026_01/` (committed demo) | 5,378 | 3.23 MB | 0.6 KB | **~150 GB** |
| `DEDUPLICATION_PROPOSAL.md` baseline | 4,591 | 38.1 MB | 8.5 KB | **~2.4 TB** |

Both fixtures are biased; the representative measurement (A12) is the F.1 slice of `PLAN_SCHEMA_V2.md` — human reviewed + unreviewed plus 1M random TrEMBL entries, sampled with `bin/sample_jsonl.py` (Step 3 of `PLAN_SCHEMA_V2_STEPS.md`). As of 2026-09-16 the sampler exists but the slice has not been built; the F.1 table is still empty. Production is dominated by lean TrEMBL entries (~248M, ~20 xrefs/entry, ~1 comment/entry, ~330 AA sequence). Best-guess range:

- **Parquet lake: ~300 GB – 1 TB per release**
- **Plus `sorted.jsonl.zst`: ~180-220 GB**
- **Total per release: ~500 GB – 1.2 TB**

Compare to ~186 GB for all of UniProt XML.gz today (Swiss-Prot + TrEMBL combined).

### 6.2 Why Parquet ends up bigger than the source

1. **Denormalization.** `acc`, `taxid`, `from_reviewed` repeat on every child row. ~5 B xref rows = billions of repeated keys (dictionary encoding helps but isn't free).
2. **Lossless nested duplication.** Every `features` row carries both flat columns and the full original `feature` struct. Same for `comment` and `reference`. `DEDUPLICATION_PROPOSAL.md:33-36`: ~40% of compressed lake bytes are duplicated.
3. **`comments.comment` as VARCHAR JSON** — defeats columnar compression for that column.

### 6.3 Shrink path

- **D3 / A11 / A13** (dedup Phases 1-2): ~17% shrink.
- **S2** (comment column tagged as JSON / eventually typed STRUCT): expected meaningful shrink on the comments table specifically.
- **A12** (measure on representative slice before committing storage budget).

### 6.4 Decade-scale archival math (informational)

If hypothetically retaining all releases: ~7-8 TB/year, ~70-80 TB/decade. **D1 (latest-only) collapses this to ~600 GB resident** with the previous release retained briefly during transition (O1).

---

## 7. Roadmap

### Tier 1 — Decisions to execute (hours each)

| ID | Action | Resolves | Effort |
|---|---|---|---|
| **A1** | Execute D1 latest-only model: README update + transition window (O1) decision | D1, O1 | hours |
| **A2** | ~~Delete VARIANT code~~ Reversed by `NEXT_TASKS.md` 3.6: `--variant-children` and the benchmark scripts stay as documented evaluation-only code. `VARIANT_EVALUATION.md` remains the rationale for D2. | D2, Q-H2 | done (kept) |
| **A3** | Pick canonical EBI URL (O2). Update README, `uniprot_parquet.connect()` default, CI httpfs test. | O2 | hours |
| **A4** | Ship user survey (Google Form or similar) — see §4.4 for question set. Distribute via UniProt mailing list, EBI Slack, BioStars, related forums. | O6 + informs S8-S12 | 1 day to build, 2-4 weeks to collect |

### Tier 2 — Pre-publish critical (1-2 weeks)

| ID | Action | Resolves | Effort | Depends on |
|---|---|---|---|---|
| **A5** | **Partly done 2026-09-17:** `SHA256SUMS.txt`, `RELEASE.metalink` and `RELEASE_COMPLETE` are produced by the pipeline (Steps 15, 19); the swap itself remains. — Atomic publish-swap: build into `releases/<rel>/`, compute per-file `SHA256SUMS.txt`, fsync + atomic-rename manifests, flip `latest` (and `previous`) symlinks, prune. | Q-C1, Q-C3 | 2-3 days | A1 |
| **A6** | De-personalize `datapackage.json` via `pipeline_config.yaml` driving `homepage`/`contributors`/`licenses`/`sources`. Separate `pipeline_version` from `uniprotRelease`. | Q-H1 | hours | — |
| **A7** | Provenance + Parquet footer metadata: record Python/DuckDB/PyArrow/zstd versions, hostname, per-stage duration, embedded validator outcome. Embed `uniprot_release`/`schema_version`/`extraction_date`/`pipeline_commit` + 10-protein golden set in Parquet `key_value_metadata` (per survey: `BIOINFORMATICS_PARQUET_SURVEY.md:629-647`). | Q-H4 | 1-2 days | — |
| **A8** | Extend round-trip validator to nested fields. Reuse `tests/test_roundtrip.py:30-60` helpers (`deep_sort`, `normalize_value`) in `validate_lake.py:509-554`. Same n=1000 reservoir; deep-compare all nested struct columns. | Q-C4 | 1 day | — |
| **A9** | Sequence hash verification step (S7). | Q-H3, S7 | 1 day | — |
| **A10** | Re-enable schema-drift detection. `--strict-on-type-changes` (block) + `--warn-on-name-changes` (emit diff into `RELEASE_NOTES.md`). **2026-09-16:** the semver enforcement in `PLAN_SCHEMA_V2.md` H.4 (`check_schema_evolution` classifying a change as major/minor/patch against a committed baseline) starts with the *second* public release — there is no published baseline to compare against yet. `SCHEMA_VERSION` and the policy are in place (Step 9). | Q-C5 | 1 day | — |

### Tier 3 — High-value product moves (2-4 weeks)

| ID | Action | Resolves | Effort | Depends on |
|---|---|---|---|---|
| **A11** | **Done 2026-09-17** (`bin/reconstruct.py`, `tests/test_reconstruct.py`, validator check 16; Step 10). — DEDUPLICATION_PROPOSAL Phase 1: build `g()` reconstruction in `tests/`, round-trip test as CI gate, full-scale release-candidate validator. No schema change. | D3 | 1-2 weeks | A8 |
| **A12** | Measure on representative slice: human reviewed+unreviewed (~210K) + 1M random TrEMBL sample. Publish actual per-entry/per-row sizes in this document (§6.1). | §6 uncertainty | hours of compute + 1 day analysis | — |
| **A13** | **Done 2026-09-17** (Step 11; ships before the first public release, no flip). **Rule (plan F.3):** no plan adds a column to `features`, `xrefs`, `comments` or `publications` ahead of this trim — now moot for the launch schema, kept for later plans. — DEDUPLICATION_PROPOSAL Phase 2: residual trim per `DEDUPLICATION_PROPOSAL.md:368-694`. Single-release flip. | D3 | 1-2 weeks | A11, A12 |
| **A14** | **Partly done 2026-09-17:** `SCHEMA.md` is generated by `bin/gen_schema_md.py` (Step 9/21). — Four-doc bundle (per survey recommendation, `SURVEY_COMPARISON_GUIDE.md:114-138`): | — | 1-2 weeks | A3, O3 |
| | • `SCHEMA.md` — column → source JSON path mapping (auto-generate from `parquet_transform.py:978-1085` `COLUMN_DESCRIPTIONS`) | | | |
| | • `EXAMPLES.md` — Spark, DuckDB, Polars, pandas, R | | | |
| | • `EXTRACTION_REPORT.md` — auto-generated from `validation_report.json` per release | | | |
| | • `VERSION_HISTORY.md` — auto-generated from A10 drift diffs per release | | | |
| **A15** | **Done 2026-09-17** (`tests/test_client.py`, Step 16; httpfs auto-install is exercised by the `--serve` benchmark, not the suite). — Client tests (`tests/test_client.py`): every README example query, every helper (`connect`, `manifest`, `schema`, `tables`, `datapackage`), the `_split_sql` helper, and httpfs auto-install path. | Q-M10, Q-M7 | 2-3 days | — |

### No-regrets schema improvements (parallelizable with Tier 3)

| ID | Action | Resolves | Effort |
|---|---|---|---|
| **A-S1** | Ship S1 (`gene_name` singular column) | S1 | hours |
| **A-S2** | Ship S2 (tag `comments.comment` as JSON logical type) | S2, Q-M8 | 1 day |
| **A-S3** | Ship S3 (first-class `isoforms` table) | S3 | 2-3 days |
| **A-S4** | Run S4 measurement (verify features start_pos order) | S4 | hours |
| **A-S5** | Ship S5 (reverse-accession lookup table) | S5 | 1 day |
| **A-S6** | Ship S6 (`annotation_quality` tinyint + `is_high_confidence` boolean) | S6 | hours |

### Tier 4 — Operational quality (background / ongoing)

| ID | Action | Resolves | Effort |
|---|---|---|---|
| **A16** | CI matrix: Python {3.11, 3.12, 3.13, 3.14} × DuckDB {1.5, 1.6, latest} × PyArrow {17, 18, latest} for the *client* (`uniprot_parquet.py`). Pipeline can stay pinned. Add DuckDB version check in `connect()`. | Q-M1 | 1 day |
| **A17** | Idempotency test for kill-and-resume case (Q-M2): kill `parquet_transform.py` after entries table written, restart with `--skip-existing`, assert row counts + no leftover `.tmp/`. | Q-M2 | 1 day |
| **A18** | Per-acc count consistency check in validator (`validate_lake.py`). DuckDB join with `WHERE entries.feature_count != fc.cnt LIMIT 10`. | Q-H6 | hours |
| **A19** | SQL hygiene: parameterize where possible; apply `_sql_escape` consistently to all `.format()`/`.replace()` interpolations; reject shell-metacharacter in `--release`; switch `validate_lake.py:544` IN-list to `con.register("sample_accs", arrow_table)` + join. | Q-H8, Q-M3 | 1 day |
| **A20** | Generate `lake/profile.json` per release: null rate, distinct count, p50/p99 per column via `pyarrow.compute`. | Q-M5 | 2 days |
| **A21** | **Done 2026-09-17** (`validation_report.json`, Step 15; not yet embedded in `provenance.json`). — Emit `validation_report.json` alongside `.txt`. Per-check `{name, status, duration_seconds, message}`. Embed in `provenance.json`. Feeds A14's `EXTRACTION_REPORT.md`. | Q-M6 | 1 day |
| **A22** | Add OOM-retry directives to local Nextflow profile, or document explicitly that local mode has no safety net. | Q-M9 | hours |
| **A23** | Generate `setup_views.sql` from `uniprot_parquet.py` `_SETUP_SQL` at build time so they can't drift. | Q-L10 | hours |
| **A24** | Repo hygiene: untrack `.vscode/`, `__pycache__/`, `.DS_Store`, `benchmarks/results/*.json`, `lake/reports/`. Resolve format-string mismatch (Q-L6). Lazy-import `frictionless` (Q-L7). | Q-L1–L10 | 1 day |

---

## 8. Dependencies & critical path

```
A1 (latest-only)  ──┐
A2 (kill VARIANT) ──┤
A3 (URL)          ──┼──►  A5 (atomic publish)  ──►  PUBLISH-READY
A4 (user survey)  ──┘         ▲
                              │
A6 (de-personalize) ──────────┤
A7 (provenance + footer)──────┤
A8 (nested roundtrip) ──┐     │
A9 (seq hash)           ├─►  validator hardened ─────┤
A10 (drift warning)     ┘                            │
                                                     │
A11 (Phase 1 g + RT) ────────────────────────────────┤
                                                     │
A12 (size measurement) ─►  A13 (Phase 2 dedup) ──────┘  (optional but recommended)

A4 (user survey) ────►  decides S8, S9, S10, S11, S12 ────►  future schema versions

A-S1..A-S6 (no-regrets schema) — parallelizable, no dependencies
A14 (docs) — depends on O3 and A3 being settled
A15-A24 — background, no critical-path blockers
```

---

## 9. Right now

If picking one action: **A4 (ship the user survey)**. Everything past Tier 2 is built on assumptions about what users want; even rough survey data would re-order this document. It's also the only action with a long calendar dependency (collection takes weeks), so starting it now parallelizes with everything else.

If picking three to do this week:
1. **A1 + A2** together — both are decisions-into-code that permanently shrink the project (less surface to maintain, fewer audit items).
2. **A4** in parallel — get the survey out the door before locking schema.
3. **A11 (Phase 1 of dedup)** — has no dependencies, no schema risk, and the "round-trip validated against source on every release" claim is what differentiates this project from every peer in the survey (`README_SURVEYS.md:112-122` — no bioinformatics center currently publishes this).

---

## Appendix: source artifacts (input to this document)

These remain in the repo as primary sources. Don't re-read unless you're reopening one of the decisions above.

- `benchmarks/DEDUPLICATION_PROPOSAL.md` — full residual schema spec for A13 (lines 368-694), including the no-positional-zipping rule (do not split arrays across convenience/residual layers).
- `benchmarks/VARIANT_EVALUATION.md` — rigorous evaluation behind D2: methodology, per-query latencies, storage measurements, reproduction instructions.
- `benchmarks/BIOINFORMATICS_PARQUET_SURVEY.md` + `_INTL.md` — 40+ peer project survey (Open Targets, gnomAD, ADAM, CELLxGENE, etc.); source for the "no peer publishes round-trip validation" claim and the documentation/naming/partitioning consensus.
- `benchmarks/bench_baseline.py`, `bench_variant.py`, `build_variant_lake.py`, `results/`, `variant_lake/` — benchmark code and outputs. The VARIANT ones are scheduled for deletion via A2.

Synthesis docs that previously lived here (`VARIANT_BENCHMARK_BRIEF.md`, `SURVEY_COMPARISON_GUIDE.md`, `README_SURVEYS.md`, `INDEX.txt`) have been removed — their content is fully absorbed into this document.
