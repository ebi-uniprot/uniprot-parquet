# Plan: fix the findings of the 2026-09-30 multi-agent review

## Context

A five-area multi-agent review of the whole codebase ran on 2026-09-30. It covered the transform, reconstruction and validation, workflow and operations, the consumer surface, and the tests. Each finding went to a skeptic, who tried to refute it, mostly by reproducing it. 34 findings came back and one was refuted. After merging duplicates, 24 distinct issues remain, listed below.

Three of them can put a wrong or damaged release in front of users:

- the `ec` column is incomplete;
- the validator never checks shortcut values;
- `-resume` can mark a damaged published copy as complete.

This plan fixes all 24. Each work package has the same parts:

- the problem;
- the fix;
- the files it touches;
- the tests, written first so they fail on today's code;
- when it counts as done.

Nothing here changes stored data except where a work package says so. `SCHEMA_VERSION` stays `1.0.0` because nothing is published yet.

Ground rules for every work package:

- Write the test first and watch it fail, then fix the code.
- Keep the exec bit on `bin/*.py`: edit with Python or the Edit tool, not `sed -i`.
- Run the full suite with `PATH=/home/agent/bin:$PATH python3 -m pytest tests/ -q`, so the Nextflow tests run too.
- Keep `setup_views.sql` and `uniprot_parquet.py` identical, which `test_sql_parity` enforces.

---

## Starting point (for a fresh session)

- **Branch and state.** The work is on `static-lake`, with a large uncommitted working tree:
  - the 2026-09-30 layout rewrite (source columns plus shortcuts, no residual columns);
  - the renames of shortcuts to UniProt REST names;
  - the earlier review fixes.

  The full suite passed on it: 304 passed and 1 xfailed, with Nextflow on PATH. Commit or stash that state before starting, so each work package's diff can be reviewed on its own.
- **Current layout.** Read README §2 and the `TABLE_META` / `COLUMN_TYPES` blocks in `bin/parquet_transform.py`. The main points:
  - snake_case shortcut columns carry declared types, named after UniProt REST fields;
  - camelCase source columns are the JSON element's keys, verbatim;
  - a source column whose type conflicts between entries is stored as Parquet JSON (`arrow.json`), and `reconstruct.rows_from_arrow` decodes it;
  - macros may name only shortcuts or keys every element has, because DuckDB binds them when they are created.
- **Building a lake by hand:**

  ```bash
  python3 -c "import sys; sys.path.insert(0,'tests'); from conftest import _json_gz_to_jsonl_zst as c; c('tests/fixtures/diverse.json.gz', '/tmp/x/d.jsonl.zst')"
  python3 bin/parquet_transform.py /tmp/x/d.jsonl.zst --outdir /tmp/x/lake --memory-limit 3GB --release t
  python3 bin/validate_lake.py --lake /tmp/x/lake --jsonl /tmp/x/d.jsonl.zst --spot-check-n 1000 -o /tmp/x/report.txt
  ```

  Tests build lakes with `--batch-size 500`, which gives tiny row groups; use the default batch size for any size measurement.
- **Reviewer reproductions.** They were written under `/tmp/claude-1000/-Users-dlrice-Developer-uniprot-parquet/57b85ae6-11ed-401f-a401-7b1962393358/scratchpad/review/`: mutation scripts such as `tests/mutate.py`, and TrEMBL-shaped input in `../trembl_like.jsonl`. They may be gone. Every mutation a work package needs is described in that package's tests, so don't depend on them.
- **Executing the notebook's code without Jupyter** (used for WP15). Build a lake under `<dir>/lake/2026_03/lake` from `demo/input.json.gz`, `cd <dir>`, then run each code cell of `demo/demo.ipynb` with `exec(compile(src, f"cell{i}", "exec"), ns)` in one shared namespace, with the project root on `sys.path` and cells containing `.plot` skipped (matplotlib isn't installed). Neither `nbconvert` nor Jupyter is installed in the sandbox.
- **Renaming method** (used for WP14). A Python pass with a word-boundary regex (`\bold\b` → new) over `bin/*.py`, `tests/*.py` except `fetch_fixtures.py` (it contains REST query syntax), `uniprot_parquet.py`, `setup_views.sql`, README, RUNBOOK, `upjson2lake.nf`, `run_lake.sh`, and `benchmarks/{_bench_common,bench_baseline,bench_point_lookup,bench_zstd,bench_file_size}.py`. Plus the notebook's cell sources only, not its outputs. Save and restore each file's mode, so the exec bit survives. Grep first for Python identifiers that would collide with the new name.
- **Environment.** Nextflow is at `/home/agent/bin`; without it on PATH, `tests/test_workflow.py` is skipped silently. Use `python3`. `sed -i` drops the exec bit in this sandbox.

---

## Decisions needed before starting

**Status: the owner has not yet confirmed these recommendations.** Ask before starting WP2 (D3, D4), WP11 (D5) and WP14 (D1, D2, D6). The other work packages don't depend on them.

| # | Question | Recommendation |
| --- | --- | --- |
| D1 | `organism_name` holds only the scientific name. UniProt's REST `organism_name` is "Homo sapiens (Human)". | Keep the value, since people filter on the scientific name, and rename the column to `organism_scientific_name`. The README rule "no shortcut reuses a REST name for a different value" then holds. |
| D2 | `xref_proteomes` holds bare `UP…` ids. UniProt's REST field is "UP000005640: Chromosome 12". | Rename it to `proteome_id` (a list), which states what it holds. |
| D3 | What should the transform do when `entryType` has a value other than the two known ones? | Stop the build with a message naming the value, as for a missing required path (WP2). |
| D4 | Should shortcut coverage (check 20) fail a full release rather than warn? | Yes. Make it fail in release mode, which VALIDATE turns on with a new `--release` flag. Keep it a warning for subsets and tests (WP2). |
| D5 | Should reconstruction treat JSON `5` and a lake's `5.0` as equal? A field with mixed ints and floats is stored as DOUBLE. | Yes, but say so as a fourth normalisation in the README. Do not treat booleans and numbers as equal (WP11). |
| D6 | Rename `files_for_taxid()` and the manifest's `taxid_min`/`taxid_max` to match the `organism_id` column? | Yes: `files_for_organism_id()` and `organism_id_min`/`organism_id_max`. Nothing is published yet (WP14). |

The work packages below assume the recommended answers.

---

## Work packages, in priority order

### WP1 · `ec` misses EC numbers in `includes` / `contains` (high)

**Problem.** The `ec_parts` builder in `bin/parquet_transform.py` (around lines 860–885) reads only the top-level `recommendedName`, `alternativeNames` and `submissionNames`/`submittedNames`. EC numbers of chains and domains, under `proteinDescription.contains[]` and `includes[]`, are never read. On the diverse fixture, 257 of 4,591 entries get `ec = []`, among them P0DTD1 and P0DTC1 (SARS-CoV-2 replicases) and P37142. UniProt's REST `ec` field includes these numbers, and the column description promises "all naming blocks".

**Fix.**
- Extend `ec_parts`: for each of `includes` and `contains`, when present, flatten the `ecNumbers[].value` of every element's `recommendedName` and `alternativeNames[]`, and of `submissionNames[]` when that path exists.
- Guard each path with `has()`, as now. `list_distinct` over the concatenation stays.
- Add the new paths to `DERIVED_SOURCES[("entries", "ec")]`, so `check_curated_sources` checks their types.
- Update the description of `ec` to name the blocks it reads.

**Files.** `bin/parquet_transform.py`, README column reference.

**Tests (first).**
- In `tests/test_roundtrip.py`, add a value test over the whole diverse fixture. A Python reference collects every `ecNumbers[].value` anywhere under `proteinDescription` (all naming blocks, including those in `includes`/`contains`), and the lake's `ec` must equal it as a set for every entry. It fails today for about 257 entries.
- Name P0DTD1 explicitly: its `ec` must contain `3.4.22.69`.

**Done when.** Both tests pass. `SELECT count(*) FROM entries WHERE list_contains(ec, '3.4.22.69')` is at least 1 on the diverse lake.

---

### WP2 · The validator never checks shortcut values (high)

**Problem.** Reconstruction (check 16) reads only source columns, and check 6 compares only `organism_id`, `length` and the `*_count` columns. Every other shortcut can be wrong in every row and the lake still passes. The worst case: if UniProt rewords `entryType`, `REVIEWED_EXPR` (`LIKE '%Swiss-Prot%'`) classifies every entry as TrEMBL, puts the whole lake in the `trembl` partition, and it still validates. The same happens if the `GoTerm` or `PubMed` keys are renamed. Check 20 only warns, and only when a column is NULL in every row.

**Fix, in three layers.**

1. **An independent Python reference for every shortcut.** Add `bin/shortcut_reference.py` with a function `expected_shortcuts(entry) -> {table: [row dict of shortcut values]}`. It computes each shortcut from the JSON entry with plain Python, deliberately not by reusing the SQL. It covers:
   - **entries:** `accession`, `id`, `reviewed`, `organism_id`, `organism_name`, `gene_primary`, `protein_name_primary` (the same fallback order), `length`, `gene_synonym`, `alt_protein_names`, `ec` (the WP1 semantics), `division` (the same rule list), `go_id`, `go_terms`, `xref_dbs`, `proteome_id`, `keywordid`, `keyword`, `keyword_categories`, the three dates, the four counts and `lit_pubmed_id` (the numeric-then-text order);
   - **features:** `organism_name`, `length`, `feature_index`, `start_pos`, `end_pos`, `ligand_id`, `evidence_codes`;
   - **the other child tables:** `xref_index`, `comment_index`, `text_value`, `reference_index`.

   Both the validator and the tests use it. A test asserts that every shortcut in `TABLE_META` is covered, so a new shortcut cannot skip the check.
2. **A new validator check 21, "shortcut values".** For the entries already sampled for check 16, fetch the lake's rows (the query check 16 already runs) and compare every shortcut with `expected_shortcuts`. Lists compare in order and dates as ISO strings. Any mismatch fails the check and names the table, column, accession, expected value and actual value.
3. **Guards against upstream rewording** (D3, D4):
   - The transform runs `SELECT DISTINCT entryType` on the staged Parquet and stops if a value is outside `{'UniProtKB reviewed (Swiss-Prot)', 'UniProtKB unreviewed (TrEMBL)'}`. The validator repeats the check (check 3).
   - Check 20 becomes a failure in `--release` mode for the shortcuts a full release must populate: every shortcut except those that can legitimately be all-empty, which are `ligand_id`, `alt_protein_names` and `gene_synonym` only if proven necessary on a slice. VALIDATE in `upjson2lake.nf` passes `--release`.

**Files.** New `bin/shortcut_reference.py`; `bin/validate_lake.py`, `bin/parquet_transform.py`, `upjson2lake.nf`, README (validation list), RUNBOOK (check 21, the `entryType` stop).

**Tests (first).**
- `tests/test_roundtrip.py`: for every fixture entry, every shortcut of every table equals `expected_shortcuts`. This single test replaces the scattered value tests and closes the "missing value tests" finding (WP13).
- `tests/test_validate.py`: mutation tests, one per mutation, following the existing `_rewrite_first_file` pattern. Each sets one of these in every row: `protein_name_primary='WRONG'`, `go_id=[]`, `lit_pubmed_id=[]`, `date_created=NULL`, `features.organism_name='x'`, `start_pos+1`, `reviewed` flipped. The validator must exit 1 with check 21 failed. All of these pass validation today.
- `tests/test_parquet_transform.py`: an input whose Swiss-Prot `entryType` reads "UniProtKB reviewed (SwissProt)" stops the build with a message naming the value.
- `tests/test_validate.py`: in `--release` mode, a lake with `go_id` all empty fails check 20; without `--release` it only warns.

**Done when.** Every mutation above fails validation, the fixture lakes still pass, and a reworded `entryType` stops the transform.

---

### WP3 · `-resume` can mark a damaged published release complete (high)

**Problem.** When a head job dies during `publishDir` copying, the documented recovery is to re-run with `-resume`. Nextflow then serves every task from cache and, on resume, does not overwrite files that already exist in `publishDir`. VALIDATE checked the work-directory lake, not the published copy, and `onComplete` renames `.RELEASE_COMPLETE.pending` without looking at `release_dir`. A reviewer reproduced this: they deleted a published Parquet file and truncated `sorted.jsonl.zst`, and after `-resume` `RELEASE_COMPLETE` was written anyway.

**Fix.**
1. Set `overwrite: true` explicitly on every `publishDir` in `upjson2lake.nf`. Nextflow's default is `false` on resume. Cached outputs are then republished in full.
2. Add `bin/verify_release.py`. It checks a published `<release>/` directory against its own metadata:
   - every file listed in `lake/SHA256SUMS.txt` exists, with the size recorded in `manifest.json` `file_details`;
   - no extra `*.parquet` files exist under `lake/`;
   - `sorted.jsonl.zst`, `validation_report.*`, `SCHEMA.md` and `provenance.json` exist, and their sizes and SHA-256 match `provenance.json`;
   - `--full` also re-hashes every file.
3. In `onComplete`, before renaming `.pending`, run `verify_release.py <release_dir>` (sizes only, which is fast; the SHA-256 option is off by default). Rename only if it passes. Otherwise log why and leave no marker.
4. RUNBOOK: after an interrupted publish, `-resume` is safe again. Mirrors are told to run `verify_release.py --full` if they want hash-level certainty.

**Files.** `upjson2lake.nf`, new `bin/verify_release.py`, `bin/release_manifest.py` (record the sizes and SHA-256 of the published sidecars if it doesn't already), RUNBOOK, README ("What a release contains").

**Tests (first).**
- `tests/test_workflow.py` (needs Nextflow):
  - after a successful run, delete one published Parquet file and truncate `sorted.jsonl.zst`, then re-run with `-resume`: the files are restored and `RELEASE_COMPLETE` is present;
  - with `verify_release.py` forced to fail, for example with a stub `publishDir` target made read-only, no `RELEASE_COMPLETE` is written.
- `tests/test_release_verify.py`: unit tests of `verify_release.py` for a missing file, a wrong size, an extra Parquet file and a truncated JSONL.

**Done when.** The reviewer's reproduction republishes correctly, and a damaged `release_dir` never gets a marker.

---

### WP4 · Release-marker lifecycle is untested (medium)

**Problem.** No test covers any of the following:
- the `.RELEASE_COMPLETE.pending` → `RELEASE_COMPLETE` rename;
- removal of a stale marker at the start of a re-run;
- the absence of a marker after a failure.

**Fix and tests.** Add to `tests/test_workflow.py`:
- **Success:** `RELEASE_COMPLETE` exists and `.RELEASE_COMPLETE.pending` does not.
- **Failure:** a run with a wrong `--expected_count` (VALIDATE fails) leaves neither the marker nor `.pending`.
- **Stale marker:** plant a stale `RELEASE_COMPLETE`, then launch a run that fails. The stale marker is gone.
- **Resume:** covered by WP3.

**Done when.** These tests pass under Nextflow.

---

### WP5 · Out-of-memory handling and retries (medium)

**Problem.**
- DuckDB's own `OutOfMemoryException` exits with status 1, but `oom_retry` only retries 137, 140 and 143, so the documented doubled-memory retry never happens for the most likely out-of-memory error.
- 143 is also what a SLURM wall-time kill produces, so a timeout is retried with the same time limit.
- `DUCKDB_MEM` truncates to whole GB, so small allocations become `0GB`. This breaks the RUNBOOK's step 7b.

**Fix.**
1. **Exit 75 on DuckDB OOM.** In `bin/parquet_transform.py`, `bin/sort_jsonl.py` and `bin/validate_lake.py`, catch `duckdb.OutOfMemoryException` in `main()`, print a clear message, and exit with a dedicated code, 75 (`EX_TEMPFAIL`: a temporary failure worth retrying). Add 75 to `oom_retry`.
2. **Scale the time limit on retry.** Heavy processes get `time { [base * task.attempt, cap].min() }`, so a 143 retry (out of memory or wall time; they can't be told apart) also gets more time. The caps are documented in `nextflow.config` and must not exceed the SLURM queue's maximum.
3. **Compute `DUCKDB_MEM` in MB**: `int(bytes * pct / 100 / 1048576)` followed by `MB`, in all four processes. Use one Groovy helper `duckdb_mem(task)` instead of four copies of the Python one-liner.
4. **Rewrite RUNBOOK 7b** to trigger a DuckDB out-of-memory error with a small `--process_memory`, then expect exit 75, a retry at twice the memory, and success.

**Files.** `bin/parquet_transform.py`, `bin/sort_jsonl.py`, `bin/validate_lake.py`, `upjson2lake.nf`, `nextflow.config`, RUNBOOK.

**Tests (first).**
- **Exit code:** `tests/test_parquet_transform.py` and `tests/test_sort_jsonl.py` run each script with `--memory-limit 50MB` on the diverse fixture and expect return code 75 and the message.
- **Retry (Nextflow):** with `--process_memory` small enough to hit DuckDB's out-of-memory error at attempt 1 but not at attempt 2, `trace.txt` shows two attempts and the run succeeds.
- **Memory helper:** a Groovy-free check that 300 MB at 75% gives `225MB`.

**Done when.** A DuckDB out-of-memory error is retried at double memory, and RUNBOOK 7b works as written.

---

### WP6 · `accession_map` mappings are never checked (medium)

**Problem.** Check 18 (`check_accession_map`) checks counts, duplicates, orphans and the keys of primary rows. It never checks that a secondary accession maps to the entry that lists it. A reviewer remapped 7,297 secondary rows and the lake still passed.

**Fix.** Extend check 18 with set comparisons in DuckDB, which spill at full scale:
- `(accession, primary_accession)` for the non-primary rows equals `SELECT unnest(secondaryAccessions), accession FROM entries`. Compute `EXCEPT` in both directions; both counts must be 0.
- Primary rows have `accession = primary_accession`.
- Secondary rows carry their primary entry's `reviewed` and `organism_id`: join on `primary_accession = entries.accession`.
- When no entry has `secondaryAccessions` (a subset), check only that there are no non-primary rows.

**Files.** `bin/validate_lake.py`, README (check 18).

**Tests (first).** `tests/test_validate.py` has three mutation tests:
- rotate `primary_accession` among the secondary rows;
- set a primary row's `primary_accession` to another accession;
- flip `reviewed` on a secondary row.

Each must fail check 18.

**Done when.** All three fail validation and the fixture lakes pass.

---

### WP7 · VALIDATE and PROVENANCE are not re-run when imported code changes (medium)

**Problem.** Nextflow hashes the scripts named in a process, not the modules they import. VALIDATE imports `reconstruct.py` and `parquet_transform.py`, and PROVENANCE imports `parquet_transform.py` through `gen_schema_md.py` and `release_manifest.py`. A fix to any of them is served from cache under `-resume`, and the old PASS is republished.

**Fix.**
- Compute a digest of every `bin/*.py` once, in a script-level function `bin_digest()`: the MD5 of the sorted file contents.
- Write `# code: ${bin_digest()}` into the script block of VALIDATE and PROVENANCE, so any change to `bin/` re-runs them. They are cheap compared with the transform.
- Do the same for PARQUET_TRANSFORM and SORT_JSONL, but only if they import siblings. PARQUET_TRANSFORM doesn't today; add a comment saying why it's excluded.

**Files.** `upjson2lake.nf`.

**Tests (first).** In `tests/test_workflow.py`, after a successful run, append a comment to a copy of `bin/reconstruct.py` in the launch directory's project copy and re-run with `-resume`. VALIDATE is not cached; `trace.txt` shows it as `COMPLETED`, not `CACHED`.

**Done when.** That test passes.

---

### WP8 · `schema()` gives a wrong `join_hint` for `accession_map` (medium)

**Problem.** The hint is built from the foreign-key column names (`USING (primary_accession)`), which fails because `entries` has no `primary_accession` column.

**Fix.** Build the hint from each foreign key's target: `JOIN {table} ON {table}.{col} = {ref_table}.{ref_col}`, one clause per foreign key, joined with `AND` when they share a target table. For child tables this gives `ON features.accession = entries.accession AND features.organism_id = entries.organism_id`.

**Files.** `uniprot_parquet.py`, README (client section, if it quotes a hint).

**Tests (first).** In `tests/test_client.py`, execute every `join_hint` from `schema(lake)` against the fixture lake: `SELECT count(*) FROM entries <hint>` must run.

**Done when.** Every hint executes.

---

### WP9 · Provenance marks every default run dirty (medium)

**Problem.** `run_lake.sh` writes to `datalake/` inside the repository, and `.gitignore` doesn't list it. So `git status` is never clean, `provenance.json` always records `dirty: true`, and RUNBOOK 8a's precondition fails.

**Fix.**
- Add `datalake/`, `demo/lake/`, `demo/work/`, `work/`, `.nextflow*` and `results/` to `.gitignore`, checking each against what is already there.
- Keep untracked files counting as dirty, because an untracked new `bin/` script does matter.

**Files.** `.gitignore`, RUNBOOK 8a.

**Tests (first).** In `tests/test_validate.py`, extend the `release_manifest` tests: in a temporary git repo with an ignored `datalake/` output, `git_info()` reports `dirty: False`.

**Done when.** A default small run from a clean checkout records `dirty: false`.

---

### WP10 · Swiss-Prot is almost absent from the validation sample (low; requested)

**Problem.** Checks 6, 13 and 16, and the new check 21, draw one uniform 1,000-entry reservoir from about 250M lines. With about 0.23% Swiss-Prot, that is roughly 2 Swiss-Prot entries. Swiss-Prot-only structures therefore almost never get reconstructed in production: most comment types, ligands, rich citations, `includes`/`contains`.

**Fix: fetch more Swiss-Prot into the sample.** Make `sample_and_count_jsonl` stratified:
- **Two reservoirs.** One for Swiss-Prot and one for TrEMBL, each of size `--spot-check-n` (production: 1,000 + 1,000), in the same single decompression pass.
- **Classifying a line without parsing it.** Use a compiled bytes regex, `rb'"entryType"\s*:\s*"([^"]*)"'`, searched once per line: C speed, no `json.loads`. `reviewed` means "Swiss-Prot" is in the value. A line without a match is a failure, reported by check 3.
- **Sampled lines still parse as today.** Only the lines that enter a reservoir go through `json.loads`, as now.
- **Checks 6, 13, 16 and 21 run on the union.** Their report lines show the counts per stratum, for example "reconstruction matches JSONL for 1000/1000 Swiss-Prot and 1000/1000 TrEMBL entries".
- **Swiss-Prot is contiguous in the sorted file.** The sorted JSONL puts Swiss-Prot first, but the sampler must not rely on that; the regex is the classifier.
- **The option.** Add `--spot-check-n-reviewed` for a different Swiss-Prot size, defaulting to `--spot-check-n`. VALIDATE keeps passing `--spot-check-n 1000`.

**Files.** `bin/validate_lake.py`, `upjson2lake.nf` (no change needed if the defaults suffice), README (checks 6, 13, 16), RUNBOOK (expected report lines).

**Tests (first).**
- In `tests/test_validate.py`, a unit test of `sample_and_count_jsonl` on a synthetic JSONL of 10 Swiss-Prot and 5,000 TrEMBL lines with n=5: exactly 5 Swiss-Prot and 5 TrEMBL are sampled, and the line count is exact. Also run it with a single stratum (all TrEMBL).
- `test_validator_decompresses_jsonl_once` still passes, so the pass is still single.
- The report of the validator run on the diverse lake shows both strata.

**Done when.** A production-shaped input yields a 1,000 + 1,000 sample in one pass, and every sample-based check reports both strata.

---

### WP11 · Validator and reconstruction correctness details (low)

1. **A `k=v` directory above the lake makes a correct lake fail.** DuckDB's hive auto-detection adds a column for any `key=value` directory in the path.
   - **Fix:** every validator `read_parquet` passes `hive_partitioning = false`, since the stored `reviewed` column is authoritative and check 19 reads paths itself. Add the same argument to the helper that builds validator globs.
   - **Test:** copy a valid lake to `tmp/rel=2026_01/lake`; validation passes.
2. **Reconstruction treats `5 == 5.0 == True`** (D5).
   - **Fix:** `normalize_value` maps `bool` to a tagged value (`("bool", True)`), so a bool never equals a number. Ints and floats stay numerically equal, and the README lists that as the fourth normalisation, with the reason: a field mixing integers and decimals is stored as DOUBLE.
   - **Test:** `entries_match({"x": True}, {"x": 1})` is false; `entries_match({"x": 5.0}, {"x": 5})` is true.
3. **Check 15 (comment text) can run out of memory at scale.** Its per-type `list(DISTINCT … ORDER BY …)` of accessions for NULL `text_value` rows runs for every comment type, including the non-text ones where every row matches, and it doesn't spill.
   - **Fix:** restrict the query to `commentType IN (TEXT_COMMENT_TYPES)`, and take the examples with a second query, `… WHERE text_value IS NULL AND commentType = ? LIMIT 5`, run only for failing types.
   - **Test:** the existing check 15 tests still pass. A unit test on a synthetic comments table of 2M non-text rows at `memory_limit 256MB` completes.

**Files.** `bin/validate_lake.py`, `bin/reconstruct.py`, README (normalisations).

---

### WP12 · Transform robustness (low)

1. **A child array present but empty in every entry aborts the build.** `comments: []` everywhere is inferred as `JSON[]` with no fields, and `check_required_paths` calls the input "not a UniProtKB dump".
   - **Fix:** a parent array counts as present for required-child purposes only if at least one child path exists under it. Otherwise it is treated like an absent array, in three places: `check_required_paths`, `_source_keys` (use `_EMPTY_CHILD_KEYS`) and `_child_source` (use `_EMPTY_CHILD_SOURCE`). `entries.comment_count` stays 0.
   - **Test:** set `comments = []` on all 52 small-fixture entries. The build succeeds, the comments table has zero rows and the keys `commentType`, and every entry reconstructs.
2. **File roll-over only happens after whole batches**, so each file holds at least one full `--batch-size` batch and targets of 256 MB and 512 MB give the same files.
   - **Fix:** `write_part` writes each batch in slices of `ROW_GROUP_SIZE` rows and checks the file size after each slice, rolling over when the target is reached. Files are then at most one row group over target.
   - **Test:** on the diverse fixture with `--batch-size 4000` and `--target-file-bytes 256000`, there is more than one entries file, and each is at most target plus the largest row group. Targets 256 KB and 512 KB give different file counts.

**Files.** `bin/parquet_transform.py`.

---

### WP13 · Shortcut value tests (low)

**Problem.** Several shortcuts have no value-level test on the real fixture: `start_pos`, `end_pos`, `ligand_id`, `evidence_codes`, `alt_protein_names`, `gene_synonym`, `keyword_categories`, the `go_terms` fields, and `protein_name_primary` on real data.

**Fix.** Nothing extra: WP2's test compares every shortcut of every table with `expected_shortcuts` over the whole diverse fixture. It also needs a coverage assertion: every shortcut in `TABLE_META` appears in `expected_shortcuts`' output.

**Done when.** WP2's tests pass and the coverage assertion lists no missing shortcut.

---

### WP14 · Naming and client API consistency (low; D1, D2, D6)

1. **Rename `organism_name` to `organism_scientific_name`** in entries and features (D1).
2. **Rename `xref_proteomes` to `proteome_id`** (D2).
3. **Rename `files_for_taxid()` to `files_for_organism_id()`**, and the manifest `file_details` keys `taxid_min`/`taxid_max` to `organism_id_min`/`organism_id_max` (D6).
   - **Also:** `files_for_organism_id()` raises `ValueError` for a table not sorted by organism (`accession_map`), rather than silently returning every file. Fix its docstring.
4. **Fix the README rule sentence.** It must stay true after the renames: every shortcut that shares a REST name holds the same value.
5. **Change the `keyword` / `keywordid` description examples** from the retired "Complete proteome" / KW-0181 to current ones, such as "Kinase" / KW-0418.

**Files.** `bin/parquet_transform.py`, `uniprot_parquet.py`, `setup_views.sql`, `bin/validate_lake.py` (check 19 and the manifest keys, if read), README, RUNBOOK, `demo/demo.ipynb` code, benchmarks, tests. Use the same word-boundary rename method as the 2026-09-30 renames, and leave the historical VARIANT scripts alone.

**Tests.**
- The existing suite, renamed.
- `test_client`: `files_for_organism_id(lake, 9606, table="accession_map")` raises `ValueError`.
- A README test asserts that the list of REST-named shortcuts in the README matches a curated table in the test, with a comment citing `rest.uniprot.org/configure/uniprotkb/result-fields`.

---

### WP15 · Consumer docs, macros and demo (low)

1. **Metalink.** `curl --metalink` stopped working in curl 7.78.
   - **Fix:** the README download section uses `aria2c -M RELEASE.metalink --metalink-base-uri=https://<host>/<release>/lake/`, and says the `<url>` entries are relative to the lake directory. Remove the curl claims at both places they appear.
2. **`setup_views.sql`'s remote example** points glob views at an `https://` base, which DuckDB cannot glob.
   - **Fix:** remove the https usage example and point to `uniprot_parquet.connect()` for HTTP(S), as the file's own header already says.
3. **Wrong macro comments.**
   - **Fix, in both SQL copies:** `protein_card` returns one `entries` row, not "entry + features + xrefs". `organism_comments` uses 'FUNCTION' as its example and says `text_value` is NULL for comment types whose prose is in `note` (SUBCELLULAR LOCATION, DISEASE) or that are structured (COFACTOR, CATALYTIC ACTIVITY, …).
4. **Single-accession macros scan the whole `accession` column.**
   - **Fix:** rewrite `protein_card` and `unnest_isoforms` to resolve the accession through `accession_map` first: get `(reviewed, organism_id, primary_accession)`, then filter the target table on all three. As a bonus, `protein_card('<secondary accession>')` then returns the current entry.
   - **Before changing:** check with `EXPLAIN ANALYZE` on the diverse lake that DuckDB prunes row groups from the subquery values. If it does not, keep the single-filter form and document the two-step query instead of rewriting.
   - **Test:** `protein_card` on a secondary accession returns its primary entry.
5. **Demo notebook code.**
   - **The feature-coverage cell:** it filters `length > 100` on 70–75 aa proteins, so it is always empty, and it sums overlapping spans. Filter `length >= 70`, and count distinct covered residues with `unnest(range(start_pos, end_pos + 1))`, so coverage never exceeds 100%.
   - **The isoform cell:** it calls `.df()` inside an `if`, so nothing is displayed. Restructure it so the frame is the cell's last expression.
   - **Stale outputs:** regenerate every saved output with `demo/run_demo.sh --clean` and a Jupyter re-run. This needs `nbconvert` or Jupyter; if the sandbox cannot run it, it is a manual step for the owner.
6. **RUNBOOK counts.** Replace hard-coded counts (tests passed, "172/172" checks) with statements that don't go stale ("all passed", "0 failed"), or with the current numbers checked by a test.

**Files.** README, `setup_views.sql`, `uniprot_parquet.py`, `demo/demo.ipynb`, RUNBOOK.

**Tests.**
- `test_readme_examples` still passes.
- The new `protein_card` secondary-accession test.
- Execute the notebook's code cells against a fixture lake in a test (a helper like the one used on 2026-09-30), skipping plot cells when matplotlib is missing. Assert that the coverage cell returns rows and that every coverage value is ≤ 100.

---

### WP16 · Provenance does not identify the source dump (low)

**Problem.** `provenance.json` records only the derived `sorted.jsonl.zst`. It has no input filename, size or checksum, no expected count, no tool versions and no run parameters.

**Fix.**
- **STREAM_JSONL:** already reads the whole input, so it computes the input's SHA-256 while streaming (hashlib over the bytes it reads) and writes a sidecar, `input_manifest.json`, with filename, size, SHA-256 and `expected_count`.
- **PROVENANCE:** receives the sidecar and records it under `inputs.source`, together with these tool versions:
  - Python, DuckDB and PyArrow (from the task's Python);
  - Nextflow (`workflow.nextflow.version`, passed as a value);
  - the run parameters: `process_memory`, `duckdb_pct`, the batch and file targets, and the release.

**Files.** `bin/stream_jsonl.py`, `bin/release_manifest.py`, `upjson2lake.nf`.

**Tests (first).**
- `tests/test_stream_jsonl.py`: the sidecar's SHA-256 equals `hashlib.sha256` of the fixture file.
- `tests/test_validate.py` (`release_manifest`): `provenance.json` has `inputs.source.sha256`, `tools.duckdb` and `params.release`.

---

## Order of work

1. **Correctness of the data and the gate:** WP1 → WP2 (with WP13) → WP6 → WP10. After these, the validator checks what the lake says, not just that it can be rebuilt.
2. **Release safety:** WP3 → WP4 → WP7 → WP5.
3. **Robustness and details:** WP11 → WP12 → WP16 → WP9.
4. **Naming and consumer polish, after D1/D2/D6 are confirmed:** WP14 → WP8 → WP15.

Each group is a reviewable unit on its own: the suite must pass at the end of each.

## Final verification

1. **Full suite:** run it with Nextflow on PATH; it must all pass, with none skipped.
2. **End to end:** build the diverse fixture, the small fixture and the reviewer's `trembl_like.jsonl` with `bin/parquet_transform.py`, then validate each with `--release` and `--spot-check-n 1000`:
   - every check passes;
   - the report shows both Swiss-Prot and TrEMBL strata;
   - check 21 compares every shortcut.
3. **Mutation sweep:** re-run the reviewers' mutations from `scratchpad/review/` (shortcut corruptions, a remapped `accession_map`, a reworded `entryType`, a damaged published copy followed by `-resume`). Each must now fail, or be republished.
4. **Nextflow small run:** from a clean checkout, `run_lake.sh small`. Then check:
   - `provenance.json` records `dirty: false`, the source checksum and the tool versions;
   - `verify_release.py --full` passes on the published release;
   - `RELEASE_COMPLETE` is present.
5. **Consistency greps:** no remaining `files_for_taxid`, `taxid_min`, `xref_proteomes` or `organism_name` (after WP14) outside the historical VARIANT benchmarks.
6. **Demo:** the notebook's code cells run against a fixture lake. Regenerate the saved outputs where Jupyter is available.

## Appendix: findings → work packages

| Finding (review 2026-09-30) | Severity | WP |
| --- | --- | --- |
| `ec` ignores `includes`/`contains` EC numbers (reported three times) | high | WP1 |
| No validator check of shortcut values; reworded `entryType` passes (reported twice) | high | WP2 |
| `-resume` writes `RELEASE_COMPLETE` over a damaged published copy | high | WP3 |
| DuckDB OOM exits 1, never retried; 143 retried without more time | medium | WP5 |
| `accession_map` secondary mappings never checked (reported twice) | medium | WP6 |
| `schema()` `join_hint` for `accession_map` is invalid | medium | WP8 |
| Imported `bin/` modules are not in the VALIDATE task hash | medium | WP7 |
| `provenance.json` always `dirty: true` on default runs | medium | WP9 |
| No test of the release-marker lifecycle (reported twice) | medium | WP4 |
| RUNBOOK 7b OOM test cannot work (`0GB`) | medium | WP5 |
| `organism_name` / `xref_proteomes` differ from the REST fields of the same name (reported twice) | low | WP14 (D1, D2) |
| Empty child arrays abort the build | low | WP12 |
| File roll-over only after whole batches | low | WP12 |
| About 2 Swiss-Prot entries in the 1,000-entry production sample | low | WP10 |
| A `k=v` directory above the lake fails a correct lake | low | WP11 |
| Reconstruction treats `5 == 5.0 == True` | low | WP11 |
| Check 15 may run out of memory at scale (plausible) | low | WP11 |
| `curl --metalink` no longer works; relative metalink URLs | low | WP15 |
| `setup_views.sql` remote example can't glob over HTTPS; `protein_card` comment wrong | low | WP15 |
| `organism_comments` example type always has NULL text | low | WP15 |
| `files_for_taxid` claims every table is organism-sorted; `taxid_*` manifest keys | low | WP14 |
| Retired keyword used as description example | low | WP14 |
| RUNBOOK expected counts out of date | low | WP15 |
| Demo feature-coverage cell always empty and over 100%; isoform cell shows nothing | low | WP15 |
| Several shortcuts have no value-level test | low | WP13 (via WP2) |
| `provenance.json` doesn't identify the source dump | low | WP16 |
| Single-accession macros scan the whole accession column | low | WP15 |
| *Refuted:* `connect()` silently serves partial tables (a documented feature) | — | none |
