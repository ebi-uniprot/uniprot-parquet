# Next tasks after the schema-v2 implementation

**Written:** 2026-09-17, after commit `931507b` on `static-lake` (22 commits: one per step of `PLAN_SCHEMA_V2_STEPS.md` plus review fixes).
**Read first:** `PLAN_SCHEMA_V2.md` (design, Results log with every measured or deferred item) and `PLAN_SCHEMA_V2_STEPS.md` (the work order; corrections made while implementing are marked in place).
**State of the tree:** 158 tests pass on the default and `--stress` fixtures; the demo lake (`demo/lake/2026_01/`, gitignored) regenerates end to end with Nextflow and passes all 160 validator checks; `SCHEMA.md` is generated from it.

This file lists what is *not* done, grouped by what it needs. Every item names where its result is recorded.

---

## 1. Needs the F.1 measurement slice (cluster / network; nothing runnable from the sandbox)

The slice is human reviewed + unreviewed plus 1M random TrEMBL entries. Build it once and keep `slice.sorted.jsonl.zst`; everything below reads from it. Commands are Step 3 of the work order, reproduced here:

```bash
curl --globoff -o human.json.gz \
  "https://rest.uniprot.org/uniprotkb/stream?query=organism_id:9606&format=json&compressed=true"
pigz -dc human.json.gz | python bin/stream_jsonl.py > human.jsonl
pigz -dc UniProtKB.json.gz | python bin/stream_jsonl.py \
  | python bin/sample_jsonl.py --n 1000000 --where trembl --seed 1 > trembl_1m.jsonl
cat human.jsonl trembl_1m.jsonl | zstd -3 -T0 -o slice.jsonl.zst
python bin/sort_jsonl.py slice.jsonl.zst -o slice.sorted.jsonl.zst --memory-limit 16GB
python bin/parquet_transform.py slice.sorted.jsonl.zst --outdir slice/lake --release slice --memory-limit 16GB
python bin/validate_lake.py --lake slice/lake --jsonl slice.sorted.jsonl.zst -o slice/validation_report.txt
```

| # | Task | Command / query | Record in |
| --- | --- | --- | --- |
| 1.1 | **F.1 table**: bytes per table and side, extrapolation to 248M, `entries` share, footer bytes | Step 3 items 3–4 (use `stats_min_value`/`stats_max_value` in `parquet_metadata`, not `stats_min`/`stats_max`) | `PLAN_SCHEMA_V2.md` F.1 table + Results log; `AUDIT.md` §6.1; README "Download" table sizes (currently *pending*) |
| 1.2 | **D.5 human-tier ratio**: bytes of `entries` files containing any `taxid = 9606` row group vs the human rows alone; revisit `organism=human\|other` only if > ~10× | Step 3 item 5; `uniprot_parquet.files_for_taxid(lake, 9606)` gives the file list | Results log D.5 row; D.5 `Result:` line |
| 1.3 | **Phase 0.3 on the slice**: `candidate_groups / row_groups` for one accession | plan §0.3 query (already corrected to `stats_min_value`) | Results log 0.3 row |
| 1.4 | **Part B size gates** on the slice (`go_terms`, `pubmed_ids`, `proteome_ids`, B.5 `function_text`) | Step 6 "Verify" queries; fixture-proxy numbers are in the Results log | Results log B row; B.5 decision (fixture proxy says FUNCTION text is far above the 5 % rule → stays deferred unless the slice disagrees) |
| 1.5 | **A13 byte delta** on the slice: rebuild the slice at commit `9774283^` (pre-trim) and at HEAD, compare per-table bytes | `git worktree add /tmp/pretrim 9774283^` then run both transforms | F.1 "after v2" column; Results log A13 row |
| 1.6 | **H.1 zstd level**: confirm or revise the provisional `ZSTD_LEVEL = 9` | `python benchmarks/bench_zstd.py --jsonl slice.sorted.jsonl.zst --out benchmarks/results/` | plan H.1 table; `ZSTD_LEVEL` in `bin/parquet_transform.py`; README "Production notes" |
| 1.7 | **F.4 file-size target**: 256 MB vs 512 MB vs 1 GB from footer/data bytes; page-index delta | `python benchmarks/bench_file_size.py --jsonl slice.sorted.jsonl.zst --out benchmarks/results/` | plan F.4 table; `TARGET_FILE_BYTES`; README |
| 1.8 | **Phase 3 point-lookup benchmark** (pass criteria, request counts, C.3) | `python benchmarks/bench_point_lookup.py --lake slice/lake --serve --label slice` for request/byte counts; `--url https://<real server>/…` for remote wall times (the local `http.server` adds a ~1 s per-query artefact) | Results log Phase 3 and C.3 rows; plan §8 checklist |

`benchmarks/results/*.json|txt` are gitignored by convention; copy the tables into the plan.

## 2. Needs the first full build (cluster)

| # | Task | Notes | Record in |
| --- | --- | --- | --- |
| 2.1 | **Schema guards on the full input.** `check_declared_types()` and `check_promoted_paths()` run right after staging and stop the build if the full dump has a nested field the declared types (`COLUMN_TYPES`, esp. the residual structs) do not list, or a struct child no column carries. Expect this at least once: extend the type string (DuckDB `DESCRIBE` output, verbatim) or promote the field, then rerun with `--skip-existing`. Staging is redone on a rerun (~2–4 h). | Consider staging once to a kept path and a `--staged` flag if this bites more than once. | commit |
| 2.2 | **D.5 division gate**: per-division Swiss-Prot and TrEMBL entry counts must equal the FTP `taxonomic_divisions/uniprot_{sprot,trembl}_<division>.dat.gz` counts for the same release. The CASE in `_build_entries_sql` is a first approximation (protists → `invertebrates`, everything unmatched → `unclassified`). Fix until counts match exactly, then rebuild. | `SELECT division, reviewed, count(*) FROM entries GROUP BY 1,2` | Results log D.5 row; D.5 `Result:` line; `SCHEMA.md` regen |
| 2.3 | **Validator on the full build**: all 19 checks (check 8 recomputes every file's SHA-256 — one read of the lake; check 16 reconstructs the 1,000-entry sample). Time budget in README "Production notes" is unmeasured for the new checks. | `upjson2lake.nf` `VALIDATE` | README time table |
| 2.4 | **Phase 3 benchmark on the full build** (in addition to the slice). | as 1.8 | Results log |
| 2.5 | **Part E xrefs skew and source-order queries** (plan Part E items 2–3). | queries in plan Part E | plan Part E |
| 2.6 | **`accession_map` sort spill and wall time**; confirm the "low tens of GB" note in `upjson2lake.nf` and README. | | README "Production notes" |
| 2.7 | **Regenerate `SCHEMA.md`** from the release build so the "Rows in this build" column is real. | `python bin/gen_schema_md.py --lake <release>/lake -o SCHEMA.md` | commit |

## 3. Decisions to take (no code until decided)

| # | Decision | Where it is framed | Current state |
| --- | --- | --- | --- |
| 3.1 | **Array order in `g()`**: the child tables carry no position index, so reconstruction is order-independent inside arrays. Add position columns (`feature_index`, `xref_index`, …) or accept. | Results log Step 10 row; `bin/reconstruct.py` docstring | Accepted for now (order-independent gate) |
| 3.2 | **Retention policy (F.5)** in UniProt's own terms; write it into README "Versioning". | plan F.5 | README says nothing yet |
| 3.3 | **Cloud channel (F.6)** and the canonical download host; fill the `<host>` placeholders in README "Download". | plan F.6, `AUDIT.md` A3 | placeholders |
| 3.4 | **Publish step (A5)**: atomic swap, `latest`/`previous`, `releases.json` (format fixed in plan F.2.5). `SHA256SUMS.txt`, `RELEASE.metalink`, `RELEASE_COMPLETE` already exist. | `AUDIT.md` A5 | not started |
| 3.5 | **Bloom filters (B1)**: PyArrow 23.0.1 cannot write them (Phase 0.1). Re-run the 0.1 snippet on each PyArrow upgrade; `_bloom_kwargs()` is the single hook. Minor schema bump when it lands. | plan §4.3 | deferred |
| 3.6 | **`--variant-children`**: PyArrow 23 cannot write VARIANT either (`Unsupported Arrow type VARIANT`); the flag is evaluation-only. Keep or drop. | `benchmarks/VARIANT_EVALUATION.md` | keep, documented |
| 3.7 | **Nextflow version**: ≥ 26.04 needs `NXF_SYNTAX_PARSER=v1`; either pin < 26 in `environment.yml` or port `upjson2lake.nf` to the strict syntax (move the top-level `def release_dir` into the workflow). | README "Setup" | env var documented |

## 4. Minor bumps after launch (formats already fixed in the plan)

- `croissant.json` (plan F.2.1, needs `mlcroissant` and the A6 config).
- `releases.json` top-level index (F.2.5).
- Hugging Face Swiss-Prot `entries` tier (F.2.6).
- H.3 enumerations in the manifest / `SCHEMA.md`.
- Phase 4 macros (`protein_card` / `unnest_isoforms` resolving through `accession_map`).
- `validation_report.json` embedded in `provenance.json` (`AUDIT.md` A21).
- Semver enforcement in `check_schema_evolution` starts with the second public release (`AUDIT.md` A10).

## 5. Known gotchas for whoever picks this up

- **`parquet_metadata()` statistics**: DuckDB's `stats_min`/`stats_max` are the legacy Parquet fields and are NULL for PyArrow-written string columns; use `stats_min_value`/`stats_max_value` (the validator, benchmarks and plan queries already do).
- **Stress suite memory**: ~8 GB peak; the round-trip row-dict fixtures are module-scoped for that reason — do not make them session-scoped.
- **File modes on virtiofs**: rewriting `bin/*.py` from a script can drop the executable bit; check `git diff --summary` for `mode change` before committing (the Nextflow processes call the scripts directly).
- **Demo lake is gitignored**; `cd demo && NXF_SYNTAX_PARSER=v1 ./run_demo.sh --clean` regenerates it (no network needed while `demo/input.json.gz` exists).
- **Remote benchmark timings** over the local Python `http.server` include a ~1 s per-query artefact; use its request/byte counts, and a real server for wall times.
- **Sandbox environment** (if work continues there): `/home/agent/venv`, DuckDB `httpfs` from PyPI, Nextflow launcher in `~/bin`; `rest.uniprot.org`, the FTPs and `extensions.duckdb.org` are unreachable.
