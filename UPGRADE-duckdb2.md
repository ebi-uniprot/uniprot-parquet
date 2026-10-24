# DuckDB 2.0 upgrade plan

Status: **Task 0 done; Tasks 1–6 planned.** The first full release builds on DuckDB
1.5.6, the version every local and Codon check so far has run. The upgrade
follows that release, on DuckDB **2.0.1** (planned for 2026-11-18), except
Task 0, which is due now.

Every claim below cites its evidence: a DuckDB page (quoted verbatim,
fetched 2026-10-23), a command and its output, or a file and line. File and
line numbers refer to commit `f4cfd81`.

## Evidence base

**DuckDB's pages** (fetched 2026-10-23, quoted verbatim):

- [Announcement](https://duckdb.org/2026/10/21/announcing-duckdb-200) (B)
- [Migration guide](https://duckdb.org/docs/2.0/operations_manual/migration) (M)
- [Python migration guide](https://duckdb.org/docs/2.0/clients/python/migration) (P)
- [Release calendar](https://duckdb.org/release_calendar) (C)
- [Release notes on GitHub](https://github.com/duckdb/duckdb/releases/tag/v2.0.0) (G): 837 pull requests

**Measurements**: Linux aarch64, 11 cores, 8 GB RAM; Python 3.14.4 (GIL
build); PyArrow 23.0.1; DuckDB 1.5.6 against 2.0.0, both PyPI wheels (2.0.0
ships `cp312-abi3` wheels, which serve Python 3.12 and later).

**The probe**: the test suite (408 tests) run against DuckDB 2.0.0 in a copy
of the repo, with compatibility shims added one at a time to expose each
breaking change in turn (Appendix A).

| Probe run | Shims | Result |
|---|---|---|
| 1 | none | stops at the first test: `Binder Error: Deprecated lambda arrow (->) detected` |
| 2 | old lambda syntax re-enabled | 65 failed, 25 errors, 317 passed |
| 3 | + `fetchone()` restored on relations | **2 failed, 405 passed, 1 xfailed** |

Run 3's two failures: the duplicate-key message (Task 3), and
`test_git_info_ignores_working_directory`, which fails only because the
probe's copy has no `.git`.

---

## Why upgrade, and why not yet

**For:**

- **The transform is ~25% faster, with identical output.** Building the lake
  from the ~14K-entry stress fixture (`--memory-limit 4GB --threads 4`), two
  runs each:

  | | 1.5.6 | 2.0.0 |
  |---|---|---|
  | Wall time | 15.4 s, 15.2 s | 11.5 s, 11.4 s |
  | Peak RSS | 1.87, 1.80 GiB | 2.23, 2.22 GiB |
  | Lake size | 41 MB | 42 MB |

  Every table of the two lakes has the same row count, schema and data
  (`pyarrow.Table.equals` on all six tables). The transform is the longest
  step at full scale (README budget 6–12 h), so the saving is hours.
- **Possibly less memory at scale.** On the 210,706-entry human subset
  (3.3 GiB of JSONL) at `--memory-limit 6500MB --threads 4`: 1.5.6 was killed
  at 8.23 GiB RSS during staging (the machine's 8 GB ceiling); 2.0.0
  finished in 91 s at 7.91 GiB. One run each, near a hard limit: suggestive
  only.
- **2.0 has the longer support.** B: "DuckDB 2.0 is a long-term support (LTS)
  release with support until at least October 2027."
- **The old lambda syntax ends with 2.1, whichever version runs.** B: "Both
  the flag and the single-arrow syntax will be removed entirely in v2.1."

**Against doing it now:**

- **1.5 is supported long enough.** B: "we extended the support period of
  DuckDB 1.5 until March 2027, effectively also turning it into an LTS
  release."
- **DuckDB itself advises waiting.** M: "Before migrating to DuckDB 2.0.0,
  assess whether your workload is mission-critical. For such workloads,
  consider waiting for upcoming patch releases, which address issues that
  surface shortly after a major release." C lists 2.0.1 for 2026-11-18.
- **The current setup is tested.** Every check on Codon (tests, RUNBOOK
  7a/7b) ran on 1.5.6.

**Not a reason to upgrade:** the sort. DuckDB 2.0's ORDER BY still cannot
spill whole entries. Sorting the 3.3 GiB human subset (`json_type` variant,
Appendix B) fails on 2.0.0 at 2 and 3 GB with 1 thread (passes at 4 GB), and
at 2, 3, 4 and 6 GB with 4 threads, once at "6.0 GiB/3.7 GiB used". 1.5.6
behaves the same way. `bin/sort_jsonl.py`'s chunked merge sort stays.
Neither B nor G mentions a change to sorting large payloads.

---

## Task 0 — pin DuckDB in CI (done)

**Why:** CI installs DuckDB with no version:
`.github/workflows/ci.yml:32`, `:81` and `:105` say `pip install duckdb …`.
PyPI's latest is 2.0.0 (`https://pypi.org/pypi/duckdb/json` →
`info.version == "2.0.0"`), and the code fails on it (probe run 1). So the
next CI run fails, even though `environment.yml:14` and `pyproject.toml:16`
both pin `duckdb>=1.5,<2`.

**Do:** in each of the three lines, `duckdb` → `"duckdb>=1.5,<2"`. While
there, pin `pyarrow` the same way as `environment.yml` (`>=23,<24`): it is
unpinned on lines 32 and 81 too.

**Done when:** CI is green, and its log shows DuckDB 1.5.x installed.

**Status:** done. `ci.yml:34, 83, 107` pin `"duckdb>=1.5,<2"` (and `:34, 83`
`"pyarrow>=23,<24"`); in a fresh env those specs resolve to DuckDB 1.5.6 and
PyArrow 23.0.1. The first CI run after the push confirms it.

---

## Task 1 — rewrite the lambdas as `lambda x: …` (works on both versions)

**Why:** B: "The old single-arrow lambda syntax ( x -> x + 1 ) is now disabled
by default." The probe stops on the first one (run 1).

**Evidence that it works on both:**
`list_transform([1,2], lambda x: x + 1)` and
`list_filter([1,2,3], lambda x: x > 1)` give `[2, 3]` on 1.5.6 and on 2.0.0.

**Where:** 22 lambdas on 20 lines (`grep -nE "\b[a-z_]+ -> |\([a-z_, ]+\) -> "`,
less Python return annotations and the comment at `bin/parquet_transform.py:106`):

- `bin/parquet_transform.py` (18 on 16 lines): 555, 557, 913, 919 (two), 929,
  967 (two), 977, 978, 1026, 1027, 1028, 1042, 1044, 1046, 1127, 1181
- `benchmarks/bench_variant.py` (4): 102, 134, 316, 343
- Since `f4cfd81`: `_ordered_distinct` in `bin/parquet_transform.py` (`(v, i) -> …`), used by
  `ec`, `go_id` and `xref_dbs`

`validate_lake.py`, `reconstruct.py` and `uniprot_parquet.py` have none of
their own. VALIDATE reaches these through `parquet_transform.py`'s builders.

**Watch:** `x -> x.value` becomes `lambda x: x.value`. Lambdas nested inside
lambdas (919, 967), and those inside list comprehensions (1042–1046), keep
their structure.

**Done when:** `grep -nE "\b[a-z_]+ -> " bin/ benchmarks/` finds only Python
annotations and comments, and the full suite passes on 1.5.6.

## Task 2 — replace `fetchone()` on relations with `fetchall()[0]`

**Why:** P: "1.x: rel.fetchone() , rel.fetchmany(n) , rel.execute() and
rel.fetch_df_chunk() . 2.0: these methods are gone, because a relation keeps
no result between calls and runs its query again on every read." Probe
run 2's largest group of failures:
`AttributeError: Relations no longer have fetchone()`.

**The replacement:** `con.sql(q).fetchall()[0]`, **not**
`con.execute(q).fetchone()`, which P suggests for loops. Tested on both
versions:

| | 1.5.6 | 2.0.0 |
|---|---|---|
| `con.execute("select 1").fetchone()`, then `con.sql(...).fetchall()` | OK | `ResourceInUseException: connection has an unfinished result from execute(); call complete()` |
| `con.sql("select 1").fetchall()[0]`, then `con.sql(...)` | OK | OK |

P: "running another query on the connection raises a ResourceInUseException
until the result is read to its end or ended with con.complete() or
con.abort()". A `fetchone()` leaves even a one-row result unfinished. Probe
run 3 did not catch this, because its shim used `limit(1).fetchall()`, so
re-test this task for real (Task 4).

**Where:** all 30 calls are on relations (each traced to its `.sql(`):

- Pipeline (12): `bin/sort_jsonl.py:187`; `bin/parquet_transform.py:757`;
  `bin/validate_lake.py:489, 1045, 1459, 1466, 1476, 1490, 1501, 1510, 1544`;
  `uniprot_parquet.py:381`
- Benchmarks (5): `benchmarks/build_variant_lake.py:52`,
  `bench_baseline.py:221`, `bench_variant.py:616, 899`,
  `bench_point_lookup.py:168`
- Tests (13): `tests/test_client.py:18, 30, 31`; `tests/test_demo.py:87`;
  `tests/test_idempotency.py:228, 232`;
  `tests/test_parquet_transform.py:210, 214, 498, 648, 788, 1571, 1737`

The other removed methods (`fetchmany`, `fetch_df_chunk`, `rel.execute`) are
not used (`grep -rnE "\.fetchmany\(|\.fetch_df_chunk\(|rel\.execute\("`
finds nothing).

**Watch:** a query that can return no rows (`uniprot_parquet.py:381` reads a
file's content) needs `rows = ….fetchall(); row = rows[0] if rows else None`,
not a bare `[0]`. `uniprot_parquet.py` is the client users import, so the
change also matters to anyone who installs DuckDB 2.0 next to it.

**Done when:** `grep -rn "\.fetchone()"` finds only calls on `execute()`
results that are read to the end, or none; the suite passes on both versions.

## Task 3 — accept 2.0's wording of the duplicate-key error

**Why:** `bin/parquet_transform.py:192-196` turns DuckDB's error for sibling
keys that differ only in case into a clear message, by matching
`r'Duplicate name ("[^"]*") in struct'`. DuckDB 2.0 doubles the quotes.
Reproduced with the transform's own `read_json_auto` options:

```
1.5.6  NotImplementedException | Not implemented Error: Duplicate name "ab" in struct auto-detected in JSON, try ignore_errors=true
2.0.0  NotImplementedException | Not implemented Error: Duplicate name ""ab"" in struct auto-detected in JSON, try ignore_errors=true
```

So the pattern does not match, the raw error escapes, and
`TestCaseDuplicateKeys::test_refused_with_clear_message` fails (probe run 3).

**Do:** match both, e.g. `r'Duplicate name "+([^"]*)"+ in struct'`, quoting
the captured name in the message. A unit test can feed both strings to the
pattern.

**Done when:** that test passes on both versions.

## Task 4 — run everything on both versions

**Why:** Tasks 1–3 are meant to work on both versions, but probe run 3 had
shims in place, and Task 2's trap only shows without them.

**Do:**

```bash
# the usual env (1.5.6)
REQUIRE_NEXTFLOW=1 python -m pytest tests/ -q          # 431 passed, 1 xfailed
# a second env with DuckDB 2.x, no shims
pip install "duckdb>=2.0.1,<3"
REQUIRE_NEXTFLOW=1 python -m pytest tests/ -q          # 431 passed, 1 xfailed
ruff check . && mypy
```

Optionally add a CI job with `duckdb>=2.0.1,<3`, so both stay green until the
switch.

**Done when:** both suites pass with no shims, and with no DuckDB warnings in
the transform's or validator's stderr. P: methods on `execute()` results
"copy the rows and warn".

## Task 5 — switch the pinned version

**Why:** after Task 4 the code is version-neutral. These places name 1.5:

| File | Line | Now |
|---|---|---|
| `environment.yml` | 14 | `duckdb>=1.5,<2` |
| `pyproject.toml` | 16 | `"duckdb>=1.5,<2"` |
| `.github/workflows/ci.yml` | 34, 83, 107 | `"duckdb>=1.5,<2"` (Task 0) |
| `RUNBOOK.md` | 29 | `# pyarrow 23.x, duckdb 1.5.x` |
| `RUNBOOK-macos.md` | 45 | `# 23.x 1.5.x` |

**Do:** pin `duckdb>=2.0.1,<3` everywhere and update the two comments. First
check that conda-forge has a 2.0.1 build for the platforms in use (macOS
arm64, linux-64), under the name `environment.yml` uses:
`micromamba search -c conda-forge "duckdb>=2.0.1"`.
If it has none, install DuckDB through `environment.yml`'s `pip:` section,
as `ijson` already is (its PyPI wheels are `cp312-abi3`, so Python 3.14
works; P: "free-threaded builds of CPython are not supported", and the env
already pins the GIL build).

**Done when:** a fresh `micromamba create -f environment.yml` gives
`duckdb.__version__` 2.0.x on the Mac and on Codon.

## Task 6 — prove it at scale before a release depends on it

**Why:** the 25% and the memory figures come from fixture-sized inputs on
one machine.

**Do, on Codon:**

1. The RUNBOOK-codon steps 4–5 (tests, smoke test, OOM retry) on the new env.
2. Step 6, the 2M-entry slice, once with 1.5.6 and once with 2.0.x. Compare
   the transform's `Elapsed (wall clock)` and `Maximum resident set size`,
   and check that the two lakes are equal table by table (as in Appendix B).
3. Re-check the full run's memory request: each `BUILD_TABLE` task asks for
   96 GB, with DuckDB at 75% (`params.duckdb_pct`). If 2.0's peak RSS on the
   slice is higher relative to its limit, the 25% headroom shrinks: watch
   the first full run's `trace.txt`.

**Done when:** the slice's lakes are equal and 2.0.x is not slower. Then the
next release builds on 2.0.x, and its `provenance.json` records the version
(`tools`).

---

## Checked: no action needed

| 2.0 change | Source | Why it does not apply here |
|---|---|---|
| `~`/`!~` now match part of a string | M: "'abc' ~ 'b' now returns true (previously false )" | no `~` or `!~` in the SQL (grep) |
| `//` and `%` by zero now raise | M: "now throw an error instead of returning NULL" | the one `//` (`parquet_transform.py:1493`) is Python arithmetic |
| An untyped `NULL` keeps its own type | M: "An untyped NULL now keeps the NULL type instead of defaulting to INTEGER" | every fallback is typed (`_null()`, `_cast()`; README "Typed NULLs") |
| New storage format | B: "DuckDB versions before 2.0 cannot read database files created by DuckDB 2.0" | no `.duckdb` files are kept; the lake is Parquet written by PyArrow |
| Arrow batch size default 1,000,000 → 131,072 rows | P | `stream_to_parquet` passes `batch_size` explicitly (`parquet_transform.py:1578`) |
| `rel.arrow()` | P lists Arrow changes for `execute()` results | `validate_lake.py:1346` calls it on a relation: returns a `RecordBatchReader` on both versions (tested) |
| ISO-8601 timestamps with offsets auto-detected as TIMESTAMPTZ in JSON | G | the "strings stay strings" tests pass on 2.0 (probe run 3) |
| Python 3.11+, no free-threading | P | the env pins Python 3.14, GIL build (`environment.yml`) |
| `con.sql()` of a statement now returns a relation | P: "the statement still runs at the call" | `SET …` via `con.sql()` (e.g. `duckdb_util.init_duckdb`) still takes effect: probe run 3 passes |

---

## Appendix A — the probe

In a copy of the repo (`git ls-files | tar -cf - -T - | tar -xf - -C <copy>`),
with a DuckDB 2.0.0 env first on `PATH`, put this as `sitecustomize.py` in a
directory on `PYTHONPATH`. It reaches every Python process, the bin scripts
and Nextflow's tasks included:

```python
# PROBE ONLY: undo two DuckDB 2.0 breaking changes to find the rest.
import duckdb
_connect = duckdb.connect
def connect(*args, **kwargs):
    con = _connect(*args, **kwargs)
    con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
    return con
duckdb.connect = connect

from duckdb._relation import DuckDBPyRelation
def _fetchone(self):
    rows = self.limit(1).fetchall()
    return rows[0] if rows else None
DuckDBPyRelation.fetchone = _fetchone
```

Then `REQUIRE_NEXTFLOW=1 python -m pytest tests/ -q --tb=short`. Run 2 is
the same with only the first shim.

## Appendix B — the measurements

- **Transform** (both versions; 2.0 with the probe's shims):
  `python bin/parquet_transform.py <sorted.jsonl.zst> --outdir <lake>
  --memory-limit 4GB --threads 4 --release cmp --temp-dir <tmp>`, wall time
  and children's `ru_maxrss` taken from a Python wrapper. The input is the
  stress fixture (`tests/fixtures/diverse_stress.json.gz`) streamed and
  sorted with `bin/sort_jsonl.py`.
- **Lake equality:** for each table, the concatenated files (in sorted path
  order, read with `partitioning=None`) compared with `pyarrow.Table.equals`,
  schemas without metadata.
- **Sort:** `SELECT json FROM read_ndjson_objects(<file>,
  maximum_object_size=16777216) ORDER BY <sort_jsonl.py's keys>` copied to a
  zstd CSV, at the given `memory_limit` and `threads`, with the spill
  directory's size sampled every 0.1 s.
