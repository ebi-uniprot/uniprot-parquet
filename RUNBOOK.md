# RUNBOOK — from unit tests to the full UniProtKB build

A staged path from your laptop to the production run. Each stage ends with
checks; do not move to the next stage until they pass. Commands assume you are
in the repo root with the conda env active.

Stages:

0. Environment
1. Unit tests (laptop)
2. Small lake, local executor (laptop)
3. SLURM smoke test (52 entries on the `short` queue)
4. SLURM subset (~100K–200K entries on the `short` queue)
5. Full UniProtKB build (`prod` queue)
6. After the run: publish checks and recovery

---

## 0. Environment

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-parquet
```

**Checks**

```bash
nextflow -version | grep version        # >= 26.04 (strict-syntax pipeline)
java -version                            # 21 (from environment.yml)
which pigz zstd                          # both must resolve — STREAM_JSONL needs them
python -c "import pyarrow, duckdb, orjson, ijson, zstandard, frictionless; print(pyarrow.__version__, duckdb.__version__)"
                                         # pyarrow 23.x, duckdb 1.5.x
nextflow lint upjson2lake.nf nextflow.config
                                         # "no errors"; one Channel.fromPath deprecation warning is expected
```

On SLURM, run the same checks **inside an interactive job on a compute node**
(`srun --pty bash`) — the active conda env must carry over to the nodes, as
`run_lake.sh` relies on that rather than a container.

---

## 1. Unit tests (laptop)

```bash
python -m pytest tests/ -q               # ~2.5 min; fetches a ~4K-entry fixture from rest.uniprot.org on first run
python -m pytest tests/ -q --stress      # ~8 min; ~15K entries (one-time ~60 MB fetch)
```

**Checks**

- `184 passed`, no skips, no failures (the `--stress` count is the same).
- If the fixture fetch fails (no network), run `python tests/fetch_fixtures.py`
  from a machine that can reach `rest.uniprot.org` and copy
  `tests/fixtures/diverse*.json.gz` over.
- Optional: confirm the suite exercises the recent fixes —
  `python -m pytest tests/ -q -k "sentinel or expected_count or interrupt or git_info or OptionalPathGuards or map_columns"`
  should report 19 passed.

---

## 2. Small lake, local executor (laptop)

### 2a. Committed 52-entry fixture (no download needed)

```bash
./run_lake.sh                            # small mode: tests/fixtures/small.json.gz, -profile local, 8 GB
```

Output goes to `./datalake/small/`.

**Checks**

```bash
# 1. Nextflow finished every process: the console ends with "[SUCCESS] completed=5 failed=0"
grep -c "Execution complete -- Goodbye" .nextflow.log     # 1

# 2. The release marker exists — it is written last, only after VALIDATE passed
ls datalake/small/RELEASE_COMPLETE

# 3. Validation: all checks passed, including the pre-sort entry-count anchor
grep VERDICT datalake/small/validation_report.txt          # VERDICT: ALL CHECKS PASSED (165/165 ...)
grep "expected count" datalake/small/validation_report.txt # [PASS] ... expected=52, jsonl=52, entries=52

# 4. Provenance recorded THIS checkout, whatever directory the task ran in
python -c "import json; p=json.load(open('datalake/small/provenance.json')); print(p['pipeline'])"
git rev-parse HEAD                                         # must match p['pipeline']['commit']; 'dirty' reflects git status

# 5. Lake layout and checksums
ls -a datalake/small/lake                                  # .complete/ entries/ ... manifest.json SHA256SUMS.txt RELEASE.metalink LICENSE
(cd datalake/small/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)
ls datalake/small/lake/.complete                           # one <table>.json per table (skip-existing sentinels)

# 6. Diagnostic reports landed under the outdir, not ./null/
ls datalake/reports                                        # dag.html report.html timeline.html trace.txt

# 7. The client can open it
python -c "
from uniprot_parquet import connect
con = connect('datalake/small/lake')
print(con.sql('SELECT reviewed, count(*) FROM entries GROUP BY 1').fetchall())"
```

### 2b. Re-run: resume and idempotency

```bash
./run_lake.sh                            # same command again
```

**Checks**

- Console shows `cached=5` (every process resumed from cache, nothing re-ran).
- `datalake/reports/report.html` was overwritten (mtime is now), not left stale.

### 2c. Demo-sized input (~5K entries, needs network)

```bash
cd demo && ./run_demo.sh && cd ..        # downloads demo/input.json.gz, builds demo/lake/2026_03
# or, through the wrapper:
./run_lake.sh --input demo/input.json.gz
```

**Checks**: same as 2a against `demo/lake/2026_03/` (or `datalake/small/`);
the expected-count line should read `expected=<N>, jsonl=<N>, entries=<N>` with
N equal to the download's entry count printed by `run_demo.sh`.

---

## 3. SLURM smoke test (52 entries, `short` queue)

The goal here is to prove the cluster plumbing, not the data. Run the tiny
fixture through the `subset` mode (which selects the `short` SLURM profile;
that profile caps every process at 4h wall time and 16 GB, so it is only for
subsets):

```bash
./run_lake.sh subset --input tests/fixtures/small.json.gz --outdir /scratch/$USER/uplake_smoke
```

Run `run_lake.sh` from a `tmux`/`screen` session on the login node (or submit
it as a light head job); it blocks until Nextflow finishes.

**Checks**

```bash
# 1. Jobs actually went to SLURM
squeue -u $USER                                            # during the run: five short jobs, one at a time
grep -c "executor >.*slurm" .nextflow.log                   # >= 1

# 2. The compute nodes see your env and tools
WD=$(grep -o "work/[0-9a-f]\{2\}/[0-9a-f]*" .nextflow.log | head -1)
grep "SLURM Job ID" $WD/.command.log                       # beforeScript ran on a node
grep -L "command not found" work/*/*/.command.err          # no missing pigz/zstd/python on nodes

# 3. Same end-state checks as stage 2a on /scratch/$USER/uplake_smoke/subset/
#    (RELEASE_COMPLETE, VERDICT, expected count, provenance commit, checksums)

# 4. Resource accounting works (needed to size the next stages)
column -t -s $'\t' /scratch/$USER/uplake_smoke/reports/trace.txt | cut -c1-160
#    every row: status COMPLETED, exit 0, peak_rss populated
```

### 3b. Prove the OOM retry on the cluster (recommended, ~5 min)

Force an OOM by giving the heavy processes far too little memory via a direct
invocation (the wrapper pins 16 GB for `subset`):

```bash
nextflow run upjson2lake.nf -profile short \
    --inputfile demo/input.json.gz --outdir /scratch/$USER/uplake_oom --release oom \
    --process_memory '300 MB' -ansi-log false
```

**Checks**

- `.nextflow.log` shows `SORT_JSONL` (or `PARQUET_TRANSFORM`) failing with exit
  137/140/143 and being **retried once**; `trace.txt` lists the process twice,
  the second attempt with double the memory (600 MB here; in `prod`, 96 GB → 192 GB).
- If the retry with 600 MB also fails, that is expected for this deliberately
  starved run — the point is that the retry *happened*. If instead the run
  aborted on the first failure, or `.nextflow.log` shows a `StackOverflowError`,
  stop: the retry directives are not being applied on your cluster.
- Clean up: `rm -rf /scratch/$USER/uplake_oom`.

---

## 4. SLURM subset (~100K–200K entries, `short` queue)

### 4a. Make the input

`subset` mode expects `~/uniprot100k.json.gz`. Any UniProtKB REST `stream`
query returning roughly 100K–500K entries in the `{"results": [...]}` JSON
format works; human is a convenient, varied choice (~200K entries, mostly TrEMBL):

```bash
curl --globoff -o ~/uniprot100k.json.gz \
  "https://rest.uniprot.org/uniprotkb/stream?query=organism_id:9606&format=json&compressed=true"
pigz -dc ~/uniprot100k.json.gz | python bin/stream_jsonl.py | wc -l     # note this number: N_SUBSET
```

**Checks**: the download is a valid gzip (`pigz -t ~/uniprot100k.json.gz`),
`N_SUBSET` is what you expect, and `stream_jsonl.py` exited 0 with no
`SKIPPED entry` lines on stderr.

### 4b. Build

```bash
./run_lake.sh subset --outdir /scratch/$USER/uplake_subset --duckdb-tmp /scratch/$USER/duckdb_tmp
```

(Add `--expected-count N_SUBSET` if you want the stream step itself to assert
the count; VALIDATE anchors on the streamed count regardless.)

**Checks**

```bash
OUT=/scratch/$USER/uplake_subset/subset

# 1. End state, as in stage 2a
ls $OUT/RELEASE_COMPLETE
grep -E "VERDICT|expected count" $OUT/validation_report.txt          # ALL CHECKS PASSED; expected == jsonl == entries == N_SUBSET
(cd $OUT/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)

# 2. Row counts are plausible for the query you chose
python -c "
import json; m=json.load(open('$OUT/lake/manifest.json'))
for t,v in m['tables'].items(): print(f'{t:15} {v[\"row_count\"]:>12,}  files={len(v[\"files\"])}')"
#    entries == N_SUBSET; xrefs and features are 10–50x entries; accession_map >= entries

# 3. Resource headroom — this is what you extrapolate the full run from
column -t -s $'\t' /scratch/$USER/uplake_subset/reports/trace.txt | cut -c1-200
#    peak_rss of SORT_JSONL and PARQUET_TRANSFORM well under the 16 GB allocation
#    (DuckDB is capped at 75% = 12 GB; if peak_rss is near 16 GB, raise --process_memory for the full run)
#    realtime per process: multiply by (250M / N_SUBSET) for a rough full-run estimate and
#    compare with the README time budgets (STREAM 2–4h, SORT 4–8h, TRANSFORM 6–12h)

# 4. Spill went where you pointed it
ls -d /scratch/$USER/duckdb_tmp/duckdb_spill_* 2>/dev/null            # one per job while running; removed on exit

# 5. Resume works on the cluster
./run_lake.sh subset --outdir /scratch/$USER/uplake_subset --duckdb-tmp /scratch/$USER/duckdb_tmp
#    -> cached=5

# 6. Query it the way users will
python -c "
from uniprot_parquet import connect
con = connect('$OUT/lake')
print(con.sql(\"SELECT acc, id, protein_name FROM entries WHERE id = 'INS_HUMAN'\").fetchall())
print(con.sql('SELECT division, count(*) FROM entries GROUP BY 1 ORDER BY 2 DESC').fetchall())"
```

### 4c. (Optional) Bigger rehearsal before committing 24+ hours

If you have the full dump already, a 1–5M-entry slice on the `short` queue
(16 GB, 4 h) is the best predictor of full-run memory and disk. Cut it from
the dump's JSONL rather than the REST API:

```bash
pigz -dc /path/to/UniProtKB.json.gz | python bin/stream_jsonl.py \
  | python bin/sample_jsonl.py --n 2000000 --seed 1 > /scratch/$USER/slice_2m.jsonl
```

`sample_jsonl.py` emits JSONL, not the `{"results": [...]}` JSON the pipeline
ingests, so run the two Python stages directly on it instead of the workflow:

```bash
python bin/sort_jsonl.py /scratch/$USER/slice_2m.jsonl -o /scratch/$USER/slice_2m.sorted.jsonl.zst --memory-limit 12GB --threads 4
python bin/parquet_transform.py /scratch/$USER/slice_2m.sorted.jsonl.zst --outdir /scratch/$USER/slice_2m_lake --memory-limit 12GB --threads 4 --release slice
python bin/validate_lake.py --lake /scratch/$USER/slice_2m_lake --jsonl /scratch/$USER/slice_2m.sorted.jsonl.zst --expected-count 2000000
```

**Checks**: validator exit 0; `du -sh /scratch/$USER/slice_2m_lake` × 125 is
your full-lake size estimate; `/usr/bin/time -v` peak RSS × (250M / 2M) is
*not* linear for DuckDB (it spills), but should stay under the 96 GB you will
request.

---

## 5. Full UniProtKB build (`prod` queue)

### 5a. Before submitting

```bash
# 1. Commit your working tree: provenance.json records the commit and a dirty flag.
git status --short                      # empty
git rev-parse HEAD                      # write this down

# 2. Expected entry count for the release you are building
#    https://www.uniprot.org/help/release-statistics  (Swiss-Prot + TrEMBL entries)
#    Cross-check: the FTP release's relnotes.txt states the same totals.
EXPECTED=248799253                      # replace with the real number

# 3. The dump matches that release and is intact
pigz -t /path/to/UniProtKB.json.gz && echo GZIP_OK
ls -l /path/to/UniProtKB.json.gz        # ~160 GB compressed for 2026 releases

# 4. Disk: SORT_JSONL spills up to ~1 TB, PARQUET_TRANSFORM up to ~2 TB (staging + spill),
#    the published release is sorted.jsonl.zst + lake/ (tens to a few hundred GB).
#    The processes' `disk` directives are NOT passed to SLURM (no --tmp), so nothing
#    reserves this for you — check the spill filesystem yourself:
df -h /scratch/$USER                    # >= 3 TB free on the spill filesystem, plus the output

# 5. Queue limits: SORT_JSONL asks 24h, PARQUET_TRANSFORM and STREAM_JSONL 48h, 96 GB, 4 CPUs,
#    and the OOM retry may ask 192 GB (exact headers: -c 4 -t 48:00:00 --mem 98304M -p production).
#    Confirm the production queue permits all of that:
sinfo -p production -o "%P %l %m %c"
sacctmgr show qos format=name,maxwall,maxtres | grep -i production
```

### 5b. Submit

Run from `tmux`/`screen` on the login node, or wrap the command in a small
`sbatch` head job with a wall time longer than the whole pipeline (2–3 days):

```bash
./run_lake.sh full \
    --expected-count $EXPECTED \
    --release 2026_03 \
    --input /path/to/UniProtKB.json.gz \
    --outdir /scratch/$USER/uniprot_parquet \
    --duckdb-tmp /scratch/$USER/duckdb_tmp
```

`--release` defaults to the label in `run_lake.sh` (currently `2026_03`); pass
it explicitly so the output directory unambiguously matches the dump you are
building from.

To get failure emails, invoke Nextflow directly with `--notify_email you@example.org`
(see README "Direct Nextflow invocation"); `run_lake.sh` does not pass it.

### 5c. While it runs — checks per process

Find each task's work dir with `nextflow log <run name> -f process,workdir,status`
(or from `.nextflow.log`).

**STREAM_JSONL (2–4 h)**

```bash
tail -2 $WD/.command.log                # "stream_jsonl: 123,000,000 entries (25,000/s)" advancing every ~40 s
squeue -j <jobid> -o "%T %M %L"         # RUNNING; time left comfortably > remaining entries / rate
# on completion:
cat $WD/entry_count.txt                 # == $EXPECTED (the script exits 1 otherwise, and the run stops here)
grep "Verified:" $WD/.command.log       # zstd line count matched the streamed count
```

If this process stops early with `FATAL — interrupted` or `BrokenPipeError`,
nothing downstream ran and no count file was written; fix the cause and
re-submit with the same command (`-resume` is on by default).

**SORT_JSONL (4–8 h)**

```bash
du -sh /scratch/$USER/duckdb_tmp        # grows to hundreds of GB, then shrinks
ls -l /scratch/$USER/uniprot_parquet/2026_03/sorted.jsonl.zst   # appears (published) when done
```

**PARQUET_TRANSFORM (6–12 h)**

```bash
grep -E "^--- |rows so far|rows in" $WD/.command.log | tail -5
#   "--- ENTRIES ---", then "entries: 250,000,000 rows in N files", then FEATURES, XREFS, ...
ls $WD/lake/.complete/                  # a <table>.json appears as each table finishes — the
                                        # sentinel that a later --skip-existing run trusts
```

If it OOMs: expect exit 137/140 in `.exitcode`, then a second attempt with
192 GB in `trace.txt`. If the second attempt also fails, the run stops; raise
`MEMORY` in `run_lake.sh`'s `full` mode (or `--process_memory`) and re-submit —
`-resume` restarts from the transform, not from the stream.

**VALIDATE (1–2 h)** — watch `$WD/.command.log` for `[PASS]` lines; a single
`[FAIL]` exits 1 and PROVENANCE does not run, so no `RELEASE_COMPLETE` appears.

**PROVENANCE (< 1 min)** — the workflow's `onComplete` renames
`.RELEASE_COMPLETE.pending` to `RELEASE_COMPLETE` after every publish copy has
finished.

### 5d. After the run

```bash
OUT=/scratch/$USER/uniprot_parquet/2026_03

# 1. Completion marker and verdict
ls $OUT/RELEASE_COMPLETE
grep -E "VERDICT|expected count" $OUT/validation_report.txt
#   VERDICT: ALL CHECKS PASSED; expected == jsonl == entries == $EXPECTED

# 2. Provenance: the commit you wrote down, and NOT dirty
python -c "import json; p=json.load(open('$OUT/provenance.json')); print(p['pipeline'], p['release'])"
#   {'commit': '<the hash you wrote down>', 'dirty': False}

# 3. Whole-lake checksums (slow on the full lake — run it as a job, ~30–60 min)
(cd $OUT/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)

# 4. Totals against the release statistics page
python -c "
import json; m=json.load(open('$OUT/lake/manifest.json'))
print('entries', m['tables']['entries']['row_count'])
print(m['tables']['entries']['partitioning']['keys'][0]['row_counts'])"
#   entries == $EXPECTED; {'swissprot': N, 'trembl': M} — N == the Swiss-Prot entry count on the stats page

# 5. Resource actuals — keep them for the next release's budget
column -t -s $'\t' /scratch/$USER/uniprot_parquet/reports/trace.txt | cut -c1-220

# 6. Query smoke tests
python -c "
from uniprot_parquet import connect
con = connect('$OUT/lake')
print(con.sql(\"SELECT acc, protein_name FROM entries WHERE acc = 'P01308'\").fetchall())      # human insulin
print(con.sql('SELECT reviewed, count(*) FROM entries GROUP BY 1').fetchall())
print(con.sql(\"SELECT count(*) FROM accession_map WHERE NOT is_primary\").fetchall())"
```

---

## 6. Recovery cheatsheet

| Symptom | What to do |
|---|---|
| Head job / login session killed while Nextflow was running | Re-run the identical `run_lake.sh` command; `-resume` reuses every completed process. |
| A SLURM job hit its wall time | Same: re-submit. If it was `PARQUET_TRANSFORM`, the retry starts from a fresh work dir (Nextflow limitation); the whole transform re-runs. |
| `PARQUET_TRANSFORM` OOM twice | Raise memory (`MEMORY=` in `run_lake.sh` or `--process_memory`) and re-submit. |
| `STREAM_JSONL` count mismatch (`FATAL — count mismatch`) | The dump is not the release you think, or is truncated: re-check `EXPECTED` and `pigz -t`. |
| `VALIDATE` fails | Read `validation_report.txt`; the lake was not marked complete. Fix, then re-submit — VALIDATE alone re-runs. |
| Manual re-run of the transform on an existing `lake/` (outside Nextflow) | `parquet_transform.py ... --skip-existing` skips only tables whose `.complete/<table>.json` matches the files on disk; anything else is rebuilt, with a `REBUILD <table>` line on stderr. |
| Reports in `<outdir>/reports/` look stale | They are overwritten on every run; check the mtime, then `.nextflow.log` for `Failed to render` (should not appear). |

A subset in which *no* entry has, say, comments produces a zero-row `comments`
table holding one empty schema-valid Parquet file, so `**/*.parquet` globs,
dataset readers, and the validator all still work (previously the table got no
files at all and the validator's glob failed).
