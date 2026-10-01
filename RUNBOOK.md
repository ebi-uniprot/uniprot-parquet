# RUNBOOK — from unit tests to the full UniProtKB build

A staged path to the production run. **Part A runs everything on your own
machine** with Nextflow's `local` executor: tests, the small lake, the demo,
a ~200K-entry rehearsal and, optionally, a slice of the real dump. **Part B
moves to the SLURM cluster** only once Part A is clean: a short plumbing check,
then the full build. Each stage ends with checks; do not move to the next stage
until they pass. Commands assume you are in the repo root with the conda env
active.

**Part A — local**

0. Environment
1. Unit tests
2. Small lake (52-entry committed fixture)
3. Demo lake and notebook (~5K entries)
4. Local subset rehearsal (~200K entries)
5. (Optional) Real-data slice and benchmarks

**Part B — SLURM cluster**

6. Cluster environment
7. SLURM smoke test and OOM retry (`short` queue)
8. Full UniProtKB build (`prod` queue)
9. After the run
10. Recovery cheatsheet

---

# Part A — local

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
                                         # "no errors"
```

The `local` executor refuses a task that asks for more memory than the machine
has (`Process requirement exceeds available memory`). The small and demo stages
ask for 8 GB; stage 4 asks for what you give it. CPUs need no check: the
`local` profile caps each process's request (VALIDATE 8, the other heavy
processes 4) at the machine's CPU count.

---

## 1. Unit tests

```bash
python -m pytest tests/ -q               # ~3 min; fetches a ~4.6K-entry fixture from rest.uniprot.org on first run
python -m pytest tests/ -q --stress      # ~8 min; ~14K entries (one-time ~60 MB fetch); needs ~8 GB RAM
```

**Checks**

- All passed: no failures, no errors, no skips, and exactly 1 xfailed (the
  same with `--stress`). Without `nextflow` on the PATH, the workflow tests
  (`tests/test_workflow.py`) are skipped: put it on the PATH so they run.
- If the fixture fetch fails (no network), run `python tests/fetch_fixtures.py`
  from a machine that can reach `rest.uniprot.org` and copy
  `tests/fixtures/diverse*.json.gz` over.
- Optional: confirm the suite exercises the recent fixes —
  `python -m pytest tests/ -q -k "sentinel or expected_count or interrupt or git_info or OptionalPathGuards or map_columns or EmptyChildTables or decompresses_jsonl_once or sql_parity or path_with_quote or null_bounds"`
  should all pass; and the schema design (source columns are the JSON keys,
  new fields kept, fields changing shape, array order, schema diff) —
  `python -m pytest tests/ -q -k "ShortcutAndSourceColumns or NewUniProtFields or ArrayPositions or UpstreamShape or schema_diff or array_positions or child_count or source_columns or swapped or placed_by_index or empty_child_array or reconstruction_error or bracketed"`
  should all pass.

---

## 2. Small lake (52-entry committed fixture)

### 2a. Build

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
grep VERDICT datalake/small/validation_report.txt          # VERDICT: ALL CHECKS PASSED (N/N checks passed, 0 failed, ...)
grep "expected count" datalake/small/validation_report.txt # [PASS] ... expected=52, jsonl=52, entries=52
grep -E "reconstruction matches|shortcut values match" datalake/small/validation_report.txt
#   [PASS] reconstruction matches JSONL for 31/31 Swiss-Prot and 21/21 TrEMBL entries
#   [PASS] <table> shortcut values match the JSONL (... rows of 31 Swiss-Prot + 21 TrEMBL entries), one per table
#   On a full release: 1000/1000 Swiss-Prot and 1000/1000 TrEMBL

# 4. Provenance recorded THIS checkout, whatever directory the task ran in
python -c "import json; p=json.load(open('datalake/small/provenance.json')); print(p['pipeline'])"
git rev-parse HEAD                                         # must match p['pipeline']['commit']; 'dirty' reflects git status

# 5. Lake layout and checksums
ls -a datalake/small/lake                                  # entries/ ... manifest.json SHA256SUMS.txt RELEASE.metalink LICENSE datapackage.json
(cd datalake/small/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)
test ! -e datalake/small/lake/.complete && echo NO_SENTINELS  # build sentinels stay in the work dir, never published
head -3 datalake/small/SCHEMA.md                           # this release's schema, generated by PROVENANCE

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

### 2c. A second release: the schema diff

Build the same input as a second release and compare it with the first. This
is how every production release after the first is run (stage 8).

```bash
./run_lake.sh --release small2 --previous-manifest datalake/small/lake/manifest.json
```

**Checks**

```bash
grep VERDICT datalake/small2/validation_report.txt         # ALL CHECKS PASSED
grep -A2 "^--- 14" datalake/small2/validation_report.txt
#   [INFO] previous: release small, ...
#   [INFO] no table or column differences                   (same input, same schema)
```

---

## 3. Demo lake and notebook (~5K entries)

```bash
cd demo && ./run_demo.sh && cd ..        # downloads demo/input.json.gz (needs network), builds demo/lake/2026_03
```

**Checks**

```bash
# 1. Same end state as stage 2a, against demo/lake/2026_03/
ls demo/lake/2026_03/RELEASE_COMPLETE
grep -E "VERDICT|expected count" demo/lake/2026_03/validation_report.txt
#   ALL CHECKS PASSED; expected == jsonl == entries == the entry count run_demo.sh printed

# 2. The notebook runs top to bottom (what end users will do first)
jupyter nbconvert --to notebook --execute demo/demo.ipynb --output-dir /tmp --output demo_run
#   exits 0; any cell error stops it with the traceback (the committed notebook is not modified)

# 3. What a schema diff with real differences looks like: the demo lake against
#    the 52-entry lake. Keys (source columns) and sub-fields that one input has and
#    the other lacks show up as warnings; the lake still passes.
python bin/validate_lake.py --lake demo/lake/2026_03/lake \
    --jsonl demo/lake/2026_03/sorted.jsonl.zst \
    --previous-manifest datalake/small/lake/manifest.json -o /tmp/demo_vs_small.txt
grep -E "WARN|VERDICT" /tmp/demo_vs_small.txt
#   [WARN] entries: column retyped: organism  —  fields removed: synonyms
#   ... (a handful more)
#   VERDICT: ALL CHECKS PASSED  (... 0 failed, N warnings)
```

---

## 4. Local subset rehearsal (~200K entries)

This is the last local stage: a varied input large enough that DuckDB spills,
sorts for real and writes multi-row-group files. Its `trace.txt` is what you
size the full run from.

### 4a. Make the input

Any UniProtKB REST `stream` query returning roughly 100K–500K entries in the
`{"results": [...]}` JSON format works; human is a convenient, varied choice
(~200K entries, mostly TrEMBL):

```bash
curl --globoff -o ~/uniprot100k.json.gz \
  "https://rest.uniprot.org/uniprotkb/stream?query=organism_id:9606&format=json&compressed=true"
N_SUBSET=$(pigz -dc ~/uniprot100k.json.gz | python bin/stream_jsonl.py | wc -l); echo $N_SUBSET
```

**Checks**: the download is a valid gzip (`pigz -t ~/uniprot100k.json.gz`),
`N_SUBSET` is what you expect, and `stream_jsonl.py` exited 0 with no
`FATAL` line on stderr.

(`~/uniprot100k.json.gz` is also the default input of `run_lake.sh subset`, if
you ever want to repeat this stage on the cluster.)

### 4b. Build

`run_lake.sh`'s small mode is capped at 8 GB, so call Nextflow directly and give
the heavy processes most of the machine (here 16 GB; DuckDB gets 75% of it):

```bash
mkdir -p datalake/reports
nextflow run upjson2lake.nf -profile local \
    --inputfile ~/uniprot100k.json.gz \
    --outdir $(pwd)/datalake --release subset \
    --process_memory '16 GB' \
    --duckdb_temp /path/with/space/duckdb_tmp \
    --expected_count $N_SUBSET \
    -resume
```

**Checks**

```bash
OUT=datalake/subset

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
column -t -s $'\t' datalake/reports/trace.txt | cut -c1-200
#    peak_rss of SORT_JSONL and PARQUET_TRANSFORM under the 16 GB you gave
#    realtime per process × (250M / N_SUBSET) is a rough full-run estimate; compare it with
#    the README time budgets (STREAM 2–4h, SORT 4–8h, TRANSFORM 6–12h), allowing for the
#    difference between this machine and the cluster nodes

# 4. Spill went where you pointed it
ls -d /path/with/space/duckdb_tmp/duckdb_spill_* 2>/dev/null        # one per task while running; removed on exit

# 5. Resume: the same nextflow command again
#    -> cached=5

# 6. Upstream fields no column claims (usually none) and warnings
grep "\[WARN\]" $OUT/validation_report.txt

# 7. Query it the way users will
python -c "
from uniprot_parquet import connect
con = connect('$OUT/lake')
print(con.sql(\"SELECT accession, id, protein_name_primary FROM entries WHERE id = 'INS_HUMAN'\").fetchall())
print(con.sql('SELECT division, count(*) FROM entries GROUP BY 1 ORDER BY 2 DESC').fetchall())
print(con.sql(\"SELECT feature_index, type, start_pos FROM features WHERE accession = 'P01308' ORDER BY feature_index\").fetchall()[:5])"
```

---

## 5. (Optional) Real-data slice and benchmarks

If you have the full dump on a local machine with the disk for it, a
1–5M-entry slice is the best predictor of full-run memory and disk, and the
input the provisional settings are waiting for (README, "Open decisions"). Cut
it from the dump's JSONL rather than the REST API:

```bash
pigz -dc /path/to/UniProtKB.json.gz | python bin/stream_jsonl.py \
  | python bin/sample_jsonl.py --n 2000000 --seed 1 > slice_2m.jsonl
```

`sample_jsonl.py` emits JSONL, not the `{"results": [...]}` JSON the pipeline
ingests, so run the Python stages directly on it instead of the workflow:

```bash
python bin/sort_jsonl.py slice_2m.jsonl -o slice_2m.sorted.jsonl.zst --memory-limit 12GB --threads 4
python bin/parquet_transform.py slice_2m.sorted.jsonl.zst --outdir slice_2m_lake --memory-limit 12GB --threads 4 --release slice
python bin/validate_lake.py --lake slice_2m_lake --jsonl slice_2m.sorted.jsonl.zst --expected-count 2000000

# the measurements the provisional settings wait for
python benchmarks/bench_zstd.py --jsonl slice_2m.sorted.jsonl.zst --out benchmarks/results/
python benchmarks/bench_file_size.py --jsonl slice_2m.sorted.jsonl.zst --out benchmarks/results/
python benchmarks/bench_point_lookup.py --lake slice_2m_lake --serve --label slice
```

**Checks**: validator exit 0; `du -sh slice_2m_lake` × 125 is your full-lake
size estimate; `/usr/bin/time -v` peak RSS × (250M / 2M) is *not* linear for
DuckDB (it spills), but should stay under the 96 GB the full run requests.

**Part A is done** when stages 1–4 pass on a commit you are happy to run in
production. Commit (or at least note) that state now; stage 8 checks for it.

---

# Part B — SLURM cluster

## 6. Cluster environment

Check out the same commit on the cluster, create the environment as in stage 0,
then repeat the stage 0 checks **inside an interactive job on a compute node**
(`srun --pty bash`): the active conda env must carry over to the nodes, as
`run_lake.sh` relies on that rather than a container.

```bash
git rev-parse HEAD                       # the commit Part A passed on
srun --pty bash
#   on the node: the stage 0 checks, then
python -m pytest tests/ -q               # optional: all passed, 1 xfailed (as in step 1), proves the node's libraries
```

---

## 7. SLURM smoke test and OOM retry (`short` queue)

### 7a. Smoke test (52 entries)

The goal is to prove the cluster plumbing, not the data: jobs reach SLURM, the
nodes see the tools, spill goes where you point it, and resume works. The
`subset` mode selects the `short` SLURM profile (4 h wall time and 16 GB per
process), which is enough for the fixture:

```bash
./run_lake.sh subset --input tests/fixtures/small.json.gz \
    --outdir /scratch/$USER/uplake_smoke --duckdb-tmp /scratch/$USER/duckdb_tmp
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
#    (RELEASE_COMPLETE, VERDICT, expected count, provenance commit, checksums, SCHEMA.md)

# 4. Resource accounting works (needed to read the full run's trace)
column -t -s $'\t' /scratch/$USER/uplake_smoke/reports/trace.txt | cut -c1-160
#    every row: status COMPLETED, exit 0, peak_rss populated

# 5. Resume works on the cluster: the same command again
#    -> cached=5
```

### 7b. Prove the OOM retry (recommended, ~5 min)

Give the heavy processes too little memory for DuckDB to sort or transform
even the 52-entry fixture, but enough at twice the size. With
`--process_memory '2.5 GB'`, DuckDB gets 1920MB at attempt 1. That is too
little: the sort alone allocates a 1 GiB JSON buffer. The scripts exit 75
(DuckDB's out-of-memory error) and are retried at 5 GB, where DuckDB gets
3840MB and both pass. Run it directly, because the wrapper pins 16 GB for
`subset`:

```bash
nextflow run upjson2lake.nf -profile short \
    --inputfile tests/fixtures/small.json.gz --outdir /scratch/$USER/uplake_oom --release oom \
    --process_memory '2.5 GB' -ansi-log false
```

**Checks**

- The run succeeds, and `/scratch/$USER/uplake_oom/oom/RELEASE_COMPLETE` exists.
- `trace.txt` lists `SORT_JSONL` and `PARQUET_TRANSFORM` twice each:
  - first with status `FAILED`, exit `75` and memory 2.5 GB;
  - then `COMPLETED` with 5 GB (in `prod`, 96 GB → 192 GB).
- Each failed attempt's `.command.err` says `FATAL — DuckDB ran out of memory`.
- If the run aborted on the first failure, or `.nextflow.log` shows a
  `StackOverflowError`, stop: the retry directives are not being applied on
  your cluster.
- Clean up: `rm -rf /scratch/$USER/uplake_oom /scratch/$USER/uplake_smoke`.

The kernel's OOM killer (exit 137/140) and SLURM's pre-kill (143) are retried
the same way. A 143 can also be a wall-time kill, so a retry also gets twice
its base time, capped at `--max_time` (default 72h). Set that to at most the
queue's wall-time limit.

If the cluster nodes are very different from the machine you ran stage 4 on,
repeat stage 4 here with `./run_lake.sh subset --outdir /scratch/$USER/uplake_subset
--duckdb-tmp /scratch/$USER/duckdb_tmp` and size the full run from that trace.

---

## 8. Full UniProtKB build (`prod` queue)

### 8a. Before submitting

```bash
# 1. The commit Part A passed on, with a clean tree: provenance.json records the commit and a dirty flag.
git status --short                      # empty (datalake/, results/, work/, .nextflow* are git-ignored;
                                        # any other untracked file, such as a new bin/ script, counts as dirty)
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

### 8b. Submit

Run from `tmux`/`screen` on the login node, or wrap the command in a small
`sbatch` head job with a wall time longer than the whole pipeline (2–3 days):

```bash
./run_lake.sh full \
    --expected-count $EXPECTED \
    --release 2026_03 \
    --input /path/to/UniProtKB.json.gz \
    --outdir /scratch/$USER/uniprot_parquet \
    --duckdb-tmp /scratch/$USER/duckdb_tmp \
    --previous-manifest /scratch/$USER/uniprot_parquet/<previous release>/lake/manifest.json
```

`--release` defaults to the label in `run_lake.sh` (currently `2026_03`); pass
it explicitly so the output directory unambiguously matches the dump you are
building from.

`--previous-manifest` makes the validation report list how this release's
schema differs from the previous one, as in stage 2c. The diff never fails the
run, but full mode requires the flag: for the very first release, pass
`--first-release` instead. `run_lake.sh` stops before launching if the file
does not exist, or if it lies inside the release being built (a re-run would
diff the release against itself).

To get failure emails, invoke Nextflow directly with `--notify_email you@example.org`
(see README "Nextflow directly"); `run_lake.sh` does not pass it.

### 8c. While it runs — checks per process

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
grep -E "New UniProt fields|stored as JSON" $WD/.command.log
#   keys UniProt added (new source columns, listed per table until they get a
#   description), and fields whose type varies between entries (their source
#   column is stored as JSON); informational — the build carries on and stores
#   them. If such a field feeds a shortcut column, the build stops instead (see
#   the recovery cheatsheet)
ls $WD/lake/.complete/                  # a <table>.json appears as each table finishes — the
                                        # sentinel that a later --skip-existing run trusts;
                                        # removed once the transform succeeds (not published)
```

If it OOMs: expect exit 137/140 in `.exitcode`, then a second attempt with
192 GB in `trace.txt`. If the second attempt also fails, the run stops; raise
`MEMORY` in `run_lake.sh`'s `full` mode (or `--process_memory`) and re-submit —
`-resume` restarts from the transform, not from the stream.

**VALIDATE (not yet timed at full scale; 8 cpus, 24 h limit, OOM retry)** —
watch `$WD/.command.log` for `[PASS]` lines; a single `[FAIL]` exits 1 and
PROVENANCE does not run, so no `RELEASE_COMPLETE` appears. Its DuckDB gets
`--duckdb_pct` of the task memory and `--duckdb_temp` for spill, like the
transform. `[WARN] <table>.<column> has no values` means a shortcut column is
NULL or empty in every row: compare with the transform's
`WARNING: shortcut sources absent from input` lines — a renamed upstream field
is kept as a new source column, but the shortcut needs a code change.

**PROVENANCE (< 1 min)** — writes `SCHEMA.md` and `provenance.json` (with the
size and SHA-256 of every file published beside `lake/`); the workflow's
`onComplete` runs `bin/verify_release.py` on the published release directory
once every publish copy has finished, and renames `.RELEASE_COMPLETE.pending`
to `RELEASE_COMPLETE` only if it passes. Otherwise the log says
`RELEASE_COMPLETE not written` with the problems, and no marker is left:
re-run with `-resume` to republish.

---

## 9. After the run

```bash
OUT=/scratch/$USER/uniprot_parquet/2026_03

# 1. Completion marker and verdict
ls $OUT/RELEASE_COMPLETE
grep -E "VERDICT|expected count" $OUT/validation_report.txt
#   VERDICT: ALL CHECKS PASSED; expected == jsonl == entries == $EXPECTED

# 2. Provenance: the commit you wrote down, and NOT dirty
python -c "import json; p=json.load(open('$OUT/provenance.json')); print(p['pipeline'], p['release'])"
#   {'commit': '<the hash you wrote down>', 'dirty': False}
#   and the dump it was built from, which should match the file you downloaded:
python -c "import json; p=json.load(open('$OUT/provenance.json')); print(p['inputs']['source'], p['tools'])"
sha256sum /path/to/uniprot.json.gz      # == inputs.source.sha256

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
print(con.sql(\"SELECT accession, protein_name_primary FROM entries WHERE accession = 'P01308'\").fetchall())      # human insulin
print(con.sql('SELECT reviewed, count(*) FROM entries GROUP BY 1').fetchall())
print(con.sql(\"SELECT count(*) FROM accession_map WHERE NOT is_primary\").fetchall())"

# 7. What changed upstream: warnings never fail a release, but read them
grep "\[WARN\]" $OUT/validation_report.txt
#   "... new UniProt fields (source columns without a description)": UniProt added
#   keys, each now a column — describe them in COLUMN_DESCRIPTIONS, and decide
#   whether any deserves a shortcut
#   "... SHORTCUT column ...": the shortcut contract changed — was schema_version bumped?
head -5 $OUT/SCHEMA.md
```

---

## 10. Recovery cheatsheet

| Symptom | What to do |
|---|---|
| Head job / login session killed while Nextflow was running | Re-run the identical `run_lake.sh` command; `-resume` reuses every completed process and republishes every output whole (`overwrite: true`), so a copy cut short mid-publish is repaired, and `RELEASE_COMPLETE` is written only after `bin/verify_release.py` passes on it. Mirrors that want hash-level certainty run `bin/verify_release.py <release> --full`. |
| Log says `RELEASE_COMPLETE not written: the published copy ... does not match its metadata` | Something changed the release directory during or after publishing (a full disk, another process). Re-run with `-resume`. |
| A SLURM job hit its wall time | Same: re-submit. If it was `PARQUET_TRANSFORM`, the retry starts from a fresh work dir (Nextflow limitation); the whole transform re-runs. |
| `PARQUET_TRANSFORM` OOM twice | Raise memory (`MEMORY=` in `run_lake.sh` or `--process_memory`) and re-submit. |
| `Process requirement exceeds available memory` (local executor) | `--process_memory` is more than the machine has; lower it (DuckDB spills to `--duckdb_temp`). |
| `STREAM_JSONL` count mismatch (`FATAL — count mismatch`) | The dump is not the release you think, or is truncated: re-check `EXPECTED` and `pigz -t`. |
| `STREAM_JSONL` stops with `FATAL — LoneSurrogate` or `Integer exceeds 64-bit range` | The dump holds data a JSON line cannot carry exactly (a `\uD800`-style escape without its pair, an integer outside −2^63 … 2^64−1). The message names the entry number (and the byte offset for a surrogate). Report it to UniProt; the pipeline refuses rather than change the data. |
| `VALIDATE` fails | Read `validation_report.txt`; the lake was not marked complete. Fix, then re-submit — VALIDATE alone re-runs. |
| `PARQUET_TRANSFORM` stops with `Input lacks fields every UniProtKB entry carries` | UniProt removed or renamed a field a shortcut column is built from (or the input is not a UniProtKB dump). Update the SQL builder and `REQUIRED_PATHS` in `bin/parquet_transform.py`; this is a shortcut-schema change. New fields, and retyped fields that no shortcut reads, never stop the build: they are source columns. |
| `PARQUET_TRANSFORM` stops with `Shortcut columns whose source field has a different type in different entries` | UniProt ships a field a shortcut is built from with more than one type (for example a string in some entries and an object in others), so DuckDB inferred JSON. The message names the column and the source path. Decide how the shortcut should read the field, update its SQL builder in `bin/parquet_transform.py`, and bump `schema_version` if the column's type or meaning changes. |
| `PARQUET_TRANSFORM` stops with `Input has entryType value(s) [...]` | UniProt reworded or added an `entryType` value. The review flag and the swissprot/trembl partitions depend on it, so the build refuses rather than file every such entry as TrEMBL. Add the value to `ENTRY_TYPES` in `bin/parquet_transform.py` and to `ENTRY_TYPES` in `bin/shortcut_reference.py`, mapping it to reviewed or unreviewed. |
| `VALIDATE` fails check 21 (`<table> shortcut values match the JSONL`) | A shortcut column disagrees with the independent computation in `bin/shortcut_reference.py`; the detail names the column, accession, expected and actual value. Either the SQL in `bin/parquet_transform.py` or the reference is wrong, often because UniProt changed a field a shortcut reads. Fix the wrong side and rebuild. |
| `VALIDATE` fails check 20 (`<table>.<column> has values`) | A shortcut is empty in every row of a full release: its source field is absent or renamed upstream (the transform log lists absent sources). Update the shortcut's SQL, or the field it reads. |
| `PARQUET_TRANSFORM` stops with `input key(s) [...] have the name of a shortcut column` | UniProt added a key named like one of the table's shortcuts (compared case-insensitively). Rename the shortcut in `bin/parquet_transform.py` (a major schema version change). |
| Manual re-run of the transform on an existing `lake/` (outside Nextflow) | `parquet_transform.py ... --skip-existing` skips only tables whose `.complete/<table>.json` matches the files on disk and records the same input (size, mtime) and release; anything else is rebuilt, with a `REBUILD <table>: ..., <reason>` line on stderr. |
| Reports in `<outdir>/reports/` look stale | They are overwritten on every run; check the mtime, then `.nextflow.log` for `Failed to render` (should not appear). |

A subset in which *no* entry has, say, comments produces a zero-row `comments`
table holding one empty schema-valid Parquet file, so `**/*.parquet` globs,
dataset readers, and the validator all still work.
