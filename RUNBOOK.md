# RUNBOOK — from unit tests to the full UniProtKB build

A staged path to the production run: **Part A** (stages 0–5) on your own
machine with Nextflow's `local` executor, then **Part B** (stages 6–10) on the
SLURM cluster once Part A is clean. Each stage ends with checks: do not move on
until they pass. Commands assume the repo root and an active conda env.

Walkthroughs with concrete settings, step by step: [RUNBOOK-macos.md](RUNBOOK-macos.md)
(Part A on a Mac) and [RUNBOOK-codon.md](RUNBOOK-codon.md) (Part B on EBI's Codon).

---

# Part A — local

## 0. Environment

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-parquet
```

**Checks**

```bash
nextflow -version | grep version         # >= 26.04 (strict syntax)
java -version                            # 21
which pigz zstd                          # both resolve (STREAM_JSONL needs them)
python -c "import pyarrow, duckdb, orjson, ijson, zstandard, frictionless; print(pyarrow.__version__, duckdb.__version__)"
                                         # pyarrow 23.x, duckdb 1.5.x
python -c "import ijson; print(ijson.backend)"   # yajl2_c: stream_jsonl.py refuses the pure-Python one
nextflow lint upjson2lake.nf nextflow.config   # "2 files had no errors"
```

The `local` executor refuses a task that asks for more memory than the machine
has (`Process requirement exceeds available memory`): stages 2 and 3 ask for
8 GB, stage 4 for what you give it. CPUs need no check: the `local` profile caps
each request (VALIDATE 8, the other heavy processes 4) at the machine's count.

## 1. Unit tests

```bash
REQUIRE_NEXTFLOW=1 python -m pytest tests/ -q            # a few minutes; first run fetches a ~4.6K-entry fixture
REQUIRE_NEXTFLOW=1 python -m pytest tests/ -q --stress   # ~8 min; ~14K entries (one-time ~70 MB fetch); ~8 GB RAM
ruff check . && mypy                                     # "All checks passed!", "Success: no issues found"
```

`REQUIRE_NEXTFLOW=1` makes the workflow tests fail, rather than skip, when
`nextflow` is not on the PATH.

**Checks**: all passed, no errors, no skips, exactly 1 xfailed (also with
`--stress`). If the fixture fetch fails (no network), run
`python tests/fetch_fixtures.py [--scale stress]` where `rest.uniprot.org` is
reachable and copy `tests/fixtures/diverse*.json.gz` over.

## 2. Small lake (52-entry committed fixture)

### 2a. Build

```bash
./run_lake.sh                            # small mode: tests/fixtures/small.json.gz, -profile local, 8 GB -> datalake/small/
```

**Checks**

```bash
# 1. run_lake.sh exited 0 (it exits 1 unless RELEASE_COMPLETE was written); every process succeeded
ls datalake/small/RELEASE_COMPLETE
grep -o "succeededCount=[0-9]*; failedCount=[0-9]*" .nextflow.log   # succeededCount=5; failedCount=0

# 2. Validation, including the pre-sort entry-count anchor
grep -E "VERDICT|expected count|reconstruction matches|shortcut values match" datalake/small/validation_report.txt
#   [PASS] ... expected=52, jsonl=52, entries=52
#   [PASS] reconstruction matches JSONL for 31/31 Swiss-Prot and 21/21 TrEMBL entries   (full release: 1000/1000)
#   [PASS] <table> shortcut values match the JSONL (... rows of 31 Swiss-Prot + 21 TrEMBL entries), one per table
#   VERDICT: ALL CHECKS PASSED  (N/N checks passed, 0 failed, ...)

# 3. Provenance: commit == git rev-parse HEAD; 'dirty' reflects git status
python -c "import json; print(json.load(open('datalake/small/provenance.json'))['pipeline'])"

# 4. Layout, checksums, no build sentinels, schema doc, reports
ls datalake/small/lake                   # 6 table dirs, manifest.json SHA256SUMS.txt RELEASE.metalink LICENSE datapackage.json
(cd datalake/small/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)
test ! -e datalake/small/lake/.complete && echo NO_SENTINELS
ls datalake/small/SCHEMA.md datalake/reports   # reports: dag.html report.html timeline.html trace.txt

# 5. The client opens it
python -c "from uniprot_parquet import connect; print(connect('datalake/small/lake').sql('SELECT reviewed, count(*) FROM entries GROUP BY 1').fetchall())"
```

### 2b. Re-run: resume and idempotency

```bash
./run_lake.sh                            # same command again
```

**Checks**

```bash
grep -o "cachedCount=[0-9]*" .nextflow.log   # cachedCount=5: every process cached
ls datalake/small/RELEASE_COMPLETE           # the marker is back
ls -l datalake/reports/report.html           # a fresh mtime
```

### 2c. A second release: the schema diff

Every production release after the first runs this way (stage 8):

```bash
./run_lake.sh --release small2 --previous-manifest datalake/small/lake/manifest.json
```

**Checks**

```bash
grep VERDICT datalake/small2/validation_report.txt          # VERDICT: ALL CHECKS PASSED ...
grep -A2 "^--- 14" datalake/small2/validation_report.txt    # previous: release small, ...
                                                            # no table or column differences
```

## 3. Demo lake and notebook (~5K entries)

```bash
demo/run_demo.sh                         # downloads demo/input.json.gz (needs network), builds demo/lake/2026_03/
```

**Checks**

```bash
# 1. End state as in 2a: expected == jsonl == entries == the "Entries:" count run_demo.sh printed
ls demo/lake/2026_03/RELEASE_COMPLETE
grep -E "VERDICT|expected count" demo/lake/2026_03/validation_report.txt

# 2. The notebook runs top to bottom (exits 0; the committed notebook is not modified)
jupyter nbconvert --to notebook --execute demo/demo.ipynb --output-dir /tmp --output demo_run

# 3. A schema diff with real differences (demo vs 52-entry lake): warnings, but the lake passes
#    (stderr repeats the whole report: only the grep's lines matter; a crash leaves no report to grep)
rm -f /tmp/demo_vs_small.*
python bin/validate_lake.py --lake demo/lake/2026_03/lake --jsonl demo/lake/2026_03/sorted.jsonl.zst \
    --previous-manifest datalake/small/lake/manifest.json -o /tmp/demo_vs_small.txt 2>/dev/null
grep -E "WARN|VERDICT" /tmp/demo_vs_small.txt
#   [WARN] entries: column retyped: organism  —  fields removed: synonyms
#   ... (about a dozen more)
#   VERDICT: ALL CHECKS PASSED  (... 0 failed, N warnings)
```

## 4. Local subset rehearsal (~200K entries)

Large enough that DuckDB spills, sorts for real and writes multi-row-group
files. Its `trace.txt` is what you size the full run from.

### 4a. Make the input

Any UniProtKB REST `stream` query of roughly 100K–500K entries (JSON format)
will do, but VALIDATE treats every run as a release (check 20): a table with
no rows, or a shortcut column with no values, fails. Human (~200K, mostly
TrEMBL) is varied enough:

```bash
curl --globoff -o ~/uniprot100k.json.gz \
  "https://rest.uniprot.org/uniprotkb/stream?query=organism_id:9606&format=json&compressed=true"
pigz -t ~/uniprot100k.json.gz            # valid gzip
N_SUBSET=$(pigz -dc ~/uniprot100k.json.gz | python bin/stream_jsonl.py | wc -l); echo $N_SUBSET
#   stderr ends "stream_jsonl: DONE", no FATAL line
```

(`~/uniprot100k.json.gz` is also `run_lake.sh subset`'s default input.)

### 4b. Build

`run_lake.sh`'s small mode is fixed at 8 GB, so call Nextflow directly and give
the heavy processes most of the machine (here 16 GB; DuckDB gets 75%):

```bash
TMPD=${TMPDIR:-/tmp}/duckdb_tmp          # spill dir: a few GB here; elsewhere if that disk is short
mkdir -p datalake/reports
nextflow run upjson2lake.nf -profile local --inputfile ~/uniprot100k.json.gz \
    --outdir $(pwd)/datalake --release subset --process_memory '16 GB' \
    --duckdb_temp $TMPD --expected_count $N_SUBSET -resume
```

**Checks**

```bash
OUT=datalake/subset
# 1. End state as in 2a (a direct nextflow run exits 0 even without the marker)
ls $OUT/RELEASE_COMPLETE
grep -E "VERDICT|expected count" $OUT/validation_report.txt   # ALL CHECKS PASSED; all three == N_SUBSET
(cd $OUT/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)
grep "\[WARN\]" $OUT/validation_report.txt                    # usually none

# 2. Row counts: entries == N_SUBSET; xrefs and features 10–50x entries; accession_map >= entries
python -c "
import json; m=json.load(open('$OUT/lake/manifest.json'))
for t,v in m['tables'].items(): print(f'{t:15} {v[\"row_count\"]:>12,}  files={len(v[\"files\"])}')"

# 3. Resource headroom: peak_rss of SORT_JSONL, PARQUET_TRANSFORM, VALIDATE under 16 GB;
#    realtime × (release entries / N_SUBSET; 150M for 2026_03) is a rough full-run estimate (README budgets)
column -t -s $'\t' datalake/reports/trace.txt | cut -c1-200
#    macOS without a container engine records no peak_rss (nor %cpu, I/O; realtime it does): run this
#    stage on Linux, or time a task by hand in a copy of its work dir (a copy, so the cached task's
#    outputs keep their mtimes and -resume still reuses them); "maximum resident set size" is peak RSS, in bytes
#    (repeat for PARQUET_TRANSFORM and VALIDATE; bin/ goes on PATH as Nextflow puts it there).
#    Only with no run of this release in progress: the task clears its spill dir under $TMPD
WD=$(nextflow log last -f process,status,workdir | awk '$1=="SORT_JSONL" && $2!="FAILED" {print $3}' | tail -1)
rm -rf /tmp/measure && cp -RL "$WD" /tmp/measure
(B=$(pwd)/bin; cd /tmp/measure && PATH="$B:$PATH" /usr/bin/time -l bash .command.sh)

# 4. Spill went to $TMPD: subset.{SORT_JSONL,PARQUET_TRANSFORM,VALIDATE}.<hash>/, each holding a
#    duckdb_spill_*/ only while its task runs
ls $TMPD

# 5. Resume: the same nextflow command again -> cachedCount=5 in .nextflow.log.  -resume continues
#    the launch directory's last run: do this before 4c (or any other run from here), or name
#    the run to continue: -resume <run name from nextflow log>

# 6. Query it the way users will
python -c "
from uniprot_parquet import connect
con = connect('$OUT/lake')
print(con.sql(\"SELECT accession, id, protein_name_primary FROM entries WHERE id = 'INS_HUMAN'\").fetchall())
print(con.sql('SELECT division, count(*) FROM entries GROUP BY 1 ORDER BY 2 DESC').fetchall())
print(con.sql(\"SELECT feature_index, type, start_pos FROM features WHERE accession = 'P01308' ORDER BY feature_index\").fetchall()[:5])"
```

### 4c. Rehearse the SLURM runs (stand-in SLURM)

`tests/fake_slurm/` holds stand-ins for `sbatch`, `squeue` and `scancel`: on
the PATH, the `short` and `prod` profiles go through Nextflow's real SLURM
executor, and each job script runs on this machine. Requests are recorded in
`$FAKE_SLURM_DIR/<jobid>.sbatch`, not enforced. Each run gets its own copy of
the input, so nothing is served from an earlier stage's cache.

```bash
R=${TMPDIR:-/tmp}/uplake_rehearse; mkdir -p $R/oom/reports   # outside the checkout; ~1 GB
export FAKE_SLURM_DIR=$R/slurm PATH=$(pwd)/tests/fake_slurm:$PATH   # this shell only: see below
for run in smoke oom full; do cp tests/fixtures/small.json.gz $R/$run.json.gz; done

# 7a: short profile through run_lake.sh
./run_lake.sh subset --input $R/smoke.json.gz --outdir $R/smoke --duckdb-tmp $R/duckdb_tmp
# 7b: DuckDB out of memory at 2.5 GB, retried at 5 GB
nextflow run upjson2lake.nf -profile short --inputfile $R/oom.json.gz --outdir $R/oom \
    --release oom --process_memory '2.5 GB' -ansi-log false
# 8b: prod profile through run_lake.sh (on the cluster, --max-time is your queue's MaxWall: 8a)
./run_lake.sh full --expected-count 52 --first-release --release full --input $R/full.json.gz \
    --outdir $R/full --duckdb-tmp $R/duckdb_tmp --max-time 72h
```

The stand-ins stay on this shell's PATH: open a new shell (or `unset FAKE_SLURM_DIR` and
restore `PATH`) before running anything else, or its jobs go to the stand-in SLURM too.

**Checks**: after each run, the checks of the stage it rehearses (7a checks
2–4, 7b, and stage 9 checks 1 and 4); then the requests each job was submitted with:

```bash
for f in $FAKE_SLURM_DIR/*.sbatch; do echo "$(grep -o 'nf-[A-Z_]*' $f) $(grep -oE -- '(-c|-t|--mem|-p) \S+' $f | tr '\n' ' ')"; done
#   smoke: -t 04:00:00 -p short, --mem 16384M for SORT/TRANSFORM/VALIDATE
#   oom:   PARQUET_TRANSFORM twice: --mem 2560M, then 5120M (SORT_JSONL's chunked sort fits in 2560M)
#   full:  the 8a table (no -p; transform -c 4 -t 48:00:00 --mem 98304M; VALIDATE -c 8)
```

This proves the profiles, the headers, the retry scaling, `run_lake.sh` and
the `wd` helper. It cannot prove what only the cluster knows (queue limits,
the environment on the nodes, shared filesystems, the OOM killer): stage 7
still runs there. `tests/test_workflow.py` runs the prod case on every CI run.

## 5. (Optional) Real-data slice and benchmarks

A 1–5M-entry slice of the full dump best predicts full-run memory and disk, and
is the input the provisional settings wait for (README, "Open decisions").
`sample_jsonl.py` emits JSONL, not the JSON the workflow takes, so run the
scripts directly. Write the slice outside the checkout (`S`): an untracked
`slice_2m_lake/` in it would make later provenance record `dirty: true`.

```bash
S=$HOME/uplake_slice; mkdir -p $S         # outside the checkout, with tens of GB free
DUMP=$HOME/UniProtKB.json.gz              # the full UniProtKB JSON dump (~160 GB; 8a), wherever it is
pigz -dc $DUMP | python bin/stream_jsonl.py \
  | python bin/sample_jsonl.py --n 2000000 --seed 1 > $S/slice_2m.jsonl
python bin/sort_jsonl.py $S/slice_2m.jsonl -o $S/slice_2m.sorted.jsonl.zst --memory-limit 12GB --threads 4
/usr/bin/time $([ "$(uname)" = Darwin ] && echo -l || echo -v) \
    python bin/parquet_transform.py $S/slice_2m.sorted.jsonl.zst --outdir $S/slice_2m_lake \
    --memory-limit 12GB --threads 4 --release slice          # peak RSS: "maximum resident set size"
python bin/validate_lake.py --lake $S/slice_2m_lake --jsonl $S/slice_2m.sorted.jsonl.zst --expected-count 2000000 \
    -o $S/validation_report.txt           # not the default ./validation_report.txt: an untracked file in the checkout
# the measurements the provisional settings wait for (results/ is git-ignored)
python benchmarks/bench_zstd.py --jsonl $S/slice_2m.sorted.jsonl.zst --out benchmarks/results/
python benchmarks/bench_file_size.py --jsonl $S/slice_2m.sorted.jsonl.zst --out benchmarks/results/
python benchmarks/bench_point_lookup.py --lake $S/slice_2m_lake --serve --label slice
```

**Checks**: the validator exits 0; `du -sh $S/slice_2m_lake` × (release entries / 2M; 75 for
2026_03's 150M) estimates the
full lake; peak RSS does not scale linearly (DuckDB spills) but should stay well
under the 96 GB the full run requests.

**Part A is done** when stages 1–4 pass on a commit you are happy to run in
production. Commit that state; stage 8 checks for it.

---

# Part B — SLURM cluster

## 6. Cluster environment

Once, on the login node:

```bash
# 1. The commit Part A passed on (git rev-parse HEAD on your machine)
git clone <repo URL> ~/uniprot-parquet        # or in an existing clone: git fetch
cd ~/uniprot-parquet && git checkout static-lake && git pull
git rev-parse HEAD                            # == the commit Part A passed on

# 2. The environment, as in stage 0
micromamba create -f environment.yml -y
```

Then, in **every** shell you launch from (each `tmux`/`screen` window, each new
login; an `sbatch` head job inherits them from the shell that submits it):

```bash
cd ~/uniprot-parquet                     # run_lake.sh and the checks run from the checkout
micromamba activate uniprot-parquet      # carried to the compute nodes (no container)
export SCRATCH=/scratch/$USER            # your scratch filesystem, seen by every compute node
export NXF_WORK=$SCRATCH/nf_work         # Nextflow's work dir; the same on every re-submit (-resume looks there)
wd() { grep "submitted process $1" .nextflow.log | tail -1 | sed 's/.*workDir: //' | grep . \
       || nextflow log last -f process,workdir | awk -v p="$1" '$1 == p {d = $2} END {print d}'; }
```

Part B writes everything under `$SCRATCH`. `NXF_WORK` holds every intermediate
file (the JSONL twice, the transform's staging Parquet, each output before it
is published): hundreds of GB at full scale, so not `work/` in the checkout,
which is often under a home-directory quota. `wd PROCESS` prints the work
directory of PROCESS's latest attempt in the latest run from this directory:
from `.nextflow.log` (rewritten by every run) for a task that run submitted,
also while it runs; else from `nextflow log` for a task it took from the
cache. Before the first run it has nothing to show; the checks from stage 7
on use it.

**Checks**

```bash
# 1. The stage 0 checks, on the login node
# 2. The same on a compute node, as Nextflow's jobs see it
srun -p short -c 4 --mem 16G -t 1:00:00 --pty bash
#    in that shell: the every-shell block above, the stage 0 checks, and optionally
#    stage 1's tests (~10 min); then exit
# 3. Scratch is writable and has room (stage 8a: ~3 TB for the full run)
mkdir -p $NXF_WORK && df -h $SCRATCH
```

## 7. SLURM smoke test and OOM retry (`short` queue)

Run `run_lake.sh` from `tmux`/`screen` on the login node, or as a light `sbatch`
head job: it blocks until Nextflow finishes. Each shell needs stage 6's
every-shell block.

### 7a. Smoke test (52 entries)

Proves the plumbing, not the data. `subset` mode uses the `short` profile:
4 h wall time for every process (retries included), 16 GB for SORT_JSONL,
PARQUET_TRANSFORM and VALIDATE.

```bash
./run_lake.sh subset --input tests/fixtures/small.json.gz \
    --outdir $SCRATCH/uplake_smoke --duckdb-tmp $SCRATCH/duckdb_tmp
```

**Checks**

```bash
# 1. Jobs went to SLURM
squeue -u $USER                                       # during the run: five jobs, one at a time
grep -c "executor > slurm" .nextflow.log              # >= 1

# 2. Nodes see the env and tools
grep "SLURM Job ID" $(wd STREAM_JSONL)/.command.log   # beforeScript ran on a node
grep -l "command not found" $NXF_WORK/*/*/.command.err   # no output

# 3. The 2a end-state checks on $SCRATCH/uplake_smoke/subset/
#    (RELEASE_COMPLETE, VERDICT, expected count, provenance commit, checksums, SCHEMA.md)

# 4. Resource accounting works: every row COMPLETED, exit 0, peak_rss populated
column -t -s $'\t' $SCRATCH/uplake_smoke/reports/trace.txt | cut -c1-160

# 5. Resume: the same command again -> cachedCount=5 (before 7b: -resume continues the
#    launch directory's last run)
```

### 7b. Prove the OOM retry (recommended, ~5 min)

With `--process_memory '2.5 GB'` DuckDB gets 1920MB: too little for the
transform, which allocates a 1 GiB JSON buffer per thread, so it exits 75
(DuckDB's out-of-memory error) and is retried at 5 GB (DuckDB 3840MB), which
passes. (The sort's chunks fit in 1920MB.) `run_lake.sh subset` fixes 16 GB, so
run Nextflow directly:

```bash
mkdir -p $SCRATCH/uplake_oom/reports
nextflow run upjson2lake.nf -profile short --inputfile tests/fixtures/small.json.gz \
    --outdir $SCRATCH/uplake_oom --release oom --process_memory '2.5 GB' -ansi-log false
```

**Checks**

```bash
ls $SCRATCH/uplake_oom/oom/RELEASE_COMPLETE
#   the run succeeded (exit 0, marker written) although an attempt failed
nextflow log last -f name,status,exit,memory,time
#   PARQUET_TRANSFORM twice: FAILED 75 2.5 GB, then COMPLETED 0 5 GB (prod: 96 -> 192 GB);
#   SORT_JSONL once (its chunked sort fits in 2.5 GB)
WD=$(nextflow log last -f workdir -F "status == 'FAILED'")   # the failed attempt's work dir
grep "FATAL — DuckDB ran out of memory" $WD/.command.err
rm -rf $SCRATCH/uplake_oom $SCRATCH/uplake_smoke
```

If the run stopped at the first failure, the retry directives are not applied
on your cluster: stop and find out why. (The OOM killer, 137/140, and SLURM's
pre-kill, 143, are retried the same way; 8a lists the retry limits.)

If the nodes differ much from the stage 4 machine, repeat stage 4 here
(`./run_lake.sh subset --outdir $SCRATCH/uplake_subset --duckdb-tmp
$SCRATCH/duckdb_tmp`) and size the full run from that trace.

## 8. Full UniProtKB build (`prod` queue)

### 8a. Before submitting

```bash
# 1. The commit Part A passed on, clean: provenance.json records the commit and a dirty flag
git status --short        # empty (datalake/, results/, work/, .nextflow*, slurm-*.out are ignored;
                          # any other untracked file counts as dirty)
git rev-parse HEAD        # write this down

# 2. Expected entry count (Swiss-Prot + TrEMBL): https://www.uniprot.org/help/release-statistics,
#    or the FTP release's relnotes.txt
EXPECTED=248799253        # replace with the real number

# 3. The dump is that release and intact
DUMP=$SCRATCH/UniProtKB.json.gz           # wherever you downloaded it; 8b and 9 use $DUMP
pigz -t $DUMP && echo GZIP_OK
ls -l $DUMP                               # ~160 GB compressed for 2026 releases

# 4. Disk: under --duckdb_temp, SORT_JSONL's chunks and runs (~1.5x the compressed JSONL), then
#    PARQUET_TRANSFORM's spill (~1-2 TB);
#    the transform also stages the JSONL as Parquet in its work dir; the published release is
#    tens to a few hundred GB. `disk` directives are not passed to SLURM, so nothing reserves this
df -h $SCRATCH                            # >= 3 TB free if $NXF_WORK and spill share it, plus the output
echo $NXF_WORK                            # set (stage 6), on that scratch filesystem

# 5. Queue limits. Requests, attempt 1 -> retry:
#      STREAM_JSONL       4 CPUs   4 GB          48h         (not retried)
#      SORT_JSONL         4 CPUs  96 -> 192 GB   24h -> 48h
#      PARQUET_TRANSFORM  4 CPUs  96 -> 192 GB   48h -> 72h  (capped at --max_time)
#      VALIDATE           8 CPUs  96 -> 192 GB   24h -> 48h
#    (transform headers: -c 4 -t 48:00:00 --mem 98304M, no -p: the cluster's default
#    partition, or run_lake.sh --queue <partition>). If that partition's MaxWall is under
#    72h, pass --max-time <limit> to run_lake.sh, or SLURM rejects the retry.
sinfo -o "%P %l %m %c"                    # the default partition is marked *
sacctmgr show qos format=name,maxwall,maxtres
```

### 8b. Submit

From `tmux`/`screen` on the login node, or an `sbatch` head job with a wall
time longer than the whole pipeline (2–3 days) and its output outside the
checkout (`sbatch -o $SCRATCH/uplake-%j.out ...`; only the default
`slurm-<jobid>.out` is git-ignored).

```bash
./run_lake.sh full \
    --expected-count $EXPECTED \
    --release 2026_03 \
    --input $DUMP \
    --outdir $SCRATCH/uniprot_parquet \
    --duckdb-tmp $SCRATCH/duckdb_tmp \
    --previous-manifest $SCRATCH/uniprot_parquet/<previous release>/lake/manifest.json
#   --first-release instead of --previous-manifest for the very first release
#   --max-time <limit> if the queue's MaxWall is under 72h
#   --queue <partition> for one other than the cluster's default
```

Pass `--release` explicitly (the default is `2026_03`) so the output directory
matches the dump. `run_lake.sh` refuses a previous manifest that is missing or
inside the release being built. For failure emails, run Nextflow directly with
`--notify_email` (README, "Nextflow directly").

### 8c. While it runs — checks per process

Set `WD=$(wd <PROCESS>)` for each (stage 7).

```bash
# STREAM_JSONL (2–4 h)
tail -2 $WD/.command.log           # "stream_jsonl: 123,000,000 entries (25,000/s)", every 1M entries
squeue -j <jobid> -o "%T %M %L"    # RUNNING; time left > remaining entries / rate
cat $WD/entry_count.txt            # on completion: == $EXPECTED (otherwise it exits 1 and the run stops)
grep "Verified:" $WD/.command.log  # zstd line count matched the streamed count

# SORT_JSONL (4–8 h)
grep -E "chunks of|chunk\(s\)|run [0-9]+/|runs merged" $WD/.command.err | tail -3
#   "N chunk(s)" once the input is cut, then "run i/N sorted" per chunk, then "N runs merged"
du -sh $SCRATCH/duckdb_tmp         # the chunks and runs: ~1.5x the compressed JSONL at most
ls -l $SCRATCH/uniprot_parquet/2026_03/sorted.jsonl.zst   # published when done

# PARQUET_TRANSFORM (6–12 h)
grep -E "^--- |rows so far|rows in" $WD/.command.log | tail -5
#   "--- ENTRIES ---", "entries: 250,000,000 rows in N files", then FEATURES, XREFS, ...
grep -E "New UniProt fields|stored as JSON|shortcut sources absent" $WD/.command.log
#   keys UniProt added (new source columns) and fields whose type varies (stored as JSON):
#   informational, unless such a field feeds a shortcut, when the build stops (cheatsheet)
ls $WD/lake/.complete/             # a <table>.json per finished table, for a manual
                                   # --skip-existing re-run; removed on success, not published
```

- **STREAM_JSONL** `FATAL — interrupted` or `BrokenPipeError`: nothing
  downstream ran and no count was written; fix the cause and re-submit.
- **PARQUET_TRANSFORM** OOM (exit 75, 137, 140 or 143): a second attempt follows
  at 192 GB (a retry is capped at 256 GB). If that fails too, see the
  cheatsheet; a re-submit restarts at the transform.
- **VALIDATE** (not yet timed at full scale): `[PASS]` lines in
  `$WD/.command.log`. Any `[FAIL]` exits 1, so PROVENANCE does not run and no
  `RELEASE_COMPLETE` appears (cheatsheet).
- **PROVENANCE** (< 1 min) writes `SCHEMA.md` and `provenance.json`. When every
  publish copy has finished, `onComplete` runs `bin/verify_release.py` on the
  published release and renames `.RELEASE_COMPLETE.pending` to
  `RELEASE_COMPLETE` only if it passes; otherwise the log says `RELEASE_COMPLETE
  not written`, Nextflow still exits 0 and `run_lake.sh` exits 1.

## 9. After the run

```bash
OUT=$SCRATCH/uniprot_parquet/2026_03

# 1. Marker and verdict
ls $OUT/RELEASE_COMPLETE
grep -E "VERDICT|expected count" $OUT/validation_report.txt    # ALL CHECKS PASSED; all three == $EXPECTED

# 2. Provenance: {'commit': '<the hash you wrote down>', 'dirty': False}, and the dump you downloaded
python -c "import json; p=json.load(open('$OUT/provenance.json')); print(p['pipeline'], p['release']); print(p['inputs']['source'], p['tools'])"
sha256sum $DUMP                          # == inputs.source.sha256

# 3. Whole-lake checksums (slow: run it as a job, ~30–60 min)
(cd $OUT/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)

# 4. Totals: entries == $EXPECTED; {'swissprot': N, 'trembl': M}, N == the stats page's Swiss-Prot count
python -c "
import json; m=json.load(open('$OUT/lake/manifest.json'))['tables']['entries']
print(m['row_count'], m['partitioning']['keys'][0]['row_counts'])"

# 5. Resource actuals: keep them for the next release's budget
column -t -s $'\t' $SCRATCH/uniprot_parquet/reports/trace.txt | cut -c1-220

# 6. Query smoke tests
python -c "
from uniprot_parquet import connect
con = connect('$OUT/lake')
print(con.sql(\"SELECT accession, protein_name_primary FROM entries WHERE accession = 'P01308'\").fetchall())   # human insulin
print(con.sql('SELECT reviewed, count(*) FROM entries GROUP BY 1').fetchall())
print(con.sql('SELECT count(*) FROM accession_map WHERE NOT is_primary').fetchall())"

# 7. What changed upstream: warnings never fail a release, but read them
grep "\[WARN\]" $OUT/validation_report.txt
#   "new UniProt fields (source columns without a description)": describe them in
#   COLUMN_DESCRIPTIONS; decide whether any deserves a shortcut
#   "SHORTCUT column ...": the shortcut contract changed; was schema_version bumped?
head -5 $OUT/SCHEMA.md
```

## 10. Recovery cheatsheet

"Re-submit" means the identical `run_lake.sh` command, from the same directory
with the same `NXF_WORK`: `-resume` reuses every
completed process and republishes every output whole.

| Symptom | What to do |
|---|---|
| Head job / login session killed mid-run | Re-submit. A copy cut short mid-publish is repaired; `RELEASE_COMPLETE` is written only after `bin/verify_release.py` passes. For hash-level certainty: `bin/verify_release.py <release dir> --full`. |
| `RELEASE_COMPLETE not written: the published copy ... does not match its metadata` | Something changed the release directory during or after publishing (full disk, another process). Re-submit. |
| A job hit its wall time | SORT_JSONL, PARQUET_TRANSFORM and VALIDATE retry once with twice the time (capped at `--max_time`); STREAM_JSONL and PROVENANCE do not. If it still fails, raise `--max-time` within the queue's limit, or re-submit. PARQUET_TRANSFORM always restarts in a fresh work dir: the whole transform re-runs. |
| `PARQUET_TRANSFORM` OOM twice | Raise `MEMORY=` in `run_lake.sh`'s `full` mode and commit it (an uncommitted edit makes provenance record `dirty: true`), or run Nextflow directly with `--process_memory`; then re-submit. |
| `Process requirement exceeds available memory` (local) | `--process_memory` exceeds the machine; lower it (DuckDB spills to `--duckdb_temp`). |
| `STREAM_JSONL`: `FATAL — ijson's C backend (yajl2_c) is not available` | The env's ijson has no C extension (the pure-Python parser would take ~11x as long). `python -m pip install --force-reinstall --no-deps ijson`, check stage 0's `ijson.backend` line, and re-submit. |
| `STREAM_JSONL`: `FATAL — count mismatch` | The dump is not the release you think, or is truncated: re-check `EXPECTED` and `pigz -t`. |
| `STREAM_JSONL`: `FATAL — LoneSurrogate` or `Integer exceeds 64-bit range` | The dump holds data a JSON line cannot carry exactly (an unpaired `\uD800`-style escape; an integer outside −2^63 … 2^64−1); the message names the entry (and byte offset). Report it to UniProt; the pipeline refuses rather than change data. |
| `VALIDATE` fails | Read the failed task's report: `WD=$(nextflow log last -f process,status,workdir \| awk '$1=="VALIDATE" && $2=="FAILED" {print $3}' \| tail -1)`, then `$WD/validation_report.txt` (a failing report is not published: `$OUT/validation_report.txt` is missing or a previous run's). Fix and re-submit: only processes whose code or inputs changed re-run. |
| `VALIDATE` check 20: `<table>.<column> has values` or `<table> has rows` fails | A shortcut is empty in every row (its source field is absent or renamed upstream: compare the transform's `shortcut sources absent from input` warning), or a table is empty. Update the shortcut's SQL in `bin/parquet_transform.py`. |
| `VALIDATE` check 21: `<table> shortcut values match the JSONL` fails | A shortcut disagrees with `bin/shortcut_reference.py`; the detail names column, accession, expected and actual value. One side is wrong, often because UniProt changed a field the shortcut reads: fix it and rebuild. |
| `PARQUET_TRANSFORM`: `Input lacks fields every UniProtKB entry carries` | UniProt removed or renamed a field a shortcut is built from (or the input is not UniProtKB). Update the SQL builder and `REQUIRED_PATHS` in `bin/parquet_transform.py` (a shortcut-schema change). New or retyped fields no shortcut reads never stop the build. |
| `PARQUET_TRANSFORM`: `Shortcut columns whose source field has a different type in different entries` | DuckDB inferred JSON for a field a shortcut reads; the message names column and path. Decide how the shortcut reads it, update its SQL builder, and bump `schema_version` if the column's type or meaning changes. |
| `PARQUET_TRANSFORM`: `Input has entryType value(s) [...]` | UniProt reworded or added an `entryType`. Add it to `ENTRY_TYPES` in both `bin/parquet_transform.py` and `bin/shortcut_reference.py`, mapped to reviewed or unreviewed. |
| `PARQUET_TRANSFORM`: `input key(s) [...] have the name of a shortcut column` | UniProt added a key named like a shortcut (case-insensitive). Rename the shortcut in `bin/parquet_transform.py` (a major schema version). |
| Manual transform re-run on an existing `lake/` (outside Nextflow) | `parquet_transform.py ... --skip-existing` skips only tables whose `.complete/<table>.json` matches the files on disk, the input (size, mtime) and release; others are rebuilt, with a `REBUILD <table>: ...` line on stderr. |
| Reports in `<outdir>/reports/` look stale | They are overwritten every run: check the mtime, then `.nextflow.log` for `Failed to render`. |
