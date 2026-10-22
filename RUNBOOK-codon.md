# Codon walkthrough — RUNBOOK Part B on EBI's Codon cluster

A step-by-step path through [RUNBOOK.md](RUNBOOK.md) Part B (stages 6–9) on
Codon, after Part A passed on your machine ([RUNBOOK-macos.md](RUNBOOK-macos.md)).
Each step is a block to paste, then what you should see. RUNBOOK.md has the
explanations and, in §10, what to do when a check fails.

How the run is laid out on Codon:

- **Your tmux session** (an interactive job on `datamover`) is where you
  prepare, run the short tests, and watch. It can end without harm once the
  full run is submitted.
- **The full run's head job** is a light `sbatch` job on `datamover` (30-day
  limit): Nextflow itself, which submits one SLURM job per step.
- **The pipeline's jobs** go to the cluster's default partition (`standard`,
  7 days), or wherever `--queue` sends them.
- **Everything is written under `$SCRATCH`** on `/hps/nobackup`: Nextflow's
  work directory, DuckDB's spill, the release.

Two rules that save most of the trouble:

- **Run the "every shell" block in each new shell** (each tmux window, each
  new login, an `srun` shell). The head job inherits it from the shell that
  submits it.
- **Run `nextflow` and `run_lake.sh` only from `~/uniprot-parquet`**, one run
  at a time: `-resume` continues the directory's *last* run, and `wd` reads
  its `.nextflow.log`.

## Every shell

```bash
cd ~/uniprot-parquet
micromamba activate uniprot-parquet
export SCRATCH=/hps/nobackup/martin/uniprot/users/$USER
export NXF_WORK=$SCRATCH/nf_work       # the same on every re-submit: -resume looks for its cache here
wd() { grep "submitted process $1" .nextflow.log | tail -1 | sed 's/.*workDir: //'; }
```

`wd PROCESS` prints the work directory of PROCESS's latest attempt in the last
run from this directory. It has nothing to show until a run has started.

---

## 1. The code

```bash
git pull
git status --short          # nothing
git rev-parse HEAD          # write it down: provenance.json must record this commit
```

## 2. Environment (once, and after `environment.yml` changes)

```bash
micromamba deactivate; micromamba env remove -n uniprot-parquet -y   # only when rebuilding
micromamba create -f environment.yml -y
micromamba activate uniprot-parquet
```

**Check**

```bash
nextflow -version | grep version                  # 26.04 or later
which pigz zstd                                    # both found
python -c "import sys, ijson; print(sys._is_gil_enabled(), ijson.backend)"   # True yajl2_c
nextflow lint upjson2lake.nf nextflow.config       # "2 files had no errors"
```

## 3. What the compute nodes see

The pipeline's jobs run on `standard` nodes, from the environment you activate
and on `/hps/nobackup`. Check both from such a node. Start it **from the login
node**: inside your interactive job, `srun` starts a step of that job rather
than a new one on `standard`.

```bash
srun -p standard -c 4 --mem 16G -t 1:00:00 --pty bash
```

then, in that shell, the "every shell" block, and:

```bash
python -c "import sys, ijson; print(sys._is_gil_enabled(), ijson.backend)"   # True yajl2_c
which pigz zstd                                                            # both found
mkdir -p $NXF_WORK $SCRATCH/duckdb_tmp
touch $SCRATCH/duckdb_tmp/.w && rm $SCRATCH/duckdb_tmp/.w && echo WRITABLE   # WRITABLE
exit
```

## 4. Tests (RUNBOOK stage 1)

In your tmux session (the interactive job needs about 4 CPUs and 8 GB), from
one shell only:

```bash
REQUIRE_NEXTFLOW=1 python -m pytest tests/ -q
```

**Expect**: `403 passed, 1 xfailed`. The first run downloads a fixture from
`rest.uniprot.org` before any test starts and says so ("Downloading the
default test fixture…", one line per query, a few minutes). If a test fails
with a zlib or gzip error, `gzip -t tests/fixtures/*.json.gz` names the damaged
fixture: delete it and run again.

## 5. Smoke test and out-of-memory retry (RUNBOOK 7a, 7b)

**5a. Smoke test** (52 entries through the `short` queue, ~5 minutes):

```bash
./run_lake.sh subset --input tests/fixtures/small.json.gz \
    --outdir $SCRATCH/uplake_smoke --duckdb-tmp $SCRATCH/duckdb_tmp
```

**Check**

```bash
grep "SLURM Job ID" $(wd STREAM_JSONL)/.command.log      # the job ran on a node
ls $SCRATCH/uplake_smoke/subset/RELEASE_COMPLETE
grep VERDICT $SCRATCH/uplake_smoke/subset/validation_report.txt   # ALL CHECKS PASSED
column -t -s $'\t' $SCRATCH/uplake_smoke/reports/trace.txt | cut -c1-160
#   every row COMPLETED, exit 0, peak_rss filled in (Linux records it)
```

Then the same `run_lake.sh` command again, straight away:

```bash
grep -o "cachedCount=[0-9]*" .nextflow.log               # cachedCount=5
```

**5b. Out-of-memory retry** (~5 minutes):

```bash
mkdir -p $SCRATCH/uplake_oom/reports
nextflow run upjson2lake.nf -profile short --inputfile tests/fixtures/small.json.gz \
    --outdir $SCRATCH/uplake_oom --release oom --process_memory '2.5 GB' -ansi-log false
```

**Expect**: `PARQUET_TRANSFORM ... terminated with an error exit status (75) --
Execution is retried (1)`, then `RELEASE_COMPLETE written`.

```bash
nextflow log last -f name,status,exit,memory,time
```

exactly:

```
STREAM_JSONL (stream)           COMPLETED  0   4 GB    4h
SORT_JSONL (sort_jsonl)         COMPLETED  0   2.5 GB  4h
PARQUET_TRANSFORM (transform)   FAILED     75  2.5 GB  4h
PARQUET_TRANSFORM (transform)   COMPLETED  0   5 GB    4h
VALIDATE (validate)             COMPLETED  0   2.5 GB  4h
PROVENANCE (provenance)         COMPLETED  0   1 GB    4h
```

```bash
grep "FATAL — DuckDB ran out of memory" $(nextflow log last -f workdir -F "status == 'FAILED'")/.command.err
rm -rf $SCRATCH/uplake_oom $SCRATCH/uplake_smoke
```

## 6. Real-data slice (RUNBOOK stage 5; optional, recommended)

The full run has not been sized: a 2M-entry sample of the real dump gives the
memory, time and disk to expect (×125 for the full ~250M entries). It reads
the whole dump once (2–4 hours), so it runs as a job of its own. With the
"every shell" block and `DUMP` (step 7) set:

```bash
cat > $SCRATCH/slice.sh <<'EOF'
#!/bin/bash
set -euo pipefail
cd ~/uniprot-parquet
S=$SCRATCH/uplake_slice; mkdir -p $S
pigz -dc $DUMP | python bin/stream_jsonl.py \
  | python bin/sample_jsonl.py --n 2000000 --seed 1 > $S/slice_2m.jsonl
echo "== sort";      /usr/bin/time -v python bin/sort_jsonl.py $S/slice_2m.jsonl \
    -o $S/slice_2m.sorted.jsonl.zst --memory-limit 12GB --threads 4 --temp-dir $SCRATCH/duckdb_tmp
echo "== transform"; /usr/bin/time -v python bin/parquet_transform.py $S/slice_2m.sorted.jsonl.zst \
    --outdir $S/slice_2m_lake --memory-limit 12GB --threads 4 --release slice --temp-dir $SCRATCH/duckdb_tmp
python bin/validate_lake.py --lake $S/slice_2m_lake --jsonl $S/slice_2m.sorted.jsonl.zst \
    --expected-count 2000000 --temp-dir $SCRATCH/duckdb_tmp -o $S/validation_report.txt 2>/dev/null || true
grep VERDICT $S/validation_report.txt
du -sh $S/slice_2m_lake
EOF
sbatch -p standard -c 4 --mem 16G -t 24:00:00 -J uplake-slice -o $SCRATCH/slice-%j.out \
    --export=ALL,DUMP=$DUMP $SCRATCH/slice.sh
```

When `squeue -u $USER` no longer lists `uplake-slice`:

```bash
grep -E "^== |Elapsed \(wall clock\)|Maximum resident|VERDICT|slice_2m_lake" $SCRATCH/slice-<id>.out
```

- **Expect**: `VERDICT: ALL CHECKS PASSED`; for the sort and the transform, a
  peak RSS (`Maximum resident set size`, in kB) well under 12 GB.
- **Full-run estimate**: wall time ×125 (rough: memory does not grow with the
  input, time roughly does) against the README budgets (sort 4–8 h, transform
  6–12 h); the lake's size ×125 against your space on `$SCRATCH`.
- If the transform's estimate is over 48 h, raise the retry's time cap before
  step 8 (`--max-time`, default 72 h; `standard` allows 7 days).

Then `rm -rf $SCRATCH/uplake_slice $SCRATCH/slice.sh`.

## 7. Before submitting (RUNBOOK 8a)

```bash
# 1. The code: clean, the commit you wrote down in step 1
git status --short                         # nothing
git rev-parse HEAD

# 2. The release's entry count (Swiss-Prot + TrEMBL):
#    https://www.uniprot.org/help/release-statistics, or the FTP release's relnotes.txt
EXPECTED=<the count>

# 3. The dump: that release, intact (reads all ~160 GB: ~30-60 min)
DUMP=$SCRATCH/UniProtKB.json.gz            # wherever it is
pigz -t $DUMP && echo GZIP_OK

# 4. Space: ~3 TB free on $SCRATCH (work dir, spill, release)
df -h $SCRATCH

# 5. No QOS caps below the retry's requests (192 GB, 72 h)
sacctmgr show qos format=name,maxwall,maxtres

# 6. The variables the submit command expands
echo "EXPECTED=$EXPECTED DUMP=$DUMP NXF_WORK=$NXF_WORK"
```

## 8. Submit the full run (RUNBOOK 8b)

From `~/uniprot-parquet`, in a shell with the "every shell" block and step 7's
`EXPECTED` and `DUMP`. Use the dump's release as `--release`.

```bash
sbatch -p datamover -c 2 --mem 8G -t 7-00:00:00 -J uplake -o $SCRATCH/uplake-%j.out \
    --wrap "./run_lake.sh full --expected-count $EXPECTED --release 2026_03 --input $DUMP \
            --outdir $SCRATCH/uniprot_parquet --duckdb-tmp $SCRATCH/duckdb_tmp --first-release"
```

- `--first-release` for the first lake; after that,
  `--previous-manifest $SCRATCH/uniprot_parquet/<previous release>/lake/manifest.json`.
- `--queue <partition>` sends the pipeline's jobs somewhere other than `standard`.

**Expect**: `Submitted batch job <id>`, then in the log the banner with
`Queue: the profile's (prod: the cluster's default)`:

```bash
tail -f $SCRATCH/uplake-<id>.out
```

**While it runs**, leave `~/uniprot-parquet` alone: no `git pull`, no edits
(a step that has not started yet would run the changed code), and no other
`nextflow` run from it.

## 9. Watch it (RUNBOOK 8c)

From `~/uniprot-parquet`, in any shell with the "every shell" block:

```bash
squeue -u $USER                            # the head job (uplake) and the current step's job
tail -3 $SCRATCH/uplake-<id>.out
```

Per step, with `WD=$(wd <STEP>)`:

```bash
# STREAM_JSONL (2-4 h): progress every 1M entries; on completion the count == $EXPECTED
WD=$(wd STREAM_JSONL); tail -2 $WD/.command.log; cat $WD/entry_count.txt

# SORT_JSONL (4-8 h): "N chunk(s)", then "run i/N sorted", then "N runs merged"
WD=$(wd SORT_JSONL); grep -E "chunk\(s\)|run [0-9]+/|runs merged" $WD/.command.err | tail -3

# PARQUET_TRANSFORM (6-12 h): one table after another
WD=$(wd PARQUET_TRANSFORM); grep -E "^--- |rows in" $WD/.command.log | tail -4

# VALIDATE: [PASS] lines accumulate; any [FAIL] stops the run before the release is marked
WD=$(wd VALIDATE); grep -c "\[PASS\]" $WD/.command.log; grep "\[FAIL\]" $WD/.command.log
```

A step that fails with exit 75, 137, 140 or 143 (memory, or SLURM's kill) is
re-submitted once with twice the memory and time: `NOTE: ... Execution is
retried (1)` in the log. To stop the whole run: `scancel <head job id>`
(Nextflow cancels its jobs).

## 10. When it finishes (RUNBOOK stage 9)

The log ends `RELEASE_COMPLETE written to ...` and `'-- ENDED`.

```bash
OUT=$SCRATCH/uniprot_parquet/2026_03
ls $OUT/RELEASE_COMPLETE
grep -E "VERDICT|expected count" $OUT/validation_report.txt     # ALL CHECKS PASSED; expected = jsonl = entries = $EXPECTED
python -c "import json; p=json.load(open('$OUT/provenance.json')); print(p['pipeline'])"
#   {'commit': '<step 1's commit>', 'dirty': False, ...}
python -c "
import json; m=json.load(open('$OUT/lake/manifest.json'))['tables']['entries']
print(m['row_count'], m['partitioning']['keys'][0]['row_counts'])"
#   == $EXPECTED; swissprot == the release statistics' Swiss-Prot count
column -t -s $'\t' $SCRATCH/uniprot_parquet/reports/trace.txt | cut -c1-220   # keep: next release's budget
grep "\[WARN\]" $OUT/validation_report.txt                      # what changed upstream (never fails a release)
```

The whole-lake checksum check reads every file (~30–60 min), so as a job:

```bash
sbatch -p datamover -c 1 --mem 2G -t 4:00:00 -o $SCRATCH/checksums-%j.out \
    --wrap "cd $OUT/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK"
```

## If something goes wrong

- **The head job died, or you cancelled it**: submit the same `sbatch`
  command again, from `~/uniprot-parquet` with the same `NXF_WORK`.
  `run_lake.sh` passes `-resume`: finished steps are reused, the interrupted
  one starts again.
- **A step failed**: the log names it and its work directory; its
  `.command.err` says why. RUNBOOK.md §10 lists each failure message and what
  to do.
- **`RELEASE_COMPLETE not written`**: the published copy did not match its
  metadata (full disk?); fix the cause and re-submit.
