# macOS walkthrough — RUNBOOK Part A on a Mac

A step-by-step path through [RUNBOOK.md](RUNBOOK.md) Part A (stages 0–4) on
macOS with zsh and micromamba. Each step is a block to paste, then what you
should see. RUNBOOK.md has the explanations and, in §10, what to do when a
check fails.

Two rules that save most of the trouble:

- **Run the "every terminal" block in each new terminal.** Variables such as
  `N_SUBSET` and `TMPD` do not carry over, and an empty one breaks the next
  command (`--expected_count $N_SUBSET` with `N_SUBSET` unset is refused).
- **Run `nextflow` only from the repo root**, one run at a time. `-resume`
  continues the directory's *last* run, whatever it was.

## Every terminal

```bash
cd ~/Developer/uniprot-parquet
micromamba activate uniprot-parquet
TMPD=${TMPDIR:-/tmp}/duckdb_tmp          # DuckDB spill for stage 4 (a few GB)
N_SUBSET=210706                          # stage 4's entry count: set it from step 6 once known
```

Your prompt should start with `(uniprot-parquet)`. Without it, `python` is
another interpreter (`No module named 'duckdb'`) and `nextflow` is missing.

---

## 1. Environment (once, and after `environment.yml` changes)

```bash
micromamba deactivate; micromamba env remove -n uniprot-parquet -y   # only when rebuilding
micromamba create -f environment.yml -y
micromamba activate uniprot-parquet
```

**Check**

```bash
nextflow -version | grep version                 # 26.04 or later
java -version 2>&1 | head -1                      # 21
which pigz zstd                                   # both found
python -c "import sys, ijson; print(sys._is_gil_enabled(), ijson.backend)"   # True yajl2_c
python -c "import pyarrow, duckdb; print(pyarrow.__version__, duckdb.__version__)"  # 23.x 1.5.x
nextflow lint upjson2lake.nf nextflow.config      # "2 files had no errors"
```

`False` (a free-threaded Python) or `python` (no C parser) means the env is
wrong: rebuild it from `environment.yml`, which pins both.

## 2. Tests (RUNBOOK stage 1)

```bash
REQUIRE_NEXTFLOW=1 python -m pytest tests/ -q
REQUIRE_NEXTFLOW=1 python -m pytest tests/ -q --stress     # ~8 min, ~8 GB RAM
ruff check . && mypy
```

**Expect**: `418 passed, 1 xfailed` for each pytest run (no failures, no
skips); `All checks passed!` and `Success: no issues found in 38 source files`.

The first run of each downloads a fixture from `rest.uniprot.org` before any
test starts, and says so: "Downloading the default test fixture, ~4.6K
entries…", then one line per query (a few minutes; `--stress` fetches ~14K
entries). If that fails, `gzip -t tests/fixtures/*.json.gz` names a damaged
file: delete it and run again.

## 3. Small lake (RUNBOOK 2a–2c)

```bash
./run_lake.sh
```

**Check**

```bash
ls datalake/small/RELEASE_COMPLETE
grep -E "VERDICT|expected count" datalake/small/validation_report.txt
#   [PASS] ... expected=52, jsonl=52, entries=52
#   VERDICT: ALL CHECKS PASSED  (N/N checks passed, 0 failed, ...)
(cd datalake/small/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)
```

Then the same command again, and a second release diffed against the first:

```bash
./run_lake.sh
grep -o "cachedCount=[0-9]*" .nextflow.log                          # cachedCount=7
./run_lake.sh --release small2 --previous-manifest datalake/small/lake/manifest.json
grep -A2 "^--- 14" datalake/small2/validation_report.txt            # previous: release small ...
                                                                    # no table or column differences
```

## 4. Demo lake and notebook (RUNBOOK stage 3)

```bash
demo/run_demo.sh
```

**Check**

```bash
grep -E "VERDICT|expected count" demo/lake/2026_03/validation_report.txt
#   expected = jsonl = entries (5,378 when last run); ALL CHECKS PASSED
jupyter nbconvert --to notebook --execute demo/demo.ipynb --output-dir /tmp --output demo_run
#   "Writing ... bytes to /tmp/demo_run.ipynb"
rm -f /tmp/demo_vs_small.*
python bin/validate_lake.py --lake demo/lake/2026_03/lake --jsonl demo/lake/2026_03/sorted.jsonl.zst \
    --previous-manifest datalake/small/lake/manifest.json -o /tmp/demo_vs_small.txt 2>/dev/null
grep VERDICT /tmp/demo_vs_small.txt
#   VERDICT: ALL CHECKS PASSED  (... 0 failed, 13 warnings): schema differences are warnings
git status --short demo/                                            # nothing: the notebook is unchanged
```

## 5. Subset input (RUNBOOK 4a, once)

```bash
curl --globoff -o ~/uniprot100k.json.gz \
  "https://rest.uniprot.org/uniprotkb/stream?query=organism_id:9606&format=json&compressed=true"
pigz -t ~/uniprot100k.json.gz && echo GZIP_OK
```

About 450 MB (human: ~210K entries, mostly TrEMBL).

## 6. Count it

```bash
N_SUBSET=$(pigz -dc ~/uniprot100k.json.gz | python bin/stream_jsonl.py | wc -l); echo $N_SUBSET
```

**Expect**: `stream_jsonl: DONE — 210,706 entries in ~40s` (the number moves
with UniProt releases), then the count. Put that number in your "every
terminal" block. A `FATAL — ijson's C backend` line means step 1's check
failed: fix the env first.

## 7. Build the subset lake (RUNBOOK 4b)

`9 GB` suits a 16 GB Mac (DuckDB gets 75%); give more if you have it.

```bash
mkdir -p datalake/reports
nextflow run upjson2lake.nf -profile local --inputfile ~/uniprot100k.json.gz \
    --outdir $(pwd)/datalake --release subset --process_memory '9 GB' \
    --duckdb_temp $TMPD --expected_count $N_SUBSET -resume
```

**Expect** (about 3 minutes): seven processes ✔, no retries,
`RELEASE_COMPLETE written to .../datalake/subset`, and the macOS warning
`Task runtime metrics are not reported` (harmless; see step 9).

## 8. Check the subset lake (RUNBOOK 4b checks 1, 2, 4, 6)

```bash
OUT=datalake/subset
grep -E "VERDICT|expected count" $OUT/validation_report.txt
#   expected = jsonl = entries = N_SUBSET; ALL CHECKS PASSED (167/167 ..., 0 warnings)
(cd $OUT/lake && sha256sum -c --quiet SHA256SUMS.txt && echo CHECKSUMS_OK)
python -c "
import json; m=json.load(open('$OUT/lake/manifest.json'))
for t,v in m['tables'].items(): print(f'{t:15} {v[\"row_count\"]:>12,}  files={len(v[\"files\"])}')"
#   entries 210,706; features ~1.8M; xrefs ~7.1M; comments ~0.6M; publications ~0.9M; accession_map ~283K
ls $TMPD                                          # subset.{SORT_JSONL,INFER_SCHEMA,STAGE_PARQUET,PARQUET_TRANSFORM,VALIDATE}.<hash>
python -c "
from uniprot_parquet import connect
con = connect('$OUT/lake')
print(con.sql(\"SELECT accession, id, protein_name_primary FROM entries WHERE id = 'INS_HUMAN'\").fetchall())"
#   [('P01308', 'INS_HUMAN', 'Insulin')]
```

## 9. Resume, then timings (RUNBOOK 4b checks 5 and 3)

Straight after step 7, before any other `nextflow` run from this directory:

```bash
nextflow run upjson2lake.nf -profile local --inputfile ~/uniprot100k.json.gz \
    --outdir $(pwd)/datalake --release subset --process_memory '9 GB' \
    --duckdb_temp $TMPD --expected_count $N_SUBSET -resume
grep -o "cachedCount=[0-9]*" .nextflow.log       # cachedCount=7
```

`cachedCount=0` means another run came in between (`nextflow log` lists
them): this one has now re-run everything, so the same command once more
gives 5.

macOS records each task's time but not its memory. Times:

```bash
column -t -s $'\t' datalake/reports/trace.txt | cut -c1-200     # realtime per process
```

Peak memory of one process, measured by hand in a copy of its work directory
(repeat with `PARQUET_TRANSFORM` and `VALIDATE` in place of `SORT_JSONL`):

```bash
WD=$(nextflow log last -f process,status,workdir | awk '$1=="SORT_JSONL" && $2!="FAILED" {print $3}' | tail -1)
rm -rf /tmp/measure && cp -RL "$WD" /tmp/measure
(B=$(pwd)/bin; cd /tmp/measure && PATH="$B:$PATH" /usr/bin/time -l bash .command.sh)
#   "maximum resident set size" in bytes: SORT_JSONL ~3.9 GB at 9 GB
```

## 10. Rehearse the SLURM runs (RUNBOOK 4c)

Use a **new terminal** for this, and close it afterwards: it puts stand-in
`sbatch`/`squeue`/`scancel` first on the PATH, and any `nextflow` run in that
terminal would use them.

```bash
cd ~/Developer/uniprot-parquet && micromamba activate uniprot-parquet
R=${TMPDIR:-/tmp}/uplake_rehearse; mkdir -p $R/oom/reports
export FAKE_SLURM_DIR=$R/slurm PATH=$(pwd)/tests/fake_slurm:$PATH
for run in smoke oom full; do cp tests/fixtures/small.json.gz $R/$run.json.gz; done
./run_lake.sh subset --input $R/smoke.json.gz --outdir $R/smoke --duckdb-tmp $R/duckdb_tmp
nextflow run upjson2lake.nf -profile short --inputfile $R/oom.json.gz --outdir $R/oom \
    --release oom --process_memory '2.5 GB' -ansi-log false
./run_lake.sh full --expected-count 52 --first-release --release full --input $R/full.json.gz \
    --outdir $R/full --duckdb-tmp $R/duckdb_tmp --max-time 72h
```

**Expect**: three `RELEASE_COMPLETE written` lines; in the middle run,
`STAGE_PARQUET` exits 75 and is re-submitted (that is the test).

```bash
for f in $FAKE_SLURM_DIR/*.sbatch; do echo "$(grep -o 'nf-[A-Z_]*' $f) $(grep -oE -- '(-c|-t|--mem|-p) \S+' $f | tr '\n' ' ')"; done
```

Twenty-two lines, exactly:

```
nf-STREAM_JSONL_ -c 4 -t 04:00:00 --mem 4096M -p short
nf-SORT_JSONL_ -c 4 -t 04:00:00 --mem 16384M -p short
nf-INFER_SCHEMA_ -c 4 -t 04:00:00 --mem 16384M -p short
nf-STAGE_PARQUET_ -c 4 -t 04:00:00 --mem 16384M -p short
nf-PARQUET_TRANSFORM_ -c 4 -t 04:00:00 --mem 16384M -p short
nf-VALIDATE_ -c 8 -t 04:00:00 --mem 16384M -p short
nf-PROVENANCE_ -t 04:00:00 --mem 1024M -p short
nf-STREAM_JSONL_ -c 4 -t 04:00:00 --mem 4096M -p short
nf-SORT_JSONL_ -c 4 -t 04:00:00 --mem 2560M -p short
nf-INFER_SCHEMA_ -c 4 -t 04:00:00 --mem 2560M -p short
nf-STAGE_PARQUET_ -c 4 -t 04:00:00 --mem 2560M -p short
nf-STAGE_PARQUET_ -c 4 -t 04:00:00 --mem 5120M -p short
nf-PARQUET_TRANSFORM_ -c 4 -t 04:00:00 --mem 2560M -p short
nf-VALIDATE_ -c 8 -t 04:00:00 --mem 2560M -p short
nf-PROVENANCE_ -t 04:00:00 --mem 1024M -p short
nf-STREAM_JSONL_ -c 4 -t 48:00:00 --mem 4096M
nf-SORT_JSONL_ -c 4 -t 24:00:00 --mem 98304M
nf-INFER_SCHEMA_ -c 4 -t 24:00:00 --mem 16384M
nf-STAGE_PARQUET_ -c 4 -t 24:00:00 --mem 98304M
nf-PARQUET_TRANSFORM_ -c 4 -t 48:00:00 --mem 98304M
nf-VALIDATE_ -c 8 -t 24:00:00 --mem 98304M
nf-PROVENANCE_ -t 24:00:00 --mem 1024M
```

(The last seven carry no `-p`: the prod profile leaves the partition to the
cluster's default.) Now close this terminal.

## 11. Finish Part A

```bash
git status --short          # nothing: an untracked or modified file makes provenance record dirty: true
git log --oneline -1        # the commit you are taking to the cluster
git push
```

RUNBOOK stage 5 (a 2M-entry slice of the full ~160 GB dump) is optional and
needs the dump: run it on Codon instead ([RUNBOOK-codon.md](RUNBOOK-codon.md)
step 6). Clean up when done: `rm -rf ~/uplake_slice ${TMPDIR:-/tmp}/uplake_rehearse $TMPD`.
