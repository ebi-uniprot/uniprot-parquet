# dat2jsonl

An investigation, not part of the lake pipeline: can the lake be built from
the release's flat file (`.dat`) instead of the REST JSON dump?

`dat2jsonl` turns a UniProtKB flat file into JSON lines, one entry per line,
with UniProt's own code (uniprot-core), the way uniprot-store's
`UniProtKBDataStoreIndexer` builds the entries REST serves. Each entry goes
through the indexer's steps, in its order:

1. `FlatFileToUniprotEntry`: the flat-file parser, then `EntryObjectConverter`
   with `keywlist.txt`, `humdisease.txt` and `subcell.txt`;
2. `joinGoEvidences`: the PubMed evidences in `GO.dr_extended`, on each GO
   cross-reference with that GO id, in file order, duplicates dropped. The ECO
   code is the row's when it is one of UniProt's, else the GAF code's default
   ECO from `gaf-eco-mapping.txt`;
3. `joinUniParcId`: `extraAttributes.uniParcId` from `uniparc_map/` (lines
   `UPI accession status`, kept when the status is not `Y`, as the indexer
   does: its comment says the source data is inverted);
4. `UniProtKBAnnotationScoreMapper`: `UniProtEntryScored`, scaled to 1-5.

The JSON is uniprot-core's "simple" object mapper (`--mapper simple`), the
form REST serves. Output keeps the input's entry order.

The Spark joins become in-memory maps holding only the accessions of the
entries converted, which a first pass over the input collects. For a whole
release, `--range` converts one byte range of a `.dat` at a time, so memory
follows the range (see "A whole release on SLURM").

## uniprot-core version

| uniprot-core | UniProt release |
|---|---|
| 1.0.42 | 2026_03 |
| **1.0.43** (default) | **2026_04** |
| 2027.1 | 2027_01 |

Each tag comes just after that release's cross-reference merge on GitHub
(`ebi-uniprot/uniprot-core`); hotfix tags 1.0.43.1-3 change only the version
number. Build for another release with `-Duniprot-core.version=…`.

## Build

The jars are in UniProt's Artifactory, which needs a login: a `<server>` with
id `uniprot-artifactory-release` in `~/.m2/settings.xml`. Without one, build
uniprot-core at the matching tag first (`mvn install -DskipTests -pl
ff-parser,json-parser,tools-entry-scorer -am`), and Maven finds its jars
locally.

```bash
micromamba create -y -n dat2jsonl -c conda-forge openjdk=17 maven zstd   # once
micromamba activate dat2jsonl
mvn -B -q package -f dat2jsonl/pom.xml        # → dat2jsonl/target/dat2jsonl.jar
```

## Run

```bash
SRC=/hps/software/users/martin/uniprot/uapi/source_data/2026_04/uniprot
java -jar dat2jsonl/target/dat2jsonl.jar \
    --dat $SRC/uniprot_sprot.dat \
    --keywords $SRC/keywlist.txt --diseases $SRC/humdisease.txt --subcell $SRC/subcell.txt \
    --go $SRC/go/GO.dr_extended --gaf-eco gaf-eco-mapping.txt \
    --uniparc $SRC/uniparc_map > sprot.jsonl
```

- `--gaf-eco` is required with `--go`. Without a local copy, uniprot-core
  downloads the mapping from GitHub, and if that fails it logs an error and
  maps every GO code to no ECO. dat2jsonl stops if the mapping reads empty.
  The file: `https://raw.githubusercontent.com/evidenceontology/evidenceontology/master/gaf-eco-mapping.txt`
- `--threads` defaults to the CPUs the JVM may use, which follows a SLURM
  allocation.
- JSON lines go to stdout, progress and a summary to stderr.

Then compare with what REST serves for the same release:

```bash
python bin/compare_json.py -e rest_sprot.json.gz -a sprot.jsonl --report-json report.json
```

## A whole release on SLURM

`slurm/submit.sh` splits the uncompressed `.dat`s into byte ranges and
submits three stages, chained by dependencies:

1. **uniparc**: `dat2jsonl uniparc-active` writes the `uniparc_map/` lines
   the indexer keeps (status not `Y`; about 154M of 494M for 2026_04) to
   `OUTDIR/uniparc_active.txt`, once, so each task reads that instead of the
   whole map;
2. **convert**: a job array, one task per range (`CHUNK_GB`, 5 by default:
   about 126 tasks for TrEMBL, 1 for Swiss-Prot). A task converts the
   entries whose first byte is in its range, keeps in memory only those
   entries' GO evidences and UniParc ids, and writes
   `OUTDIR/parts/part-NNNNN.jsonl.zst`, then its `.stats.tsv`, which marks
   it done;
3. **check**: from the stats alone, every `.dat` is covered: in file order
   the first entry starts at byte 0, each task's last entry ends where the
   next task's first starts, and the last ends at the file's size. It sums
   the entries, compares them with `EXPECTED` if given, and writes
   `OUTDIR/CHECK.txt`, plus `OUTDIR/COMPLETE` if all is well.

```bash
micromamba install -y -n dat2jsonl -c conda-forge zstd     # the tasks compress with it
micromamba activate dat2jsonl                               # sbatch hands this environment on
SRC=/hps/software/users/martin/uniprot/uapi/source_data/2026_04/uniprot \
GAF_ECO=$FFC/gaf-eco-mapping.txt \
EXPECTED=154222205 \
MAX_PARALLEL=20 \
    dat2jsonl/slurm/submit.sh $SCRATCH/dat2jsonl_2026_04
```

`EXPECTED` is REST's `x-total-results` for `query=*` while it serves that
release. Other settings, with defaults: `DATS` (`uniprot_sprot.dat` and
`uniprot_trembl.dat` in `$SRC`), `CHUNK_GB` (5), `CPUS` (8), `MEM` (16G,
three quarters of it the Java heap), `TIME` (2:00:00), `MAX_PARALLEL` (50),
`PARTITION`, `JAR`.

Running `submit.sh` again with the same `OUTDIR` resumes: finished parts are
kept, and it refuses a different plan. `OUTDIR` keeps a copy of the jar and
of `gaf-eco-mapping.txt`, the plan, and `inputs.txt` (sizes and dates of the
inputs, the jar's checksum).

The parts, in plan order, are the entries in `.dat` order, so
`zstd -dc $(ls OUTDIR/parts/*.jsonl.zst)` is the whole release as one JSON
lines stream.

Checked locally on Swiss-Prot 2026_04 split into 15 ranges (with decoy `Y`
lines in the UniParc map): the parts concatenated are byte-identical to the
single run that matched REST. Also checked: ranges whose boundary falls on an
entry's first byte, one byte either side of it, or inside an entry; a
missing part and a gap fail the check; a rerun redoes only the missing part.

On Codon, 2026_04 (Swiss-Prot and TrEMBL `.dat`, 127 tasks of 8 CPUs, 20 at
a time): `CHECK PASSED`, 154,222,205 entries, REST's count exactly; a UniParc
id on every entry; GO evidences on 141,033; 12,543 task-seconds in all, the
slowest task 142 s.

Without a REST dump of the release, `rest_sample.py` checks a random sample:
it keeps each entry of the parts with probability n/total, fetches those
accessions from REST (`/uniprotkb/accessions`, 5 s apart: faster, REST
answers 429), and `compare_json.py` compares them. For 2026_04, 1,000,917
entries from all 127 parts were identical: with 95% confidence fewer than 3
in a million entries differ (at most about 460 of the release).

The question below is settled by that count: `uniprot_sprot.dat` and
`uniprot_trembl.dat` hold every entry REST serves.

Open question (settled): the indexer's flat file may be `uniprot-release.dat`, which is
0.6 GB larger than `uniprot_sprot.dat` and `uniprot_trembl.dat` together.
The entry count against `EXPECTED` would show whether the two give the same
entries.

## Into the lake

### As a stage of the pipeline (`--input_dat`)

The pipeline converts the release's flat files itself, given UniProt's
release directory, a local `gaf-eco-mapping.txt` and the built jar:

```bash
./run_lake.sh full --input-dat /hps/software/users/martin/uniprot/uapi/source_data/2026_04/uniprot \
    --gaf-eco $FFC/gaf-eco-mapping.txt \
    --expected-count 154222205 --release 2026_04 --previous-manifest ... \
    --outdir $SCRATCH/uniprot_parquet --duckdb-tmp $SCRATCH/duckdb_tmp
```

(`--dat2jsonl-jar` if the jar is not `dat2jsonl/target/dat2jsonl.jar`;
`nextflow run upjson2lake.nf --input_dat DIR --gaf_eco FILE` underneath.)
Three processes replace `STREAM_JSONL`, the same steps as `slurm/` but under
Nextflow, so `-resume` reuses every finished range:

1. `UNIPARC_ACTIVE`: the `uniparc_map/` lines the indexer keeps, once;
2. `DAT2JSONL`: one task per byte range (`--dat_chunk_gb`, 5; `--dat_cpus`
   8, `--dat_memory` 16 GB, `--dat_forks` 20 at a time), a part and its
   stats each;
3. `FROM_DAT`: `parts_input.py check` proves from the stats that the ranges
   cover every `.dat` once and add up to `--expected-count` (`check.sh`'s
   check), then the parts are joined as `FROM_PARTS` joins them.

`provenance.json` records the stage (`inputs.source.kind`: `dat2jsonl
stage`), the release directory, the jar's checksum and uniprot-core version,
the files' sizes and dates, and the check. `--dat_files` converts other flat
files than `uniprot_sprot.dat` and `uniprot_trembl.dat`.

Checked locally: 2,000 Swiss-Prot entries through the stage (4 ranges) give
the lake built from REST's JSON for the same entries (sorted JSONL
byte-identical, the same Parquet files, 156/156 checks); all of Swiss-Prot
through the stage in 15 ranges, with decoy `Y` lines in the UniParc map,
joins to exactly the JSONL that matched REST. `tests/test_workflow.py`
builds a lake through the stage when `DAT2JSONL_TEST_SRC` names a small
release directory (and `gaf-eco-mapping.txt` in it) and the jar is built.

### From a finished output (`--input_parts`)

A finished output of `slurm/submit.sh` (with `COMPLETE`) is an input of the
lake pipeline in place of a REST dump:

```bash
./run_lake.sh full --input-parts $SCRATCH/dat2jsonl_2026_04 \
    --expected-count 154222205 --release 2026_04 --previous-manifest ... \
    --outdir $SCRATCH/uniprot_parquet --duckdb-tmp $SCRATCH/duckdb_tmp
```

(`nextflow run upjson2lake.nf --input_parts DIR` underneath.) `FROM_PARTS`
replaces `STREAM_JSONL`, which streams a dump through Python for about 3.5
hours: it joins the parts in plan order (zstd frames concatenate, so this
is a copy), counts the lines, which must equal the parts' own counts and
`--expected-count`, and records the source in `provenance.json`
(`inputs.source`: the `.dat` files, `inputs.txt` with the jar's checksum and
uniprot-core version, `CHECK.txt`, the joined JSONL's SHA-256). Everything
from `SORT_JSONL` on is unchanged.

Each line is the JSON REST serves for its entry, as Jackson writes it, where
`STREAM_JSONL` writes the same values with orjson (raw UTF-8, numbers with
their own digits, an exponent in Python's Decimal notation). Validation
compares parsed values and the Parquet columns are typed from the values, so
the two inputs should give the same lake. `tests/test_workflow.py` builds the
small fixture's release both ways from the same lines (the plumbing: the
sorted JSONL and every Parquet file's data and metadata but the release label
are the same). On real data, 2,000 Swiss-Prot 2026_04 entries built once from
REST's JSON and once from dat2jsonl's output gave a byte-identical sorted
JSONL and the same Parquet files (data and metadata, but the release label);
both validations passed, 156/156 checks.

## Results so far

The whole 2026_04 lake, built on Codon from the parts (`run_lake.sh full
--input-parts`): RELEASE_COMPLETE, validation 169/169 checks passed, and no
table or column differences from the 2026_03 lake built from the REST dump.
`FROM_PARTS` took 12 min where `STREAM_JSONL` takes about 3.5 h; the run took
6 h 51 min from submission to RELEASE_COMPLETE (plus dat2jsonl's ~15 min
conversion), and it needs no REST download.

Swiss-Prot 2026_04, from the FTP's `uniprot_sprot.dat.gz` and the docs
files, against REST's JSON for the same release (`compare_json.py`, strict:
values, JSON types and key order):

- all 576,073 entries: identical, converted in 89 s on 11 cores (about
  6,400 entries/s; a 1.3 GB heap);
- building the parser and converter per entry, as the indexer does, gives
  byte-identical output to one per thread (first 2,000 entries).

On Codon, from the production files in `source_data/2026_04/uniprot/`
(the uncompressed `.dat`s, `GO.dr_extended`, `uniparc_map/`):

- Swiss-Prot, all 576,073 entries: identical (123 s on 8 cores, about
  8,000 entries/s once `uniparc_map/` is read);
- a TrEMBL test set of 3,964 entries from 23 REST queries (viruses,
  archaea, fragments, metagenomes, automatic annotation, GO IDA, new this
  release, ...): identical.

`uniparc_map/` holds 200 files and 493.6M lines; 154.29M have a status other
than `Y` (REST: 154.22M entries for 2026_04), which bears out the indexer's
inverted status filter. Every test entry found its UniParc id.
