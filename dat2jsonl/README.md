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

The Spark joins become in-memory maps holding only the input's accessions,
which a first pass over the input collects. That suits Swiss-Prot and test
sets. The whole of TrEMBL would need a different join (`uniparc_map` covers
every UniProtKB entry).

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
micromamba create -y -n dat2jsonl -c conda-forge openjdk=17 maven   # once
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

## Results so far

Swiss-Prot 2026_04, from the FTP's `uniprot_sprot.dat.gz` and the docs
files, against REST's JSON for the same release (`compare_json.py`, strict:
values, JSON types and key order):

- all 576,073 entries: identical, converted in 89 s on 11 cores (about
  6,400 entries/s; a 1.3 GB heap);
- building the parser and converter per entry, as the indexer does, gives
  byte-identical output to one per thread (first 2,000 entries).

UniParc ids were not tested: `uniparc_map/` is on Codon only.
