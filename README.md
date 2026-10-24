# UniProtKB Parquet Data Lake

[![CI](https://github.com/ebi-uniprot/uniprot-parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/ebi-uniprot/uniprot-parquet/actions/workflows/ci.yml)

A Nextflow pipeline that turns a UniProtKB JSON dump into sorted, denormalized Parquet tables that any language with a Parquet reader can query. The tables hold the complete source data: every entry can be rebuilt exactly, array order included.

Six tables (`entries`, `features`, `xrefs`, `comments`, `publications`, `accession_map`) are Hive-partitioned by `review_status=swissprot|trembl`. A single-file DuckDB client (`uniprot_parquet.py`) and a plain-SQL equivalent (`setup_views.sql`) add views and helper macros over them.

---

## Design

### 1. The input schema is never declared

Every run infers the dump's schema from the dump itself: DuckDB's `read_json_auto` with `sample_size=-1` scans every entry, so a field that appears in one entry out of 250 million is still part of the schema (`build_read_clause` in `bin/parquet_transform.py`). Every JSON object is read as a struct, never a MAP, so every field keeps its name and type, and a string that looks like a date, time or UUID stays a string with its exact text. The dump is the source of truth: a hand-written schema would go stale at UniProt's first change, then reject a valid release or silently drop the new field.

### 2. Two kinds of column: UniProt's JSON, and shortcuts

Every table holds UniProt's JSON as shipped, one column per key, next to a few typed shortcut columns. **A camelCase column is a key of UniProt's JSON; a snake_case column is a shortcut.** A shortcut that shares its name with a UniProt REST API return field (listed at `rest.uniprot.org/configure/uniprotkb/result-fields`) holds that field's value: `accession`, `id`, `reviewed`, `organism_id`, `gene_primary`, `gene_synonym`, `length`, `ec`, `go_id`, `keyword`, `keywordid`, `date_created`, `date_modified`, `date_sequence_modified`, `lit_pubmed_id` (as typed values: `reviewed` is a boolean, lists are arrays, dates are DATEs). Two names are the exception: REST's `feature_count` and `comment_count` give a count per type ("Chain (1); Domain (2)"), where the lake's give the number of rows. Every other shortcut has a name REST does not use (`organism_scientific_name`, `protein_name_primary`, `proteome_id`, `division`, `go_terms`, `*_index`, `start_pos`, ...), because REST's nearest field holds something else: `organism_name` is "Homo sapiens (Human)", and `protein_name` lists every name.

- **Source columns:** one per top-level key of the table's JSON element (entry, feature, cross-reference, comment or reference), named exactly as the key, typed as inferred from the release's input. A row's source columns are the JSON object. Not versioned by `schema_version`; changes are reported per release.
- **Shortcut columns:** keys (`accession`, `reviewed`, `organism_id`, `*_index`), UniProt's default result columns and values derived from several fields (`ec`, `go_terms`, `division`, ...). Fixed names; types declared in code (`COLUMN_TYPES`), the same in every release and subset; versioned by `schema_version`. Never needed to rebuild the JSON.

A field is read where UniProt puts it: a ligand's ChEBI id is `features.ligand.id`, a citation's title `publications.citation.title`, the amino acids `entries.sequence.value`. DuckDB reads only the struct field a query names.

When UniProt changes its JSON:

- **A key is added, removed or retyped:** its source column (or struct field) is added, removed or retyped. The build succeeds; the transform log, the validation report and the release's schema diff list the change.
- **A key's type, or a nested field's, differs between entries:** the whole source column is stored as JSON (Parquet's JSON type; in DuckDB `->` and `->>` work without a cast).
- **An optional shortcut source is absent** (a subset without ligands): the shortcut is a typed NULL, and the transform log and validation report say so. A shortcut with no value in any row fails the pipeline's validation (check 20; `VALIDATE` treats every build as a release), so a pipeline subset needs entries that carry every shortcut's source.
- **The build stops**, and the code needs updating, when: a field a shortcut requires disappears (`check_required_paths`); a field a shortcut reads differs in type between entries, or takes a shape the shortcut cannot hold, such as an object where one value is declared or a plain value where an object is expected (`check_curated_sources`); a new key has the name of one of its table's shortcuts, case-insensitively (`_source_columns`); or two keys of one object differ only in case (`build_read_clause`), which a DuckDB struct cannot hold.

**Subsets.** On a subset (the demo, a test fixture, a custom slice) a key no entry has is not a column, so a query naming it (`geneLocations`, say) raises a binder error instead of returning NULL. Shortcuts are the same everywhere, and every child table always has the keys every element carries (`features.type` and `location`, `xrefs.database` and `id`, `comments.commentType`, `publications.referenceNumber` and `citation`). `DESCRIBE <table>` or the release's `SCHEMA.md` shows what a lake holds.

### 3. Lossless, including order

`bin/reconstruct.py` rebuilds each JSON entry from the six tables: an entries row's source columns are the entry, and each child row's source columns are an element of its array, placed by `feature_index`, `xref_index`, `comment_index` or `reference_index`. Shortcut columns are never read. The comparison is exact, with four normalisations:

- object key order is ignored;
- a key whose value is `null` equals an absent key;
- an empty `features`, `uniProtKBCrossReferences`, `comments` or `references` array equals an absent one (both are zero child rows; UniProt omits empty arrays);
- numbers compare by value, so `5` equals `5.0` (a field with integers in some entries and decimals in others is stored as DOUBLE). A boolean equals only the same boolean, never `1` or `0`.

The tests rebuild every fixture entry (including new keys injected at every nesting level and a key whose type conflicts between entries); the validator rebuilds 1,000 Swiss-Prot and 1,000 TrEMBL entries of every release and fails it on any difference (check 16).

`sorted.jsonl.zst`, published next to the lake, is the same data as JSON lines: each line is one element of the dump's `results` array with the same keys, in the same order, and exact values (numbers keep their digits); only the layout changes (one compact UTF-8 line per entry). `SORT_JSONL` moves lines without rewriting them, so `VALIDATE` compares the lake with the dump's own data. `STREAM_JSONL` stops on input a JSON line cannot carry exactly (a lone surrogate escape, an integer outside −2^63 … 2^64−1). Not checked: a key repeated within one object keeps only its last value (UniProt does not produce them, and checking would add about 40% to the streaming time).

### 4. Layout, sort order and partitioning

- **Tables.** `entries`, four child tables (one row per element of the entry's `features`, `uniProtKBCrossReferences`, `comments` or `references` array) and `accession_map`. Child tables carry the entry's `accession`, `reviewed` and `organism_id`, so most queries need no join.
- **Comments have one column per key:** the union of every comment type's keys (`commentType`, `texts`, `note`, `reaction`, `isoforms`, ...), NULL where a comment has none. `text_value` joins the `texts` of the types that have them.
- **No DuckDB VARIANT columns.** PyArrow cannot write VARIANT, and the April 2026 evaluation (`benchmarks/VARIANT_EVALUATION.md`) found no layout that beat the typed schema overall.
- **Sort order.** `entries` is sorted by `(reviewed DESC, organism_id ASC, accession ASC)`; child tables add their `*_index`, so rows keep UniProt's order within an entry and a rebuild of the same input writes identical tables. Row-group min/max statistics on these columns let any engine skip row groups; organism filters (`organism_id = 9606`) prune well.
- **Partitioning.** Each table is Hive-partitioned by `review_status=swissprot|trembl`, so a Swiss-Prot-only read never opens a TrEMBL file; `reviewed` is also stored in every file. There is no organism partition: organisms are heavily skewed, and the sort already gives locality.
- **Accession lookups.** `accession` alone is not prunable (every row group's range spans the alphabet), so `accession_map`, sorted by `accession` within each partition, maps primary and secondary accessions to the primary and carries `reviewed` and `organism_id` for a pruned second read.

### 5. Why these columns

Checked against an 8-week sample of live UniProt REST API logs (January–August 2026): 94% of the fields programmatic users filter on or return are a shortcut or a field of a source column, and all 20 most requested field pairs fall within the first nine columns of `entries`, stored together: `accession`, `id`, `reviewed`, `organism_id`, `organism_scientific_name`, `gene_primary`, `protein_name_primary`, `length`, `sequence`.

---

## Tables

| Table | Grain | Rows (full UniProtKB, approx.) |
| --- | --- | --- |
| `entries` | One row per protein | 250M |
| `features` | One row per positional annotation | 1.3B |
| `xrefs` | One row per cross-reference | 5B |
| `comments` | One row per comment | 300M |
| `publications` | One row per citation | 500M |
| `accession_map` | One row per primary or secondary accession | 250M + secondaries |

```
lake/
  entries/
    review_status=swissprot/entries_00001.parquet …
    review_status=trembl/entries_00001.parquet …
  features/  xrefs/  comments/  publications/  accession_map/   (same layout)
  manifest.json  datapackage.json  LICENSE  SHA256SUMS.txt  RELEASE.metalink
```

<details>
<summary>Column reference</summary>

Shortcut columns first, then the **source** columns (the JSON element's top-level keys; one appears only if some element of the input has that key). The release's `SCHEMA.md` has every column with its type, source path and description.

**entries.** Shortcuts: `accession`, `id`, `reviewed`, `organism_id`, `organism_scientific_name`, `gene_primary` (one element per gene, NULL for a gene with only ORF or locus names), `protein_name_primary` (falls back to the submitted name for TrEMBL entries), `length`, `gene_synonym`, `alt_protein_names`, `ec` (every EC number of the entry's naming blocks and of its domains' and chains' — `includes` and `contains` — as REST's `ec`), `division` (UniProt FTP taxonomic division: `archaea`, `bacteria`, `fungi`, `human`, `invertebrates`, `mammals`, `plants`, `rodents`, `vertebrates`, `viruses`, `unclassified`), `go_id`, `go_terms` (`{id, aspect (P/F/C), term, evidence_type}`), `xref_dbs`, `proteome_id`, `keywordid`, `keyword`, `keyword_categories`, `date_created`, `date_modified`, `date_sequence_modified` (DATEs), `feature_count`, `xref_count`, `comment_count`, `reference_count`, `lit_pubmed_id` (distinct, as strings: numeric ids in numeric order, then any non-numeric id). **Source:** `sequence` (`value`, `length`, `molWeight`, `crc64`, `md5`; stored after `length`), `entryType`, `primaryAccession`, `secondaryAccessions`, `uniProtkbId`, `entryAudit` (dates and versions as strings), `annotationScore`, `organism`, `organismHosts`, `proteinExistence`, `proteinDescription`, `genes`, `geneLocations`, `keywords`, `extraAttributes`.

**features.** Shortcuts: `accession`, `reviewed`, `organism_id`, `organism_scientific_name`, `length`, `feature_index`, `start_pos`, `end_pos`, `ligand_id`, `evidence_codes`. **Source:** `type`, `location` (`start` / `end` as `{value, modifier}`), `description`, `featureId`, `evidences`, `featureCrossReferences`, `ligand`, `ligandPart`, `alternativeSequence`.

**xrefs.** Shortcuts: `accession`, `reviewed`, `organism_id`, `xref_index`. **Source:** `database`, `id`, `properties` (`[{key, value}]`), `isoformId`, `evidences`.

**comments.** Shortcuts: `accession`, `reviewed`, `organism_id`, `comment_index`, `text_value` (the joined `texts[*].value`; NULL for types whose prose lives in `note`). **Source:** every key of every comment type: `commentType`, `texts`, `note` (JSON: its shape differs between types), `reaction`, `cofactors`, `interactions`, `subcellularLocations`, `isoforms`, `disease`, ...

**publications.** Shortcuts: `accession`, `reviewed`, `organism_id`, `reference_index`. **Source:** `referenceNumber`, `citation` (`citationType`, `id`, `title`, `authors`, `publicationDate`, `journal`, `citationCrossReferences`, ...), `referencePositions`, `referenceComments`, `evidences`.

**accession_map** (shortcuts only): `accession` (lookup key; a secondary accession can map to more than one primary), `primary_accession` (→ `entries.accession`), `is_primary`, `reviewed`, `organism_id` (copied from the primary entry).

</details>

---

## Using the lake

### Python client

`uniprot_parquet.py` is a single-file client whose only dependency is DuckDB. It builds the views from `manifest.json`, adds seven macros, and loads httpfs for remote lakes:

```python
from uniprot_parquet import connect

con = connect("/data/uniprot/2026_03/lake")                          # local
con = connect("https://<host>/<release>/lake")                       # remote (auto-installs httpfs; s3:// works too)

con.sql("SELECT accession, gene_primary, protein_name_primary FROM entries WHERE organism_id = 9606 LIMIT 5").show()
con.sql("SELECT * FROM protein_card('P04637')").show()
con.sql("SELECT * FROM organism_features(9606, 'Domain')").show()
```

`connect()` returns a standard `duckdb.DuckDBPyConnection` and takes optional `memory_limit` and `threads`. It works on a partial download: querying a table you did not download fails with a message naming the directory to fetch. `schema()`, `tables()`, `manifest()`, `datapackage()` and `files_for_organism_id()` describe a lake without scanning it.

### setup_views.sql (plain SQL)

`setup_views.sql` creates the same views and macros for any DuckDB client. Replace `${BASE}` with the lake path:

```python
import duckdb

con = duckdb.connect()
base = '/path/to/2026_03/lake'
with open('setup_views.sql') as f:
    con.sql(f.read().replace('${BASE}', base))
```

Its views are globs, so it needs all six tables and a listable file system (local or `s3://`, not HTTP(S)). For a partial or HTTP(S) lake, use `connect()`.

### Helper macros

```sql
-- Annotation card for a single protein
SELECT * FROM protein_card('P04637');
-- All transmembrane features for human
SELECT * FROM organism_features(9606, 'Transmembrane');
-- PDB and AlphaFold cross-references for human
SELECT * FROM organism_xrefs(9606, ['PDB', 'AlphaFoldDB']);
-- Function comments for mouse
SELECT * FROM organism_comments(10090, 'FUNCTION');
-- Join entries + features for an organism (filter first, join second)
SELECT * FROM entries_with_features(9606) WHERE type = 'Signal';
-- Join entries + xrefs for an organism + specific databases
SELECT * FROM entries_with_xrefs(9606, ['PDB', 'Ensembl']);
-- Isoforms for a protein (from ALTERNATIVE PRODUCTS comments)
SELECT * FROM unnest_isoforms('P04637');
```

### Direct access (no setup needed)

The lake is plain Parquet in directories, so any engine that reads Parquet works:

```python
# Polars (directory scan; Hive partitioning auto-detected, review_status as String)
import polars as pl
df = pl.scan_parquet("lake/entries/").filter(pl.col("organism_id") == 9606).collect()

# pandas / PyArrow
import pandas as pd
df = pd.read_parquet("lake/entries/", filters=[("organism_id", "==", 9606)])

# DuckDB (standalone): ** crosses the partition directories
import duckdb
duckdb.sql("SELECT * FROM read_parquet('lake/entries/**/*.parquet') WHERE organism_id = 9606")
# Swiss-Prot only: one directory, no TrEMBL file is opened
duckdb.sql("SELECT count(*) FROM read_parquet('lake/entries/review_status=swissprot/*.parquet')")

# PySpark
df = spark.read.parquet("lake/entries/")
df.filter(df.organism_id == 9606).show()
```

A flat glob (`lake/entries/*.parquet`) matches nothing; use `<table>/*/*.parquet` or `<table>/**/*.parquet`. Reading with `hive_partitioning = false` gives exactly the stored schema (no `review_status` column).

```r
# R (arrow)
library(arrow)
ds <- open_dataset("lake/entries/")
ds |> filter(organism_id == 9606) |> select(accession, protein_name_primary, length) |> collect()
```

### Looking up by accession

Resolve the accession in `accession_map`, then read the target table with the keys it is sorted on: two small row-group reads instead of a scan of the whole `accession` column.

```sql
-- step 1: resolve (also the only way to follow a retired / secondary accession)
SELECT primary_accession, reviewed, organism_id FROM accession_map WHERE accession = 'P04637';
-- step 2: pruned by (reviewed, organism_id) row-group statistics, then accession
SELECT * FROM entries  WHERE reviewed = true AND organism_id = 9606 AND accession = 'P04637';
SELECT * FROM features WHERE reviewed = true AND organism_id = 9606 AND accession = 'P04637';
```

Pass step 1's values to step 2 as literals: DuckDB (1.5) does not prune on values from a subquery or a join, as `EXPLAIN ANALYZE` shows. For that reason the single-accession macros (`protein_card`, `unnest_isoforms`) filter on `accession` alone.

`WHERE accession = 'P04637'` alone also works, but scans the whole `accession` column: fine locally on `entries`, slow on the child tables, and one HTTP range request per row group remotely.

### Example queries

```python
from uniprot_parquet import connect
con = connect('/path/to/2026_03/lake')

# All human kinases
con.sql("""
    SELECT accession, gene_primary, protein_name_primary, ec
    FROM entries
    WHERE organism_id = 9606
      AND list_contains(keyword, 'Kinase')
""").show()

# Features of one protein, in UniProt's order
con.sql("""
    SELECT feature_index, type, start_pos, end_pos, description
    FROM features
    WHERE accession = 'P04637'
    ORDER BY feature_index
""").show()

# Comment fields by type: each key of a comment is a column (NULL where a type lacks it)
con.sql("""
    SELECT accession, commentType,
           texts[1].value                           AS text,
           subcellularLocations[1].location.value   AS location,
           reaction.name                            AS reaction
    FROM comments
    WHERE accession = 'P04637'
    ORDER BY comment_index
""").show()

# Publications by a specific author: citation fields are read where UniProt puts them
con.sql("""
    SELECT accession, citation.title, citation.publicationDate
    FROM publications
    WHERE list_contains(citation.authors, 'Levitsky A.A.')
""").show()
```

`demo/demo.ipynb` walks through DuckDB, Polars, pandas and PyArrow on the ~5,000-entry demo lake (see [Demo](#demo)).

### JSONL

`sorted.jsonl.zst`, published next to `lake/`, is the complete dataset as JSON lines, sorted like the tables (see [Lossless](#3-lossless-including-order)):

```bash
zstd -dc sorted.jsonl.zst | head -10 | jq '.primaryAccession, .organism.scientificName'
```

---

## Download

Hosting and sizes are not settled yet (see [Open decisions](#open-decisions-and-pending-work)); `<host>` and `<release>` are placeholders.

Tiers: the **`entries` tier**, the recommended start (`entries/` + `accession_map/` + metadata files: accession, organism, gene, names, sequence, GO, keywords, PubMed ids, proteomes); **Swiss-Prot only** (`review_status=swissprot/` of every table); the **full lake**; and **`sorted.jsonl.zst`** (the same data as JSON lines).

```bash
# entries tier
rsync -av --include='entries/***' --include='accession_map/***' --include='*.json' \
      --include='SHA256SUMS.txt' --include='RELEASE.metalink' --include='LICENSE' --exclude='*' \
      rsync://<host>/<release>/lake/ ./lake/
grep -E '  (entries|accession_map)/' lake/SHA256SUMS.txt | (cd lake && sha256sum -c --quiet)
# Swiss-Prot only, all tables
rsync -avm --include='*/' --include='review_status=swissprot/***' --include='*.json' \
      --include='SHA256SUMS.txt' --include='RELEASE.metalink' --include='LICENSE' --exclude='*' \
      rsync://<host>/<release>/lake/ ./lake/
# full lake
rsync -av rsync://<host>/<release>/lake/ ./lake/
```

- Mirrors should check for `RELEASE_COMPLETE` next to `lake/` first: it means the release is complete.
- **Check a downloaded `lake/`:** `(cd lake && sha256sum -c --quiet SHA256SUMS.txt)`, or the `grep` form above for a tier (`'review_status=swissprot/'` for Swiss-Prot only).
- **Check a full mirror of the release directory** (`lake/` plus `provenance.json` and the files beside it): `python bin/verify_release.py <release-dir> --full` checks every file is listed and matches its SHA-256 in `SHA256SUMS.txt` or `provenance.json`, with no stray Parquet file. It fails on `lake/` alone or a tier.
- **Over HTTP(S):** `aria2c -M RELEASE.metalink --metalink-base-uri=https://<host>/<release>/lake/` checks every file's size and checksums (the `<url>` entries are relative to the lake directory; curl dropped Metalink support in 7.78).
- **Without downloading:** `connect("https://<host>/<release>/lake")` reads only the byte ranges a query needs; use the two-step accession lookup for point queries.

---

## What a release contains

Each release is a full rebuild into its own directory; other releases are not touched.

```
<outdir>/<release>/
  lake/                     the six tables + manifest.json, datapackage.json, LICENSE,
                            SHA256SUMS.txt, RELEASE.metalink
  sorted.jsonl.zst          the source data as sorted JSON lines
  SCHEMA.md                 this release's schema: every column, type, source path, description
  validation_report.txt     the validator's report (also as validation_report.json)
  provenance.json           the source dump (name, size, SHA-256, counts), git commit, tool
                            versions, run parameters, row counts, and the size and SHA-256
                            of every file published beside lake/
  RELEASE_COMPLETE          written last, only if validation passed and the published
                            copy matches its checksums (bin/verify_release.py)
```

- **`manifest.json`**: every Parquet file with its size, SHA-256, MD5 and organism_id range; every table's schema, row count, sort order, partitioning, keys and column categories (`shortcut`, `source`). Enough for tools and LLM agents to find data and write correct joins without scanning files.
- **`datapackage.json`**: a [Frictionless Data Package](https://specs.frictionlessdata.io/data-package/) with every column's Arrow type, nullability and description, primary and foreign keys, licensing and provenance.
- **Parquet footers**: release, `schema_version`, licence and zstd level; every field has its description, category and source path.
- **`SHA256SUMS.txt`** (`sha256sum -c` format) covers the whole lake; **`RELEASE.metalink`** (Metalink 4) lists the same files with sizes and SHA-256/MD5 hashes.
- **`LICENSE`**: the CC BY 4.0 text with a header naming UniProtKB as the source, so a copied directory keeps its terms.

## Schema versioning

`schema_version` (in `manifest.json`, `datapackage.json` and every Parquet footer) is a semver of the **shortcut** columns and the layout, independent of the UniProt release. Current: 1.0.0; no release has been published yet.

- **Major:** a shortcut column renamed, removed or retyped; a table renamed or removed; a partition or sort-order change; a change in the NULL / empty-list convention.
- **Minor:** a shortcut column or a table added; a new manifest key.
- **Patch:** a bug fix that changes values but not the schema; compression or file-size changes; documentation.

Source columns follow UniProt's JSON and never change the schema version. Each release's validation report lists every table and column added, removed or retyped since the previous release (`--previous_manifest`), marks shortcut changes, and flags a shortcut change, or a table added or removed, without a `schema_version` bump; it never fails a release. `manifest.json`'s integer `version` is the manifest *format* version.

---

## Building the lake

### Setup

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-parquet
```

`upjson2lake.nf` uses Nextflow's strict syntax and needs Nextflow ≥ 26.04, plus `pigz` and `zstd` on `PATH` (all in `environment.yml`). If `INSTALL httpfs` cannot reach `extensions.duckdb.org`, the extension is on PyPI as `duckdb-extension-httpfs`.

### Demo

Downloads ~5,000 proteins (reviewed and unreviewed, five model organisms) and builds a complete lake under `demo/lake/<release>/`; then open `demo/demo.ipynb`:

```bash
cd demo && ./run_demo.sh
```

### run_lake.sh

```bash
./run_lake.sh                                          # small: 52-entry committed fixture, local
./run_lake.sh --input demo/input.json.gz               # ~5K demo entries (after demo/run_demo.sh)
./run_lake.sh subset                                   # 100K entries (~/uniprot100k.json.gz), SLURM short queue
./run_lake.sh full --expected-count 248799253 --first-release   # full UniProtKB, first release only
./run_lake.sh full --expected-count 248799253 \
    --release 2026_04 --input /path/to/UniProtKB.json.gz --outdir /scratch/uniprot_parquet \
    --duckdb-tmp /scratch/$USER/duckdb_tmp \
    --previous-manifest /scratch/uniprot_parquet/2026_03/lake/manifest.json
```

- Output goes to `./datalake/<release>/` unless `--outdir` is given.
- Full mode requires `--expected-count`, the entry count from the [UniProt release statistics](https://www.uniprot.org/help/release-statistics), which guards against silent loss while streaming; and either `--previous-manifest` (the previous release's `lake/manifest.json`, for the schema diff; one inside the release being built is refused) or `--first-release`.
- `--max-time` lowers the wall-time cap of a retried heavy process (`--max_time`, 72h by default) to the queue's limit.
- `run_lake.sh` exits 1 unless the run wrote `RELEASE_COMPLETE`. A direct `nextflow run` exits 0 even when the published copy fails verification, so check for the marker.

### Nextflow directly

```bash
nextflow run upjson2lake.nf -profile local \
    --inputfile tests/fixtures/small.json.gz --release test_2026 --process_memory '4 GB'

nextflow run upjson2lake.nf -profile prod \
    --inputfile /path/to/UniProtKB.json.gz --release 2026_03 \
    --outdir /scratch/uniprot_parquet --process_memory '96 GB' \
    --expected_count 248799253 --duckdb_temp /scratch/$USER/duckdb_tmp \
    --notify_email you@example.com -resume
```

| Parameter | Default | Description |
| --- | --- | --- |
| `--inputfile` | `tests/fixtures/small.json.gz` | UniProtKB JSON (`{"results": [...]}`), gzipped or plain |
| `--outdir` | `results/uniprot_parquet` | Output base directory |
| `--release` | `2026_03` | Release label; output goes to `<outdir>/<release>/` |
| `--process_memory` | `96 GB` | Memory for the heavy processes |
| `--duckdb_pct` | `75` | Share of process memory given to DuckDB; the rest is headroom for Python and PyArrow |
| `--max_time` | `72h` | Wall-time cap of a retried heavy process; at most the SLURM queue's limit |
| `--duckdb_temp` | `$TMPDIR`, else `/tmp/duckdb_temp` | DuckDB spill directory for out-of-core sorts |
| `--expected_count` | `null` | Expected entry count, checked while streaming |
| `--previous_manifest` | `null` | Previous release's `lake/manifest.json`, for the schema diff in the validation report |
| `--notify_email` | `null` | Email for SLURM failure notifications |
| `--queue` | `null` | SLURM partition: `prod` uses the cluster's default unless given, `short` uses `short` |

| Profile | Executor | Queue | Notes |
| --- | --- | --- | --- |
| `local` | local | — | Laptop testing; CPU requests capped to the machine's cores |
| `prod` | SLURM | the cluster's default (`--queue` names one) | Defaults 16 GB / 1 day; process directives take priority |
| `short` | SLURM | `short` (or `--queue`) | Defaults 4 GB / 4 hours, and every process capped at 4 hours; for subset runs |

`RUNBOOK.md` is the step-by-step operator guide: locally (tests, the small lake, a schema diff, the demo, a ~200K-entry rehearsal, an optional real-data slice), then on SLURM (smoke test and OOM retry, the full build, post-run checks, recovery).

### Architecture

| Process | Does | Publishes | Time¹ | Memory |
| --- | --- | --- | --- | --- |
| `STREAM_JSONL` | pigz + ijson + zstd: JSON to JSONL; records the entry count, checks it against `--expected_count` and the compressed line count | — | 2–4 h (single-threaded decompression) | 4 GB |
| `SORT_JSONL` | External merge sort: DuckDB sorts chunks that fit in memory, then the runs are merged; ~1.5× the compressed JSONL on disk | `sorted.jsonl.zst` | ~1 h (2026_03) | 96 GB |
| `INFER_SCHEMA` | Infers the input's columns from the whole JSONL (`read_json_auto`, `sample_size=-1`): `schema.json` | — | ~1.5 h (2026_03) | 16 GB |
| `STAGE_PARQUET` | Parses the JSONL once into a staged Parquet file (~80 GB), read by every table query | — | ~35 min (2026_03) | 96 GB |
| `BUILD_TABLE` | One task per table (six, side by side, `--table_forks` at a time): sorts and writes it from the staged file (DuckDB + PyArrow); up to ~1 TB spill each (xrefs) | — | about the slowest table (xrefs); 6–12 h one after another | 96 GB each |
| `ASSEMBLE_LAKE` | Puts the six tables side by side (hard links) and writes `manifest.json`, `datapackage.json`, `LICENSE`, `SHA256SUMS.txt` and `RELEASE.metalink` over them | `lake/` | hashes every file once | 16 GB |
| `VALIDATE` | The checks under [Validation](#validation); one pass over the JSONL, files hashed 8 at a time | `validation_report.{txt,json}` | not yet timed (24 h limit) | 96 GB |
| `PROVENANCE` | Metadata only | `SCHEMA.md`, `provenance.json`, `RELEASE_COMPLETE`² | < 1 min | 1 GB |

¹ For the full ~250M-entry dataset; estimates, not yet measured on a full build.
² Renamed into place by the workflow's `onComplete` section, after every `publishDir` copy has finished and `bin/verify_release.py` has passed on the published copy.

DuckDB streams Arrow record batches to PyArrow, which writes zstd-compressed Parquet with a page index, so memory stays bounded whatever the dataset size. The transform is idempotent: a re-run rewrites the tables, and `--skip-existing` skips a table whose completion sentinel (`lake/.complete/<table>.json`) matches the files on disk and was written for the same input (size and modification time) and release.

### Production notes (SLURM)

- **OOM recovery.** `SORT_JSONL`, `INFER_SCHEMA`, `STAGE_PARQUET`, `BUILD_TABLE` (each table on its own), `ASSEMBLE_LAKE` and `VALIDATE` retry once with doubled memory (at most 256 GB) after DuckDB's out-of-memory error (exit 75), the OOM killer (137/140) or SLURM's pre-kill SIGTERM (143). A 143 may be a wall-time kill, so the retry also doubles the time limit, capped at `--max_time`. Other failures are terminal.
- **Work directory.** Nextflow's `work/` (every intermediate file: hundreds of GB at full scale) defaults to the launch directory, the checkout. On a cluster, `export NXF_WORK=/scratch/...` on a filesystem every compute node sees, and keep it for re-submits (`-resume` looks there).
- **Spill directory.** Point `--duckdb_temp` at a scratch filesystem with 1–2 TB free (node-local NVMe is best); the `disk` directives are not passed to SLURM, so check the space yourself. Each process spills into `<duckdb_temp>/<release>.<process>.<hash>/` (hash of the absolute release directory, so runs into different outdirs never share one), emptied at process start, so a retry reclaims the spill of an attempt killed by OOM.
- **Resume.** Resubmit with `-resume` after a wall-time kill or preemption. Every `publishDir` overwrites, so a resumed run republishes cached outputs whole, repairing a damaged copy.
- **Provisional settings.** zstd level 9 (`--zstd-level`) and a 256 MB file target (`--target-file-bytes`) were chosen on test fixtures and are recorded in `manifest.json`; `benchmarks/bench_zstd.py` and `benchmarks/bench_file_size.py` measure them on real data.

### Validation

`VALIDATE` checks the lake against the sorted JSONL, with bounded memory (DuckDB joins, no Python dicts of billion-row tables). Any failed check exits 1, so no provenance and no `RELEASE_COMPLETE` are written. Checks 6, 13, 16 and 21 use a sample of 1,000 Swiss-Prot and 1,000 TrEMBL entries (`--spot-check-n`, `--spot-check-n-reviewed`), drawn in the pass that counts the JSONL (a uniform sample would hold about 2 Swiss-Prot entries).

1. **Completeness:** JSONL lines == entries rows == the pre-sort count; child row counts == `sum(entries.*_count)`
2. **Uniqueness:** no duplicate `entries.accession`
3. **Null keys:** `accession`, `reviewed`, `organism_id` never null; identity columns never empty; `entryType` only the two known values
4. **Referential integrity:** every child `accession` exists in `entries`
5. **Sort order and array positions:** every table in its declared order; each child `*_index` runs `0..n-1` per accession, `n` being the entry's count
6. **Round-trip spot check:** sampled entries' keys and counts match the JSONL
7. **Parquet integrity:** every file is readable
8. **Manifest:** file list, sizes, SHA-256 and MD5 match disk
9. **Denormalized sync:** child `organism_id` / `reviewed` match `entries`
10. **Sequences:** `len(sequence.value) == length`, no empty sequences
11. **Feature coordinates:** `start_pos <= end_pos`
12. **Shortcut types:** every shortcut column has its declared type
13. **Field completeness:** every top-level key of the sampled elements is a column; undescribed source columns (keys UniProt added) are warnings
14. **Schema diff (informational):** tables and columns added, removed or retyped since `--previous_manifest`
15. **Comment text:** every text-bearing comment type has a `text_value`
16. **Reconstruction:** the sample rebuilt by `bin/reconstruct.py` equals the JSONL, array order included; a rebuild that raises counts as a mismatch
17. *(reserved for bloom filters; see open decisions)*
18. **Accession map:** primaries == entries; secondary rows are exactly the `(secondary, entry)` pairs of `entries.secondaryAccessions`; primaries map to themselves; no duplicates or orphans; each row's `(reviewed, organism_id)` is its entry's
19. **Partitions:** directories agree with the stored `reviewed`; per-partition counts match the manifest; Swiss-Prot files first
20. **Shortcut coverage:** a shortcut with no value in any row (source absent or renamed upstream?) is a warning on a subset; with `--release`, which `VALIDATE` passes, it fails, as does a table with no rows
21. **Shortcut values:** every shortcut of the sample equals an independent Python computation from the JSON (`bin/shortcut_reference.py`), since reconstruction never reads shortcuts

### Testing

```bash
python -m pytest tests/ -v                      # default suite, ~4,600 entries (fetched on first run)
python tests/fetch_fixtures.py --scale stress   # one-time fetch, ~70 MB
python -m pytest tests/ -v --stress             # stress suite, ~14,000 entries; needs ~8 GB RAM
ruff check . && mypy                            # lint, and strict type checking (config in pyproject.toml)
```

Use `python -m pytest` so the repository root is on `sys.path`. The default fixture is fetched from the UniProt REST API on first use (targeted queries plus the most heavily annotated Swiss-Prot and TrEMBL entries); `python tests/fetch_fixtures.py --force` re-fetches it after a UniProt release. The committed `tests/fixtures/small.json.gz` (52 entries) needs no network.

The suites cover the tables, sort order and array positions, manifests, checksums and data package, idempotency and `--skip-existing`, lossless reconstruction of every entry (also with fields injected at every level or changing shape), the client on full and partial lakes, every README SQL example, and the validator. `tests/test_workflow.py` runs the pipeline end to end, including the `prod` profile through Nextflow's SLURM executor on the stand-ins in `tests/fake_slurm/` (RUNBOOK 4c); it is skipped without `nextflow` on `PATH` unless `REQUIRE_NEXTFLOW=1` is set. CI (`.github/workflows/ci.yml`) runs the default suite, the workflow tests, `ruff check`, `mypy` (strict; every function is annotated) and `nextflow lint`.

---

## Open decisions and pending work

**Needs real data.** Build a measurement slice (human reviewed + unreviewed, plus 1M random TrEMBL entries, drawn by `bin/sample_jsonl.py`) to fill in the download sizes per tier, confirm or change zstd level 9 (`benchmarks/bench_zstd.py`) and the 256 MB file target (`benchmarks/bench_file_size.py`), and run the point-lookup benchmark (`benchmarks/bench_point_lookup.py`).

**Needs the first full build.** Check `division` (a first approximation of UniProt's FTP taxonomic divisions): per-division Swiss-Prot and TrEMBL counts must equal the FTP `taxonomic_divisions/` counts. Measure the validator's run time and the `accession_map` sort spill.

**Decisions to take.**

- **Retention.** UniProt's FTP keeps at least two years of releases and each `YYYY_01` indefinitely. Options: the latest release only (the FTP archives the source data), or latest + previous + each `YYYY_01`.
- **Hosting.** The canonical download host is undecided. A cloud copy could sit next to UniProt's AWS Open Data bucket (`s3://aws-open-data-uniprot-rdf`, RDF only today).
- **Publishing step.** Atomic swap of `latest` / `previous`, a `releases.json` index, and `RELEASE_COMPLETE` written after the swap.
- **The sorted JSONL.** `SORT_JSONL` costs about an hour per release (2026_03: 89 chunks, 150,006,383 lines in 3,600 s) and publishes a 100 GB `sorted.jsonl.zst`. Its benefit to the pipeline is unmeasured: every table is sorted again on its full key, so the lake does not depend on the input's order, and the validator samples rather than relying on it. For readers, Swiss-Prot is a prefix of the file (its first 575,748 lines in 2026_03), but an organism's block cannot be reached without decompressing everything before it. Options: keep it as is (the current choice, while the format is in beta); measure the transform from sorted and from dump-order input and drop the sort if the lake is byte-identical and no slower; or publish the JSONL split to match the lake (`swissprot`/`trembl`, or ~1 GB pieces by taxon range with an index).
- **DuckDB's share of a task's memory (`--duckdb_pct`, default 75).** At full scale DuckDB overshoots its own cap: 1.27x when 2026_03's first transform crashed (96 GB, DuckDB at 75%; a segfault at 91% of the job's memory), 1.51x in its second attempt (192 GB at 60%, peak 165.6 GiB, during xrefs). The slice (2M entries) showed at most 1.43x. Set the default (likely 50) from the first full build's `trace.txt`, per process.
- **DuckDB 2.0.** The code runs on 1.5.6 (supported to March 2027). `UPGRADE-duckdb2.md` lists what moving takes, each item with its evidence, and plans it for 2.0.1.
- **Bloom filters on `accession`,** for single-step accession lookups. PyArrow 23 cannot write them; the options would go into `ParquetWriter` in `stream_to_parquet` (`bin/parquet_transform.py`), and validator check 17 is reserved.

**Later additions.** Croissant metadata beside `datapackage.json`; enumerations (for example `commentType` values) in the manifest; an `isoforms` table; `lineage_taxids`, for filtering by clade; function and subcellular-location text columns on `entries`; sequence-hash verification (`sequence.md5`, `sequence.crc64`) in the validator; a CI version matrix.

## Contributor notes

- **Adding a shortcut column.** Add it to the table's SELECT (before the source columns, wrapped in `_cast`, or `_typed` if its source is optional), `COLUMN_TYPES`, `COLUMN_SOURCES` (or `DERIVED_SOURCES` for a value read from several fields), `COLUMN_DESCRIPTIONS` (the build fails without it), `TABLE_META[table]["columns"]["shortcut"]`, `bin/shortcut_reference.py` (the tests fail without it) and the column reference above; if it takes a REST field name, also the list in section 2 and `REST_NAMED_SHORTCUTS` in `tests/test_client.py`. Then bump the minor schema version. Source columns need no code; when UniProt adds a key, describe it in `COLUMN_DESCRIPTIONS` so the validation report stops listing it as new.
- **SQL builders.** `_build_*_sql` return f-strings that `main()` then `.format(read_clause=...)`s: write `{{read_clause}}`, and double any literal brace. In the child builders, `unnest` is the element and `ordinality` its 1-based position.
- **Typed NULLs.** DuckDB types a bare `NULL` as `INTEGER`; shortcut fallbacks use `_null()`.
- **JSON columns** are written with Parquet's JSON type (`_as_json_columns`): PyArrow reads `arrow.json`, DuckDB `JSON`. A DuckDB query result hands them over as plain strings; `reconstruct.rows_from_arrow` decodes them.
- **Row-group statistics.** In `parquet_metadata()`, use `stats_min_value` / `stats_max_value`; the legacy `stats_min` / `stats_max` are NULL for PyArrow-written strings.
- **Fixtures are small:** one file and one row group per table, so pruning and file-size effects can only be measured on the slice or a full build. The stress suite peaks around 8 GB, which is why the round-trip fixtures are module-scoped.
- **Executable bits.** Nextflow calls `bin/*.py` directly; some tools drop the bit when rewriting a file, so check `git diff --summary` for `mode change`.
- **Local benchmark timing.** Remote timings over the local Python `http.server` include a ~1 s per-query artefact; use its request and byte counts, and a real server for wall times.

## Licence and citation

The pipeline code is MIT-licensed (`LICENSE`). UniProtKB data is CC BY 4.0; every lake carries the licence text and every Parquet footer names it. `CITATION.cff` has the entry for this pipeline and the UniProt Consortium's current paper as `preferred-citation`.
