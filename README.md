# UniProtKB Parquet Data Lake

[![CI](https://github.com/ebi-uniprot/uniprot-parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/ebi-uniprot/uniprot-parquet/actions/workflows/ci.yml)

A Nextflow pipeline that turns a UniProtKB JSON dump into analysis-ready Parquet tables. The tables are sorted and denormalized, they can be queried from any language that reads Parquet, and they hold the complete source data: every entry can be rebuilt exactly, array order included.

Six tables (`entries`, `features`, `xrefs`, `comments`, `publications`, `accession_map`) are Hive-partitioned by `review_status=swissprot|trembl`. A single-file DuckDB client (`uniprot_parquet.py`) and a plain-SQL equivalent (`setup_views.sql`) add views and helper macros over them.

---

## Design

This section is the intended design of the repository. The code, the tests and the validator implement it; where it names an open question, that question is listed under [Open decisions and pending work](#open-decisions-and-pending-work).

### 1. The input schema is never declared

Every run infers the schema of the JSON dump from the dump itself: DuckDB's `read_json_auto` with `sample_size=-1` scans every entry, so a field that appears in one entry out of 250 million is still part of the schema (`build_read_clause` in `bin/parquet_transform.py`). Every JSON object is inferred as a struct, never as a MAP, however rare its keys, so every field keeps its name and its own column type. A string stays a string even when it looks like a date, a time or a UUID, so it is stored and rebuilt with its exact text. Nothing in the repository describes UniProt's JSON ahead of time and hopes the data matches.

The dump is the source of truth. A hand-written copy of its schema would go stale the first time UniProt changes the JSON, and would then either reject a valid release or silently drop the new field.

### 2. Two kinds of column: UniProt's JSON, and shortcuts

Every table holds UniProt's JSON as it is shipped, one column per key, next to a few typed shortcut columns. Both kinds use UniProt's own names: **a camelCase column is a key of UniProt's JSON; a snake_case column is a shortcut, named like the UniProt REST API's return field for the same value** (`accession`, `organism_id`, `gene_primary`, `length`, `ec`, `go_id`, `keyword`, `keywordid`, `date_created`, `lit_pubmed_id`, ...; the list is at `rest.uniprot.org/configure/uniprotkb/result-fields`). A shortcut with no REST equivalent (`division`, `go_terms`, `*_count`, `*_index`, `start_pos`, ...) has a descriptive name, and none reuses a REST name for a different value: UniProt's `protein_name` lists every name, so the one main name here is `protein_name_primary`.

| | Source columns | Shortcut columns |
| --- | --- | --- |
| What | One per top-level key of the table's JSON element: the entry (`entryType`, `organism`, `proteinDescription`, `sequence`, `genes`, ...), the feature (`type`, `location`, `ligand`, `evidences`, ...), the cross-reference, the comment or the reference | Keys (`accession`, `reviewed`, `organism_id`, `*_index`), UniProt's default result columns (`id`, `organism_name`, `gene_primary`, `protein_name_primary`, `length`) and values derived from several fields (`ec`, `go_terms`, `division`, `lit_pubmed_id`, `start_pos`, `text_value`, ...) |
| Names | UniProt's JSON keys, exactly | snake_case, fixed |
| Types | Inferred from the release's input, the value as shipped | Declared in code (`COLUMN_TYPES`), identical in every release and every subset build |
| Holds | The element itself: the source columns of a row are the JSON object | Copies and derivations, never needed to rebuild the JSON |
| Versioned by `schema_version` | No; changes are reported per release | Yes |

A field is read where UniProt puts it: a ligand's ChEBI id is `features.ligand.id`, a citation's title `publications.citation.title`, the amino acids `entries.sequence.value`. Struct fields are read like columns: DuckDB reads only the field a query names from the Parquet file (its plan projects `sequence.value`, not the whole `sequence`). When UniProt changes its JSON, the effect is:

| Upstream change | What happens |
| --- | --- |
| A new key | A new source column of that table (a new top-level entry key: an `entries` column; a new feature key: a `features` column). A new key inside an object appears inside that column's struct. The build succeeds; the transform log and the validation report list the new column. |
| A key is removed, or its type changes | That source column disappears or changes type. The build succeeds; the release's schema diff lists it. |
| A key's type differs between entries | Its source column is stored as JSON (Parquet's JSON type: DuckDB reads it as `JSON`, so `->` and `->>` work without a cast). |
| A field a shortcut requires disappears | The build stops (`check_required_paths`). This is a real breaking change, and the code has to be updated. |
| A field a shortcut reads has a type that differs between entries (DuckDB infers JSON for the field, for an array of it, for an object containing it or for a field inside it) | The build stops (`check_curated_sources`): the shortcut has one declared type and cannot hold both. The code has to be updated. |
| A field a shortcut reads changes type in every entry to one the shortcut cannot hold: an object or a list where a single value is declared (a `ligand.id` that becomes `{"value": ..., "db": ...}`), or a plain value where an object is expected (`keywords` shipped as a string, which `keywordid` reads into) | The build stops (`check_curated_sources`), rather than storing the object's text rendering or a typed NULL. The code has to be updated. |
| A new key has the name of a shortcut of its table (`accession` in an entry, say) | The build stops (`_source_columns`): the two cannot both be columns. The code has to be updated. |
| Two keys in the same object differ only in case (`ab` and `AB`) | The build stops (`build_read_clause`) with a message naming the key: a DuckDB struct cannot hold both. The code has to be updated. |
| An optional shortcut source is absent (for example, a subset without ligands) | The shortcut is a typed NULL, and the transform log and the validation report say so. |

**Consequence for subsets.** On the full release, the source columns hold every key UniProt uses. On a subset (the demo, a test fixture, a custom slice), a key that no entry in the subset has is not a column, so a query that names it (for example `geneLocations` on a subset without any) raises a binder error instead of returning NULL. Shortcuts are the same everywhere, and every child table always has the keys every element carries (`features.type` and `location`, `xrefs.database` and `id`, `comments.commentType`, `publications.referenceNumber` and `citation`). `DESCRIBE <table>` or the release's `SCHEMA.md` shows what a lake holds.

### 3. Lossless, including order

`bin/reconstruct.py` rebuilds the original JSON entry from the six tables: an entries row's source columns are the entry, each child row's source columns an element of its array, placed by its position (`feature_index`, `xref_index`, `comment_index`, `reference_index`). Shortcut columns are never read. The comparison is exact, with four normalisations: JSON object key order is ignored; a key whose value is `null` is treated the same as an absent key; an empty `features`, `uniProtKBCrossReferences`, `comments` or `references` array is treated the same as an absent one (both are stored as zero child rows; UniProt omits empty arrays); and numbers compare by value, so `5` equals `5.0` (a field holding integers in some entries and decimals in others is stored as DOUBLE). A boolean equals only the same boolean, never `1` or `0`.

- The test suite rebuilds every fixture entry, including a fixture with new keys injected at every nesting level and a key whose type conflicts between entries.
- The validator rebuilds a sample of 1,000 Swiss-Prot and 1,000 TrEMBL entries of every release (check 16) and fails the release on any difference, and checks that every key of the sample is a column (check 13).
- `sorted.jsonl.zst`, published next to the lake, is the same data in its original JSON form: each line holds one element of the dump's `results` array with the same keys, in the same order, and exact values (numbers keep their digits). Only the layout changes: one compact line per entry, in UTF-8. `SORT_JSONL` moves these lines without rewriting them, so `VALIDATE` compares the lake with the dump's own data. Input a JSON line cannot carry exactly (a lone surrogate escape, an integer outside −2^63 … 2^64−1) stops `STREAM_JSONL`. One case is not checked: a key repeated within one object keeps only its last value. UniProt's serialiser does not produce repeated keys, and checking every key would add about 40% to the streaming time.

### 4. Layout

- **One `entries` table plus four child tables** (`features`, `xrefs`, `comments`, `publications`), one row per element of the corresponding JSON array, plus `accession_map`. Child tables carry the entry's `accession`, `reviewed` and `organism_id`, so most queries need no join.
- **Comments have one column per key.** Comment types differ in shape (25 types, most with type-specific keys), so the `comments` table's columns are the union of every type's keys (`commentType`, `texts`, `note`, `reaction`, `isoforms`, `subcellularLocations`, ...), NULL where a comment has none. `text_value` joins the `texts` of the comment types that have them.
- **No DuckDB VARIANT columns.** PyArrow cannot write VARIANT, and the April 2026 evaluation (`benchmarks/VARIANT_EVALUATION.md`) found no layout that beat the typed schema overall.

### 5. Sort order and partitioning

- `entries` is sorted by `(reviewed DESC, organism_id ASC, accession ASC)`: Swiss-Prot first, then organism, then accession. Child tables use the same keys followed by their `*_index`, so rows appear in UniProt's own order within an entry and a rebuild of the same input writes identical tables.
- Parquet row-group min/max statistics on these columns let every engine skip irrelevant row groups without configuration. Organism filters (`organism_id = 9606`) prune well.
- Each table is Hive-partitioned by `review_status=swissprot|trembl`, so a Swiss-Prot-only read never opens a TrEMBL file. `reviewed` is also stored in every file, so files are self-describing.
- There is no organism partition. The organism_id distribution is heavily skewed, and the sort already gives organism locality.
- `accession` alone is not prunable (every row group's `accession` range spans the alphabet), so `accession_map` exists for point lookups: it is sorted by `accession`, maps primary and secondary accessions to the primary, and carries `reviewed` and `organism_id` for a pruned second read.

### 6. Why these columns

An 8-week sample of live UniProt REST API logs (January–August 2026) was used to check the design against real demand:

- 94% of the fields that programmatic users filter or return mapped to a named column of `entries` in the schema this sample was checked against; each of them is a shortcut or a field of a source column today.
- Every one of the 20 most requested field pairs is among nine columns: `accession`, `id`, `reviewed`, `organism_id`, `organism_name`, `gene_primary`, `protein_name_primary`, `length`, `sequence` (the sequence object; the amino acids are `sequence.value`). These are the first nine columns of `entries`, stored together.
- The one pattern row-group statistics cannot serve is lookup by accession, which is why `accession_map` exists.

### 7. Releases

Each release is a full rebuild into its own directory, `<outdir>/<release>/`; other releases are not touched. `RELEASE_COMPLETE` is written last, only after validation has passed, so a mirror that checks for it never copies a half-written release.

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

Shortcut columns (snake_case, declared types) are listed first, then the **source** columns: the JSON element's keys, as UniProt ships them. A source column appears only if some element of the input has that key. The release's `SCHEMA.md` has every column with its type, source path and description.

**entries**: one row per protein.

- Shortcuts: `accession`, `id`, `reviewed`, `organism_id`, `organism_name`, `gene_primary` (one element per gene, NULL for a gene with only ORF or locus names), `protein_name_primary` (falls back to the submitted name for TrEMBL entries), `length`, `gene_synonym`, `alt_protein_names`, `ec` (every EC number of the entry's naming blocks and of its domains' and chains' — `includes` and `contains` — as REST's `ec`), `division` (the UniProt FTP taxonomic division: `archaea`, `bacteria`, `fungi`, `human`, `invertebrates`, `mammals`, `plants`, `rodents`, `vertebrates`, `viruses`, `unclassified`), `go_id`, `go_terms` (`{id, aspect (P/F/C), term, evidence_type}`), `xref_dbs`, `xref_proteomes`, `keywordid`, `keyword`, `keyword_categories`, `date_created`, `date_modified`, `date_sequence_modified` (DATEs), `feature_count`, `xref_count`, `comment_count`, `reference_count`, `lit_pubmed_id` (distinct, stored as strings: numeric ids in numeric order, then any non-numeric id)
- **Source:** `sequence` (`value`, `length`, `molWeight`, `crc64`, `md5`; stored after `length`), `entryType`, `primaryAccession`, `secondaryAccessions`, `uniProtkbId`, `entryAudit` (dates and versions as strings), `annotationScore`, `organism` (`scientificName`, `commonName`, `synonyms`, `taxonId`, `lineage`, `evidences`), `organismHosts`, `proteinExistence`, `proteinDescription` (`recommendedName`, `alternativeNames`, `submissionNames`, `flag`, `contains`, `includes`, ...), `genes`, `geneLocations`, `keywords`, `extraAttributes` (`countByCommentType`, `countByFeatureType`, `uniParcId`)

**features**: one row per positional annotation.

- Shortcuts: `accession`, `reviewed`, `organism_id`, `organism_name`, `length`, `feature_index`, `start_pos`, `end_pos`, `ligand_id`, `evidence_codes`
- **Source:** `type`, `location` (`start` / `end` as `{value, modifier}`, `sequence`), `description`, `featureId`, `evidences`, `featureCrossReferences`, `ligand` (`name`, `id`, `label`, `note`), `ligandPart`, `alternativeSequence` (`originalSequence`, `alternativeSequences`)

**xrefs**: one row per cross-reference.

- Shortcuts: `accession`, `reviewed`, `organism_id`, `xref_index`
- **Source:** `database`, `id`, `properties` (`[{key, value}]`), `isoformId`, `evidences`

**comments**: one row per comment.

- Shortcuts: `accession`, `reviewed`, `organism_id`, `comment_index`, `text_value` (the joined `texts[*].value`; NULL for the comment types whose prose lives in `note`)
- **Source:** `commentType`, `texts`, `note` (JSON: its shape differs between comment types), `molecule`, `reaction`, `physiologicalReactions`, `cofactors`, `interactions`, `subcellularLocations`, `events`, `isoforms`, `disease`, `sequenceCautionType`, `sequence`, `evidences`, `kineticParameters`, ... (every key of every comment type)

**publications**: one row per citation.

- Shortcuts: `accession`, `reviewed`, `organism_id`, `reference_index`
- **Source:** `referenceNumber`, `citation` (`citationType`, `id`, `title`, `authors`, `authoringGroup`, `publicationDate`, `journal`, `volume`, `firstPage`, `lastPage`, `submissionDatabase`, `citationCrossReferences`, ...), `referencePositions`, `referenceComments`, `evidences`

**accession_map**: one row per primary or secondary accession.

- `accession` (lookup key; a secondary accession can map to more than one primary), `primary_accession` (→ `entries.accession`), `is_primary`, `reviewed`, `organism_id` (copied from the primary entry)

</details>

---

## Using the lake

### Python client

`uniprot_parquet.py` is a single-file client whose only dependency is DuckDB. It builds the views from `manifest.json`, adds seven macros, and loads httpfs for remote lakes:

```python
from uniprot_parquet import connect

con = connect("/data/uniprot/2026_03/lake")                          # local
con = connect("https://<host>/<release>/lake")                       # remote (auto-installs httpfs)

con.sql("SELECT accession, gene_primary, protein_name_primary FROM entries WHERE organism_id = 9606 LIMIT 5").show()
con.sql("SELECT * FROM protein_card('P04637')").show()
con.sql("SELECT * FROM organism_features(9606, 'Domain')").show()
```

The returned object is a standard `duckdb.DuckDBPyConnection`. `connect()` also works on a partial download: querying a table you did not download fails with a message naming the directory to fetch. `schema()`, `tables()`, `manifest()`, `datapackage()` and `files_for_taxid()` describe a lake without scanning it.

### setup_views.sql (plain SQL)

`setup_views.sql` creates the same views and macros for any DuckDB client. Replace `${BASE}` with the lake path:

```python
import duckdb

con = duckdb.connect()
base = '/path/to/2026_03/lake'
with open('setup_views.sql') as f:
    con.sql(f.read().replace('${BASE}', base))
```

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
```

A flat glob (`lake/entries/*.parquet`) matches nothing. Reading with `hive_partitioning = false` gives exactly the stored schema (no `review_status` column).

```r
# R (arrow)
library(arrow)
ds <- open_dataset("lake/entries/")
ds |> filter(organism_id == 9606) |> select(accession, protein_name_primary, length) |> collect()
```

```python
# PySpark
df = spark.read.parquet("lake/entries/")
df.filter(df.organism_id == 9606).show()
```

### Looking up by accession

Resolve the accession in `accession_map` first, then read the target table with the keys it is sorted on. That is two small row-group reads instead of a scan of the whole `accession` column:

```sql
-- step 1: resolve (also the only way to follow a retired / secondary accession)
SELECT primary_accession, reviewed, organism_id FROM accession_map WHERE accession = 'P04637';
-- step 2: pruned by (reviewed, organism_id) row-group statistics, then accession
SELECT * FROM entries  WHERE reviewed = true AND organism_id = 9606 AND accession = 'P04637';
SELECT * FROM features WHERE reviewed = true AND organism_id = 9606 AND accession = 'P04637';
```

`SELECT * FROM entries WHERE accession = 'P04637'` also works, but it scans the whole `accession` column. That is fine locally on `entries`, slow on the child tables, and one HTTP range request per row group remotely.

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

# Entries joined with features
con.sql("""
    SELECT accession, gene_primary, type, start_pos, end_pos, description
    FROM entries_with_features(9606)
    WHERE type = 'Transmembrane'
""").show()

# Publications by a specific author: citation fields are read where UniProt puts them
con.sql("""
    SELECT accession, citation.title, citation.publicationDate
    FROM publications
    WHERE list_contains(citation.authors, 'Levitsky A.A.')
""").show()

# Reviewed vs unreviewed counts
con.sql("SELECT reviewed, count(*) as n FROM entries GROUP BY reviewed").show()
```

`demo/demo.ipynb` walks through DuckDB, Polars, pandas and PyArrow on a ~5,000-entry lake (see [Demo](#demo)).

### JSONL

`sorted.jsonl.zst`, published next to `lake/`, is the complete dataset in a format any language can read, sorted like the Parquet tables:

```bash
zstd -dc sorted.jsonl.zst | head -10 | jq '.primaryAccession, .organism.scientificName'
```

---

## Download

Hosting and sizes are not settled yet (see [Open decisions](#open-decisions-and-pending-work)); `<host>` and `<release>` are placeholders.

| What | Contents |
| --- | --- |
| **`entries` tier** (recommended starting point) | `entries/` + `accession_map/` + the metadata files: accession, organism, gene, names, sequence, GO, keywords, PubMed ids, proteomes |
| **Swiss-Prot only, all tables** | `review_status=swissprot/` of every table |
| **Full lake** | every table, both partitions |
| **`sorted.jsonl.zst`** | the same data as JSON lines |

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

A release is complete when `RELEASE_COMPLETE` exists next to `lake/`; mirrors should check it first. `python bin/verify_release.py <release> --full` re-checks a downloaded or mirrored copy: every file listed, every SHA-256 matching `SHA256SUMS.txt` and `provenance.json`, no stray Parquet file. `curl --metalink RELEASE.metalink` works as on the rest of the UniProt FTP. Without downloading, `connect("https://<host>/<release>/lake")` reads only the byte ranges a query needs; use the two-step accession lookup for remote point queries.

---

## What a release contains

```
<outdir>/<release>/
  lake/                     the six tables + manifest.json, datapackage.json, LICENSE,
                            SHA256SUMS.txt, RELEASE.metalink
  sorted.jsonl.zst          the source data as sorted JSON lines
  SCHEMA.md                 this release's schema: every column, type, source path, description
  validation_report.txt     the validator's report (also as validation_report.json)
  provenance.json           the source dump (name, size, SHA-256, counts), git commit, tool
                            versions, run parameters, row counts, and the size and SHA-256
                            of every file above (published beside lake/)
  RELEASE_COMPLETE          written last, only if validation passed and the published
                            copy matches its checksums (bin/verify_release.py)
```

- **`manifest.json`** lists every Parquet file with its size, SHA-256, MD5 and organism_id range, and every table's schema, row count, sort order, partitioning, keys and column categories (`shortcut`, `source`). Tools and LLM agents can read it to discover the data and write correct joins without scanning files.
- **`datapackage.json`** is a [Frictionless Data Package](https://specs.frictionlessdata.io/data-package/) descriptor with the full Arrow type, nullability and description of every column, primary and foreign keys, licensing and provenance.
- **Parquet footers** carry the release, `schema_version`, licence and zstd level; every field carries its description, category and source path.
- **`SHA256SUMS.txt`** (`sha256sum -c` format) covers the whole lake; **`RELEASE.metalink`** (Metalink 4) lists the same files for `curl --metalink`.
- **`LICENSE`** carries the CC BY 4.0 text with a header naming UniProtKB as the source, so a copied directory keeps its terms.

## Schema versioning

`schema_version` (in `manifest.json`, `datapackage.json` and every Parquet footer) is a semver of the **shortcut** columns and the layout, independent of the UniProt release. A data release normally ships with an unchanged schema version.

- **Major:** a shortcut column renamed, removed or retyped; a table renamed or removed; a partition or sort-order change; a change in the NULL / empty-list convention.
- **Minor:** a shortcut column or a table added; a new manifest key.
- **Patch:** a bug fix that changes values but not the schema; compression or file-size changes; documentation.

Source columns follow UniProt's JSON: a key UniProt adds, removes or retypes adds, removes or retypes its column, with no schema version change. Each release's validation report compares its schema with the previous release (`--previous_manifest`) and lists every table and column added, removed or retyped, marking shortcut changes; a shortcut change, or a table added or removed, without a `schema_version` bump is flagged. The list is informational and never fails a release. `manifest.json` also has an integer `version`, the manifest *format* version.

The current schema version is 1.0.0; no release has been published yet.

---

## Building the lake

### Setup

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-parquet
```

`upjson2lake.nf` is written in Nextflow's strict syntax and needs Nextflow ≥ 26.04. The pipeline also needs `pigz` and `zstd` on `PATH` (both in `environment.yml`). If `INSTALL httpfs` cannot reach `extensions.duckdb.org`, the extension is on PyPI as `duckdb-extension-httpfs`.

### Demo

Downloads ~5,000 proteins (reviewed and unreviewed) from five model organisms and builds a complete lake under `demo/lake/<release>/`, then open `demo/demo.ipynb`:

```bash
cd demo && ./run_demo.sh
```

### run_lake.sh

```bash
./run_lake.sh                                          # small: 52-entry committed fixture, local
./run_lake.sh --input demo/input.json.gz               # ~5K demo entries (after demo/run_demo.sh)
./run_lake.sh subset                                   # 100K entries, SLURM short queue
./run_lake.sh full --expected-count 248799253 --first-release   # full UniProtKB, first release only
./run_lake.sh full --expected-count 248799253 \
    --release 2026_04 --input /path/to/UniProtKB.json.gz --outdir /scratch/uniprot_parquet \
    --duckdb-tmp /scratch/$USER/duckdb_tmp \
    --previous-manifest /scratch/uniprot_parquet/2026_03/lake/manifest.json
```

`--expected-count` is required in full mode: take the entry count from the [UniProt release statistics](https://www.uniprot.org/help/release-statistics). It guards against silent loss during the JSON-to-JSONL streaming step. Full mode also requires `--previous-manifest` (the previous release's `lake/manifest.json`, for the schema diff) or, when there is no previous release, `--first-release`. A manifest inside the release being built is refused.

### Nextflow directly

```bash
nextflow run upjson2lake.nf -profile local \
    --inputfile tests/fixtures/small.json.gz --release test_2026

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
| `--duckdb_pct` | `75` | Share of process memory given to DuckDB's buffer pool |
| `--max_time` | `72h` | Wall-time cap of a retried heavy process; at most the SLURM queue's limit |
| `--duckdb_temp` | `$TMPDIR` or `/tmp` | DuckDB spill directory for out-of-core sorts |
| `--expected_count` | `null` | Expected entry count, checked while streaming |
| `--previous_manifest` | `null` | Previous release's `lake/manifest.json`, for the schema diff in the validation report |
| `--notify_email` | `null` | Email for SLURM failure notifications |

| Profile | Executor | Queue | Notes |
| --- | --- | --- | --- |
| `local` | local | — | Laptop testing |
| `prod` | SLURM | `production` | Defaults 16 GB / 1 day; process directives take priority |
| `short` | SLURM | `short` | 4 GB / 4 hours; for subset runs |

`RUNBOOK.md` is the step-by-step operator guide. Part A runs everything locally: unit tests, the small lake, a second release with a schema diff, the demo and notebook, a ~200K-entry rehearsal and an optional real-data slice. Part B moves to SLURM: a smoke test with the OOM-retry check, then the full build, post-run checks and a recovery cheatsheet.

### Architecture

```
UniProtKB.json.gz
    |
    v
+---------------+   pigz + ijson + zstd
| STREAM_JSONL  |------------------------> uniprot.jsonl.zst (+ entry_count.txt)
+-------+-------+   --expected_count verified, post-hoc line count check
        |
        v
+------------+   DuckDB out-of-core sort (schema inferred by read_json_auto)
| SORT_JSONL |────────────────────────────> sorted.jsonl.zst
+------+-----+
       v
+--------------------+   DuckDB + PyArrow: JSONL staged to Parquet once,
| PARQUET_TRANSFORM  |   schema discovered, six tables written
+--------+-----------+──> lake/ + manifest.json, datapackage.json, LICENSE,
         |                SHA256SUMS.txt, RELEASE.metalink
         v
+----------+   validation_report.{txt,json}: counts, keys, sort order and array
| VALIDATE |   positions, reconstruction g(f(x)) == x, checksums, partitions,
+----+-----+   schema diff vs the previous release (informational)
     v
+------------+   SCHEMA.md, provenance.json, then RELEASE_COMPLETE — renamed
| PROVENANCE |   into place by the workflow's onComplete section, after every
+------------+   publishDir copy has finished and verify_release.py passed on it
```

DuckDB does the JSON parsing, the SQL transforms and the sorting; it streams Arrow record batches to PyArrow, which writes zstd-compressed Parquet with a page index. Memory stays bounded whatever the dataset size. The transform is idempotent: re-running it on the same release directory rewrites the tables, and `--skip-existing` skips tables whose completion sentinel (`lake/.complete/<table>.json`) matches the files on disk and was written for the same input (size and modification time) and release.

### Production notes (SLURM)

- **OOM recovery.** `SORT_JSONL`, `PARQUET_TRANSFORM` and `VALIDATE` retry once on an out-of-memory failure with doubled memory, capped at 256 GB. That covers DuckDB's own out-of-memory error (the scripts exit 75), the OOM killer (137/140), and SLURM's pre-kill SIGTERM (143). A 143 may also be a wall-time kill, so the retry also doubles the time limit, capped at `--max_time` (72h by default; keep it within the queue's limit). Other failures are terminal.
- **Spill directory.** Point `--duckdb_temp` at a large scratch filesystem (1–2 TB free); node-local NVMe is best. The processes' `disk` directives are not passed to SLURM, so check the space yourself. Each process spills into `<duckdb_temp>/<release>.<process>/`, which is emptied when the process starts, so the spill of an attempt killed by OOM is reclaimed by its retry.
- **Resume.** Runs use `-resume`; resubmitting after a wall-time kill or preemption picks up from the last completed process. Every `publishDir` overwrites, so a resumed run republishes cached outputs whole, repairing a copy that an interrupted run left damaged; the marker is written only if `bin/verify_release.py` finds the published copy matches its metadata.
- **Integrity.** `STREAM_JSONL` writes an entry count, checks the compressed line count against it and, with `--expected_count`, against UniProt's published count. `VALIDATE` checks the lake against the same count.
- **Memory model.** DuckDB gets 75% of process memory by default; the rest is headroom for Python and PyArrow.
- **Provisional settings.** zstd level 9 (`--zstd-level`) and a 256 MB file target (`--target-file-bytes`) were chosen on test fixtures; `benchmarks/bench_zstd.py` and `benchmarks/bench_file_size.py` measure them on real data. Both are recorded in `manifest.json`.

Approximate time budgets for the full ~250M-entry dataset (not yet measured on a full build):

| Process | Time | Memory | Notes |
| --- | --- | --- | --- |
| STREAM_JSONL | 2–4 h | 4 GB | Decompression is single-threaded |
| SORT_JSONL | 4–8 h | 96 GB | Out-of-core sort, up to 1 TB spill |
| PARQUET_TRANSFORM | 6–12 h | 96 GB | One JSON parse to staged Parquet, then six Parquet reads; up to 2 TB disk |
| VALIDATE | not yet timed (24 h limit) | 96 GB | Bounded-memory checks, DuckDB sized to the task; one pass over the JSONL; files hashed 8 at a time |
| PROVENANCE | < 1 min | 1 GB | Metadata only |

### Validation

`VALIDATE` checks the lake against the sorted JSONL as ground truth, with bounded memory (DuckDB joins, no Python dicts of billion-row tables). Any failed check exits 1, so no provenance and no `RELEASE_COMPLETE` are written.

1. **Completeness:** JSONL line count == entries rows (== the pre-sort count); child row counts == `sum(entries.*_count)`
2. **Uniqueness:** no duplicate `entries.accession`
3. **Null keys and empty strings:** `accession`, `reviewed`, `organism_id` never null; identity columns never empty; `entryType` holds only the two values the transform knows (the transform also stops on any other)
4. **Referential integrity:** every child `accession` exists in `entries`
5. **Sort order and array positions:** every table in its declared order; each child table's `*_index` runs `0..n-1` per accession with `n` equal to the entry's count (one streaming pass per child table, merged with `entries` in their shared sort order)
6. **Round-trip spot check:** sampled entries' keys and counts match the JSONL. The sample behind checks 6, 13, 16 and 21 is stratified: 1,000 Swiss-Prot and 1,000 TrEMBL entries (`--spot-check-n`, `--spot-check-n-reviewed`), drawn in the one pass that counts the JSONL, since a uniform sample would hold about 2 Swiss-Prot entries. Each report line gives the count per stratum
7. **Parquet integrity:** every file is readable
8. **Manifest:** file list, sizes and SHA-256 match disk
9. **Denormalized sync:** child `organism_id` / `reviewed` match `entries`
10. **Sequences:** `len(sequence.value) == length`, no empty sequences
11. **Feature coordinates:** `start_pos <= end_pos`
12. **Shortcut column types:** every shortcut column has its declared type
13. **Field completeness:** every key of a sampled entry, and of its features, cross-references, comments and references, is a column of its table; source columns without a description (keys UniProt added) are listed as warnings. Deeper fields are covered by check 16
14. **Schema diff (informational):** tables and columns added, removed or retyped since `--previous_manifest`
15. **Comment text:** every text-bearing comment type has a `text_value`
16. **Reconstruction:** the 1,000 + 1,000 sample rebuilt by `bin/reconstruct.py` equals the JSONL, array order included; an entry whose rebuild raises an error counts as a mismatch
17. *(reserved for bloom filters; see open decisions)*
18. **Accession map:** primaries == entries, secondaries == `sum(len(secondaryAccessions))`, no duplicates, no orphans; the secondary rows are exactly the `(secondary, entry)` pairs of `entries.secondaryAccessions`, primary rows map to themselves, and every row's `(reviewed, organism_id)` is its entry's
19. **Partitions:** directories agree with the stored `reviewed`; per-partition counts match the manifest; Swiss-Prot files first
20. **Shortcut coverage:** a shortcut with no value in any row (its source may be absent or renamed upstream). A warning for a subset; with `--release`, which `VALIDATE` passes, a failure, as is a table with no rows
21. **Shortcut values:** for the check 16 sample, every shortcut of every table equals an independent Python computation from the JSON entry (`bin/shortcut_reference.py`). Reconstruction reads only source columns, so this is the check that catches a wrong shortcut

### Testing

```bash
python -m pytest tests/ -v                      # default suite, ~4,600 entries (fetched on first run)
python tests/fetch_fixtures.py --scale stress   # one-time fetch, ~60 MB
python -m pytest tests/ -v --stress             # stress suite, ~14,000 entries; needs ~8 GB RAM
```

Use `python -m pytest` so the repository root is on `sys.path` (the tests import `uniprot_parquet`). The default fixture is fetched from the UniProt REST API on the first run: targeted queries (viruses, fragments, isoforms, bacteria, fungi, several TrEMBL organisms) plus the most heavily annotated Swiss-Prot and TrEMBL entries, which stress every child table. The committed `tests/fixtures/small.json.gz` (52 entries) needs no network. `python tests/fetch_fixtures.py --force` re-fetches after a UniProt release.

Both suites check row counts, schemas and schema tiers, partitions, sort order and array positions, manifests and checksums, the data package, idempotency and `--skip-existing`, lossless reconstruction of every entry (also with new fields injected at every level, and with fields changing shape), the client on full and partial lakes, every README SQL example, and the validator. CI (`.github/workflows/ci.yml`) runs the default suite, compiles every script, and lints the workflow with `nextflow lint`.

---

## Open decisions and pending work

**Needs real data.** Build a measurement slice (human reviewed + unreviewed, plus 1M random TrEMBL entries; `bin/sample_jsonl.py` draws the sample) and use it to:

- fill in the download sizes per tier;
- confirm or change zstd level 9 (`benchmarks/bench_zstd.py`) and the 256 MB file target (`benchmarks/bench_file_size.py`);
- run the point-lookup benchmark (`benchmarks/bench_point_lookup.py`).

**Needs the first full build.**

- The `division` column is a first approximation of UniProt's FTP taxonomic divisions. Per-division Swiss-Prot and TrEMBL counts must equal the FTP `taxonomic_divisions/` counts for the same release.
- Measure the validator's run time and the `accession_map` sort spill.

**Decisions to take.**

- **Retention.** UniProt's FTP keeps at least two years of releases and each year's `YYYY_01` release indefinitely. An earlier proposal was to publish only the latest release, since the FTP already archives the source data. A middle option is latest + previous + each `YYYY_01`.
- **Hosting.** The canonical download host is undecided. A cloud copy could sit next to UniProt's existing AWS Open Data bucket (`s3://aws-open-data-uniprot-rdf`, RDF only today).
- **Publishing step.** Atomic swap of `latest` / `previous`, a `releases.json` index, and `RELEASE_COMPLETE` written after the swap.
- **Bloom filters on `accession`.** Would make single-step accession lookups prunable. PyArrow 23 cannot write them; the options would go into the `ParquetWriter` arguments in `stream_to_parquet` (`bin/parquet_transform.py`), and validator check 17 is reserved.

**Later additions.**

- Croissant metadata beside `datapackage.json`.
- Enumerations (for example `commentType` values) in the manifest.
- An `isoforms` table.
- `lineage_taxids`, for filtering by clade.
- Function and subcellular-location text columns on `entries`.
- Sequence-hash verification (`sequence.md5`, `sequence.crc64`) in the validator.
- A CI version matrix.

## Contributor notes

- **Adding a shortcut column.** Add it to:
  - the table's SELECT, before the source columns;
  - `COLUMN_TYPES` (its declared type), and wrap its SELECT expression in `_cast` (or `_typed`, if its source is optional);
  - `COLUMN_SOURCES`, or `DERIVED_SOURCES` for a value read from several fields;
  - `COLUMN_DESCRIPTIONS` (the build fails without it);
  - `TABLE_META[table]["columns"]["shortcut"]`;
  - the column reference above.

  Then bump the minor schema version. Source columns need no code: every key is one. When UniProt adds a key, add its description to `COLUMN_DESCRIPTIONS` so the validation report stops listing it as new.
- **SQL builders.** `_build_*_sql` return f-strings that `main()` then `.format(read_clause=...)`s: write `{{read_clause}}`, and double any literal brace. In the child builders, `unnest` is the unnested element and `ordinality` its 1-based position.
- **Typed NULLs.** DuckDB types a bare `NULL` as `INTEGER`. Shortcut fallbacks use `_null()` (declared type).
- **JSON columns.** A source column whose value holds a JSON-typed field is written with Parquet's JSON type (`_as_json_columns`), which PyArrow reads as `arrow.json` and DuckDB as `JSON`. A DuckDB query result hands it over as a plain string: `reconstruct.rows_from_arrow` decodes the columns named JSON in the file schema.
- **Globs.** `<table>/*.parquet` matches nothing; use `<table>/*/*.parquet` or `<table>/**/*.parquet`.
- **Row-group statistics.** In `parquet_metadata()`, use `stats_min_value` / `stats_max_value`; the legacy `stats_min` / `stats_max` are NULL for PyArrow-written strings.
- **Fixtures are small.** Each fixture lake has one file and one row group per table, so row-group pruning and file-size effects can only be measured on the slice or a full build.
- **Stress suite memory.** It peaks around 8 GB; the round-trip fixtures are module-scoped for that reason.
- **Executable bits.** Nextflow calls `bin/*.py` directly, so they must stay executable. Rewriting them with some tools drops the bit; check `git diff --summary` for `mode change`.
- **Local benchmark timing.** Remote timings over the local Python `http.server` include a ~1 s per-query artefact; use its request and byte counts, and a real server for wall times.

## Licence and citation

The pipeline code is MIT-licensed (`LICENSE`). UniProtKB data is CC BY 4.0; every lake carries the licence text and every Parquet footer names it. `CITATION.cff` has the entry for this pipeline and the UniProt Consortium's current paper as `preferred-citation`.
