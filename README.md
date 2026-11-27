# UniProtKB Parquet

[![CI](https://github.com/ebi-uniprot/uniprot-parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/ebi-uniprot/uniprot-parquet/actions/workflows/ci.yml)

UniProtKB as sorted, denormalized Parquet tables that any language with a Parquet reader can query. The tables hold the complete source data: every entry can be rebuilt exactly, array order included. Releases are on Hugging Face as [`uniprot/uniprotkb-parquet`](https://huggingface.co/datasets/uniprot/uniprotkb-parquet) (beta).

Six tables (`entries`, `features`, `xrefs`, `comments`, `publications`, `accession_map`) are Hive-partitioned by `review_status=swissprot|trembl`. This repository holds a single-file DuckDB client (`uniprot_parquet.py`) and a plain-SQL equivalent (`setup_views.sql`), which add views and helper macros over them, and this guide. Each release also publishes the copy of the client it was built with.

---

## Columns: UniProt's JSON, and shortcuts

Every table holds UniProt's JSON as shipped, one column per key, next to a few typed shortcut columns. **A camelCase column is a key of UniProt's JSON; a snake_case column is a shortcut.** A shortcut that shares its name with a UniProt REST API return field (listed at `rest.uniprot.org/configure/uniprotkb/result-fields`) holds that field's value: `accession`, `id`, `reviewed`, `organism_id`, `gene_primary`, `gene_synonym`, `length`, `ec`, `go_id`, `keyword`, `keywordid`, `date_created`, `date_modified`, `date_sequence_modified`, `lit_pubmed_id` (as typed values: `reviewed` is a boolean, lists are arrays, dates are DATEs). Two names are the exception: REST's `feature_count` and `comment_count` give a count per type ("Chain (1); Domain (2)"), where the lake's give the number of rows. Every other shortcut has a name REST does not use (`organism_scientific_name`, `protein_name_primary`, `proteome_id`, `division`, `go_terms`, `*_index`, `start_pos`, ...), because REST's nearest field holds something else: `organism_name` is "Homo sapiens (Human)", and `protein_name` lists every name.

- **Source columns:** one per top-level key of the table's JSON element (entry, feature, cross-reference, comment or reference), named exactly as the key, typed as inferred from the release's input. A row's source columns are the JSON object. Not versioned by `schema_version`; changes are reported per release.
- **Shortcut columns:** keys (`accession`, `reviewed`, `organism_id`, `*_index`), UniProt's default result columns and values derived from several fields (`ec`, `go_terms`, `division`, ...). Fixed names and types, the same in every release and subset; versioned by `schema_version`. Never needed to rebuild the JSON.

A field is read where UniProt puts it: a ligand's ChEBI id is `features.ligand.id`, a citation's title `publications.citation.title`, the amino acids `entries.sequence.value`. DuckDB reads only the struct field a query names.

When UniProt changes its JSON:

- **A key is added, removed or retyped:** its source column (or struct field) is added, removed or retyped. The build succeeds; the transform log, the validation report and the release's schema diff list the change.
- **A key's type, or a nested field's, differs between entries:** the whole source column is stored as JSON (Parquet's JSON type; in DuckDB `->` and `->>` work without a cast).
- **An optional shortcut source is absent** (a subset without ligands): the shortcut is a typed NULL.
- **A change a shortcut cannot follow** (a field it requires disappears, or changes type or shape) stops the build, so no release has a shortcut with a changed meaning.

**Subsets.** On a subset (the demo, a test fixture, a custom slice) a key no entry has is not a column, so a query naming it (`geneLocations`, say) raises a binder error instead of returning NULL. Shortcuts are the same everywhere, and every child table always has the keys every element carries (`features.type` and `location`, `xrefs.database` and `id`, `comments.commentType`, `publications.referenceNumber` and `citation`). `DESCRIBE <table>` or the release's `SCHEMA.md` shows what a lake holds.

## Layout, sort order and partitioning

- **Tables.** `entries`, four child tables (one row per element of the entry's `features`, `uniProtKBCrossReferences`, `comments` or `references` array) and `accession_map`. Child tables carry the entry's `accession`, `reviewed` and `organism_id`, so most queries need no join.
- **Comments have one column per key:** the union of every comment type's keys (`commentType`, `texts`, `note`, `reaction`, `isoforms`, ...), NULL where a comment has none. `text_value` joins the `texts` of the types that have them.
- **Sort order.** `entries` is sorted by `(reviewed DESC, organism_id ASC, accession ASC)`; child tables add their `*_index`, so rows keep UniProt's order within an entry and a rebuild of the same input writes identical tables. Row-group min/max statistics on these columns let any engine skip row groups; organism filters (`organism_id = 9606`) prune well.
- **Partitioning.** Each table is Hive-partitioned by `review_status=swissprot|trembl`, so a Swiss-Prot-only read never opens a TrEMBL file; `reviewed` is also stored in every file. There is no organism partition: organisms are heavily skewed, and the sort already gives locality.
- **Accession lookups.** `accession` alone is not prunable (every row group's range spans the alphabet), so `accession_map`, sorted by `accession` within each partition, maps primary and secondary accessions to the primary and carries `reviewed` and `organism_id` for a pruned second read.

## Lossless

The tables hold the complete source data. An entries row's source columns are the entry; each child row's source columns are an element of the entry's array, placed by `feature_index`, `xref_index`, `comment_index` or `reference_index`. Shortcut columns are never needed to rebuild the JSON. Every release is checked by rebuilding 1,000 Swiss-Prot and 1,000 TrEMBL entries from the tables and comparing them with the source, array order included; object key order, a `null` key against an absent one, an empty array against an absent one, and `5` against `5.0` count as equal.

---

## Tables

| Table | Grain | Rows (2026_03) | Parquet (2026_03) |
| --- | --- | --- | --- |
| `entries` | One row per protein | 150.0M | 47.5 GB |
| `features` | One row per positional annotation | 637.9M | 7.0 GB |
| `xrefs` | One row per cross-reference | 2.52B | 19.7 GB |
| `comments` | One row per comment | 309.5M | 6.5 GB |
| `publications` | One row per citation | 170.9M | 1.6 GB |
| `accession_map` | One row per primary or secondary accession | 151.2M (1.18M secondaries) | 0.7 GB |

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

`uniprot_parquet.py` is a single-file client whose only dependency is DuckDB. It builds the views from `manifest.json`, adds seven macros, and loads httpfs for remote lakes. Each release publishes the copy it was built with beside `lake/` (`<release>/uniprot_parquet.py`, with `setup_views.sql`); use that one, since `main` follows the latest schema. `connect()` warns on a lake whose `schema_version` the copy was not written for (see [Schema versioning](#schema-versioning)):

```python
from uniprot_parquet import connect

con = connect("/data/uniprot/2026_03/lake")                          # local
con = connect("hf://datasets/uniprot/uniprotkb-parquet@2026_04/2026_04/lake")   # remote (auto-installs httpfs; https:// and s3:// work too)

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

Its views are globs, so it needs all six tables and a listable file system (local, `s3://` or `hf://`, not HTTP(S)). For a partial or HTTP(S) lake, use `connect()`.

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

A flat glob (`lake/entries/*.parquet`) matches nothing; use `<table>/*/*.parquet` or `<table>/**/*.parquet`. Reading with `hive_partitioning = false` gives exactly the stored schema (no `review_status` column). A direct read with Hive partitioning has both `review_status` (string, `'swissprot'`/`'trembl'`, from the path) and `reviewed` (boolean, stored); the views of `uniprot_parquet.py` and `setup_views.sql` keep only `reviewed`, so query those with `WHERE reviewed` / `GROUP BY reviewed`.

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

### JSONL

`sorted.jsonl.zst`, published next to `lake/`, is the complete dataset as JSON lines, sorted like the tables (see [Lossless](#lossless)):

```bash
zstd -dc sorted.jsonl.zst | head -10 | jq '.primaryAccession, .organism.scientificName'
```

---

## Download

The beta is on Hugging Face as [`uniprot/uniprotkb-parquet`](https://huggingface.co/datasets/uniprot/uniprotkb-parquet); its schema may change between releases (see [Schema versioning](#schema-versioning)). Each release is a directory of the repo (`2026_04/`, laid out as [below](#what-a-release-contains)) and a git tag of the same name: pin the tag. `sorted.jsonl.zst` is not on the Hub (at ~100 GB it is over the per-file limit). The canonical download host is not settled yet.

Tiers: the **`entries` tier**, the recommended start (`entries/` + `accession_map/` + metadata files: accession, organism, gene, names, sequence, GO, keywords, PubMed ids, proteomes); **Swiss-Prot only** (`review_status=swissprot/` of every table); and the **full lake**.

```bash
pip install -U huggingface_hub     # the hf CLI
R=2026_04
META=(--include "$R/lake/*.json" --include "$R/lake/SHA256SUMS.txt" --include "$R/lake/RELEASE.metalink" --include "$R/lake/LICENSE")
# entries tier
hf download uniprot/uniprotkb-parquet --repo-type dataset --revision $R --local-dir . \
    --include "$R/lake/entries/*" --include "$R/lake/accession_map/*" "${META[@]}"
grep -E '  (entries|accession_map)/' $R/lake/SHA256SUMS.txt | (cd $R/lake && sha256sum -c --quiet)
# Swiss-Prot only, all tables
hf download uniprot/uniprotkb-parquet --repo-type dataset --revision $R --local-dir . \
    --include "$R/lake/*/review_status=swissprot/*" "${META[@]}"
# full release: lake/, SCHEMA.md, the client, the validation report, provenance.json
hf download uniprot/uniprotkb-parquet --repo-type dataset --revision $R --local-dir . --include "$R/*"
```

- Each `--include` takes one pattern; `*` also matches `/`.
- Mirrors should check for `RELEASE_COMPLETE` next to `lake/` first: it means the release is complete.
- **Check a downloaded `lake/`:** `(cd lake && sha256sum -c --quiet SHA256SUMS.txt)`, or the `grep` form above for a tier (`'review_status=swissprot/'` for Swiss-Prot only).
- **Check a full mirror of the release directory:** `provenance.json` records the size and SHA-256 of every file beside `lake/`.
- **Without downloading:** `connect("hf://datasets/uniprot/uniprotkb-parquet@2026_04/2026_04/lake")` reads only the byte ranges a query needs (`@2026_04` pins the tag; the path after it is the release directory); use the two-step accession lookup for point queries.

---

## What a release contains

Each release is a full rebuild into its own directory; other releases are not touched.

```
<outdir>/<release>/
  lake/                     the six tables + manifest.json, datapackage.json, LICENSE,
                            SHA256SUMS.txt, RELEASE.metalink
  sorted.jsonl.zst          the source data as sorted JSON lines
  SCHEMA.md                 this release's schema: every column, type, source path, description
  uniprot_parquet.py        the client and views this release was built with (copies of this
  setup_views.sql           repository's, recorded in provenance.json)
  validation_report.txt     the validator's report (also as validation_report.json)
  provenance.json           the source dump (name, size, SHA-256, counts), pipeline commit, tool
                            versions, run parameters, row counts, and the size and SHA-256
                            of every file published beside lake/
  RELEASE_COMPLETE          written last, only if validation passed and the published
                            copy matches its checksums
```

- **`manifest.json`**: every Parquet file with its size, SHA-256, MD5 and organism_id range; every table's schema, row count, sort order, partitioning, keys and column categories (`shortcut`, `source`). Enough for tools and LLM agents to find data and write correct joins without scanning files.
- **`datapackage.json`**: a [Frictionless Data Package](https://specs.frictionlessdata.io/data-package/) with every column's Arrow type, nullability and description, primary and foreign keys, licensing and provenance.
- **Parquet footers**: release, `schema_version`, licence and zstd level; every field has its description, category and source path.
- **`SHA256SUMS.txt`** (`sha256sum -c` format) covers the whole lake; **`RELEASE.metalink`** (Metalink 4) lists the same files with sizes and SHA-256/MD5 hashes.
- **`LICENSE`**: the CC BY 4.0 text with a header naming UniProtKB as the source, so a copied directory keeps its terms.

## Schema versioning

`schema_version` (in `manifest.json`, `datapackage.json` and every Parquet footer) is a semver of the **shortcut** columns and the layout, independent of the UniProt release. Current: 0.1.0.

**The lake is in beta, so the version is 0.x and any release may change the schema.** A breaking change bumps the minor version, anything else the patch version:

- **Minor (breaking):** a shortcut column renamed, removed or retyped; a table renamed or removed; a partition or sort-order change; a change in the NULL / empty-list convention.
- **Patch:** a shortcut column or a table added; a new manifest key; a bug fix that changes values but not the schema; compression or file-size changes; documentation.

1.0.0 will mark the end of the beta. From then on, a breaking change bumps the major version, an addition the minor and anything else the patch.

Source columns follow UniProt's JSON and never change the schema version. Each release's validation report lists every table and column added, removed or retyped since the previous release, marks shortcut changes, and flags a shortcut change, or a table added or removed, without a `schema_version` bump; it never fails a release. `manifest.json`'s integer `version` is the manifest *format* version.

---

## This repository

The lake is built by UniProt's pipeline, which is maintained separately; each release's `provenance.json` names the pipeline commit that built it. This repository is exported from the pipeline's, together with a small test lake the pipeline built (`tests/fixtures/lake/`), so a change made here directly is overwritten by the next export: please [open an issue](https://github.com/ebi-uniprot/uniprot-parquet/issues) instead.

```bash
pip install "duckdb>=1.5" pytest
python -m pytest tests/ -v          # the client on the test lake: full and partial copies, every SQL example in this README
ruff check . && mypy                # lint, and strict type checking (config in pyproject.toml)
```

## Licence and citation

The client is MIT-licensed (`LICENSE`). UniProtKB data is CC BY 4.0; every lake carries the licence text and every Parquet footer names it. `CITATION.cff` has the entry for this software and the UniProt Consortium's current paper as `preferred-citation`.
