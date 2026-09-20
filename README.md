# UniProtKB Parquet Data Lake

[![CI](https://github.com/dlrice/uniprot-parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/dlrice/uniprot-parquet/actions/workflows/ci.yml)

Analysis-ready Parquet tables covering the complete UniProtKB dataset — sorted, denormalized, and queryable from any language that reads Parquet.

Six tables (`entries`, `features`, `xrefs`, `comments`, `publications`, `accession_map`), each Hive-partitioned by `review_status=swissprot|trembl`, with flattened convenience columns for everyday queries and residual structs that keep the release lossless (`bin/reconstruct.py` rebuilds the original JSON; the validator proves it on every release). Schema version 1.0.0 — see `SCHEMA.md` for every column, type, source path and the versioning policy.

## Tables

| Table          | Grain                             | Rows (full UniProtKB) |
| -------------- | --------------------------------- | --------------------- |
| `entries`      | One row per protein               | ~250M                 |
| `features`     | One row per positional annotation | ~1.3B                 |
| `xrefs`        | One row per cross-reference       | ~5B                   |
| `comments`     | One row per comment               | ~300M                 |
| `publications` | One row per citation              | ~500M                 |
| `accession_map` | One row per primary or secondary accession → current primary | ~250M + secondaries |

All tables are sorted Swiss-Prot first (`reviewed DESC`), then `taxid ASC`, then `acc ASC`. Parquet row-group min/max statistics mean predicate pushdown works automatically — engines skip irrelevant row groups without configuration.

Every table is Hive-partitioned by `review_status=swissprot|trembl` (Swiss-Prot / TrEMBL). `reviewed` is also stored in every file, so files are self-describing; the directory is what lets engines skip TrEMBL entirely. Raw DuckDB reads use `<table>/**/*.parquet`; Polars, pandas, PyArrow, Spark and R take the table directory.

```
lake/
  entries/
    review_status=swissprot/entries_00001.parquet …
    review_status=trembl/entries_00001.parquet …
  features/  xrefs/  comments/  publications/  accession_map/   (same layout)
  manifest.json  datapackage.json  LICENSE
```

---

## Download

Sizes are filled in from the F.1 measurement slice of `PLAN_SCHEMA_V2.md` once it has been built; until then no size is quoted here. The client works on any of the three Parquet subsets; a Swiss-Prot-only copy simply has one partition per table.

| What | Contents | Size |
| --- | --- | --- |
| **`entries` tier** (recommended starting point) | `lake/entries/` + `accession_map/` + the metadata files; answers ~94% of measured API demand: accession, organism, gene, names, sequence, GO ids and terms, keywords, PubMed ids, proteomes | _measured on the F.1 slice; pending_ |
| **Swiss-Prot only, all tables** | `review_status=swissprot/` of every table | _pending_ |
| **Full lake** | every table, both sides | _pending_ |
| **`sorted.jsonl.zst`** | optional; the same data in a language-neutral format, beside `lake/` | _pending_ |

```bash
# entries tier (recommended starting point)
rsync -av --include='entries/***' --include='accession_map/***' --include='*.json' \
      --include='SHA256SUMS.txt' --include='RELEASE.metalink' --include='LICENSE' --exclude='*' \
      rsync://<host>/<release>/lake/ ./lake/
grep -E '  (entries|accession_map)/' lake/SHA256SUMS.txt | (cd lake && sha256sum -c --quiet)
# Swiss-Prot only, all tables (one directory per table)
rsync -avm --include='*/' --include='review_status=swissprot/***' --include='*.json' \
      --include='SHA256SUMS.txt' --include='RELEASE.metalink' --include='LICENSE' --exclude='*' \
      rsync://<host>/<release>/lake/ ./lake/
# full lake
rsync -av rsync://<host>/<release>/lake/ ./lake/
```

(Substitute the canonical URL once it is chosen; use `wget -r` equivalents if the host does not expose rsync. `curl --metalink RELEASE.metalink` works as on the rest of the UniProt FTP.)

`connect()` works on any of these copies; querying a table you did not download fails with a message naming the directory to fetch. A release directory is complete when `RELEASE_COMPLETE` exists next to `lake/`; mirrors should check it first.

If you do not want to download at all, `connect("https://<host>/<release>/lake")` reads over HTTP with DuckDB's httpfs, fetching only the byte ranges a query needs. The honest caveat: a point lookup by accession alone reads every row group's `acc` column (one range request per row group, thousands on the full lake), so remote lookups should go through `accession_map` first — see "Looking up by accession".

---

## Using the lake

### Python (one-liner)

`uniprot_parquet.py` is a single-file client with no dependencies beyond DuckDB. It sets up views, macros, and httpfs automatically:

```python
from uniprot_parquet import connect

con = connect("/data/uniprot/2026_01/lake")                          # local
con = connect("https://ftp.ebi.ac.uk/.../2026_01/lake")              # remote (auto-installs httpfs)

con.sql("SELECT acc, gene_name, protein_name FROM entries WHERE taxid = 9606 LIMIT 5").show()
con.sql("SELECT * FROM protein_card('P04637')").show()
con.sql("SELECT * FROM organism_features(9606, 'Domain')").show()
```

The returned object is a standard `duckdb.DuckDBPyConnection`. Six views (`entries`, `features`, `xrefs`, `comments`, `publications`, `accession_map`) and seven macros are ready to use immediately.

For remote access, DuckDB's httpfs reads only the byte ranges it needs — a query touching 3 columns of one organism downloads a fraction of the full dataset.

### setup_views.sql (pure SQL, no Python)

If you prefer raw SQL or a non-Python DuckDB client, `setup_views.sql` creates the same views and macros. Replace `${BASE}` with your lake path:

```python
import duckdb

con = duckdb.connect()
base = '/path/to/2026_01/lake'
with open('setup_views.sql') as f:
    con.sql(f.read().replace('${BASE}', base))
```

### Helper macros

Both `uniprot_parquet.py` and `setup_views.sql` define parameterised table macros for common patterns:

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

### Direct access (no setup file needed)

The lake is plain Parquet files in directories — any engine that reads Parquet works out of the box:

```python
# Polars (directory scan; Hive partitioning auto-detected, review_status as String)
import polars as pl
df = pl.scan_parquet("lake/entries/").filter(pl.col("taxid") == 9606).collect()

# pandas / PyArrow
import pandas as pd
df = pd.read_parquet("lake/entries/", filters=[("taxid", "==", 9606)])

# DuckDB (standalone, no setup_views.sql): ** crosses the partition directories
import duckdb
duckdb.sql("SELECT * FROM read_parquet('lake/entries/**/*.parquet') WHERE taxid = 9606")
# Swiss-Prot only: one directory, no TrEMBL file is opened
duckdb.sql("SELECT count(*) FROM read_parquet('lake/entries/review_status=swissprot/*.parquet')")
```

A flat glob (`lake/entries/*.parquet`) matches nothing — DuckDB reports "No files found". Reading with `hive_partitioning = false` gives exactly the stored schema (no `review_status` column).

#### Looking up by accession

`accession_map` is the lookup table: one row per primary *and* secondary accession, sorted by `acc` within each review side, with the primary entry's `reviewed` and `taxid`. Resolve the accession there first, then read the target table with the keys it already sorts on — two small row-group reads instead of a scan of the whole `acc` column:

```sql
-- step 1: resolve (also the only way to follow a retired / secondary accession)
SELECT primary_acc, reviewed, taxid FROM accession_map WHERE acc = 'P04637';
-- step 2: pruned by (reviewed, taxid) row-group statistics, then acc
SELECT * FROM entries  WHERE reviewed = true AND taxid = 9606 AND acc = 'P04637';
SELECT * FROM features WHERE reviewed = true AND taxid = 9606 AND acc = 'P04637';
```

The one-step form `SELECT * FROM entries WHERE acc = 'P04637'` also works, but nothing prunes on `acc` alone (every row group's `acc` range spans the alphabet), so it scans the whole column: fine locally on `entries`, slow on the child tables, and over HTTP it costs one range request per row group. Use the two-step form for remote reads and for child tables.

```r
# R (arrow)
library(arrow)
ds <- open_dataset("lake/entries/")
ds |> filter(taxid == 9606) |> select(acc, protein_name, seq_length) |> collect()
```

```python
# PySpark
df = spark.read.parquet("lake/entries/")
df.filter(df.taxid == 9606).show()
```

### Example queries

```python
from uniprot_parquet import connect
con = connect('/path/to/2026_01/lake')

# All human kinases
con.sql("""
    SELECT acc, gene_name, protein_name, ec_numbers
    FROM entries
    WHERE taxid = 9606
      AND list_contains(keyword_names, 'Kinase')
""").show()

# Domain architecture for a single protein
con.sql("SELECT * FROM protein_card('P04637')").show()
con.sql("""
    SELECT type, start_pos, end_pos, description
    FROM features
    WHERE acc = 'P04637'
    ORDER BY start_pos
""").show()

# All signal peptides in human
con.sql("SELECT * FROM organism_features(9606, 'Signal peptide')").show()

# Proteins with PDB structures
con.sql("SELECT * FROM organism_xrefs(9606, ['PDB'])").show()

# Function annotations for mouse
con.sql("SELECT * FROM organism_comments(10090, 'FUNCTION')").show()

# Polymorphic comment fields (varies by comment type)
con.sql("""
    SELECT acc, comment_type,
           comment->>'$.texts[0].value'                         AS text,
           comment->>'$.subcellularLocations[0].location.value' AS location,
           comment->>'$.reaction.name'                          AS reaction
    FROM comments
    WHERE acc = 'P04637'
""").show()

# Entries joined with features
con.sql("""
    SELECT acc, gene_name, type, start_pos, end_pos, description
    FROM entries_with_features(9606)
    WHERE type = 'Transmembrane'
""").show()

# Publications by a specific author
con.sql("""
    SELECT acc, title, publication_date
    FROM publications
    WHERE list_contains(authors, 'Levitsky A.A.')
""").show()

# Reviewed vs unreviewed counts
con.sql("SELECT reviewed, count(*) as n FROM entries GROUP BY reviewed").show()
```

### Metadata

`manifest.json` inside the lake directory lists every Parquet file, its schema, row count, sort order, partitioning, and semantic metadata (table descriptions, primary keys, foreign keys, column categories). Each table's `size_bytes` and per-file `file_details` (size, SHA-256, MD5, `taxid_min`/`taxid_max`) are there too; `SHA256SUMS.txt` (`sha256sum -c` format) covers the whole lake and `RELEASE.metalink` (Metalink 4) lists the same files for `curl --metalink`. Tools and LLM agents can read the manifest to discover the data and generate correct joins without scanning files. The `VALIDATE` step's report is published beside the lake as `validation_report.txt` and, as data, `validation_report.json`.

`LICENSE` inside the lake directory carries the CC BY 4.0 text with a header naming UniProtKB as the source, so a copied directory keeps its terms. **Citing:** `CITATION.cff` at the repository root holds the pipeline entry and the UniProt Consortium's current paper as `preferred-citation`.

`datapackage.json` is a [Frictionless Data Package](https://specs.frictionlessdata.io/data-package/) descriptor generated alongside the manifest. It makes the lake self-describing and machine-readable per [FAIR data principles](https://www.go-fair.org/fair-principles/) (Findable, Accessible, Interoperable, Reusable). Each resource includes the full Arrow type for every column, nullability constraints, semantic descriptions, primary keys, foreign keys, sort orders, and column categories (convenience vs nested). The descriptor also records the UniProt release, dual licensing (MIT for the pipeline code, CC-BY-4.0 for UniProt data), and provenance.

### JSONL (universal fallback)

`sorted.jsonl.zst` is the complete dataset in a format any language can read. Sorted identically to the Parquet tables.

```bash
zstd -dc sorted.jsonl.zst | head -10 | jq '.primaryAccession, .organism.scientificName'
```

### Schema design

The lake adopts a **denormalized-first + residual** design. Each table has two layers: flattened convenience columns (e.g. `gene_names`, `protein_name`, `go_ids`) that cover 90% of use cases with simple SQL, and residual / full nested columns (`organism_residual`, `protein_desc_residual`, `genes_full`, `keywords_full`, `feature_residual`, `comment`, `reference_residual`) that hold everything the convenience columns do not. Residual structs hold only what the convenience columns do not; `bin/reconstruct.py` rebuilds the original JSON from both, and the validator proves it on every release (check 16).

No UniProtKB data is discarded. Users needing isoforms, GO aspects, EC numbers from alternative names, multi-paragraph comments, or evidence codes can always query the nested columns. Parquet's columnar storage means queries touching 5 of 40+ columns only read those 5 from disk.

Child tables (`features`, `xrefs`, `comments`, `publications`) include denormalized entry-level fields (`acc`, `reviewed`, `taxid`) so most queries don't need joins.

<details>
<summary>Column reference</summary>

**entries** — one row per protein, the primary table for 90% of use cases:

- Identity: `acc`, `id`, `reviewed`, `secondary_accs`, `entry_type`
- Organism: `taxid`, `organism_name`, `organism_common`, `lineage`, `division` (the UniProt FTP taxonomic division: `archaea`, `bacteria`, `fungi`, `human`, `invertebrates`, `mammals`, `plants`, `rodents`, `vertebrates`, `viruses`, `unclassified`)
- Gene/protein: `gene_name` (primary, = `gene_names[1]`), `gene_names`, `gene_synonyms`, `protein_name` (falls back to submittedName for TrEMBL entries), `alt_protein_names`, `protein_flag`, `ec_numbers`, `protein_existence`, `annotation_score`
- Sequence: `sequence`, `seq_length`, `seq_mass`, `seq_md5`, `seq_crc64`
- Shortcuts: `go_ids`, `go_terms` (`{id, aspect (P/F/C), term, evidence_type}`), `xref_dbs`, `proteome_ids`, `keyword_ids`, `keyword_names`
- Versioning: `first_public`, `last_modified`, `last_seq_modified`, `entry_version`, `seq_version`
- Counts: `feature_count`, `xref_count`, `comment_count`, `reference_count`, `pubmed_ids` (distinct, numerically sorted, stored as strings), `uniparc_id`
- Lossless: `extra_attributes` (countByCommentType, countByFeatureType)
- Residual: `organism_residual` (synonyms, evidences), `protein_desc_residual` (proteinDescription minus `flag`, kept whole), `genes_full`, `keywords_full`, `organism_hosts`, `gene_locations`; `keyword_categories` is a projection of `keywords_full`

**features** — one row per positional annotation:

- `acc`, `reviewed`, `taxid`, `organism_name`, `seq_length`
- Flattened: `type`, `start_pos`, `end_pos`, `start_modifier`, `end_modifier`, `description`, `feature_id`, `evidence_codes`, `original_sequence`, `alternative_sequences`, `ligand_name`, `ligand_id`, `ligand_label`, `ligand_note`
- Residual: `feature_residual` (evidences with source/id, featureCrossReferences, ligand, ligandPart, alternativeSequence); `location_sequence` is promoted

**xrefs** — one row per cross-reference:

- `acc`, `reviewed`, `taxid`
- `database`, `id`, `properties`, `isoform_id`, `evidences`

**comments** — one row per comment annotation:

- `acc`, `reviewed`, `taxid`
- Flattened: `comment_type`, `text_value` (covers 15 of 25 comment types; the other 10 store data in type-specific keys)
- Full nested: `comment` (JSON — preserves all polymorphic comment fields). Use `comment->>'$.key'` to extract text values or `comment->'$.key'` for nested objects.

**publications** — one row per literature citation:

- `acc`, `reviewed`, `taxid`
- Flattened: `reference_number`, `citation_type`, `citation_id`, `title`, `authors`, `authoring_group`, `publication_date`, `journal`, `volume`, `first_page`, `last_page`, `submission_database`, `citation_xrefs`, `reference_positions`, `reference_comments`, `evidences`
- Residual: `reference_residual` (citation extras: bookName, editors, publisher, address, institute, patentNumber, locator)

**accession_map** — one row per primary or secondary accession:

- `acc` (lookup key; a secondary accession can map to more than one primary), `primary_acc` (→ `entries.acc`), `is_primary`, `reviewed`, `taxid` (copied from the primary entry)

</details>

### Versioning

Each release is a full rebuild into its own isolated directory (`<outdir>/<release>/`). Previous releases are preserved untouched. `provenance.json` in each release records input file checksums, git commit, and row counts.

The **schema version** (`schema_version` in `manifest.json`, `version` in `datapackage.json`, and the `schema_version` key in every Parquet footer) is a semver that is independent of the UniProt data release: a data release normally ships with an unchanged schema. The policy (also in `SCHEMA.md`):

- **Major**: any column renamed, removed, retyped or reordered; any table renamed or removed; a partition or sort-order change; a change in the NULL / empty-list convention.
- **Minor**: a column or table added; an enumeration gaining a value; a new manifest key.
- **Patch**: a bug fix that changes values without changing the schema, compression or file-size changes, documentation.

`manifest.json` also carries an integer `version`, the manifest *format* version (currently 2), which changes only when the manifest's own structure changes.

---

## Building the lake

Everything below is for pipeline operators who want to rebuild the lake from a UniProtKB JSON dump.

### Setup

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-parquet
```

Or with pip (core dependencies only):

```bash
pip install duckdb pyarrow orjson ijson zstandard pytest frictionless
```

`upjson2lake.nf` is written in Nextflow's strict syntax and requires Nextflow ≥ 26.04 (`nextflowVersion` in `nextflow.config`). If `INSTALL httpfs` cannot reach `extensions.duckdb.org`, the same extension is on PyPI as `duckdb-extension-httpfs`.

### Demo

Downloads ~5,000 proteins (reviewed + unreviewed) from five model organisms (Human, Mouse, Fruit fly, Arabidopsis, Yeast) and builds a complete lake:

```bash
cd demo && ./run_demo.sh
```

### Running the pipeline

#### Quick start with run_lake.sh

```bash
./run_lake.sh                                          # small: 52-entry committed fixture, local
./run_lake.sh --input demo/input.json.gz               # small: ~5K demo entries (after demo/run_demo.sh)
./run_lake.sh subset                                   # subset: 100K entries, SLURM short queue
./run_lake.sh full --expected-count 248799253           # full UniProtKB, SLURM prod queue
```

The `--expected-count` flag is **required for full mode** — get the entry count from the [UniProt release statistics](https://www.uniprot.org/help/release-statistics). This guards against silent data loss during the JSON-to-JSONL streaming step.

Override defaults with flags:

```bash
./run_lake.sh full \
    --expected-count 248799253 \
    --release 2026_03 \
    --input /path/to/UniProtKB.json.gz \
    --outdir /scratch/uniprot_parquet \
    --duckdb-tmp /scratch/$USER/duckdb_tmp
```

#### Direct Nextflow invocation

```bash
# Local test (uses committed small.json.gz, 52 entries incl. annotation champions)
nextflow run upjson2lake.nf -profile local \
    --inputfile tests/fixtures/small.json.gz \
    --release test_2026

# Production (SLURM)
nextflow run upjson2lake.nf -profile prod \
    --inputfile /path/to/UniProtKB.json.gz \
    --release 2026_03 \
    --outdir /scratch/uniprot_parquet \
    --process_memory '96 GB' \
    --expected_count 248799253 \
    --duckdb_temp /scratch/$USER/duckdb_tmp \
    --notify_email you@example.com \
    -resume
```

#### Pipeline parameters

| Parameter          | Default                        | Description                                                                                |
| ------------------ | ------------------------------ | ------------------------------------------------------------------------------------------ |
| `--inputfile`      | `tests/fixtures/small.json.gz` | Input UniProtKB JSON(.gz) file                                                             |
| `--outdir`         | `results/uniprot_parquet`         | Output base directory                                                                      |
| `--release`        | `2026_03`                      | Release label (output goes to `<outdir>/<release>/`)                                       |
| `--process_memory` | `96 GB`                        | Memory for heavy processes (DuckDB gets 75% of this)                                       |
| `--duckdb_pct`     | `75`                           | Percentage of process memory allocated to DuckDB buffer pool                               |
| `--duckdb_temp`    | `$TMPDIR` or `/tmp`            | DuckDB spill directory for out-of-core sorts                                               |
| `--expected_count` | `null`                         | Expected entry count for integrity verification (required for full mode via `run_lake.sh`) |
| `--notify_email`   | `null`                         | Email for SLURM failure/requeue notifications                                              |

#### Nextflow profiles

| Profile | Executor | Queue        | Default memory | Default time | Notes                                  |
| ------- | -------- | ------------ | -------------- | ------------ | -------------------------------------- |
| `local` | local    | —            | —              | —            | For testing on your laptop             |
| `prod`  | SLURM    | `production` | 16 GB          | 1 day        | Process-level directives take priority |
| `short` | SLURM    | `short`      | 4 GB           | 4 hours      | For subset runs                        |

### Architecture

```
UniProtKB.json.gz
    |
    v
+---------------+   pigz + ijson + zstd
| STREAM_JSONL  |------------------------> uniprot.jsonl.zst (+ entry_count.txt)
+-------+-------+   --expected-count verified, post-hoc line count check
        |
        v
+------------+   DuckDB out-of-core sort (schema inferred via read_json_auto)
| SORT_JSONL |─────────────────────────────> sorted.jsonl.zst
+------+-----+
       |  (pre-sorted input makes DuckDB ORDER BY nearly free)
       v
+--------------------+   DuckDB + PyArrow
| PARQUET_TRANSFORM  |──> lake/ (6 Hive-partitioned tables) + manifest.json,
+--------+-----------+   datapackage.json, LICENSE, SHA256SUMS.txt, RELEASE.metalink
         |               (JSONL staged to Parquet once, then 6 fast reads)
         v
+----------+   validation_report.{txt,json}: completeness, uniqueness, null keys,
| VALIDATE |   referential integrity, sort order, round-trip, Parquet integrity,
+----+-----+   manifest + hashes, denorm sync, seq, coords, types, field completeness,
     |         comment text, reconstruction g(f(x)) == x, accession map, partitions
     v
+------------+   provenance.json (checksums, git commit, row counts),
| PROVENANCE |   then RELEASE_COMPLETE — renamed into place by the workflow's
+------------+   onComplete section, after every publishDir copy has finished
```

DuckDB handles the heavy lifting: JSON parsing (with automatic schema inference via `read_json_auto`), SQL transformations (flattening, unnesting), and sorting. It streams Arrow record batches to PyArrow, which writes zstd-compressed Parquet files directly. Memory stays bounded regardless of dataset size.

There is no committed schema file — the data is a JSON dump from production, so whatever schema it has is what we use. DuckDB infers types directly from the data at the start of each pipeline run with `sample_size=-1` (full-file scan) to ensure rare nested struct fields are never silently dropped. Each release is a full rebuild. Optional fields that may not appear in all datasets (e.g. `organismHosts` in virus-only entries) get a typed NULL, and structs are cast to their declared shape, so the schema of a build does not depend on which optional fields its input contained; a build stops if the input carries a nested field no column would keep (`check_declared_types` / `check_promoted_paths`).

The pipeline is **idempotent** — re-running on the same release directory overwrites the Parquet files. With `--skip-existing`, partially completed runs resume from where they left off.

### Production notes (SLURM)

**OOM recovery**: `SORT_JSONL` and `PARQUET_TRANSFORM` retry once on OOM (exit 137/140) or pre-kill SIGTERM (exit 143) with doubled memory, capped at 256 GB. Non-OOM failures are terminal. The `--skip-existing` flag means `PARQUET_TRANSFORM` retries skip already-written tables.

**DuckDB spill directory**: Point `--duckdb_temp` at a large scratch filesystem (1–2 TB free). Node-local NVMe is ideal; shared Lustre/GPFS works but is slower.

**Resume**: All runs use `-resume` by default. If a SLURM job is killed (wall time, preemption), re-submitting picks up from the last completed process.

**Integrity verification**: `STREAM_JSONL` writes a sidecar entry count and verifies the compressed output line count matches. When `--expected_count` is set, it also asserts against the known count from UniProt release statistics.

**`accession_map` sort spill**: it is the one table with a real sort (`reviewed DESC, acc ASC, primary_acc ASC`; the others arrive pre-sorted from the JSONL). ~250M + secondaries rows of five narrow columns: expect low tens of GB of DuckDB spill for it alone.

**zstd level**: `ZSTD_LEVEL` in `bin/parquet_transform.py` (`--zstd-level` on the command line) is 9, chosen provisionally from a sweep on the stress fixture (`benchmarks/bench_zstd.py`; table in `PLAN_SCHEMA_V2.md` H.1: −7% `entries` and −12% `xrefs` bytes for +6% transform time vs level 1; level 15 costs +25% for three more points). Confirm on the measurement slice with `benchmarks/bench_zstd.py`; the level is recorded in `manifest.json` (`compression`) and in every Parquet footer (`zstd_level`). The file-size target (`--target-file-bytes`, 256 MB) is likewise provisional until `benchmarks/bench_file_size.py` runs on the slice.

**Memory model**: DuckDB gets 75% of process memory by default; the remaining 25% provides headroom for Python, PyArrow, and JSON parsing. DuckDB spills to disk when data exceeds the buffer pool. Configure via `--process_memory` and `--duckdb_pct`.

**Time budgets** (approximate for the full ~250M-entry dataset):

| Process           | Time  | Memory | Notes                                                               |
| ----------------- | ----- | ------ | ------------------------------------------------------------------- |
| STREAM_JSONL      | 2–4h  | 4 GB   | pigz decompression is single-threaded                               |
| SORT_JSONL        | 4–8h  | 96 GB  | DuckDB out-of-core sort, 1 TB disk                                  |
| PARQUET_TRANSFORM | 6–12h | 96 GB  | 1 JSON parse → staged Parquet, then 5 fast Parquet reads, 2 TB disk |
| VALIDATE          | 1–2h  | 96 GB  | Bounded-memory streaming checks                                     |
| PROVENANCE        | <1m   | 1 GB   | Metadata only                                                       |

### Validation

The `VALIDATE` step runs the checks below against the source JSONL as ground truth. All checks use bounded-memory streaming or DuckDB joins (no Python dicts for billion-row tables). Any single failure exits 1 and blocks the provenance manifest:

1. **Completeness** — JSONL line count == entries rows; child table counts match `sum(entries.*_count)`
2. **Uniqueness** — `entries.acc` has zero duplicates
3. **Null keys and empty strings** — `acc`, `reviewed`, `taxid` never null; identity columns never empty
4. **Referential integrity** — every `acc` in child tables exists in entries (DuckDB anti-join)
5. **Sort order** — all tables sorted by `(reviewed DESC, taxid ASC, acc ASC)`
6. **Round-trip spot check** — 1000 reservoir-sampled entries verified field-by-field against JSONL
7. **Parquet file integrity** — every `.parquet` file in the lake is readable
8. **Manifest consistency** — `manifest.json` file list matches actual files on disk; every file's size and SHA-256 match `file_details`
9. **Denormalized column sync** — `taxid` and `reviewed` in child tables match entries (DuckDB join)
10. **Sequence integrity** — `len(sequence) == seq_length` for every entry; no zero-length sequences
11. **Feature coordinate boundaries** — `start_pos <= end_pos` where both are non-null
12. **Schema type protection** — critical columns have expected Arrow types (not silently cast by inference)
13. **Field completeness** — every top-level JSON field is captured in `entries` or a child table
14. **Schema evolution guard** — Parquet schema matches a committed baseline (when `--schema-baseline` is given)
15. **Comment text** — every text-bearing comment type has a populated `text_value`
16. **Reconstruction** — sampled entries rebuilt from the five data tables by `bin/reconstruct.py` equal the source JSONL (order-independent inside arrays)
18. **Accession map** — primary rows equal `entries`, secondary rows equal the sum of `secondary_accs`, no duplicate `(acc, primary_acc)`, every `primary_acc` exists, `(reviewed, taxid)` match `entries` (17, bloom filters, is deferred: the pinned PyArrow cannot write them)
19. **Partitions** — `review_status=swissprot|trembl` directories agree with the stored `reviewed` column (row-group statistics), per-partition row counts match the manifest and sum to `row_count`, Swiss-Prot files first

### Testing

```bash
# Default suite (~4K diverse entries, auto-fetched on first run)
python -m pytest tests/ -v

# Stress suite (~15K entries from 30+ targeted queries; needs ~8 GB RAM)
python tests/fetch_fixtures.py --scale stress   # one-time fetch, ~60 MB
python -m pytest tests/ -v --stress
```

The default fixture is fetched automatically on first `pytest` run if not already present. It pulls ~4,000 entries from UniProtKB via targeted REST API queries (viruses, fragments, isoforms, bacteria, fungi, multiple TrEMBL organisms, etc.) and always includes the top 100 most heavily annotated Swiss-Prot and TrEMBL entries, discovered by sampling candidate pools from well-studied organisms and ranking by total annotation count (features + xrefs + comments + references). The Swiss-Prot champion is P0DTD1 (SARS-CoV-2 replicase, ~6,300 annotations); the TrEMBL champion is typically a titin ortholog (~570 annotations). These stress-test every child table at maximum annotation volume.

The `--stress` flag swaps in a ~15K-entry dataset assembled from 30+ queries spanning archaea, toxins, allergens, pharmaceuticals, long/short sequences, and TrEMBL from 10+ organisms. Both suites run the same tests — row counts, column schemas, typed fallbacks, partitions, data integrity, sort order, manifest consistency and checksums, data package validation, idempotency, `--skip-existing` resume, full round-trip equivalence and reconstruction (`g(f(x)) == x`), the client on full and partial lakes, every README example, and the production validator across all six tables. To re-fetch fixtures (e.g. after a UniProtKB release), pass `--force`:

```bash
python tests/fetch_fixtures.py --force                # re-fetch default
python tests/fetch_fixtures.py --scale stress --force  # re-fetch stress
```

### Tech stack

- **Orchestration**: Nextflow (DSL2, SLURM support)
- **Compute**: DuckDB (JSON parsing via `read_json_auto`, SQL transforms, out-of-core sorting)
- **Storage**: Parquet (zstd level 9, page index, sorted by `reviewed DESC`, `taxid ASC`, `acc ASC`, Hive-partitioned by `review_status`)
- **Streaming**: PyArrow (bounded-memory Arrow record batch → Parquet writing; column descriptions and source paths in the Arrow field metadata)
- **Manifest**: `manifest.json` — file list with sizes / SHA-256 / taxid ranges, schemas, sort orders, partitioning, semantic metadata; `SHA256SUMS.txt` and `RELEASE.metalink` beside it
- **Schema**: Inferred from data via DuckDB `read_json_auto`; optional columns and residual structs are cast to declared types (`COLUMN_TYPES`) so a build's schema does not depend on its input; `SCHEMA.md` is generated from a build by `bin/gen_schema_md.py`
- **FAIR metadata**: `datapackage.json` — [Frictionless Data Package](https://specs.frictionlessdata.io/data-package/) descriptor with Arrow types, nullability, semantic descriptions, keys, and licensing

---
