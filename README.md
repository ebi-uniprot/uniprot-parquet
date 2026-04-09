# UniProtKB Parquet Data Lake

Analysis-ready Parquet tables covering the complete UniProtKB dataset — sorted, denormalized, and queryable from any language that reads Parquet.

## Tables

| Table          | Grain                             | Rows (full UniProtKB) |
| -------------- | --------------------------------- | --------------------- |
| `entries`      | One row per protein               | ~250M                 |
| `features`     | One row per positional annotation | ~1.3B                 |
| `xrefs`        | One row per cross-reference       | ~5B                   |
| `comments`     | One row per comment               | ~300M                 |
| `publications` | One row per citation              | ~500M                 |

All tables are sorted Swiss-Prot first (`reviewed`/`from_reviewed DESC`), then `taxid ASC`, then `acc ASC`. Parquet row-group min/max statistics mean predicate pushdown works automatically — engines skip irrelevant row groups without configuration.

---

## Using the lake

### Python (one-liner)

`uniprot_lake.py` is a single-file client with no dependencies beyond DuckDB. It sets up views, macros, and httpfs automatically:

```python
from uniprot_lake import connect

con = connect("/data/uniprot/2026_01/lake")                          # local
con = connect("https://ftp.ebi.ac.uk/.../2026_01/lake")              # remote (auto-installs httpfs)

con.sql("SELECT acc, gene_name, protein_name FROM entries WHERE taxid = 9606 LIMIT 5").show()
con.sql("SELECT * FROM protein_card('P04637')").show()
con.sql("SELECT * FROM organism_features(9606, 'Domain')").show()
```

The returned object is a standard `duckdb.DuckDBPyConnection`. Five views (`entries`, `features`, `xrefs`, `comments`, `publications`) and seven macros are ready to use immediately.

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

Both `uniprot_lake.py` and `setup_views.sql` define parameterised table macros for common patterns:

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
# Polars
import polars as pl
df = pl.scan_parquet("lake/entries/*.parquet").filter(pl.col("taxid") == 9606).collect()

# pandas / PyArrow
import pandas as pd
df = pd.read_parquet("lake/entries/", filters=[("taxid", "==", 9606)])

# DuckDB (standalone, no setup_views.sql)
import duckdb
duckdb.sql("SELECT * FROM read_parquet('lake/entries/*.parquet') WHERE taxid = 9606")
```

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
from uniprot_lake import connect
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

`manifest.json` inside the lake directory lists every Parquet file, its schema, row count, sort order, and semantic metadata (table descriptions, primary keys, foreign keys, column categories). Tools and LLM agents can read this to discover the data and generate correct joins without scanning files.

### JSONL (universal fallback)

`sorted.jsonl.zst` is the complete dataset in a format any language can read. Sorted identically to the Parquet tables.

```bash
zstd -dc sorted.jsonl.zst | head -10 | jq '.primaryAccession, .organism.scientificName'
```

### Schema design

The lake adopts a **denormalized-first + full nested** design. Each table has two layers: flattened convenience columns (e.g. `gene_names`, `protein_name`, `go_ids`) that cover 90% of use cases with simple SQL, and full nested structures (e.g. `genes`, `protein_desc`, `comment`, `feature`, `reference`) that preserve all upstream data for lossless JSONL reconstruction.

No UniProtKB data is discarded. Users needing isoforms, GO aspects, EC numbers from alternative names, multi-paragraph comments, or evidence codes can always query the nested columns. Parquet's columnar storage means queries touching 5 of 40+ columns only read those 5 from disk.

Child tables (`features`, `xrefs`, `comments`, `publications`) include denormalized entry-level fields (`acc`, `from_reviewed`, `taxid`) so most queries don't need joins. The boolean column is called `reviewed` on entries and `from_reviewed` on child tables to clarify it's inherited from the parent entry.

<details>
<summary>Column reference</summary>

**entries** — one row per protein, the primary table for 90% of use cases:

- Identity: `acc`, `id`, `reviewed`, `secondary_accs`, `entry_type`
- Organism: `taxid`, `organism_name`, `organism_common`, `lineage`
- Gene/protein: `gene_names`, `gene_synonyms`, `protein_name` (falls back to submittedName for TrEMBL entries), `alt_protein_names`, `protein_flag`, `ec_numbers`, `protein_existence`, `annotation_score`
- Sequence: `sequence`, `seq_length`, `seq_mass`, `seq_md5`, `seq_crc64`
- Shortcuts: `go_ids`, `xref_dbs`, `keyword_ids`, `keyword_names`
- Versioning: `first_public`, `last_modified`, `last_seq_modified`, `entry_version`, `seq_version`
- Counts: `feature_count`, `xref_count`, `comment_count`, `reference_count`, `uniparc_id`
- Lossless: `extra_attributes` (countByCommentType, countByFeatureType)
- Full nested: `organism`, `protein_desc`, `genes`, `keywords`, `organism_hosts`, `gene_locations`

**features** — one row per positional annotation:

- `acc`, `from_reviewed`, `taxid`, `organism_name`, `seq_length`
- Flattened: `type`, `start_pos`, `end_pos`, `start_modifier`, `end_modifier`, `description`, `feature_id`, `evidence_codes`, `original_sequence`, `alternative_sequences`, `ligand_name`, `ligand_id`, `ligand_label`, `ligand_note`
- Full nested: `feature` (preserves evidences with source/id, featureCrossReferences, ligandPart)

**xrefs** — one row per cross-reference:

- `acc`, `from_reviewed`, `taxid`
- `database`, `id`, `properties`, `isoform_id`, `evidences`

**comments** — one row per comment annotation:

- `acc`, `from_reviewed`, `taxid`
- Flattened: `comment_type`, `text_value` (covers 15 of 25 comment types; the other 10 store data in type-specific keys)
- Full nested: `comment` (JSON — preserves all polymorphic comment fields). Use `comment->>'$.key'` to extract text values or `comment->'$.key'` for nested objects.

**publications** — one row per literature citation:

- `acc`, `from_reviewed`, `taxid`
- Flattened: `reference_number`, `citation_type`, `citation_id`, `title`, `authors`, `authoring_group`, `publication_date`, `journal`, `volume`, `first_page`, `last_page`, `submission_database`, `citation_xrefs`, `reference_positions`, `reference_comments`, `evidences`
- Full nested: `reference` (preserves complete citation structure)

</details>

### Versioning

Each release is a full rebuild into its own isolated directory (`<outdir>/<release>/`). Previous releases are preserved untouched. `provenance.json` in each release records input file checksums, git commit, and row counts.

---

## Building the lake

Everything below is for pipeline operators who want to rebuild the lake from a UniProtKB JSON dump.

### Setup

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-lake
```

Or with pip (core dependencies only):

```bash
pip install duckdb pyarrow orjson ijson zstandard pytest
```

### Demo

Downloads ~5,000 proteins (reviewed + unreviewed) from five model organisms (Human, Mouse, Fruit fly, Arabidopsis, Yeast) and builds a complete lake:

```bash
cd demo && ./run_demo.sh
```

### Running the pipeline

#### Quick start with run_lake.sh

```bash
./run_lake.sh                                          # small: ~4K entries (demo data), local
./run_lake.sh subset                                   # subset: 100K entries, SLURM short queue
./run_lake.sh full --expected-count 248799253           # full UniProtKB, SLURM prod queue
```

The `--expected-count` flag is **required for full mode** — get the entry count from the [UniProt release statistics](https://www.uniprot.org/help/release-statistics). This guards against silent data loss during the JSON-to-JSONL streaming step.

Override defaults with flags:

```bash
./run_lake.sh full \
    --expected-count 248799253 \
    --input /path/to/UniProtKB.json.gz \
    --outdir /scratch/uniprot_lake \
    --duckdb-tmp /scratch/$USER/duckdb_tmp
```

#### Direct Nextflow invocation

```bash
# Local test (uses committed small.json.gz, 44 entries)
nextflow run upjson2lake.nf -profile local \
    --inputfile tests/fixtures/small.json.gz \
    --release test_2026

# Production (SLURM)
nextflow run upjson2lake.nf -profile prod \
    --inputfile /path/to/UniProtKB.json.gz \
    --release 2026_02 \
    --outdir /scratch/uniprot_lake \
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
| `--outdir`         | `results/uniprot_lake`         | Output base directory                                                                      |
| `--release`        | `2026_01`                      | Release label (output goes to `<outdir>/<release>/`)                                       |
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
| PARQUET_TRANSFORM  |──> lake/ + manifest.json
+--------+-----------+   (JSONL staged to Parquet once, then 5 fast reads)
         |
         v
+----------+   12 checks: completeness, uniqueness, null keys,
| VALIDATE |   referential integrity, sort order, round-trip,
+----+-----+   Parquet integrity, manifest, denorm sync, seq, coords, types
     |
     v
+------------+   Checksums, git commit, row counts
| PROVENANCE |
+------------+
```

DuckDB handles the heavy lifting: JSON parsing (with automatic schema inference via `read_json_auto`), SQL transformations (flattening, unnesting), and sorting. It streams Arrow record batches to PyArrow, which writes zstd-compressed Parquet files directly. Memory stays bounded regardless of dataset size.

There is no committed schema file — the data is a JSON dump from production, so whatever schema it has is what we use. DuckDB infers types directly from the data at the start of each pipeline run with `sample_size=-1` (full-file scan) to ensure rare nested struct fields are never silently dropped. Each release is a full rebuild. Optional fields that may not appear in all datasets (e.g. `organismHosts` in virus-only entries) are handled gracefully with NULL substitution via schema-driven SQL generation.

The pipeline is **idempotent** — re-running on the same release directory overwrites the Parquet files. With `--skip-existing`, partially completed runs resume from where they left off.

### Production notes (SLURM)

**OOM recovery**: `PARQUET_TRANSFORM` retries once on OOM (exit 137/140) with doubled memory, capped at 256 GB. Non-OOM failures are terminal. The `--skip-existing` flag means retries skip already-written tables.

**DuckDB spill directory**: Point `--duckdb_temp` at a large scratch filesystem (1–2 TB free). Node-local NVMe is ideal; shared Lustre/GPFS works but is slower.

**Resume**: All runs use `-resume` by default. If a SLURM job is killed (wall time, preemption), re-submitting picks up from the last completed process.

**Integrity verification**: `STREAM_JSONL` writes a sidecar entry count and verifies the compressed output line count matches. When `--expected_count` is set, it also asserts against the known count from UniProt release statistics.

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

The `VALIDATE` step runs 12 checks against the source JSONL as ground truth. All checks use bounded-memory streaming or DuckDB joins (no Python dicts for billion-row tables). Any single failure exits 1 and blocks the provenance manifest:

1. **Completeness** — JSONL line count == entries rows; child table counts match `sum(entries.*_count)`
2. **Uniqueness** — `entries.acc` has zero duplicates
3. **Null keys and empty strings** — `acc`, `reviewed`/`from_reviewed`, `taxid` never null; identity columns never empty
4. **Referential integrity** — every `acc` in child tables exists in entries (DuckDB anti-join)
5. **Sort order** — all tables sorted by `(reviewed/from_reviewed DESC, taxid ASC, acc ASC)`
6. **Round-trip spot check** — 1000 reservoir-sampled entries verified field-by-field against JSONL
7. **Parquet file integrity** — every `.parquet` file in the lake is readable
8. **Manifest consistency** — `manifest.json` file list matches actual files on disk
9. **Denormalized column sync** — `taxid` and `from_reviewed` in child tables match entries (DuckDB join)
10. **Sequence integrity** — `len(sequence) == seq_length` for every entry; no zero-length sequences
11. **Feature coordinate boundaries** — `start_pos <= end_pos` where both are non-null
12. **Schema type protection** — critical columns have expected Arrow types (not silently cast by inference)

### Testing

```bash
python -m pytest tests/ -v
```

81 tests covering row counts, column schemas, data integrity, sort order, manifest consistency, idempotency, `--skip-existing` resume, and the production validator across all five tables. Tests use `tests/fixtures/small.json.gz` (44 reviewed entries).

### Tech stack

- **Orchestration**: Nextflow (DSL2, SLURM support)
- **Compute**: DuckDB (JSON parsing via `read_json_auto`, SQL transforms, out-of-core sorting)
- **Storage**: Parquet (zstd compression, sorted by `reviewed`/`from_reviewed DESC`, `taxid ASC`, `acc ASC`)
- **Streaming**: PyArrow (bounded-memory Arrow record batch → Parquet writing)
- **Manifest**: `manifest.json` — file list, schemas, sort orders, semantic metadata
- **Schema**: Inferred from data via DuckDB `read_json_auto` — no committed schema file
