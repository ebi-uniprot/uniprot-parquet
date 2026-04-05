# UniProtKB Parquet Data Lake

Pipeline to transform UniProtKB JSON dumps into sorted, analysis-ready Parquet tables using Nextflow, DuckDB, and PyArrow.

## What it produces

Each release lands in its own directory (`<outdir>/<release>/`):

| Output                                 | Description                                                    |
| -------------------------------------- | -------------------------------------------------------------- |
| `lake/entries/entries_*.parquet`       | One row per protein                                            |
| `lake/features/features_*.parquet`     | One row per positional feature                                 |
| `lake/xrefs/xrefs_*.parquet`           | One row per cross-reference                                    |
| `lake/comments/comments_*.parquet`     | One row per comment annotation                                 |
| `lake/references/references_*.parquet` | One row per literature citation                                |
| `lake/manifest.json`                   | File list, schemas, row counts, sort orders, semantic metadata |
| `sorted.jsonl.zst`                     | JSONL archive sorted by review status, taxid, then accession   |
| `provenance.json`                      | Provenance record (checksums, git commit, row counts)          |
| `validation_report.txt`                | 8-check production validation (see below)                      |

All five tables are sorted by Swiss-Prot first (`reviewed`/`from_reviewed DESC`), then `taxid ASC`, then `acc ASC`. The boolean column is called `reviewed` on the entries table and `from_reviewed` on child tables (features, xrefs, comments, references) to clarify that it's inherited from the parent entry, not an independent review status.

## Accessing the data

### Python (one-liner)

`uniprot_lake.py` is a single-file client with no dependencies beyond DuckDB. It sets up views, macros, and httpfs automatically:

```python
from uniprot_lake import connect

con = connect("/data/uniprot/2026_01/lake")                           # local
con = connect("https://ftp.ebi.ac.uk/.../2026_01/lake")              # remote (auto-installs httpfs)

con.sql("SELECT acc, gene_name, protein_name FROM entries WHERE taxid = 9606 LIMIT 5").show()
con.sql("SELECT * FROM protein_card('P04637')").show()
con.sql("SELECT * FROM organism_features(9606, 'Domain')").show()
```

The returned object is a standard `duckdb.DuckDBPyConnection` — use it exactly as you would any DuckDB connection. Five views (`entries`, `features`, `xrefs`, `comments`, `refs`) and six macros are ready to use immediately.

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
```

### Direct access (no setup file)

You can also query the Parquet files directly without any setup:

```python
# DuckDB
con.sql("SELECT * FROM read_parquet('lake/entries/*.parquet') WHERE taxid = 9606")

# Polars
pl.scan_parquet("lake/entries/*.parquet").filter(pl.col("taxid") == 9606)

# pandas via PyArrow
pd.read_parquet("lake/entries/", filters=[("taxid", "==", 9606)])
```

### Metadata

The `manifest.json` inside the lake directory lists every Parquet file, its schema, row count, sort order, and semantic metadata (table descriptions, primary keys, foreign keys, column categories). Tools and LLM agents can read this to discover the data and generate correct joins without scanning files.

### JSONL (universal fallback)

`sorted.jsonl.zst` is the complete dataset in a format any language can read. Useful for piping through `jq`, streaming into custom parsers, or as an archival format. Sorted identically to the Parquet tables.

```bash
zstd -dc sorted.jsonl.zst | head -10 | jq '.primaryAccession, .organism.scientificName'
```

## Setup

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-lake
```

Or with pip:

```bash
pip install -r requirements.txt
```

## Demo

The quickest way to see the pipeline in action. Downloads ~3,800 reviewed _Drosophila melanogaster_ proteins from UniProtKB and builds a complete data lake:

```bash
cd demo && ./run_demo.sh
```

## Running

### Quick start with run_lake.sh

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

### Direct Nextflow invocation

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

### Pipeline parameters

| Parameter          | Default                | Description                                                                                |
| ------------------ | ---------------------- | ------------------------------------------------------------------------------------------ |
| `--inputfile`      | `entries_test.json`    | Input UniProtKB JSON(.gz) file                                                             |
| `--outdir`         | `results/uniprot_lake` | Output base directory                                                                      |
| `--release`        | `2026_01`              | Release label (output goes to `<outdir>/<release>/`)                                       |
| `--process_memory` | `96 GB`                | Memory for heavy processes (DuckDB gets 75% of this)                                       |
| `--duckdb_pct`     | `75`                   | Percentage of process memory allocated to DuckDB buffer pool                               |
| `--duckdb_temp`    | `$TMPDIR` or `/tmp`    | DuckDB spill directory for out-of-core sorts                                               |
| `--expected_count` | `null`                 | Expected entry count for integrity verification (required for full mode via `run_lake.sh`) |
| `--notify_email`   | `null`                 | Email for SLURM failure/requeue notifications                                              |

### Nextflow profiles

| Profile | Executor | Queue        | Default memory | Default time | Notes                                  |
| ------- | -------- | ------------ | -------------- | ------------ | -------------------------------------- |
| `local` | local    | —            | —              | —            | For testing on your laptop             |
| `prod`  | SLURM    | `production` | 16 GB          | 1 day        | Process-level directives take priority |
| `short` | SLURM    | `short`      | 4 GB           | 4 hours      | For subset runs                        |

## Architecture

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
+----------+   8 checks: completeness, uniqueness, null keys,
| VALIDATE |   referential integrity, sort order, round-trip,
+----+-----+   Parquet integrity, manifest consistency
     |
     v
+------------+   Checksums, git commit, row counts
| PROVENANCE |
+------------+
```

DuckDB handles the heavy lifting: JSON parsing (with automatic schema inference via `read_json_auto`), SQL transformations (flattening, unnesting), and sorting. It streams Arrow record batches to PyArrow, which writes zstd-compressed Parquet files directly. Memory stays bounded regardless of dataset size.

There is no committed schema file — the data is a JSON dump from production, so whatever schema it has is what we use. DuckDB infers types directly from the data at the start of each pipeline run. Each release is a full rebuild. Optional fields that may not appear in all datasets (e.g. `organismHosts` in virus-only entries) are handled gracefully with NULL substitution.

The pipeline is **idempotent** — re-running on the same release directory overwrites the Parquet files, so row counts are always correct. Each release gets its own isolated directory. With `--skip-existing`, partially completed runs resume from where they left off.

## Production on SLURM

Before running the full ~250M-entry UniProtKB dataset:

**DuckDB spill directory**: DuckDB's out-of-core sort spills intermediate data to disk. Point `--duckdb_temp` at a large scratch filesystem (1-2 TB free). If your cluster has fast node-local NVMe, that's ideal; shared Lustre/GPFS works but is slower.

**OOM recovery**: The `PARQUET_TRANSFORM` step has `errorStrategy 'retry'` with `maxRetries 1` and uses `--skip-existing` on retry. If it OOMs writing the xrefs table (the largest at ~5B rows), the retry skips already-written tables and resumes from where it left off.

**Resume**: All runs use `-resume` by default. If a SLURM job is killed (wall time, preemption), re-submitting the same command picks up from the last completed process.

**Integrity verification**: The `STREAM_JSONL` step writes a sidecar entry count and verifies the compressed output line count matches. When `--expected_count` is set, it also asserts against the known count from UniProt release statistics.

**Time budgets** (approximate for the full dataset):

| Process           | Time  | Memory | Notes                                                               |
| ----------------- | ----- | ------ | ------------------------------------------------------------------- |
| STREAM_JSONL      | 2-4h  | 4 GB   | pigz decompression is single-threaded                               |
| SORT_JSONL        | 4-8h  | 96 GB  | DuckDB out-of-core sort, 1 TB disk                                  |
| PARQUET_TRANSFORM | 6-12h | 96 GB  | 1 JSON parse → staged Parquet, then 5 fast Parquet reads, 2 TB disk |
| VALIDATE          | 1-2h  | 96 GB  | Bounded-memory streaming checks                                     |
| PROVENANCE        | <1m   | 1 GB   | Metadata only                                                       |

## Production validation

The `VALIDATE` step runs 8 checks against the source JSONL as ground truth. All checks use bounded-memory streaming. Any single failure exits 1 and blocks the provenance manifest:

1. **Completeness** — JSONL line count == entries rows; child table counts match `sum(entries.*_count)`
2. **Uniqueness** — `entries.acc` has zero duplicates
3. **Null keys** — `acc`, `reviewed`/`from_reviewed`, `taxid` never null across all tables
4. **Referential integrity** — every `acc` in features/xrefs/comments/references exists in entries
5. **Sort order** — all tables sorted by `(reviewed/from_reviewed DESC, taxid ASC, acc ASC)`
6. **Round-trip spot check** — 1000 reservoir-sampled entries verified field-by-field against JSONL
7. **Parquet file integrity** — every `.parquet` file in the lake is readable
8. **Manifest consistency** — `manifest.json` file list matches actual files on disk

## Schema design

We adopt a **denormalized-first + full nested** design. Each table has two layers:

1. **Flattened convenience columns** (e.g. `gene_name`, `gene_synonyms`, `ec_numbers`, `go_ids`) — cover the 90% use case with simple SQL. These are intentionally lossy shortcuts: `gene_name` is only the first gene's primary name, `ec_numbers` are only from `recommendedName`, and `go_ids` are IDs without aspect/evidence.
2. **Full nested structures** (e.g. `genes`, `protein_desc`, `comment`, `feature`, `reference`) — preserve all upstream data for power users and lossless JSONL reconstruction. DuckDB's struct/list syntax makes these queryable without ETL.

No UniProtKB data is discarded. Users needing isoforms, GO aspects, EC numbers from alternative names, multi-paragraph comments, or evidence codes can always query the nested columns.

Parquet's columnar storage mitigates any read-amplification cost — if a query touches 5 of 40+ columns, only those 5 are read from disk. Normalized child tables (features, xrefs, comments, references) are provided for high-cardinality relationships that would make the main table unwieldy as arrays.

All five tables are sorted by Swiss-Prot first, then `taxid ASC`, then `acc ASC`, and all include denormalized entry-level fields (`acc`, `reviewed`/`from_reviewed`, `taxid`) so most queries don't need joins.

**entries** — one row per protein, the primary table for 90% of use cases:

- Identity: `acc`, `id`, `reviewed`, `secondary_accs`, `entry_type`
- Organism: `taxid`, `organism_name`, `organism_common`, `lineage`
- Gene/protein: `gene_name`, `gene_synonyms`, `protein_name`, `alt_protein_names`, `protein_flag`, `ec_numbers`, `protein_existence`, `annotation_score`
- Sequence: `sequence`, `seq_length`, `seq_mass`, `seq_md5`, `seq_crc64`
- Shortcuts: `go_ids`, `xref_dbs`, `keyword_ids`, `keyword_names`
- Versioning: `first_public`, `last_modified`, `last_seq_modified`, `entry_version`, `seq_version`
- Counts: `feature_count`, `xref_count`, `comment_count`, `reference_count`, `uniparc_id`
- Lossless: `extra_attributes` (countByCommentType, countByFeatureType)
- Full nested: `organism`, `protein_desc`, `genes`, `keywords`, `organism_hosts`, `gene_locations`

**features** — one row per positional annotation (sorted by `start_pos` within each protein):

- `acc`, `from_reviewed`, `taxid`, `organism_name`, `seq_length`
- Flattened: `type`, `start_pos`, `end_pos`, `start_modifier`, `end_modifier`, `description`, `feature_id`, `evidence_codes`, `original_sequence`, `alternative_sequences`, `ligand_name`, `ligand_id`, `ligand_label`, `ligand_note`
- Full nested: `feature` (preserves evidences with source/id, featureCrossReferences, ligandPart)

**xrefs** — one row per cross-reference:

- `acc`, `from_reviewed`, `taxid`
- `database`, `id`, `properties`, `isoform_id`, `evidences`

**comments** — one row per comment annotation:

- `acc`, `from_reviewed`, `taxid`
- Flattened: `comment_type`, `text_value`
- Full nested: `comment` (preserves all polymorphic comment fields)

**references** — one row per literature citation:

- `acc`, `from_reviewed`, `taxid`
- Flattened: `reference_number`, `citation_type`, `citation_id`, `title`, `authors`, `authoring_group`, `publication_date`, `journal`, `volume`, `first_page`, `last_page`, `submission_database`, `citation_xrefs`, `reference_positions`, `reference_comments`, `evidences`
- Full nested: `reference` (preserves complete citation structure)

## Example queries

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

# All signal peptides in human (using the macro)
con.sql("SELECT * FROM organism_features(9606, 'Signal peptide')").show()

# Proteins with PDB structures (using the macro)
con.sql("SELECT * FROM organism_xrefs(9606, ['PDB'])").show()

# Function annotations for mouse
con.sql("SELECT * FROM organism_comments(10090, 'FUNCTION')").show()

# Entries joined with features — one row per feature, entry columns attached
con.sql("""
    SELECT acc, gene_name, type, start_pos, end_pos, description
    FROM entries_with_features(9606)
    WHERE type = 'Transmembrane'
""").show()

# Publications by a specific author
con.sql("""
    SELECT acc, title, publication_date
    FROM refs
    WHERE list_contains(authors, 'Levitsky A.A.')
""").show()

# Reviewed vs unreviewed counts
con.sql("SELECT reviewed, count(*) as n FROM entries GROUP BY reviewed").show()
```

## Versioning

Each release is a full rebuild into its own isolated directory (`<outdir>/<release>/`). Previous releases are preserved untouched. The `provenance.json` in each release directory records input file checksums, git commit, and row counts.

## Memory model

The pipeline separates process memory (Nextflow directive) from the DuckDB buffer pool. By default, DuckDB gets 75% of the process memory — the remaining 25% provides headroom for the Python interpreter, PyArrow heap, and JSON parsing overhead. Configure via `--process_memory` and `--duckdb_pct`.

DuckDB spills to disk when data exceeds the buffer pool. The spill directory defaults to `$TMPDIR` (which SLURM typically sets to node-local scratch) or `/tmp`. Override with `--duckdb_temp` to point at a large scratch filesystem.

## Testing

```bash
python -m pytest tests/ -v
```

The test suite (43 tests) covers row counts, column schemas, data integrity, sort order, manifest consistency, idempotency, `--skip-existing` resume, and the production validator across all five tables. Tests use `tests/fixtures/small.json.gz` (44 reviewed entries).

## Tech stack

- **Orchestration**: Nextflow (DSL2, SLURM support)
- **Compute**: DuckDB (JSON parsing via `read_json_auto`, SQL transforms, out-of-core sorting)
- **Storage**: Parquet (zstd compression, sorted by `reviewed`/`from_reviewed DESC`, `taxid ASC`, `acc ASC`)
- **Streaming**: PyArrow (bounded-memory Arrow record batch → Parquet writing)
- **Manifest**: `manifest.json` — file list, schemas, sort orders, semantic metadata (similar to Hugging Face datasets)
- **Schema**: Inferred from data via DuckDB `read_json_auto` — no committed schema file needed
