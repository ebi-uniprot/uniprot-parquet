# UniProtKB Iceberg Data Lake

Pipeline to transform UniProtKB JSON dumps into an Apache Iceberg data lake using Nextflow, DuckDB, and PyIceberg.

## What it produces

Each release lands in its own directory (`<outdir>/<release>/`):

| Output                          | Description                                           |
| ------------------------------- | ----------------------------------------------------- |
| `warehouse/uniprotkb/entries/`  | Iceberg table — one row per protein                   |
| `warehouse/uniprotkb/features/` | Iceberg table — one row per positional feature        |
| `warehouse/uniprotkb/xrefs/`    | Iceberg table — one row per cross-reference           |
| `warehouse/uniprotkb/comments/` | Iceberg table — one row per comment annotation        |
| `catalog.db`                    | SQLite Iceberg catalog                                |
| `sorted.jsonl.zst`              | JSONL archive sorted by review status then taxid      |
| `manifest.json`                 | Provenance record (checksums, git commit, row counts) |
| `validation_report.txt`         | 8-check production validation (see below)             |

All tables are sorted by `reviewed DESC` (Swiss-Prot first) then `taxid ASC`. Iceberg handles data skipping via per-file column statistics — queries filtering on either column skip most data files automatically. No Hive partitioning needed.

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

The quickest way to see the pipeline in action. Downloads ~3,800 reviewed *Drosophila melanogaster* proteins from UniProtKB and builds a complete Iceberg data lake:

```bash
cd demo && ./run_demo.sh
```

This creates `demo/lake/2026_01/` with the full output (warehouse, catalog, sorted JSONL, validation report, manifest). The download is cached — re-running skips it. Use `--clean` to wipe the lake and rebuild from scratch.

## Running

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
    --duckdb_temp /hps/nobackup/.../tmp
```

## Schema management

`schema.json` is the **single source of truth** for column types. It maps each top-level JSON field to its DuckDB type and is used by `read_json(columns={...})` for deterministic parsing. The Iceberg schemas are derived from it at transform time (cheap LIMIT 1 inference, sub-second).

**Bootstrap** (run once on the full dataset):

```bash
python bin/schema_bootstrap.py /path/to/full_uniprotkb.jsonl.zst \
    --schema-json-out schema.json \
    --release 2026_01
git add schema.json && git commit -m "chore: bootstrap schema.json for 2026_01"
```

**Validation** (runs automatically in the pipeline): the `SCHEMA_CHECK` step reads every row with the declared types from `schema.json`. If any entry doesn't conform, DuckDB throws a type error and the pipeline stops before the expensive transform.

**Manual edits**: `schema.json` is human-readable. Adding new fields is safe (they'll be NULL until the data has them). If you know what will change in a new release, you can update it by hand.

## Architecture

```
UniProtKB.json.gz
    |
    v
+---------------+   pigz + ijson + zstd
| STREAM_JSONL  |------------------------> uniprot.jsonl.zst
+-------+-------+
        |
        v
+---------------+   Full-scan JSONL validation against schema.json
| SCHEMA_CHECK  |   (reads every row with declared types; ~30-60 min)
+-------+-------+
        |  (gate: sort + transform only run if validation passes)
        v
+------------+   DuckDB out-of-core sort
| SORT_JSONL |──────────────────────────> sorted.jsonl.zst
+------+-----+
       |  (pre-sorted input makes DuckDB ORDER BY nearly free)
       v
+-------------------+   DuckDB + PyIceberg
| ICEBERG_TRANSFORM |──> warehouse/ + catalog.db
+-------+-----------+
        |
        v
+----------+   Completeness, referential integrity, sort order,
| VALIDATE |   round-trip spot checks, Parquet integrity
+----+-----+
     |
     v
+----------+   Checksums, git commit, row counts
| MANIFEST |
+----------+
```

DuckDB handles the heavy lifting: JSON parsing, SQL transformations (flattening, unnesting), and sorting. It streams Arrow record batches to PyIceberg, which writes the Iceberg data files and manages the SQLite catalog. Memory stays bounded regardless of dataset size.

The pipeline is **idempotent** — re-running on the same release directory drops and recreates all tables, so row counts are always correct. Each release gets its own isolated directory and catalog. With `--skip-existing`, partially completed runs resume from where they left off (tables already written are preserved).

## Production validation

The `VALIDATE` step runs 8 checks against the source JSONL as ground truth. Any single failure exits 1 and blocks the provenance manifest:

1. **Completeness** — JSONL line count == entries rows; child table counts match `sum(entries.*_count)`
2. **Uniqueness** — `entries.acc` has zero duplicates
3. **Null keys** — `acc`, `reviewed`, `taxid` never null across all tables
4. **Referential integrity** — every `acc` in features/xrefs/comments exists in entries
5. **Sort order** — all tables sorted by `(reviewed DESC, taxid ASC)`
6. **Round-trip spot check** — 1000 reservoir-sampled entries verified field-by-field against JSONL
7. **Parquet file integrity** — every `.parquet` file in the warehouse is readable
8. **Snapshot sanity** — each table has exactly 1 APPEND snapshot

## Schema

We adopt a **denormalized-first + full nested** design. Each table has two layers:

1. **Flattened convenience columns** (e.g. `gene_name`, `ec_numbers`, `go_ids`) — cover the 90% use case with simple SQL. These are intentionally lossy shortcuts: `gene_name` is only the first gene's primary name, `ec_numbers` are only from `recommendedName`, and `go_ids` are IDs without aspect/evidence.
2. **Full nested structures** (e.g. `genes`, `protein_desc`, `references`, `comment`) — preserve all upstream data for power users. DuckDB's JSON path syntax makes these queryable without ETL. For example, to get all gene synonyms: `SELECT e.genes[1].synonyms FROM entries e`.

This means no UniProtKB data is discarded. Users needing isoforms, gene synonyms, GO aspects, EC numbers from alternative names, multi-paragraph comments, or evidence codes can always query the nested columns. If a flattened shortcut proves too lossy for common queries, it can be expanded or a dedicated table added in a future release.

Parquet's columnar storage mitigates any read-amplification cost — if a query touches 5 of 30+ columns, only those 5 are read from disk. Supplementary normalized tables (features, xrefs, comments) are provided for high-cardinality relationships that would make the main table unwieldy as arrays.

All four tables are sorted by `reviewed DESC` (Swiss-Prot first) then `taxid ASC`, and all include denormalized entry-level fields (`acc`, `reviewed`, `taxid`) so most queries don't need joins.

**entries** — one row per protein, the primary table for 90% of use cases:

- Identity: `acc`, `id`, `reviewed`, `secondary_accs`
- Organism: `taxid`, `organism_name`, `organism_common`, `lineage`
- Gene/protein: `gene_name` (first gene only), `protein_name`, `ec_numbers` (recommendedName only), `protein_existence`, `annotation_score`
- Sequence: `sequence` (canonical only), `seq_length`, `seq_mass`, `seq_md5`, `seq_crc64`
- Shortcuts: `go_ids` (IDs only), `xref_dbs`, `keyword_ids`, `keyword_names`
- Versioning: `first_public`, `last_modified`, `entry_version`, `seq_version`
- Full nested: `organism`, `protein_desc`, `genes`, `keywords`, `references`, `organism_hosts`, `gene_locations`

**features** — one row per positional annotation:

- `acc`, `reviewed`, `taxid`, `organism_name`, `seq_length`
- `type`, `start_pos`, `end_pos`, `description`, `feature_id`
- `evidence_codes`, `ligand_name`, `ligand_id`

**xrefs** — one row per cross-reference:

- `acc`, `reviewed`, `taxid`
- `database`, `id`, `properties`

**comments** — one row per comment annotation:

- `acc`, `reviewed`, `taxid`
- `comment_type`, `text_value` (extracted from common texts array)
- `comment` (full nested structure for polymorphic comment types)

## Querying

### DuckDB

```python
import duckdb

con = duckdb.connect()
con.load_extension("iceberg")

base = 'results/uniprot_lake/2026_01/warehouse/uniprotkb'
con.sql(f"CREATE VIEW entries AS SELECT * FROM iceberg_scan('{base}/entries')")
con.sql(f"CREATE VIEW features AS SELECT * FROM iceberg_scan('{base}/features')")
con.sql(f"CREATE VIEW xrefs AS SELECT * FROM iceberg_scan('{base}/xrefs')")
con.sql(f"CREATE VIEW comments AS SELECT * FROM iceberg_scan('{base}/comments')")

# All human kinases
con.sql("""
    SELECT acc, gene_name, protein_name, ec_numbers
    FROM entries
    WHERE taxid = 9606
      AND list_contains(keyword_names, 'Kinase')
""").show()

# Domain features for human proteins
con.sql("""
    SELECT acc, type, start_pos, end_pos, description
    FROM features
    WHERE taxid = 9606 AND type = 'Domain'
""").show()

# Proteins with both PDB and AlphaFold structures
con.sql("""
    SELECT acc, database, id
    FROM xrefs
    WHERE database IN ('PDB', 'AlphaFoldDB')
      AND taxid = 9606
""").show()

# Function annotations for human proteins
con.sql("""
    SELECT acc, text_value
    FROM comments
    WHERE comment_type = 'FUNCTION'
      AND taxid = 9606
""").show()

# Reviewed vs unreviewed counts
con.sql("SELECT reviewed, count(*) as n FROM entries GROUP BY reviewed").show()
```

### PyIceberg + Pandas

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "uniprot",
    uri="sqlite:///results/uniprot_lake/2026_01/catalog.db",
    warehouse="results/uniprot_lake/2026_01/warehouse",
)
entries = catalog.load_table("uniprotkb.entries")

# Scan human entries, selecting only what you need
df = entries.scan(
    row_filter="taxid = 9606",
    selected_fields=("acc", "gene_name", "protein_name"),
).to_pandas()
```

### Polars

```python
import polars as pl

base = 'results/uniprot_lake/2026_01/warehouse/uniprotkb'
df = pl.scan_parquet(f"{base}/entries/data/*.parquet")
df.filter(pl.col("taxid") == 9606).select("acc", "gene_name").collect()
```

## Versioning

Each release is a full rebuild into its own isolated directory (`results/uniprot_lake/<release>/`). Previous releases are preserved untouched. The `manifest.json` in each release directory records provenance: input file checksums, `schema.json` checksum, git commit, row counts, and snapshot IDs.

## Memory model

The pipeline separates process memory (Nextflow directive) from the DuckDB buffer pool. By default, DuckDB gets 75% of the process memory — the remaining 25% provides headroom for the Python interpreter, PyArrow heap, and JSON parsing overhead. Configure via `--process_memory` and `--duckdb_pct`.

## Tech Stack

- **Orchestration**: Nextflow (DSL2, SLURM support)
- **Compute**: DuckDB (JSON parsing, SQL transforms, sorting)
- **Storage**: Apache Iceberg (PyIceberg + SQLite catalog)
- **Format**: Parquet (zstd compression), sorted by `reviewed DESC`, `taxid ASC`
- **Schema**: `schema.json` — single source of truth, human-readable, manually editable
