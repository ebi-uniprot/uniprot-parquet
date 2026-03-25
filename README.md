# UniProtKB Iceberg Data Lake

Pipeline to transform UniProtKB JSON dumps into an Apache Iceberg data lake using Nextflow, DuckDB, and PyIceberg.

## What it produces

| Output                        | Description                                                                    |
| ----------------------------- | ------------------------------------------------------------------------------ |
| `warehouse/uniprot/entries/`  | Iceberg table — one row per protein, sorted by `reviewed`, `taxid`             |
| `warehouse/uniprot/features/` | Iceberg table — one row per positional feature, sorted by `reviewed`, `taxid`  |
| `catalog.db`                  | SQLite Iceberg catalog                                                         |
| `uniprot.jsonl.zst`           | Taxid-sorted JSONL archive (for downstream consumers)                          |

No Hive partitioning — Iceberg handles data skipping via per-file column statistics. Data is sorted by `reviewed` then `taxid`, so queries filtering on either skip most data files automatically.

## Setup

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-lake
```

Or with pip:

```bash
pip install -r requirements.txt
```

## Running

```bash
# Small test (25 entries)
./run_lake.sh test_small

# Medium test (~100k entries)
./run_lake.sh test_med

# Production (SLURM)
./run_lake.sh prod --input /path/to/UniProtKB.json.gz --outdir /scratch/lake
```

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
+---------------+   Infer post-transform schema, diff against previous release
| SCHEMA_CHECK  |   (exit 1 if schema changed — review before proceeding)
+-------+-------+
        |
        v
+-------------------+   DuckDB (read + transform + sort)
| ICEBERG_TRANSFORM |   PyIceberg (write Iceberg tables)
+-------+-----------+
        |
        v
+----------+   Row counts, snapshot checks, consistency
| VALIDATE |
+----------+
```

DuckDB handles the heavy lifting: JSON parsing, SQL transformations (flattening, unnesting), and sorting. It streams Arrow record batches to PyIceberg, which writes the Iceberg data files and manages the SQLite catalog. Memory stays bounded regardless of dataset size.

The Iceberg schema is inferred at runtime from DuckDB's Arrow output — there is no hand-maintained schema file. The `SCHEMA_CHECK` step captures the post-transform schema and diffs it against the previous release, so you can review any changes before proceeding.

## Schema

Two tables, both sorted by `reviewed` (Swiss-Prot vs TrEMBL) then `taxid`:

**entries** — one row per protein, 30+ top-level columns for fast filtering plus full nested structures (zero cost if not selected, thanks to Parquet column pruning):

- Identity: `acc`, `id`, `reviewed`, `secondary_accs`
- Organism: `taxid`, `organism_name`, `organism_common`, `lineage`
- Gene/protein: `gene_name`, `protein_name`, `ec_numbers`, `protein_existence`, `annotation_score`
- Sequence: `sequence`, `seq_length`, `seq_mass`, `seq_md5`, `seq_crc64`
- Shortcuts: `go_ids`, `xref_dbs`, `keyword_ids`, `keyword_names`
- Versioning: `first_public`, `last_modified`, `entry_version`, `seq_version`
- Nested: `organism`, `protein_desc`, `genes`, `keywords`, `comments`, `xrefs`, `references`, `features`

**features** — one row per positional annotation, denormalized with entry-level fields so most feature queries don't need joins:

- `acc`, `reviewed`, `taxid`, `organism_name`, `seq_length`
- `type`, `start_pos`, `end_pos`, `description`, `feature_id`
- `evidence_codes`, `ligand_name`, `ligand_id`

## Querying

### DuckDB

```python
import duckdb

con = duckdb.connect()
con.load_extension("iceberg")

base = 'datalake/uniprot_lake_test_med/warehouse/uniprot'
con.sql(f"CREATE VIEW entries AS SELECT * FROM iceberg_scan('{base}/entries')")
con.sql(f"CREATE VIEW features AS SELECT * FROM iceberg_scan('{base}/features')")

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

# Reviewed vs unreviewed counts
con.sql("SELECT reviewed, count(*) as n FROM entries GROUP BY reviewed").show()
```

### PyIceberg + Pandas

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "uniprot",
    uri="sqlite:///datalake/uniprot_lake_test_med/catalog.db",
    warehouse="datalake/uniprot_lake_test_med/warehouse",
)
entries = catalog.load_table("uniprot.entries")

# Scan human entries, selecting only what you need
df = entries.scan(
    row_filter="taxid = 9606",
    selected_fields=("acc", "gene_name", "protein_name"),
).to_pandas()
```

### Polars

```python
import polars as pl

base = 'datalake/uniprot_lake_test_med/warehouse/uniprot'
df = pl.scan_parquet(f"{base}/entries/data/*.parquet")
df.filter(pl.col("taxid") == 9606).select("acc", "gene_name").collect()
```

## Versioning

Each release is a full rebuild of the Iceberg tables. The pipeline drops and recreates tables on each run, so there is no snapshot history across releases. Schema changes between releases are caught by the `SCHEMA_CHECK` step, which diffs the inferred schema against the previous release and requires manual review before proceeding.

## Tech Stack

- **Orchestration**: Nextflow (DSL2, SLURM support)
- **Compute**: DuckDB (JSON parsing, SQL transforms, sorting)
- **Storage**: Apache Iceberg (PyIceberg + SQLite catalog)
- **Format**: Parquet (zstd compression), sorted by `reviewed`, `taxid`
