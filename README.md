# UniProtKB Iceberg Data Lake

Pipeline to transform UniProtKB JSON dumps into an Apache Iceberg data lake using Nextflow, DuckDB, and PyIceberg.

## What it produces

| Output                        | Description                                                     |
| ----------------------------- | --------------------------------------------------------------- |
| `warehouse/uniprot/entries/`  | Iceberg table — one row per protein, sorted by taxid            |
| `warehouse/uniprot/features/` | Iceberg table — one row per positional feature, sorted by taxid |
| `catalog.db`                  | SQLite Iceberg catalog                                          |
| `uniprot.jsonl.zst`           | Taxid-sorted JSONL archive (for downstream consumers)           |

No Hive partitioning — Iceberg handles data skipping via per-file column statistics. Queries filtering on `taxid` skip most data files automatically.

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
# Small test
./run_lake.sh test_small

# Production (SLURM)
./run_lake.sh prod --input /path/to/UniProtKB.json.gz --outdir /scratch/lake
```

## Architecture

```
UniProtKB.json.gz
    │
    ▼
┌─────────────┐   pigz + ijson + zstd
│ STREAM_JSONL │──────────────────────► uniprot.jsonl.zst
└─────┬───────┘
      │
      ▼
┌──────────────────┐   DuckDB (read + transform + sort)
│ ICEBERG_TRANSFORM│   PyIceberg (write Iceberg tables)
└─────┬────────────┘
      │
      ▼
┌──────────┐   Row counts, snapshot checks, consistency
│ VALIDATE │
└──────────┘
```

DuckDB handles the heavy lifting: JSON parsing, SQL transformations (flattening, unnesting), and sorting by taxid. It streams Arrow record batches to PyIceberg, which writes the Iceberg data files and manages the SQLite catalog. Memory stays bounded regardless of dataset size.

## Schema

Defined in `bin/iceberg_schema.py`. Two tables:

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

# All human kinases
con.sql("""
    SELECT acc, gene_name, protein_name, ec_numbers
    FROM iceberg_scan('warehouse/uniprot/entries')
    WHERE taxid = 9606
      AND list_contains(keyword_names, 'Kinase')
""")
```

### PyIceberg + Pandas

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog("uniprot", uri="sqlite:///catalog.db", warehouse="./warehouse")
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
df = pl.scan_parquet("warehouse/uniprot/entries/data/*.parquet")
df.filter(pl.col("taxid") == 9606).select("acc", "gene_name").collect()
```

## Versioning

Each pipeline run creates a new Iceberg snapshot. Previous releases are accessible via time travel — Iceberg deduplicates unchanged data files across snapshots, so storage grows only by the size of actual changes.

## Tech Stack

- **Orchestration**: Nextflow (DSL2, SLURM support)
- **Compute**: DuckDB (JSON parsing, SQL transforms, sorting)
- **Storage**: Apache Iceberg (PyIceberg + SQLite catalog)
- **Format**: Parquet (zstd compression), sorted by taxid
