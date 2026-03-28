# UniProtKB Iceberg Data Lake

Pipeline to transform UniProtKB JSON dumps into an Apache Iceberg data lake using Nextflow, DuckDB, and PyIceberg.

## What it produces

| Output                          | Description                                           |
| ------------------------------- | ----------------------------------------------------- |
| `warehouse/uniprotkb/entries/`  | Iceberg table — one row per protein                   |
| `warehouse/uniprotkb/features/` | Iceberg table — one row per positional feature        |
| `warehouse/uniprotkb/xrefs/`    | Iceberg table — one row per cross-reference           |
| `warehouse/uniprotkb/comments/` | Iceberg table — one row per comment annotation        |
| `catalog.db`                    | SQLite Iceberg catalog                                |
| `uniprot.jsonl.zst`             | JSONL archive sorted by review status then taxid      |

All tables are sorted by `reviewed` then `taxid`. Iceberg handles data skipping via per-file column statistics — queries filtering on either column skip most data files automatically. No Hive partitioning needed.

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

We adopt a denormalized-first design, prioritizing query simplicity for the typical bioinformatics user. Parquet's columnar storage mitigates any read-amplification cost — if a query touches 5 of 30+ columns, only those 5 are read from disk. Supplementary normalized tables (features, xrefs, comments) are provided for high-cardinality relationships that would make the main table unwieldy as arrays.

All four tables are sorted by `reviewed` (Swiss-Prot vs TrEMBL) then `taxid`, and all include denormalized entry-level fields (`acc`, `reviewed`, `taxid`) so most queries don't need joins.

**entries** — one row per protein, the primary table for 90% of use cases:

- Identity: `acc`, `id`, `reviewed`, `secondary_accs`
- Organism: `taxid`, `organism_name`, `organism_common`, `lineage`
- Gene/protein: `gene_name`, `protein_name`, `ec_numbers`, `protein_existence`, `annotation_score`
- Sequence: `sequence`, `seq_length`, `seq_mass`, `seq_md5`, `seq_crc64`
- Shortcuts: `go_ids`, `xref_dbs`, `keyword_ids`, `keyword_names`
- Versioning: `first_public`, `last_modified`, `entry_version`, `seq_version`
- Nested: `organism`, `protein_desc`, `genes`, `keywords`, `references`, `organism_hosts`, `gene_locations`

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

base = 'datalake/uniprot_lake_test_med/warehouse/uniprotkb'
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
    uri="sqlite:///datalake/uniprot_lake_test_med/catalog.db",
    warehouse="datalake/uniprot_lake_test_med/warehouse",
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

base = 'datalake/uniprot_lake_test_med/warehouse/uniprotkb'
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
