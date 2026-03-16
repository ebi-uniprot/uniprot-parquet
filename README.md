# UniProtKB-Parquet

Pipeline to transform UniProtKB JSON data into a Hive-partitioned Parquet data lake using Nextflow and DuckDB.

## Setup

### 1. Environment

Requires micromamba.

```bash
micromamba create -f environment.yml -y
micromamba activate uniprot-lake

```

### 2. Execution

Trivial test for Nextflow setup.

```bash
nextflow run test.nf
```

Run the pipeline with a stratified JSONL sample.

```bash
nextflow run main.nf --input "sample.jsonl" --outdir "results/"

```

#### Execution in SLURM
SSH to SLURM login node

```
ssh codon-slurm-login
```

Clone the repository

```
git clone https://github.com/ebi-uniprot/uniprot-parquet.git
cd uniprot-parquet
```

Run the sbatch script

```
sbatch slurm.batch
```

## Architecture

Nextflow parallelizes the workload while DuckDB handles schema inference and Parquet generation.

### Star Schema

- core.parquet: Metadata and analytical aliases (acc, taxid, seq_mass).
- seqs.parquet: Amino acid sequences.
- features.parquet: Unnested positional annotations.

### Partitioning Strategy

Physical directory structure:
`results/uniprot_lake/review_status={sp|tr}/tax_division={HUM|BCT|VRL...}/`

## Usage Examples

### DuckDB (SQL)

```python
import duckdb
con = duckdb.connect()
con.execute("SELECT * FROM read_parquet('results/uniprot_lake/**/core.parquet', hive_partitioning=true) LIMIT 5").df()

```

### Polars

```python
import polars as pl
df = pl.scan_parquet('results/uniprot_lake/**/core.parquet', hive_partitioning=True)
print(df.collect())

```

## Tech Stack

- Orchestration: Nextflow
- Engine: DuckDB CLI
- Language: Python >3.14
- Format: Apache Parquet (Zstd)
