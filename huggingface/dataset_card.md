---
pretty_name: UniProtKB Parquet (beta)
license: cc-by-4.0
language:
- en
tags:
- biology
- proteins
- uniprot
- uniprotkb
- parquet
- duckdb
- beta
size_categories:
- 1B<n<10B
configs:
- config_name: entries
  default: true
  data_files:
  - split: swissprot
    path: 2026_03/lake/entries/review_status=swissprot/*.parquet
  - split: trembl
    path: 2026_03/lake/entries/review_status=trembl/*.parquet
- config_name: features
  data_files:
  - split: swissprot
    path: 2026_03/lake/features/review_status=swissprot/*.parquet
  - split: trembl
    path: 2026_03/lake/features/review_status=trembl/*.parquet
- config_name: xrefs
  data_files:
  - split: swissprot
    path: 2026_03/lake/xrefs/review_status=swissprot/*.parquet
  - split: trembl
    path: 2026_03/lake/xrefs/review_status=trembl/*.parquet
- config_name: comments
  data_files:
  - split: swissprot
    path: 2026_03/lake/comments/review_status=swissprot/*.parquet
  - split: trembl
    path: 2026_03/lake/comments/review_status=trembl/*.parquet
- config_name: publications
  data_files:
  - split: swissprot
    path: 2026_03/lake/publications/review_status=swissprot/*.parquet
  - split: trembl
    path: 2026_03/lake/publications/review_status=trembl/*.parquet
- config_name: accession_map
  data_files:
  - split: swissprot
    path: 2026_03/lake/accession_map/review_status=swissprot/*.parquet
  - split: trembl
    path: 2026_03/lake/accession_map/review_status=trembl/*.parquet
---

# UniProtKB Parquet (beta)

> **⚠️ Beta: the schema is subject to change.**
> This is a preview of UniProtKB as Parquet. Between releases, tables, column
> names and types, partitioning and sort order may change without notice, and a
> release may be rebuilt or removed. Do not depend on it in production yet.
> `schema_version` (in `lake/manifest.json`) stays at 0.x until the beta ends,
> and a minor bump (0.1 → 0.2) means a breaking change. Pin a revision: each
> release is a git tag, e.g. `2026_03`. Feedback and issues:
> https://github.com/ebi-uniprot/uniprot-parquet/issues

UniProtKB release **2026_03** as six sorted, denormalized Parquet tables,
Hive-partitioned by `review_status=swissprot|trembl`. The tables hold the
complete source JSON (one column per key) beside typed shortcut columns, and
every entry can be rebuilt exactly. Built and validated by
[uniprot-parquet](https://github.com/ebi-uniprot/uniprot-parquet).

| Table | Grain | Rows | Size |
| --- | --- | --- | --- |
| `entries` | One row per protein | 150.0M | 47.5 GB |
| `features` | One row per positional annotation | 637.9M | 7.0 GB |
| `xrefs` | One row per cross-reference | 2.52B | 19.7 GB |
| `comments` | One row per comment | 309.5M | 6.5 GB |
| `publications` | One row per citation | 170.9M | 1.6 GB |
| `accession_map` | One row per primary or secondary accession | 151.2M | 0.7 GB |

`2026_03/SCHEMA.md` lists every column with its type, source path and
description; `2026_03/lake/manifest.json` and `datapackage.json` describe the
files, keys and sort order.

## Querying without downloading

```sql
-- DuckDB
INSTALL httpfs; LOAD httpfs;
SELECT accession, gene_primary, protein_name_primary, length
FROM read_parquet('hf://datasets/uniprot/uniprotkb-parquet@2026_03/2026_03/lake/entries/review_status=swissprot/*.parquet')
WHERE organism_id = 9606
LIMIT 10;
```

With the [`uniprot_parquet.py`](https://github.com/ebi-uniprot/uniprot-parquet/blob/main/uniprot_parquet.py)
client (views over all six tables, plus helper macros):

```python
from uniprot_parquet import connect
con = connect("hf://datasets/uniprot/uniprotkb-parquet@2026_03/2026_03/lake")
con.sql("SELECT * FROM protein_card('P04637')").show()
```

## Downloading

```bash
pip install -U huggingface_hub
# entries tier: entries + accession_map + metadata
hf download uniprot/uniprotkb-parquet --repo-type dataset --revision 2026_03 --local-dir . \
    --include "2026_03/lake/entries/*" --include "2026_03/lake/accession_map/*" \
    --include "2026_03/lake/*.json" --include "2026_03/lake/SHA256SUMS.txt" --include "2026_03/lake/LICENSE"
# Swiss-Prot only, every table
hf download uniprot/uniprotkb-parquet --repo-type dataset --revision 2026_03 --local-dir . \
    --include "2026_03/lake/*/review_status=swissprot/*" \
    --include "2026_03/lake/*.json" --include "2026_03/lake/SHA256SUMS.txt" --include "2026_03/lake/LICENSE"
```

Then query the local copy (`connect("2026_03/lake")`) or read it with DuckDB,
Polars, pandas, PyArrow, Spark or R. `sorted.jsonl.zst`, the same data as JSON
lines, is not on the Hub yet.

## Licence and citation

UniProtKB data are licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Please cite:

The UniProt Consortium. *UniProt: the Universal Protein Knowledgebase in 2025.*
Nucleic Acids Research 53(D1):D609–D617 (2025).
[doi:10.1093/nar/gkae1010](https://doi.org/10.1093/nar/gkae1010)
