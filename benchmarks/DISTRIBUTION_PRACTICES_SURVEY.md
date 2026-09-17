# Distribution Practice Survey: how peers package, host, verify and retain bulk analytical data

**Date:** 2026-09-11
**Scope:** distribution and packaging practice only (formats, tiers, file sizing, hosting, integrity, retention, metadata, remote-read guidance). Schema design is covered by `BIOINFORMATICS_PARQUET_SURVEY.md` and `BIOINFORMATICS_PARQUET_SURVEY_INTL.md`.
**Purpose:** check the `static-lake` offering, and `PLAN_SCHEMA_V2.md` Part F in particular, against what UniProt itself and comparable resources publish today.
**Method:** four parallel web-research passes on 2026-09-11. Direct fetches of many provider hosts (uniprot.org, ftp.ebi.ac.uk, huggingface.co, opentargets.org, gnomad, ncbi, overturemaps docs, duckdb.org) were blocked by the sandbox firewall; facts were taken from the providers' GitHub-hosted documentation sources, anonymous S3/GCS bucket listings, Parquet footers read over HTTP range requests, and search snippets (marked *snippet* where nothing better was reachable). Items marked **unverified** could not be confirmed and should be checked before being quoted externally.

---

## 1. What UniProt offers today

| Aspect | Current UniProt practice | Source |
| --- | --- | --- |
| Formats on FTP | Flat text (`.dat`), XML, FASTA, RDF. No JSON on FTP (JSON only via REST). No Parquet. | `ebi-uniprot/uniprot-manual` `help/downloads.md` |
| Layout | `current_release/knowledgebase/complete/` with `uniprot_sprot.*` / `uniprot_trembl.*`; sibling tiers `taxonomic_divisions/`, `reference_proteomes/`, `idmapping/`, `variants/`, `embeddings/` (HDF5) | same |
| Tiers | Swiss-Prot vs TrEMBL files; taxonomic divisions; reference proteomes; id mapping as a separate small download | same |
| Integrity | `RELEASE.metalink` in every FTP folder: size + **MD5** per file, consumed by `curl --metalink`. No SHA-256. | `help/metalink.md` |
| Retention | "at least 2 years" of previous releases, then the `YYYY_01` release of each year kept indefinitely | `help/synchronization.md` |
| Cadence | "every 2-3 months" (2026: `2026_01` Jan 28, `2026_02` Jun 10, `2026_03` Sep 2) | `release-notes/` in the manual repo |
| Cloud | AWS Open Data `s3://aws-open-data-uniprot-rdf/` (eu-west-3, SIB-managed): **RDF N-Triples only**, all 27 releases since 2021-01 retained, `2026-02/` = 550 objects / 2.74 TB, per-file 250 MB – 11 GB, **no checksum or manifest objects in the bucket**. No official GCS/BigQuery UniProtKB dataset. | `awslabs/open-data-registry` `datasets/uniprot.yaml`; direct bucket listing |
| Remote query | SPARQL endpoint (232 B triples, 2026_02, *snippet*); REST `stream` endpoint capped at 10 M entries, 429 under parallel load; larger pulls via async file-generation job | `help/api_queries.md`, `help/file-generation-download.md` |
| Parquet elsewhere | No complete UniProtKB Parquet exists anywhere. Hugging Face has small third-party Swiss-Prot mirrors (`damlab/uniprot`, `opendatalab/SA-Prot-annot`); an official `huggingface.co/uniprot` org appears to exist with no public datasets (*snippet*, unverified). This repository's `main` branch (`ebi-uniprot/uniprot-parquet`, Hive-partitioned `review_status=/tax_division=` star layout) is the only official code and publishes no data. | search; `git log main` |

Two consequences for the lake. First, UniProt users already expect **per-file size + hash in a metalink** and **multi-year retention**; the lake's D1 "latest-only" decision (`AUDIT.md`) is a departure from UniProt's own FTP norm and needs to be stated as such. Second, UniProt already has an AWS Open Data bucket with a release-per-prefix layout; a `parquet/` sibling there is the obvious cloud channel.

---

## 2. Peer matrix

Legend: ✓ = practised and documented; ~ = partial or undocumented; ✗ = not offered; ? = unverified.

| Provider | Primary bulk format | Separable tiers | File sizing | Hosting / egress | Per-file size + hash | Retention policy | Machine-readable manifest / schema | Remote-read guidance | Published validation |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Open Targets Platform** | Parquet only (since 25.03); `p2j` tool for NDJSON | Per-dataset directories; evidence Hive-partitioned `sourceId=` | `part-*.parquet`, hundreds of MB (counts unverified) | EBI FTP/HTTPS/rsync + GCS (**requester-pays**) + AWS Open Data (free, SNS topic) + BigQuery | ~ `release_data_integrity` SHA1 list introduced 22.09; presence in 26.x unverified | ? not written down | ✓ Croissant JSON-LD per dataset (25.06+); release notes as schema changelog | ~ Spark/sparklyr/pandas/polars; no DuckDB, row-group or sort docs | ✓ QC metrics on Hugging Face + Streamlit; no losslessness claim |
| **ChEMBL** | SQLite/MySQL/Postgres dumps, SDF, RDF | Whole-DB only | n/a | EBI FTP + `latest/`; AWS Parquet/Athena copy **deprecated** | ? | ✓ all releases kept | Schema doc per release; no Croissant/Parquet | ✗ | ✗ |
| **eQTL Catalogue** | tabix TSV → **Parquet from r8 (2026)** | Per-dataset dirs | ? | EBI FTP only; tabix over FTP with rate-limit warning | ? | release notes | GitHub TSV metadata + `Columns.md` | ~ tabix range queries | ✗ |
| **GWAS Catalog sumstats** | GWAS-SSF TSV + YAML sidecar | Per-study dirs in 1,000-accession buckets | n/a | EBI FTP, Globus for submission; ~56 TB | ✓ MD5 in YAML sidecar and `.md5` files | nightly updates | ✓ pydantic-validated YAML | ✗ | ✗ |
| **Ensembl** | FASTA/GTF/GFF3/MySQL/VCF/RDF | Per-species, per-type | n/a | FTP/HTTPS/rsync/Globus | ~ `CHECKSUMS` via Unix `sum`, missing in some dirs | `release-NNN/` + `current_*`; new site announces deletions | ✓ JSON manifest per genome on the new FTP (2026) | ✗ | ✗ |
| **InterPro** | XML, TSV | Per-file | n/a | FTP `current_release/` + `releases/` | ✓ `.md5` per file, "strongly recommend md5sum" | previous releases kept (count ?) | release notes | ✗ | ✗ |
| **gnomAD v4.1.1** | Hail Tables (primary), sites VCF per chromosome, TSV | ✓ sites-only vs full; exomes/genomes/joint | 2–41 GiB per chromosome VCF | GCS (free) + S3 Open Data + Azure; separate requester-pays staging bucket | ✓ size + MD5 per VCF on download page; **none for Hail dirs** | ✓ all releases online | Hail `describe()`; BigQuery only v2/v3 | ~ client lib auto-picks bucket by cloud | ✗ |
| **NCBI Datasets** | zip data package: JSONL reports + `dataset_catalog.json` + `md5sum.txt` | ✓ **dehydrated** package (metadata + `fetch.txt`) recommended > 15 GB | per-assembly | HTTPS/rsync/FTP; CLI | ✓ `md5sum.txt` + catalog | `assembly_summary_historical` | ✓ OpenAPI + per-report schemas | ✗ | ✗ |
| **ClinVar** | XML, VCF, TSV | ✓ VCV/RCV/VCF/summary | ~ single files | FTP/HTTPS | ✓ `.md5` | ✓ monthly archived indefinitely, weekly not; `latest` stable names | XSD | ✗ | ✗ |
| **ENCODE** | many; portal-driven | per-file | n/a | HTTPS + S3 + Azure (free) | ✓ `md5sum` + `file_size` mandatory on every file object | ✓ | ✓ JSON Schema per object | ✗ | ✗ |
| **1000 Genomes / IGSR** | VCF/CRAM | per-collection | 36 TB CRAMs | FTP/Aspera/Globus + S3 (frozen) | ✓ `current.tree` lists every file with MD5 | ✓ frozen | index files | ✗ | ✗ |
| **AlphaFold DB** | mmCIF + JSON per entry | ✓ per-proteome tars, per-organism tars on website; Swiss-Prot subset advertised (file unverified) | 1,015,797 tar shards ≤10 k proteins, ~1 GB | GCS (free, anonymous) + EBI FTP + BigQuery metadata | ✗ manifests are path lists only; **no hashes** | v3/v4 buckets; older bucket not listable | BigQuery column docs | ✗ | ✗ |
| **ESM Metagenomic Atlas** | PDB tarballs by confidence bin; 16 GB metadata Parquet | ✓ high-confidence clust30 subset (~1 TB) vs full (15 TB) | 500 k – 1 M structures per tar | CDN URL lists | ✗ | v0, v2023_02, no `latest` | README | ✗ | documents what was excluded |
| **wwPDB / RCSB** | mmCIF (primary), BinaryCIF, PDB, XML | per-entry | small files, `divided/` tree | HTTPS + rsync (FTP deprecated 2024-11); yearly snapshots on `s3://pdbsnapshots` | ✗ holdings JSON lists files + dates, no hashes | ✓ annual snapshots since 2005, versioned archive | `current_file_holdings.json.gz` | ✗ | ✗ |
| **CELLxGENE Census** | TileDB-SOMA | ✓ per-organism experiments | 14,217 objects / ~1.05 TiB (2025-01-30 LTS) | single S3 bucket, free, us-west-2 | ✗ | ✓ **written policy**: LTS ≥5 years, weekly ~1 month; `stable`/`latest` aliases | ✓ `release.json` (aliases, URIs, LTS flag); semver schema with MUST language | ~ "compute in us-west-2" | ✗ |
| **Hugging Face Hub** (channel) | Parquet | per-config/split | auto-shards ≈500 MB; row groups 100–300 MB uncompressed; page index recommended | HF (free best-effort; ≤100 k files/repo, ≤10 k per folder, 500 GB hard cap per file) | ✗ Croissant `sha256` left empty | git-style revisions | ✓ auto Croissant for every dataset | ✓ `hf://` in DuckDB/Polars/PyArrow, documented streaming | ✗ |
| **Overture Maps** (non-bio exemplar) | GeoParquet | ✓ `theme=/type=` Hive dirs | **987 files / 612 GB, avg 591 MB**, 256 row groups per file (~20 k rows, 2–2.5 MB each), sorted by geohash, `bbox` struct for pruning | S3 + Azure, free | ✗ **none** | ✓ written: 60 days / two releases | ✓ STAC catalog with `latest` | ✓ DuckDB + httpfs examples | ✗ |
| **Common Crawl index** | Parquet, `crawl=/subset=` | per-crawl | ~300 files / 205–300 GB per crawl ≈ 0.7 GB per file | S3 | ✗ | all crawls | README | ✓ Athena/Spark | ✗ |

Engine guidance that peers converge on (DuckDB docs, GeoParquet distribution guide, Athena tuning, Hugging Face): files **100 MB – 1 GB** compressed (DuckDB "100 MB to 10 GB", Athena "128 MB – 1 GB", HF 500 MB shards, Overture ~600 MB); row groups **100 k – 1 M rows** or ~100 MB; ZSTD; Hive partitioning "especially helpful when querying remote files"; page index for random access; footer parsing is a measurable per-file cost.

---

## 3. Where the `static-lake` offering stands

| Aspect | `static-lake` today (+ Part F as planned) | Peer norm | Verdict |
| --- | --- | --- | --- |
| Primary format | Parquet, zstd, plus `sorted.jsonl.zst` fallback | Parquet is the direction (Open Targets, eQTL r8, HF) but still rare at EBI | **Ahead.** Keep the JSONL sibling; Open Targets dropped JSON and had to ship a converter. |
| Separable tier | `entries/` directory; Part F.2 makes it verifiable and the client tolerant | Universal: sites-only (gnomAD), dehydrated (NCBI), subsets (AFDB, ESM, All of Us), and UniProt's own sprot/trembl + divisions | **Aligned once F.2 ships.** A Swiss-Prot-only tier of every table is one directory per table (`review_status=sp/`, Part D); say so in the README. |
| File size | 256 MB target, 100 k-row groups | Peers cluster at 500 MB – 1 GB (Overture 591 MB avg, CC 0.7 GB, HF 500 MB); DuckDB 100 MB – 10 GB | **Low end of the band.** Part F.4 should treat 512 MB – 1 GB as the candidates and 256 MB as the thing to justify, not the default. Row-group size is inside DuckDB's range. Add `write_page_index=True` (HF recommendation; cheap). |
| Layout | Per-table dirs Hive-partitioned `review_status=sp|tr` with `reviewed` also stored in every file (Part D, revised 2026-09-11); sorted `(taxid, acc)` within a partition | Hive dirs on the top filter key (Overture, CC, Open Targets, this repo's `main`) | **Aligned.** A separately named string partition key avoids the path-only-column and typing problems that led an earlier draft to reject Hive directories. Organism level deferred to a skew check before launch (D.5). |
| Integrity | `provenance.json` with input MD5; Part F.2 adds per-file SHA-256 in `manifest.json` + `SHA256SUMS.txt` | UniProt: MD5 metalink per folder. Peers: mostly MD5 sidecars; **AFDB, Census, Overture, HF publish none** | **Ahead of peers; must also match UniProt.** Emit `RELEASE.metalink` (size + MD5 + SHA-256 entries are both valid metalink hash types) per lake directory so `curl --metalink` works exactly as on the rest of the FTP. |
| Release index | Per-release `manifest.json`; canonical URL and `latest`/`previous` symlinks open (O1/O2/A5) | Census `release.json` with `stable`/`latest`; Overture STAC `latest`; ClinVar stable file names | **Gap.** Add a top-level `releases.json` (release → path, schema version, size, LTS flag, `latest`) beside the release directories. Symlinks alone are not machine-readable over HTTPS. |
| Retention | D1 latest-only, previous kept briefly | UniProt FTP: ≥2 years + `YYYY_01` forever. Census: LTS ≥5 years, written. Overture: 60 days, written. All others: keep everything | **Departure from UniProt's own norm.** At 300 GB – 1 TB a full mirror of the FTP policy is 3–12 TB. Decide and write it down; a defensible middle is latest + previous + the `YYYY_01` release each year (mirrors the long-term half of the FTP policy). |
| Machine-readable metadata | `manifest.json`, Frictionless `datapackage.json`, Parquet footer metadata planned (A7) | Croissant JSON-LD is the emerging norm (Open Targets curated, HF auto); no bio Parquet adopter of Frictionless found | **Add Croissant.** Generate `croissant.json` from the same `TABLE_META`: `FileSet` per table with `includes` globs, `FileObject` per file with `contentSize` and `sha256` (which HF leaves empty; filling it is a differentiator). Keep `datapackage.json`; it costs nothing. |
| Hosting | EBI FTP/HTTPS/rsync assumed; cloud undecided | Flagship datasets mirror to one free public bucket (gnomAD ×3 clouds, Open Targets FTP+GCS+S3+BigQuery, AFDB GCS+BigQuery); AWS "lakehouse-ready" third-party Parquet copies were all **deprecated** | **Gap, and an easy one.** UniProt already owns `s3://aws-open-data-uniprot-rdf`; a `parquet/` prefix there (or a sibling bucket) puts the lake on the channel DuckDB/Polars users already read from. Publish under UniProt's name, not via AWS's conversions. |
| Remote-read guidance | README documents DuckDB httpfs, sort order, row-group pruning; Part A adds bloom filters and their remote cost | No bio peer documents DuckDB, row groups, sort order or bloom filters; Overture and HF do | **Ahead.** Keep, and add the file/row-group/footer numbers from F.1/F.4 so the guidance is quantitative. |
| Validation | 12-check validator, 1,000-entry round-trip, `validation_report.txt` per release; A11 `g()` round-trip gate planned | Only Open Targets publishes QC metrics (HF dataset + dashboard); nobody claims losslessness | **Unique.** Publish the validation report as data (a small Parquet or JSON per release, as Open Targets does on HF), not only as text. |
| Change notification | none | Open Targets SNS topic on S3 (registry-supported); ClinVar release notes dir; gnomAD news posts | Nice-to-have once a bucket exists. |
| Size statement | `AUDIT.md` §6 range (14× uncertain); F.1 measures | Providers publish scale statements to justify format (gnomAD 18 TB VDS vs 897 TB VCF; UKB 0.87 TiB BGEN vs 533 TiB pVCF) | F.1 numbers belong in the release notes, framed as "entries tier vs XML.gz". |
| Discoverability | none beyond FTP | HF org pages, Croissant, BigQuery listings | A Swiss-Prot-only `entries` tier on the (apparently unused) `huggingface.co/uniprot` org is within HF's free limits and gets the dataset viewer and `hf://` for free. Full lake exceeds free-tier norms (Tahoe-100M at 429 GB needed a plan). |

---

## 4. Changes applied to `PLAN_SCHEMA_V2.md` on 2026-09-11 (items 1–3 and 5–7 in Part F; item 4 in Part D)

1. **F.2.1** — also write `RELEASE.metalink` per lake directory (UniProt FTP convention), and generate `croissant.json` alongside `datapackage.json`.
2. **F.2.4 / new** — add a top-level `releases.json` index with `latest` (and `lts` if adopted) beside the release directories; keep the symlinks for shell users.
3. **F.4** — reframe: candidates are 512 MB and 1 GB; 256 MB must be justified by the footer measurement, not assumed. Add `write_page_index=True` to the writer.
4. **Part D** — rewritten to Hive-partition every table as `review_status=sp|tr` while keeping `reviewed`/`from_reviewed` stored in the files. Using a separately named string key removes the path-only-column and typing objections that the earlier draft raised; the flat-glob cost for raw DuckDB users is accepted and documented. An organism level is deferred to a pre-launch skew check (D.5).
5. **Retention (A1/O1)** — record the policy in the README in UniProt's own words; recommended: latest + previous + each year's `YYYY_01`.
6. **Hosting (A3/O2)** — propose a `parquet/` prefix in UniProt's existing AWS Open Data bucket as the cloud channel; EBI FTP/HTTPS/rsync stays canonical.
7. **Validation report as data** — emit `validation_report.json` (already produced) as a published artefact per release, listed in `releases.json`.

None of these changes the schema; all are packaging.

---

## 5. Source notes

Providers' GitHub documentation repositories were the primary verifiable sources: `ebi-uniprot/uniprot-manual`, `opentargets/platform-docs`, `awslabs/open-data-registry`, `broadinstitute/gnomad-browser` and `gnomad_methods`, `ncbi/datasets`, `chanzuckerberg/cellxgene-census`, `google-deepmind/alphafold` (`afdb/README.md`), `facebookresearch/esm` (`scripts/atlas`), `huggingface/hub-docs`, `huggingface/dataset-viewer`, `huggingface/datasets`, `OvertureMaps/docs` and `data`, `duckdb/duckdb-web`, `opengeospatial/geoparquet` (`distributing-geoparquet.md`), `commoncrawl/cc-index-table`, `igsr/gca_1000genomes_website`, `EBISPOT/gwas-summary-statistics-standard`, `ENCODE-DCC/encoded`. Bucket facts came from anonymous listings of `s3://overturemaps-us-west-2`, `s3://cellxgene-census-public-us-west-2`, `s3://pdbsnapshots`, `s3://aws-open-data-uniprot-rdf`, and `gs://public-datasets-deepmind-alphafold-v4`. Everything else is marked *snippet* or unverified above.
