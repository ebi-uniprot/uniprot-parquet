# Schema review against live-API demand evidence

**Evidence:** `handoff-8x1w-v2/` (livelogs-parser @ `d8b6f16`, 8 sampled weeks, Jan–Aug 2026, 117.4M search + 3.9M stream + 10.0M accessions requests).
**Schema under review:** `README.md` "Schema design", `AUDIT.md` D4/D5/O4/S1–S12, `bin/parquet_transform.py` `TABLE_DEFS` and `_build_*_sql`.
**Reviewed:** 2026-09-11. Companion to `AUDIT.md`; IDs there are cited as-is. New IDs here are `R#`.

The evidence is demand, not design. Verdicts below say whether the demand data *supports*, *challenges*, or *does not bear on* each current choice. Every challenged item names the data-side check that must be run on the real lake before anything is adopted, because the logs carry no value cardinality.

---

## 0. How the evidence was weighed

These are the producers' reading rules plus the run-specific corrections, applied throughout:

| Rule | Consequence for this review |
| --- | --- |
| June 11–13 scraper drove June search volume to 42.1M (vs 11–19M other months) | `n_requests` for search fields is contaminated; `n_distinct_queries` and `n_distinct_clients` are used for ranking. The scraper's fingerprint is visible: in June `sequence` and `organism_id` appear on 87% of accession-mentions vs 5–18% in other months. |
| Apr 15–16 and May 10–16 logging loss | May absolute volumes are ~8× low; month trends read as composition only. |
| Truncation (~2.6% of lines, longest URLs) | Multi-field requests are under-counted, so the long tail of `go_*`, `cc_*`, `ft_*`, `xref_*` return fields is a floor. The tail is weighted up, not down. |
| Default-column credit is a floor; entry formats get the full entry | Stream's no-`fields=` share (~⅔) is read as "full entry" demand. |
| Single-entry GETs not measured; accessions endpoint surging (Jul 18%, Aug 43.5% of search volume) | Accession point-lookup demand is a floor, and August predicts better than the 8-week mean. |
| `req/query` ≈ 1 with very high `queries/client` = one pipeline walking a list | Month series were checked: `lit_author`, `length` and `uniref_cluster_90` filters collapse by ~50–100× after March (one-off pipelines); `go_p` returns and `xref` filters *rise* in Jul–Aug. Transient fields are discounted; rising ones are weighted up. |

Which lens matters: the lake's audience is programmatic and bulk. Programmatic search (94.9M requests, 15,212 clients), stream (3.78M programmatic requests, 3,670 clients) and programmatic accessions (1.40M requests fetching 51.3M entries, 888 clients) are the relevant populations. Website facet traffic is used only as a proxy for "what a person wanted".

---

## 1. Sort order — D5: `(reviewed DESC, taxid ASC, acc ASC)`, all five tables

**Current choice.** Swiss-Prot first, then organism, then accession; row-group min/max statistics are the only pruning mechanism. No partitioning, no bloom filters, no secondary index.

**What the evidence shows (query filter fields, all traffic, `query_filter_fields_usage.csv` / `_by_endpoint.csv` / `query_field_cooccurrence.csv`):**

| Filter field | n_distinct_queries | n_requests | clients | share of search requests | notes |
| --- | --- | --- | --- | --- | --- |
| `accession` | 10,413,562 | 16,663,439 | 16,374 | 13.3% | #1 by distinct queries and by clients; 99.9% equality |
| `reviewed` | 7,200,637 | 44,222,286 | 10,357 | 37.3% | #1 by requests; also the top facet (12.5M) |
| `organism_id` | 4,357,011 | 29,935,841 | 9,773 | 25.2% (stream 8.8%) | |
| `gene` + `gene_exact` | 2,909,949 + 2,020,213 | 21.6M + 15.7M | 5,310 + 4,337 | 18.3% + 13.3% | |
| `taxonomy_id` (lineage) | 2,370,362 | 6,067,202 | 5,411 | 4.9% (stream 7.8%) | not expressible in the lake, see §2 |
| `protein_name` | 2,344,536 | 15,466,618 | 2,578 | 13.1% | 38% phrase queries |

Top co-occurring filter pairs: `gene`+`organism_id` 13.4M, `organism_id`+`reviewed` 10.9M, `gene`+`reviewed` 10.1M, `protein_name`+`reviewed` 10.1M, `gene_exact`+`reviewed` 9.2M, `gene_exact`+`organism_id` 7.1M.

Point-lookup by accession beyond the `accession:` filter (`endpoint_usage.csv`, RUN-NOTES):

| Population | requests | entries fetched | entries / request |
| --- | --- | --- | --- |
| accessions, programmatic | 1,395,316 | 51,297,754 | 36.8 |
| accessions, website | 8,598,168 | 13,293,486 | 1.5 |
| single-entry GET `/uniprotkb/<acc>` | not measured | not measured | — |

**Verdict: SUPPORTED for keys 1–2; CHALLENGED on what key 3 fails to deliver.**

*Supported.* `reviewed` and `organism_id` are the two most-used equality predicates and, with `gene`, form every top co-occurrence pair. Sorting by `(reviewed, taxid)` makes "gene X in organism Y", "reviewed proteins of organism Y" and "everything for organism Y" contiguous row-group ranges, which is exactly the shape of the dominant interactive and bulk-scan pattern. `reviewed DESC` first also confines every `reviewed:true` query to the first ~6 row groups of `entries`. Nothing in the evidence argues for a different first or second key.

*Challenged.* Accession is the single broadest predicate (most distinct queries, most clients) and the fastest-growing access pattern, and the current layout cannot prune on it. `acc` is the third key: within a TrEMBL row group of 100k rows spanning many small taxids, the `acc` min/max is effectively `[A0A…, Z…]`, so an `acc = ?` predicate touches every row group of every file. A 37-accession batch (the programmatic mean) therefore scans the whole `acc` column of `entries` (~250M values) and, if it touches child tables, of `xrefs` (~5B values). Over httpfs this is the difference between a few range reads and a multi-GB download. The README's "predicate pushdown works automatically" is true for `reviewed`/`taxid` and false for `acc`.

*Alternative (R1) — keep the sort, add a point-lookup path.* Do not change the sort: any acc-first order destroys the organism locality that the top pattern depends on. Instead:

1. Write Parquet **bloom filters on `acc`** in all five tables (and on `xrefs.id`, see §2.3). DuckDB ≥ 1.2 consults them; they make `acc IN (…)` a per-row-group membership test with no data read.
2. If bloom filters are not available from the pinned writer, ship a sidecar **`acc_index`** table (`acc, table, file, row_group`) that `uniprot_parquet.py` resolves before issuing the read. This also gives S5 (secondary → primary map) a home.

Trade-off: bloom filters add roughly 120 KB per 100k distinct values at 1% false-positive rate (`entries`: ~300 MB total; child tables far less per row group because each acc repeats). A sidecar index adds a build step and a client dependency, but is engine-agnostic.

*Data-side checks before adopting R1:*

- **Prunability today:** from `parquet_metadata()` on the full `entries` and `xrefs` lakes, for 1,000 random accessions count row groups whose `[acc min, acc max]` contains the value. Expectation: ~all of them in TrEMBL files. This quantifies the problem.
- **Writer support:** confirm the pinned PyArrow (`environment.yml`: `pyarrow>=23.0,<24`) exposes bloom-filter writing on `ParquetWriter` (not verifiable in this sandbox; PyArrow is not installed here). If not, fall back to option 2 or to DuckDB's `COPY … (FORMAT PARQUET)` writer for the affected tables.
- **Distinct `acc` per row group** in each child table (sizes the filters).
- **Benchmark** a 37-accession default-column batch and a single-accession `protein_card` locally and over httpfs, before and after.

*Evidence-neutral:* within-accession secondary keys (`start_pos`, `database`, `comment_type` — S4) are not touched by the demand data; nothing in the logs is about ordering inside an entry.

---

## 2. Partitioning — currently none; O4/S8 proposes top-N `taxid=` Hive partitions for `xrefs` and `features`

### 2.1 Partition by organism (O4)

**Evidence:** organism filters as in §1 (`organism_id` 4.36M distinct / 9,773 clients; `taxonomy_id` 2.37M / 5,411; `model_organism` facet 11.7M website requests). Stream: `organism_id` 8.8%, `taxonomy_id` 7.8% of requests.

**Verdict: EVIDENCE-NEUTRAL.** The evidence confirms organism is a top predicate, which the sort already serves with row-group pruning. Hive partitioning adds *file-level* pruning on top, which only matters (a) for remote reads, where every file footer is otherwise fetched, and (b) for engines that ignore row-group statistics. The package contains **no organism values**, so it cannot say which taxids carry the demand or whether the top-20 cover "~80% of query volume" as O4 assumes; that number is not in this evidence.

*Checks required before O4 can be decided:* rows per `taxid` in `entries`, `xrefs`, `features` (skew, top-20 share of rows); number of distinct taxids (a full-cardinality partition is impossible — millions of tiny files); and organism *values* from the logs, which this package deliberately does not provide. Until then the sort is sufficient and O4 stays open.

*Cheap and unopposed by evidence (R2):* make the existing first key explicit as `reviewed=true/` and `reviewed=false/` directories. Zero cost (the data is already split there), and Spark/Polars/pandas users get file pruning on the most-requested filter without reading footers. No check needed beyond confirming manifest/`datapackage.json` handle the two-level path.

### 2.2 Lineage-based organism filtering (`taxonomy_id`) — gap, not a partitioning question

`taxonomy_id` (2.37M distinct queries, 5,411 clients; 7.8% of stream requests) matches any ancestor in the lineage. The lake's `lineage` is a list of *names* (as in the REST JSON), so this filter cannot be expressed. The `lineage_ids` return field (111,706 requests, 212 clients) shows users want the ids.

**Verdict: CHALLENGED (missing column, R3).** Add `lineage_taxids :: list<int32>` on `entries` if a taxonomy source is added to the build.

*Checks:* confirm `organism.lineage` in the source JSON carries no ids (expected); size an NCBI taxonomy join (~2.6M nodes) into the pipeline; estimate column size from mean lineage depth × 250M rows.

### 2.3 `xrefs` by database

**Evidence (return fields, `effective_return_fields_usage.csv`; filters, `query_filter_fields_usage.csv`):**

| Demand | n_requests | n_distinct_queries | clients |
| --- | --- | --- | --- |
| `go`, `go_p`, `go_c`, `go_f`, `go_id` (GO via xrefs) | 14,854,789 | — | up to 1,544 |
| `xref_pdb` | 1,301,084 | 526,183 | 1,433 |
| `xref_pfam`, `xref_kegg`, `xref_interpro`, `xref_ensembl`, `xref_alphafolddb`, `xref_geneid`, `xref_refseq`, `xref_proteomes` | 341k–674k each | | 423–997 each |
| all `xref_*` return fields | 6,575,187 | 2,904,517 | |
| `xref:` filter ("which entries have this xref id") | 4,536,707 | 3,403,591 | 1,764 (rising Jul–Aug: 1.98M, 1.14M vs ≤0.4M earlier) |
| `database:` filter | 313,503 | 109,659 | 1,406 |

**Verdict: CHALLENGED (mildly).** `xrefs` is sorted `(from_reviewed, taxid, acc)` with no database key, so "PDB ids for human" reads every human xref row of every database, and "which entry has xref id X" is a full scan of ~5B rows. Every measured way people ask for cross-references is *by database*.

*Alternative (R4):* Hive-partition `xrefs` by `database=` (top-N databases + `_other`), keeping `(from_reviewed, taxid, acc)` inside each partition; add a bloom filter on `id`. Trade-off: "all xrefs for one accession" (`protein_card`, batch hydration) then touches N partitions instead of one range — exactly the pattern §1 is trying to speed up, so R4 depends on R1 being in place.

*Checks:* rows per `database` (skew; expected EMBL/RefSeq/GO/… dominate, many databases tiny); distinct database count (~190); whether xrefs within an entry already arrive grouped by database in source order (if yes, a within-acc `database` sort is free and partly substitutes). Decide N from row counts, not from the logs.

### 2.4 `comments` by `comment_type`, `features` by `type`

**Evidence:** `cc_*` return fields total 12.3M requests (FUNCTION 2.86M / 2,708 clients; SUBCELLULAR LOCATION 2.78M / 2,612; DISEASE 1.32M; PATHWAY 619k; TISSUE SPECIFICITY 586k; CATALYTIC ACTIVITY 361k). `ft_*` total 13.5M (BINDING 1.41M; ACT_SITE 1.18M; DOMAIN 874k; TRANSMEM 857k / 1,504 clients; VAR_SEQ 850k; MUTAGEN 830k; SIGNAL 571k / 1,292 clients).

**Verdict: EVIDENCE-NEUTRAL.** Per-type access is real but these tables are 300M and 1.3B rows, the organism run is contiguous, and `type`/`comment_type` are cheap dictionary columns to filter in-run. Partitioning would help the "one type across all organisms" scan, which the evidence does not distinguish from per-organism access. Not recommended without a row-count skew check (Chain/Domain/Region likely dominate `features`).

---

## 3. Flattened convenience columns vs nested — per table

**Evidence, return-field demand bucketed by the lake location that serves it** (overall, all endpoints; field-mentions, `effective_return_fields_usage.csv`):

| Served by | n_requests | share | share excluding the 7 defaults |
| --- | --- | --- | --- |
| `entries` convenience columns | 701,162,443 | 93.7% | 67.9% |
| `features` | 13,488,799 | 1.8% | 9.2% |
| `comments` | 12,282,123 | 1.6% | 8.3% |
| `xrefs` — GO with aspect + term | 11,302,352 | 1.5% | 7.7% |
| `xrefs` — per-database ids | 6,575,187 | 0.9% | 4.5% |
| `publications` | 2,036,211 | 0.3% | 1.4% |

Per endpoint (`effective_return_fields_by_endpoint.csv`): the 7 default columns plus `sequence` and `organism_id` are the top 9 on search (83%→30% of requests), the top 7 on stream (97%→72%), and the top 7 on accessions (100%→98%). After rank 9 on search there is a cliff to 5.1% (`go_p`).

### 3.1 `entries` — **SUPPORTED**, with five evidence-backed additions

The hybrid design is validated: 94% of field-mentions resolve to an existing convenience column, and every top-20 return-field pair is among `accession, protein_name, gene_names, organism_name, length, id, reviewed, sequence, organism_id`. Nothing in the evidence argues for moving any current convenience column back into nested.

Gaps, ranked by breadth (clients) and trend, each with its verdict and check:

| ID | Field(s) demanded | Evidence | Current lake | Verdict | Check before adopting |
| --- | --- | --- | --- | --- | --- |
| **R5** | `go_p` 6.50M req / 1,544 cl (rising: 27% of accession-mentions in Aug); `go_id` 3.38M / 1,122; `go` 1.64M; `go_c` 1.59M; `go_f` 1.58M; stream 12% each | GO is 7.7% of non-default demand | `go_ids` (ids only); aspect + term text only in `xrefs.properties` (`GoTerm` = `P:…`) | **CHALLENGED** — add `go_terms :: list<struct{id, aspect, term}>` or `go_p`/`go_c`/`go_f` lists; source already has everything | GO xref null-rate and mean count by `reviewed` (TrEMBL coverage via automatic annotation); column bytes on the A12 slice |
| **R6** | `lit_pubmed_id` 1.98M / **6,052 cl** (4th-broadest non-default; website 5,703) | | `publications` join | **CHALLENGED** — add `pubmed_ids :: list<string>` on `entries` | Share of entries with ≥1 PubMed citation by `reviewed`; p99 count (SARS-CoV-2 replicase-class entries); bytes |
| **R7** | `proteome` filter 1.51M req / 579k dq / 1,771 cl, **17.8% of stream requests** (#2 stream filter); `xref_proteomes` 341k / 423; `proteomecomponent` 36k | Stream is the lake's audience | `xrefs` where `database='Proteomes'` | **CHALLENGED** — S11 (`proteome_ids` list, with component) moves from "pending survey" to evidence-backed | Distinct proteome ids; entries per proteome; null-rate in TrEMBL |
| **R8** | `cc_function` 2.86M / 2,708 cl; `cc_subcellular_location` 2.78M / 2,612 | comments = 8.3% of non-default | `comments` join; `text_value` | **CHALLENGED (mild)** — add `function_text` and `subcellular_locations :: list<string>` on `entries` | FUNCTION/SUBCELLULAR LOCATION coverage by `reviewed` (ARBA/UniRule make TrEMBL non-trivial); bytes vs `entries` total; adopt only if coverage is material and size < ~5% |
| **S1** | `gene_primary` 2.94M / 3,184 cl | | view-level `gene_names[1]` | **SUPPORTED** (already no-regrets) | none |
| **R3** | `lineage_ids` 112k / 212 cl; `taxonomy_id` filter | see §2.2 | names only | **CHALLENGED** | needs taxonomy source |

Confirmed as correctly flattened by the evidence (no action): `protein_existence` (6,498 clients), `annotation_score` (6,351), `keyword` (6,941), `feature_count` (5,832), `comment_count` (5,784), `mass` (947), `lineage` (967), `fragment` → `protein_flag`, `sequence_version`, `uniparc_id`, `ec` (1,597). `structure_3d` (5,889 clients) is derivable from `xref_dbs` with `list_contains`; a documented example or a `has_structure` boolean is judgement, not evidence. S6 (`annotation_score` as tinyint, `is_high_confidence`) is consistent with 3.0M `annotation_score` filter requests but not required by them.

Not servable from the UniProtKB JSON at all, so out of scope for this lake: `uniref_cluster_100/90/50` filters (2.67M / 2.00M / 1.49M requests; `cluster_100` from 4,708 clients — broad), `computational_pubmed_id` / `community_pubmed_id` (1.86M / 1.03M distinct queries). Worth recording so the gap is not mistaken for a layout problem.

### 3.2 `features` — **SUPPORTED**

`ft_*` return fields (13.5M) are answered entirely by the flattened `type, start_pos, end_pos, description, feature_id, evidence_codes, ligand_*` columns; the nested `feature` column is only needed for evidence sources and cross-references, which no return field in the top 90 asks for. The specific types demanded (BINDING, ACT_SITE, DOMAIN, TRANSMEM, VAR_SEQ, MUTAGEN, SIGNAL, TOPO_DOM, REGION, SITE) are all covered by the same columns.

### 3.3 `xrefs` — **SUPPORTED** on columns; layout challenged in §2.3

The evidence answers half of S9's open question ("which databases do users filter on"): PDB, Pfam, KEGG, InterPro, Ensembl, AlphaFoldDB, GeneID, RefSeq, Proteomes, Reactome, EMBL, eggNOG in that order. For those, `id` is what is asked; `properties` (method/resolution/transcript) is secondary. Typed convenience columns per database (one S9 option) are not supported by the evidence; the demand is for ids by database, which the generic layout already holds.

### 3.4 `comments` — **EVIDENCE-NEUTRAL** on representation (O5/S2/S12)

The demand is by type (FUNCTION, SUBCELLULAR LOCATION, DISEASE, PATHWAY, TISSUE SPECIFICITY, CATALYTIC ACTIVITY, PTM, INTERACTION, COFACTOR, ALTERNATIVE PRODUCTS). That validates `comment_type` as a first-class column and `text_value` for the text types, and it validates promoting the top two to `entries` (R8). It says nothing about VARCHAR-JSON vs typed struct. S3 (`isoforms` table) gets modest support: `cc_alternative_products` 176k / 564 clients, `ft_var_seq` 850k / 353, `is_isoform` 14k.

### 3.5 `publications` — **SUPPORTED**; add R6

`lit_pubmed_id` is the only publications field with broad demand and is best served from `entries` (R6). `lit_author` (2.75M distinct queries) looks large but is a Jan–Mar pipeline (1.2M/month → 16k in Aug); `authors :: list<string>` with `list_contains` is adequate. `lit_citation_id`, `lit_doi_id` are covered.

---

## 4. Column grouping / co-location within `entries`

**Current choice.** Column order in `_build_entries_sql`: `acc, id, reviewed, secondary_accs, taxid, organism_name, organism_common, lineage, gene_names, gene_synonyms, protein_name, alt_protein_names, protein_flag, ec_numbers, protein_existence, annotation_score, sequence, seq_length, …`, nested structs last. Row groups of 100,000 rows, ~256 MB files, zstd.

**Evidence.** The 7 default columns are requested together on 31.6M requests (`default_return_fields_usage.csv`), are 98–100% of accessions-endpoint requests and 72–97% of stream requests; `sequence` joins them on 37.8M requests (4,592 clients, scraper-inflated but still broad); `organism_id` on 35.9M (2,431 clients). Every top-20 return-field pair (`return_field_cooccurrence.csv`) is drawn from these nine.

**Verdict: EVIDENCE-NEUTRAL on correctness, weakly SUPPORTED for a reorder (R9).** Parquet column pruning already means only requested column chunks are read, so no query is wrong or slow locally because of column order. Order matters only for remote reads: DuckDB coalesces *adjacent* byte ranges, so a default-column read against a row group where those seven chunks are interleaved with `secondary_accs`, `lineage`, `gene_synonyms`, `alt_protein_names` issues more range requests than it needs to.

*Alternative:* emit the nine hot columns contiguously and first — `acc, id, reviewed, taxid, organism_name, gene_names, protein_name, seq_length, sequence` — then the remaining convenience columns, then nested. Cost: none (schema order only; no sort or size change). Interaction with R1: the batch point-lookup cost is one row group of each requested column per hit; at 100k rows that is a few MB for the seven defaults and tens of MB if `sequence` is included, which is acceptable but worth measuring.

*Checks:* column-chunk byte offsets per row group before/after (from `parquet_metadata()`); count of HTTP range requests for a default-column query over httpfs (`EXPLAIN ANALYZE`); and whether 100k-row groups are the right size for `entries` once R1 makes point lookups cheap (a smaller row group trades footer size and compression for less over-read per hit).

---

## 5. Does the access-pattern split change any layout assumption?

| Pattern | Evidence | Layout assumption | Changed? |
| --- | --- | --- | --- |
| **Interactive query** (`search`) | 117.4M requests; filters `reviewed`/`organism_id`/`gene`/`accession`; returns the defaults | Sort on `(reviewed, taxid)` + convenience columns | **No.** This is the pattern the schema was designed for and the evidence confirms it. |
| **Bulk scan** (`stream`) | 3.9M requests, 5,386 clients, steady; ~⅔ full-entry exports; filters `accession` 26.7%, `proteome` 17.8%, `reviewed` 11.2%, `uniref_cluster_90` 10.5% (49 clients, transient), `organism_id` 8.8%, `xref` 8.4%, `taxonomy_id` 7.8% | Lossless nested layer + `sorted.jsonl.zst` | **Partly.** Full-entry demand validates the nested layer and the JSONL artifact. But stream's filters are the ones the lake cannot express (`proteome`, `taxonomy_id`, `xref` by id). Because stream users *are* the lake's audience, R3/R4/R7 carry more weight than their all-traffic rank suggests. |
| **Batch point-lookup** (`accessions`) | 10.0M requests / 64.6M entries; programmatic 36.8 accs per request; 98–100% default columns; surging (Aug 43.5% of search volume, 86% browser-origin); single-entry GETs unmeasured on top | "Row-group min/max means predicate pushdown works automatically" | **Yes.** True for `reviewed`/`taxid`, false for `acc`. This is the one assumption the evidence overturns; R1 (bloom filters or an accession index) is the response. The website share of the surge is a frontend change and not lake demand, but the programmatic 51.3M entries in 8 weeks are. |

Formats (`format_usage.csv`) are consistent with the above: json 75.2M, none 29.4M, fasta 13.4M (1.67M distinct queries), tsv 11.5M. FASTA export needs `acc, id, protein_name, organism_name, gene_names, protein_existence, seq_version, sequence`, all present on `entries`.

---

## 6. Decisions the evidence does not touch

D1 (latest-only), D2 (no VARIANT), D3 (dedup phases), O3/S10 (naming), S2 (JSON logical type), S4 (within-acc order), S7 (hash verification), 256 MB file target, zstd. None of these is a demand question; nothing here should be read as bearing on them.

---

## 7. Consolidated: what to run against the real lake

None of these were run here: the only lake in the repo is the 5,378-entry demo, which is five model organisms and useless for cardinality. Each check is the gate for the item it names.

```sql
-- R1: can acc predicates prune today? (expect ~0 row groups skipped in TrEMBL)
SELECT count(*) AS row_groups,
       count(*) FILTER (WHERE stats_min <= 'Q9XYZ1' AND stats_max >= 'Q9XYZ1') AS candidate_groups
FROM parquet_metadata('lake/entries/*.parquet') WHERE path_in_schema = 'acc';

-- R1: distinct acc per row group in child tables (sizes bloom filters)
SELECT file_name, row_group_id, stats_distinct_count
FROM parquet_metadata('lake/xrefs/*.parquet') WHERE path_in_schema = 'acc' LIMIT 20;

-- O4: taxid skew
SELECT taxid, count(*) AS n FROM entries GROUP BY 1 ORDER BY n DESC LIMIT 50;
SELECT count(DISTINCT taxid) FROM entries;

-- R4: xrefs database skew
SELECT database, count(*) AS n FROM xrefs GROUP BY 1 ORDER BY n DESC;

-- R5: GO coverage
SELECT reviewed, avg(len(go_ids)) AS mean_go, avg(CASE WHEN len(go_ids)=0 THEN 1 ELSE 0 END) AS null_rate
FROM entries GROUP BY 1;

-- R6: PubMed coverage
SELECT e.reviewed, count(DISTINCT p.acc) * 1.0 / count(DISTINCT e.acc) AS share_with_pubmed
FROM entries e LEFT JOIN publications p
  ON p.acc = e.acc AND list_contains(list_transform(p.citation_xrefs, x -> x.database), 'PubMed')
GROUP BY 1;

-- R7: proteome coverage and cardinality
SELECT from_reviewed, count(DISTINCT acc) AS entries_with_proteome, count(DISTINCT id) AS proteomes
FROM xrefs WHERE database = 'Proteomes' GROUP BY 1;

-- R8: FUNCTION / SUBCELLULAR LOCATION coverage and bytes
SELECT comment_type, from_reviewed, count(DISTINCT acc) AS entries, sum(strlen(text_value)) AS bytes
FROM comments WHERE comment_type IN ('FUNCTION', 'SUBCELLULAR LOCATION') GROUP BY 1, 2;
```

Plus, outside SQL: confirm bloom-filter writing in the pinned PyArrow; benchmark a 37-accession default-column batch locally and over httpfs before/after R1 and R9; and obtain organism *values* from the log producers if O4 is to be decided at all.

---

## 8. Summary of verdicts

| Decision | Verdict | Action |
| --- | --- | --- |
| Sort `(reviewed, taxid, acc)` — keys 1–2 | SUPPORTED | keep |
| Sort — accession point lookup | CHALLENGED | **R1** bloom filters on `acc` (all tables) or sidecar `acc_index`; gate on prunability + writer-support checks |
| No partitioning (O4 top-N taxid) | EVIDENCE-NEUTRAL | keep open; needs taxid skew + organism values; **R2** explicit `reviewed=` dirs is free |
| Lineage filtering | CHALLENGED (gap) | **R3** `lineage_taxids`; needs taxonomy source |
| `xrefs` layout | CHALLENGED (mild) | **R4** partition by database + bloom on `id`, after R1; gate on database skew |
| `entries` hybrid | SUPPORTED | add **R5** GO with aspect/term, **R6** `pubmed_ids`, **R7** `proteome_ids` (S11 promoted), **R8** function/subcellular text (size-gated); S1 confirmed |
| `features`, `publications` flattening | SUPPORTED | none |
| `comments` representation | EVIDENCE-NEUTRAL | O5/S2/S12 unchanged; S3 modestly supported |
| Column order in `entries` | weakly SUPPORTED to reorder | **R9** hot nine columns first; measure range requests |
| D1, D2, D3, S4, S7, naming | EVIDENCE-NEUTRAL | none |

The schema's central bets — star layout, organism-major sort, denormalised keys on child tables, and a wide convenience layer on `entries` — are all confirmed by the demand data. The one assumption the data overturns is that row-group statistics serve accession lookups; everything else is additive.
