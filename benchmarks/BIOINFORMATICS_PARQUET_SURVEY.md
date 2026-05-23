# Parquet Schema Design in Bioinformatics: A Survey of Major Data Distributions

**Research Report on Columnar Data Distribution Practices**  
*Focus: Schema design, normalization, nested data handling, and metadata documentation*

---

## Executive Summary

A survey of major bioinformatics and life-science data projects reveals several converging patterns in Parquet schema design, alongside significant diversity in approach. The most important findings:

1. **Normalization vs. Denormalization**: Projects diverge sharply. Some (notably Open Targets) maintain **separate normalized tables** with explicit join keys (targetId, diseaseId), requiring users to join multiple Parquet files. Others (23andMe, ADAM, genomics pipelines) favor **single denormalized "wide" tables** for analytical speed, accepting redundancy. Neither approach dominates; choice depends on query patterns and team maturity with Spark/duckdb.

2. **Nested Data Handling**: Most projects preserve **nested structures** (arrays of structs, nested fields) in Parquet rather than flattening. Parquet's native support for structs, arrays, and definition/repetition levels makes this practical. Open Targets preserves clinical significances as arrays; gnomAD's Hail MatrixTables embed complex genotype annotations. Some projects provide both: flattened TSV/CSV for simple access, nested Parquet for power users.

3. **Schema Documentation**: Very few projects publish explicit, comprehensive schema documentation. Open Targets recommends users run `spark.read.parquet(...).printSchema()` or inspect Parquet metadata directly. Ensembl's SQL-to-Parquet pipeline requires explicit PyArrow schema definitions per table. No project explicitly documents rationale for schema choices (a gap).

4. **Naming Conventions**: **Snake_case dominates** in columnar data (SQL tradition). Open Targets uses snake_case with singular table names (e.g., `known_drug`, `target`). No project uses camelCase for Parquet columns; those projects tend to preserve source format (JSON APIs may use camelCase, but Parquet exports revert to snake_case).

5. **Boolean Naming**: Inconsistent. Best practice observed: `is_*` or `has_*` prefix (e.g., `is_reviewed`, `has_clinical_significance`). No project enforces this uniformly.

6. **Versioning & Schema Migration**: Most projects **lack explicit published versioning policies**. Open Targets moved to Parquet-only in v25.03 (breaking change). Formal schema evolution (adding nullable fields, marking old fields deprecated) is not documented publicly. Best-practice guidance exists (e.g., Merapar's S3 migration case study), but adoption is unclear.

7. **Losslessness**: **Rarely verified or claimed**. Projects that convert from XML/JSON (UniProtKB, InterPro, Ensembl) typically state conversions are "best-effort" or performed to maintain query compatibility, not lossless. Exceptions: ADAM and gnomAD explicitly preserve all VCF/GVCF fields via Hail's schema system.

8. **Residual Preservation**: **Not common practice**. Most projects extract typed columns and discard source format. No widespread adoption of "preserve original JSON/XML in a _raw column" pattern. This limits round-trip validation.

9. **Convenience Columns & Pre-Aggregates**: Projects vary. Open Targets provides summary association tables alongside evidence tables. Genomic projects (gnomAD, ADAM) compute variant-level QC flags and allele frequencies directly. No standardized approach.

10. **Client Libraries & SQL Views**: Most projects recommend **Spark, duckdb, or pandas/Arrow** for querying. Some (Open Targets) ship Python/R examples; few ship pre-baked SQL views or Spark UDFs. gnomAD provides Hail methods; ADAM provides Scala/Java bindings.

---

## Project-by-Project Findings

### 1. **Open Targets Platform** ⭐ (Highest relevance)

**Formats Published**: Parquet only (as of v25.03 release; JSONL discontinued)

**Schema Design**:
- **Normalized** with multiple files: `target`, `disease`, `evidence`, `known_drug`, `association_by_datasource_direct`, `association_by_datasource_indirect`, `association_by_similarity`
- Explicit join keys: `targetId` (Ensembl gene), `diseaseId` (EFO ontology), `studyId`
- Central evidence table contains extensive nested fields: `clinicalSignificances` (array of structs), `urls` (array with `niceName`, `url`), `synonyms`, and source-specific metadata

**Nested vs. Flat**: 
- Deliberately **preserves nesting**: "Dealing with nested information can sometimes be tedious. The Platform aims to minimise the nestiness of the data, however some level of structure is sometimes required."
- Evidence table requires users to `explode()` arrays in Spark to flatten
- Provides separate child tables (e.g., studies joined via `studyId`)

**Handling Deeply Nested Data**: 
- Parquet natively encodes arrays of structs (e.g., `clinicalSignificances`)
- Recommends Spark's `explode()` and `lateral_view` for analysis
- Python/R examples show unnesting workflows

**Convenience Columns**: 
- Summary association tables (`association_by_datasource_direct`) pre-compute target-disease pairs with aggregated evidence counts
- Reduces need for GROUP BY in common queries

**Naming Convention**: **snake_case, singular table names** (post-v25.03)

**Schema Documentation**: 
- Recommends inspecting with `printSchema()` 
- Limited formal documentation; schema described as "the best way to explore"
- GraphQL API schema available but not Parquet-specific

**Versioning Policy**: Explicit breaking change in v25.03 (dropped JSONL, restructured some fields). No published migration guide for old JSON users.

**Losslessness**: Not claimed. GraphQL API and Parquet may differ (schema designed for API-Parquet parity, but not guaranteed round-trip with original source data).

---

### 2. **ChEMBL**

**Formats Published**: TSV bulk downloads, PostgreSQL/Oracle databases, REST API JSON, **nascent Parquet** (mentioned in blog as "new collection of core files")

**Schema Design**: 
- Primarily **relational**: compounds, patents, compound-patent relationships maintained as separate tables
- Parquet exports mirror relational model: one file for compounds, one for patents, one for relationships
- Avoids duplication of compound metadata across files (normalized approach)

**Nested vs. Flat**: 
- Flattened; relationships expressed via compound IDs, not nested structures

**Handling Polymorphic Data**: 
- Compounds have heterogeneous properties (SMILES, InChI, descriptors)
- Parquet schema accommodates optional fields; polymorphic activity types handled via separate tables or TYPE column

**Naming Convention**: snake_case (inherits from SQL schema)

**Losslessness**: Not explicitly addressed; Parquet conversion is recent and underdocumented

**Status**: Parquet offering is very recent (2025); full documentation likely evolving.

---

### 3. **gnomAD** (via Hail)

**Formats Published**: VCF, TSV summary tables, **Hail native format** (can export to Parquet), hosted on Google Cloud/DNAnexus

**Schema Design**: 
- **MatrixTable**: 2D sparse matrix with variants (rows), samples (columns), genotypes (entries)
- Schema includes:
  - **Row fields**: variant annotation (AC, AF, allele frequency, etc.)
  - **Column fields**: sample metadata (population, sex, etc.)
  - **Entry fields**: genotype (GT), depth (DP), GQ, etc.
  - **Global fields**: dataset metadata
- ~250M variants, ~5 trillion unique genotypes; dense matrices infeasible

**Nested vs. Flat**: 
- MatrixTable allows **nested annotations**: VEP consequence predictions (struct with transcript, impact, SIFT, etc. nested)
- Reference blocks (gVCF) stored separately in VariantDataset (split representation)
- When exported to Parquet, maintains nesting

**Convenience Columns**: 
- Pre-computed `af` (allele frequency), `ac` (allele count), gnomAD-specific QC flags
- Aggregated across subpopulations

**Naming Convention**: snake_case (inherits from VCF/INFO field conventions)

**Schema Documentation**: 
- Extensive: `gnomad_genome_sites`, `gnomad_genome_variants` schemas published with field descriptions
- Hail method documentation with Python examples

**Versioning**: gnomAD 4.0 is latest; schema relatively stable but fields added/modified per release

**Losslessness**: High fidelity to source VCF/GVCF, though some INFO fields may be omitted for efficiency

---

### 4. **ADAM (bdgenomics)**

**Formats Published**: Parquet native, can import/export SAM/BAM, VCF, BED, GFF3

**Schema Design**: 
- **Tidy data** principle: one observation per row (one read, one variant, one genotype)
- Core schemas: `AlignmentRecord` (reads), `Variant`, `Genotype` (co-designed for lossless VCF-to-Parquet conversion)
- Variant schema: `contig`, `start`, `end`, `referenceAllele`, `alternateAlleles` (array), `annotations` (nested struct with dbSNP ID, VEP, effect, impact, etc.)

**Nested vs. Flat**: 
- **Preserves nesting**: alternateAlleles as array, annotations as struct
- Tidy data layout allows single-pass iteration over all genotypes without joins

**Handling Nested Data**: 
- VEP consequences stored as array of structs (one consequence per transcript)
- INFO fields represented as Map[String, String] in schema

**Metadata & Header Integration**: 
- Parquet metadata includes VCF header lines (contigs, INFO/FORMAT descriptions)
- VCF header reconstructed from metadata during export

**Naming Convention**: camelCase (Java/Scala convention), though Parquet column names are snake_case after conversion

**Losslessness**: **Explicitly designed for losslessness**: VCF → ADAM Parquet → VCF round-trip preserves all fields (metadata and header embedded)

**Schema Documentation**: 
- Formal Avro schema definitions in GitHub
- Scala/Python API documentation with examples

---

### 5. **Ensembl (SQL-to-Parquet Pipeline)**

**Formats Published**: FTP (various formats: GFF3, GTF, Fasta), REST API JSON, **SQL-to-Parquet pipeline** (Nextflow)

**Schema Design**: 
- Ensembl's `nf-sql-to-parquet` pipeline requires explicit **PyArrow schema definitions** per query
- Example: gene table with `gene_id`, `description`, `biotype`, `coordinates` (struct with contig, start, end, strand)
- Separate tables for genes, transcripts, exons, proteins (normalized)

**Nested vs. Flat**: 
- Hybrid: coordinates, metadata structs for complex attributes; IDs for relationships
- Transcript table has nested array of exons

**Naming Convention**: snake_case (SQL convention)

**Schema Documentation**: 
- Requires `Data_type` schema class in Python (explicit Pyarrow schema)
- JSON config per query specifying main_sql, supplementary_data
- No high-level documentation of "best practices"

**Versioning**: Ensembl releases ~98 per year; schema versioning tied to release numbers; old data accessible via release-specific URLs

**Status**: Pipeline is internal/Nextflow; not widely adopted; documentation limited to GitHub README

---

### 6. **Human Cell Atlas**

**Formats Published**: Multiple: HDF5 (`.h5ad` Anndata), Parquet, Arrow, JSON metadata

**Schema Design**: 
- Metadata: JSON-LD based on custom HCA JSON schemas (Tier 1, Tier 2, cell annotation layers)
- Parquet used for **expression matrices**: sparse, row-major format (genes × samples)
- Tidy data: one row per gene-sample pair (like ADAM)

**Nested vs. Flat**: 
- Metadata (JSON): nested ontology references, technology, protocol details preserved as nested objects
- Expression data: denormalized wide table or tall tidy table depending on use case

**Naming Convention**: snake_case for both JSON keys and Parquet columns

**Schema Documentation**: 
- Extensive JSON schema metadata repository (GitHub: `metadata-schema`)
- Tier 1/2/3 partitioning; each tier documented separately
- No explicit "design rationale" published

**Losslessness**: Best-effort; JSON schemas validated against metadata; no claim of round-trip losslessness for expression data (compression/filtering may occur)

---

### 7. **AlphaFold DB**

**Formats Published**: PDB, mmCIF, BinaryCIF, **no Parquet** (uses cloud-native: BigQuery, Cloud Storage)

**Schema Design**: 
- Metadata on BigQuery: protein_id, organism, sequence_length, pae_min_plddt, etc.
- Structure files: PDB/mmCIF (hierarchical: models → chains → residues → atoms)

**Note**: **Not a Parquet adopter**. Uses Google Cloud native formats. Included for comparison: shows genomics/structural biology may prefer specialized formats (PDB, mmCIF) over columnar.

---

### 8. **InterPro**

**Formats Published**: TSV (InterProScan output), XML (rich annotations), JSON, GFF3, **no Parquet**

**Schema Design**: 
- Primary: XML nested structure (entry → signature → methods → proteins)
- TSV: flattened (protein_id, interpro_id, interpro_name, evalue, date)

**Nested vs. Flat**: 
- XML: rich nesting; TSV: flattened for simple queries

**Note**: **Not a Parquet adopter** (yet). Conversion to Parquet would require schema design (question for future work).

---

### 9. **Reactome**

**Formats Published**: TSV (reactions, pathways, proteins), Neo4j (GraphDB), BioPAX, SBML, MySQL dumps, **no Parquet**

**Schema Design**: 
- Neo4j: nodes (proteins, reactions, pathways), edges (participates_in, regulates, etc.)
- TSV: protein_id, reaction_id, pathway_id, role (catalyzer, substrate, product)

**Note**: Graph-first approach; Parquet less suitable than Neo4j for this use case (relationships central).

---

### 10. **DisGeNET**

**Formats Published**: TSV (gene-disease, variant-disease associations), SQLite, RDF/Turtle, JSON, XML, **no Parquet**

**Schema Design**: 
- TSV (flat): gene_id, disease_id, association_score, pmid, source
- SQLite: normalized with gene, disease, evidence tables

**Note**: Not a Parquet adopter; TSV sufficient for use cases.

---

### 11. **STRING**

**Formats Published**: TSV (protein interactions), JSON, **no Parquet**

**Schema Design**: 
- TSV: protein_id_a, protein_id_b, combined_score, neighborhood, fusion, etc.
- One row per interaction

**Note**: Not a Parquet adopter; TSV widely used in bioinformatics.

---

### 12. **23andMe Genetic Datastore**

**Formats Published**: Proprietary; described in engineering blog as **Parquet-based**

**Schema Design**: 
- **Transposed VCF**: rows = samples, columns = genetic markers
- Columnar organization optimized for "retrieve all samples for marker" queries
- Achieved 80x speedup over indexed VCF for marker-centric queries

**Nested vs. Flat**: Flat; genotype call and metadata per cell

**Naming Convention**: Internal; not publicly detailed

**Losslessness**: Best-effort (designed for performance, not round-trip losslessness)

---

### 13. **NCBI / GenBank / RefSeq**

**Formats Published**: FASTA, GenBank flat file, GFF3, Entrez XML, **no Parquet**

**Schema Design**: Flat file formats designed for sequential parsing, not analytics

**Note**: Legacy formats; Parquet adoption unlikely given institutional investment in flat files

---

### 14. **ClinVar**

**Formats Published**: VCF, TSV (flattened), XML, **no native Parquet** (but converted by third parties on AWS)

**Schema Design**: 
- VCF: one row per variant-phenotype combination
- TSV: variant_id, condition_id, clinical_significance, submitter_count, review_status

**Note**: Conversions to Parquet exist (e.g., AWS registry of open data), but not officially distributed

---

---

## Specific Answers to Research Questions

### A. Open Targets Schema in Detail

**How did they design their schema?**

Open Targets maintains **separate Parquet files** (not a monolithic table):
- `target` — Ensembl gene metadata (id, symbol, class, synonyms)
- `disease` — EFO disease/phenotype (id, name, ancestors)
- `evidence` — target-disease evidence (hundreds of columns with nested arrays)
- `known_drug` — drug information (nested URLs, synonyms)
- `association_by_datasource_direct/indirect` — pre-computed aggregated associations

**Join Keys**: `targetId`, `diseaseId`, `studyId`

**Nested vs. Flat**: 
- Evidence table deliberately preserves nesting: `clinicalSignificances` (array), `urls` (array of structs), `synonyms` (struct with hasExactSynonym, hasBroadSynonym)
- Rationale: avoid explosion of rows from multiple evidence lines per target-disease pair; use explode() on demand

**Handling Cross-References**: 
- IDs (targetId, diseaseId) are the join mechanism
- No embedded full records (avoiding duplication)

**Breaking Changes**: v25.03 dropped JSONL format entirely. No migration guide published for users switching from old JSON exports.

---

### B. ChEMBL Analytical vs. SQL Schema

**Comparison:**

| Aspect | SQL Schema | Parquet Offering |
|--------|-----------|------------------|
| **Normalization** | Highly normalized (20+ tables) | Maps to relational (compounds, patents, relationships as separate files) |
| **Duplication** | Minimized | Preserved where needed for analytical speed |
| **Cross-DB Queries** | Designed for (foreign keys) | Requires explicit joins on compound_id |
| **Update Overhead** | Low (single updates ripple) | Higher (but batch updates acceptable) |

ChEMBL's Parquet layout **mirrors** the SQL schema rather than denormalizing. This suggests they prioritize consistency with existing tooling over analytic speed.

---

### C. Explicit Schema Design Rationales

**Found:**
- 23andMe blog post: "80x speedup over VCF indexing via sample-major layout" — optimization rationale explicit
- Databricks blog (2016): "Columnar storage contiguous bytes for marker lookups" — performance rationale
- ADAM paper (UC Berkeley tech report): "Tidy data + embedded metadata for lossless VCF conversion" — design principle clear

**Not Found:**
- "Why we chose denormalized over normalized" — no project articulates trade-off decision
- "How we handle polymorphic types" — handled ad-hoc (optional fields, separate tables, or Map types)
- "Schema evolution strategy" — Open Targets only example (breaking change in v25.03)

**Conclusion**: Best practices exist but are rarely documented. Projects publish schemas and provide examples, but design decisions are implicit.

---

### D. Primary Query Table vs. Child Tables

**Patterns Observed:**

1. **Multiple Independent Tables** (Open Targets):
   - Primary: `evidence` (or `association_*` aggregates)
   - Child: `target`, `disease`, `known_drug`
   - Users must join; no "do it all" denormalized table

2. **Single Tidy Table** (ADAM, HCA gene expression):
   - Primary: one table (e.g., `genotypes`) with all relevant columns
   - Joins pre-computed; tidy data layout (one observation per row)

3. **Hybrid** (gnomAD MatrixTable):
   - Primary: variant annotations (row table)
   - Child: sample metadata (column table)
   - Entries (genotypes) as entry fields
   - Requires understanding of MatrixTable structure to query

**Conclusion**: No dominant pattern. **Normalized + join keys** works for complex data with many-to-many relationships (Open Targets, ChEMBL). **Denormalized tidy tables** work for observation-centric data (genomics reads, expression counts).

---

### E. Residual Columns and Full Original JSON/XML Preservation

**Finding**: **Rarely practiced** in public distributions.

**Exceptions**:
- ADAM: VCF metadata + header preserved in Parquet metadata (but not as a residual column)
- Hail: All VCF INFO fields embedded in schema; no separate _raw column

**Not Found**:
- No project ships a `_original_json` or `_raw_xml` column alongside extracted typed columns
- This limits ability to debug schema extraction and verify losslessness

**Recommendation for UniProtKB**: Consider preserving XML source in metadata or as optional column for validation.

---

### F. Parquet Naming Conventions

**Snake_case Dominance**: 
- Open Targets (v25.03): `known_drug`, `target`, `clinical_significances`
- ADAM: internally camelCase (Java), but Parquet columns exported as snake_case
- Ensembl: snake_case (SQL tradition)
- ChEMBL: snake_case (SQL tradition)

**No Exceptions**: No major project uses camelCase for Parquet column names, even if source (JSON API) uses camelCase.

**Boolean Naming**:
- Open Targets: `is_therapeutic_conflict`, `has_synononym` (inconsistent prefix use)
- ADAM: no explicit boolean naming convention in published schemas
- Best practice (not universally followed): `is_` or `has_` prefix

**Join Key Naming**:
- Open Targets: `targetId`, `diseaseId` (camelCase for IDs, inconsistent!)
- ADAM: `variant_id`, `sample_id` (snake_case)
- Conclusion: Inconsistency even within projects

---

## Recommendations for UniProtKB Parquet Distribution

### 1. **Choose a Normalization Strategy** (Critical Decision)

**Option A: Fully Denormalized (One Wide Table)**
- Rows: proteins
- Columns: all sequence, feature, annotation data
- Pros: Simple queries, no joins required, single file
- Cons: Wide schema (100+ columns), redundancy, harder to version individual field types

**Option B: Normalized (Multiple Tables with Join Keys)**
- Primary table: `protein` (accession, sequence, taxonomy)
- Child tables: `protein_feature`, `protein_interaction`, `protein_comment`, `protein_xref`, `protein_literature`
- Pros: Modular, versioning per table, reduces redundancy, mirrors SQL schema
- Cons: Users must join, more complex queries, multiple files to manage

**Option C: Hybrid (Recommended)**
- Primary table: `protein` (denormalized essentials: accession, sequence, organism, length)
- Optional nested fields: `features` (array of structs), `comments` (array of structs), `cross_references` (array of structs)
- Child tables: full `feature`, `cross_reference` (for users needing detailed access)
- Pros: Simple queries for common case; full access for power users; nesting transparent in Parquet
- Cons: Schema complexity, nested array explosion risk

**Recommendation**: Hybrid approach. Preserve nested structures in primary table (Parquet handles well); provide separate tables for common child entities (features, xrefs).

---

### 2. **Define Naming Conventions**

- **Columns**: snake_case, singular nouns preferred (e.g., `accession`, `sequence`, `feature_type`, not `features`)
- **Booleans**: `is_*` or `has_*` (e.g., `is_reviewed`, `has_cross_reference`)
- **Join Keys**: Explicit suffix `_id` (e.g., `protein_id`, `feature_id`)
- **Arrays/Nested**: Plural for arrays in nested context (e.g., `features: [array of struct]`), singular in flat context (e.g., separate `feature` table)

**Enforce consistency in schema documentation**.

---

### 3. **Handle Nested Data Intentionally**

**Options**:

1. **Flatten Completely**: All nested into separate tables (simplest for users, more files)
2. **Preserve Nesting in Primary Table**: Features, comments, xrefs as arrays (advanced users need explode())
3. **Provide Both**: Nested primary table + flattened child tables

**Recommendation**: Option 3. Nested primary for round-trip losslessness; flattened children for accessibility.

**Example nested schema**:
```
protein {
  accession: STRING
  sequence: STRING
  features: ARRAY<STRUCT<
    type: STRING,
    location: STRUCT<start: INT, end: INT>,
    description: STRING,
    evidence: ARRAY<STRING>
  >>
  comments: ARRAY<STRUCT<
    topic: STRING,
    text: STRING,
    source: STRING
  >>
}
```

---

### 4. **Preserve Losslessness with Metadata**

- Embed original UniProtKB XML version and extraction timestamp in Parquet file metadata (`key_value_metadata`)
- Consider preserving select XML fragments in metadata (one per protein) for validation
- Do NOT include full XML as residual column (adds size), but allow round-trip verification

Example metadata:
```json
{
  "uniprot_version": "2025_04",
  "extraction_date": "2025-04-14T00:00:00Z",
  "schema_version": "1.0",
  "lossless": "yes",
  "source_format": "UniProtKB XML"
}
```

---

### 5. **Versioning and Schema Evolution Policy**

**Define clearly**:

1. **Major Version** (1.0 → 2.0): Schema-breaking changes (column removed, type changed)
   - Separate file path: `/uniprot_2025_v2.parquet`
   - Bump `schema_version` in metadata
   - Publish migration guide

2. **Minor Version** (1.0 → 1.1): Backward-compatible (new columns, nullable fields added)
   - Same file path, append `_v1.1` suffix or use Parquet's schema evolution
   - Parquet schema evolution: add new columns at end, mark as OPTIONAL

3. **Release Version** (tied to UniProtKB release date): `uniprot_2025_04.parquet` tracks UniProtKB version

**Parquet Schema Evolution Best Practices**:
- New columns always OPTIONAL (nullable)
- Never rename existing columns (create new + deprecate old)
- Use Parquet metadata to mark deprecated columns
- Test backward compatibility with older reader versions

---

### 6. **Documentation Strategy**

**Ship with the dataset**:

1. **README.md**: Overview, data source, UniProtKB version, extraction date
2. **SCHEMA.md**: Table-by-table schema with field descriptions (sample below)
3. **EXAMPLES.md**: Sample queries in PySpark, DuckDB, Pandas
4. **VERSION_HISTORY.md**: Schema changes per release

**Example SCHEMA.md structure**:
```markdown
## Table: protein

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| accession | STRING | UniProt Accession (primary key) | UniProtKB entry/@id |
| sequence | STRING | Protein sequence | UniProtKB sequence |
| organism_id | INT | NCBI Taxonomy ID | UniProtKB organism/dbReference[@id] |
| is_reviewed | BOOLEAN | Reviewed in SwissProt (Y/N) | UniProtKB entry/@dataset |
| features | ARRAY<STRUCT<...>> | Post-translational modifications, domains, etc. | UniProtKB feature |

## Table: feature (flattened child table)

| Column | Type | Description |
|--------|------|-------------|
| protein_id | STRING | Foreign key to protein.accession |
| feature_type | STRING | E.g., "domain", "motif", "modified residue" |
| ...
```

---

### 7. **SQL Views and Helper Functions**

**Provide Spark or DuckDB helper scripts** (not required, but valuable):

```sql
-- Useful view: denormalized wide table from nested features
CREATE VIEW protein_with_features AS
SELECT
  p.accession,
  p.sequence,
  p.organism_id,
  f.value.type AS feature_type,
  f.value.location.start AS feature_start,
  f.value.location.end AS feature_end,
  f.value.description AS feature_description
FROM protein p
LATERAL VIEW EXPLODE(p.features) f
WHERE p.features IS NOT NULL;
```

Or DuckDB equivalent using unnest():

```sql
SELECT
  accession,
  sequence,
  organism_id,
  feature.type AS feature_type,
  feature.location.start AS feature_start,
  feature.location.end AS feature_end
FROM protein
CROSS JOIN UNNEST(features) AS t(feature)
```

---

### 8. **Choose a Single File vs. Multiple Files**

**Single Parquet File**:
- Pros: Simple (one download), predictable row order
- Cons: Not partitionable, harder to parallelize for large scale (248M proteins)

**Multiple Files (Recommended)**:
- Partition by: `organism_id` or `first_letter_of_accession` (for parallelism)
- File structure: `/uniprot_2025_04/organism_id=9606/part-0000.parquet`, etc.
- Pros: Efficient Spark partitioning (push-down predicates), scalable, can update per partition
- Cons: User must handle multi-file reads (Spark handles transparently)

**Recommendation**: Partition by `organism_id` for Homo sapiens (most accessed) and optionally by first letter of accession for balance.

---

### 9. **Metadata Documentation**

**Parquet file metadata** can store key info:

```python
import pyarrow.parquet as pq

table = pq.read_table('uniprot_2025_04.parquet')
metadata = table.schema.metadata
# metadata = {
#   'uniprot_version': b'2025_04',
#   'extraction_date': b'2025-04-14T00:00:00Z',
#   'schema_version': b'1.0',
#   'total_proteins': b'248000000',
#   'lossless': b'yes',
#   'source_format': b'UniProtKB XML',
#   'license': b'CC-BY-4.0'
# }
```

**Recommendation**: Use Parquet metadata for version, date, row count, and schema version. Reference external SCHEMA.md for column descriptions.

---

### 10. **Validation & Round-Trip Testing**

**Best Practice**:
1. Sample ~1000 random proteins from UniProtKB XML source
2. Extract to Parquet schema
3. Validate:
   - Row count matches
   - All fields extracted (no nulls where unexpected)
   - Nested arrays unpacked correctly
4. Document: "Extraction validated on sample; best-effort for all 248M entries"

**Consider (optional, advanced)**:
- Store XML fragment of ~10 proteins in metadata as "golden set" for future schema validation
- Users can reconstruct XML from Parquet and compare with original for specific proteins

---

---

## Patterns: What Works, What Doesn't

### ✅ **Successful Patterns**

1. **Explicit Join Keys + Separate Tables** (Open Targets)
   - Reduces redundancy
   - Versioning flexibility (change one table without touching others)
   - Users experienced with Spark/SQL comfortable with joins

2. **Tidy Data Layout** (ADAM, HCA)
   - One observation per row
   - Simple to iterate, aggregate, filter
   - Parquet compression efficient (repetitive columns compress well)
   - Query examples straightforward

3. **Native Nesting in Parquet** (Open Targets, gnomAD, ADAM)
   - Preserves hierarchical structure from source
   - Avoids artificial flattening (explosion of rows)
   - Parquet's definition/repetition levels handle efficiently
   - Users learn to use explode() / unnest()

4. **Snake_case + Singular Nouns** (Consistent)
   - Familiar to SQL users
   - Tool compatibility (no need for backticks in SQL)
   - Aligns with Python/R naming conventions

5. **Metadata Versioning** (Merapar case study)
   - S3 object tags track schema version
   - Enables parallel migration with state tracking
   - Avoids re-processing of completed files

### ❌ **Pitfalls to Avoid**

1. **No Schema Documentation**
   - Users have to infer schema via `printSchema()`
   - Field semantics unclear (is this raw count or normalized?)
   - No versioning roadmap

2. **Breaking Changes Without Migration Path**
   - Open Targets v25.03 (JSONL → Parquet only)
   - Forced users to rewrite pipelines overnight
   - Better: overlap old + new format for 1 release, then deprecate

3. **Inconsistent Naming** (Open Targets `targetId` vs. `clinical_significances`)
   - camelCase IDs, snake_case fields
   - Confuses users, hard to auto-generate utilities

4. **Over-Flattening**
   - Some projects flatten all arrays into wide tables
   - Explodes row count, bloats storage
   - Nested Parquet more efficient

5. **No Losslessness Verification**
   - Cannot confirm round-trip fidelity
   - Schema extraction bugs go undetected
   - Limits adoption for compliance/reproducibility workflows

---

## Summary: Key Takeaways for UniProtKB Parquet

| Aspect | Recommendation | Rationale |
|--------|---|----------|
| **Schema Design** | Hybrid: denormalized primary + nested child arrays + separate flattened tables | Balances simplicity (primary table) with power-user needs (nested + flattened) |
| **Normalization** | Multiple related tables with explicit join keys | Like Open Targets; proven for complex biological data |
| **Nested Data** | Preserve arrays (features, comments) in primary table | Parquet handles well; users can explode() as needed |
| **Naming** | snake_case, singular nouns, `is_*`/`has_*` for booleans, `_id` suffix for keys | Consistent with SQL tradition; tool-compatible |
| **Documentation** | Embed in Parquet metadata + external SCHEMA.md + EXAMPLES.md | Multiple audiences (automated tools, analysts, developers) |
| **Versioning** | Separate file paths per major version; Parquet schema evolution for minor versions | Clear versioning; backward compatibility; no breaking surprises |
| **Losslessness** | Embed extraction version + sample XML in metadata; document as "best-effort with validation" | Supports reproducibility; enables round-trip testing by users |
| **File Layout** | Partition by organism_id (top-level users: H. sapiens); support multi-file reads | Scalable, queryable, parallelize-able |
| **Validation** | Test on sample; publish row counts, field presence, nesting structure | Build user confidence; publish findings in README |

---

## References & Sources

1. Open Targets Platform Documentation: https://platform-docs.opentargets.org/
2. Open Targets Parquet Preference Discussion: https://community.opentargets.org/t/file-formats-and-our-preference-for-parquet/1749
3. Crash Course in Open Targets Part 3: https://clarewest.github.io/blog/post/crash-course-in-open-targets-part-3/
4. Hail MatrixTable Overview: https://hail.is/docs/0.2/overview/matrix_table.html
5. gnomAD Documentation: https://broadinstitute.github.io/gnomad_methods/
6. ADAM Genomics Documentation: https://adam.readthedocs.io/en/latest/api/genomicDataset/
7. ADAM GitHub: https://github.com/bigdatagenomics/adam
8. 23andMe High-Performance Genetic Datastore: https://medium.com/23andme-engineering/genetic-datastore-4b213256db31
9. Gene Expression in Parquet Format: https://tomsing1.github.io/blog/posts/parquet/
10. Ensembl SQL-to-Parquet: https://github.com/Ensembl/nf-sql-to-parquet
11. Arrow & Parquet Nested Data: https://arrow.apache.org/blog/2022/10/08/arrow-parquet-encoding-part-2/
12. Controlled Schema Migration (Merapar): https://articles.merapar.com/controlled-schema-migration-of-large-scale-s3-parquet-data-sets
13. Parquet Schema Evolution Best Practices: https://medium.com/data-engineering-with-dremio/all-about-parquet-part-04-schema-evolution-in-parquet-c2c2b1aa6141
14. ClinVar Data Parsing: https://wellcomeopenresearch.org/articles/2-33
15. UniProtKB Programmatic Access: https://www.uniprot.org/help/programmatic_access
16. Human Cell Atlas Metadata Schema: https://github.com/HumanCellAtlas/metadata-schema
17. Reactome Data Download: https://reactome.org/download-data
18. InterPro Documentation: https://interpro-documentation.readthedocs.io/en/latest/download.html
19. Database Naming Conventions: https://codilime.com/blog/normalization-vs-denormalization-in-databases/
20. Boolean Naming Conventions: https://dev.to/michi/tips-on-naming-boolean-variables-cleaner-code-35ig
21. Parallelizing Genome Variant Analysis: https://www.databricks.com/blog/2016/05/24/parallelizing-genome-variant-analysis.html
22. ADAM: Genomics Formats and Processing Patterns (UC Berkeley Tech Report): https://www2.eecs.berkeley.edu/Pubs/TechRpts/2013/EECS-2013-207.html

---

**End of Report**

*Generated April 2025. Based on publicly available documentation, code repositories, and technical literature as of February 2025 knowledge cutoff, with web research updates through April 2026.*
