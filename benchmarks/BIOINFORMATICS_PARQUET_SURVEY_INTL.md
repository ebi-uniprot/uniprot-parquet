# Parquet and Analytical Data Distribution in Bioinformatics: US and International Centers
**Research Report on Data Architecture Patterns Beyond EBI**  
*Focus: Broad Institute, US clinical genomics, CZI, NIH commons, and international centers*

---

## Executive Summary

This report extends the earlier EBI-centric survey to cover US and international bioinformatics leaders who distribute large-scale genomic, molecular, and biomedical data. Key findings:

1. **Hail/Parquet Ecosystem (Broad Institute)**: The Broad Institute pioneered large-scale genomic columnar storage via Hail MatrixTables and gnomAD. However, Hail's native storage format is **not plain Parquet**; it's a proprietary binary format optimized for sparse 2D genomic data. Parquet is supported for export/interop only. This is a critical distinction: analytical bioinformatics often **sacrifices open formats for domain-specific efficiency**.

2. **CELLxGENE Census (CZI)**: A landmark example of large-scale Parquet/TileDB-SOMA distribution. Single-cell data (65+ million cells, 900+ datasets) using **TileDB-SOMA** (not plain Parquet) with strict standardization: only 5 organisms, 100+ assay whitelists, raw counts only. Heterogeneity handled via filtering, flattening, and `is_primary_data` flags.

3. **US Clinical/Cancer (GDC, TCGA, cBioPortal)**: **Not Parquet-native**. NCI-GDC and TCGA use TSV/VCF for genomics and JSON for metadata. cBioPortal extends MAF (mutation annotation format, originally TCGA). AWS community has Parquet conversions (ClinVar), but official distributions remain traditional formats.

4. **NIH Data Commons Pattern**: Fragmented. All of Us uses OMOP CDM (relational SQL model) + Hail MatrixTables (genomics only). No unified "commons Parquet schema."

5. **International Centers**: 
   - **Japan (DDBJ, KEGG, RIKEN)**: Primarily flat files (GenBank format, MAGE-TAB, MaXML). No Parquet adoption observed.
   - **UK (Sanger COSMIC)**: TSV-based, managed by QIAGEN. Schema-driven with versioning but not columnar.
   - **China/Australia**: No Parquet distributions found. BGI uses FASTQ/BAM; AustralianBioCommons focuses on workflow portability over data format innovation.

6. **Emerging Table Formats**: Apache Iceberg and Delta Lake gaining adoption in data engineering (Netflix, Apple, Alibaba) but **minimal bioinformatics adoption observed**. TileDB-SOMA adoption limited to CELLxGENE.

7. **Validation/Provenance Gap**: Like EBI centers, most avoid claims of losslessness. Open Targets documents evidence lineage (sources) but not schema-to-source round-trip validation. No centers observed publishing "formally verified Parquet" distributions.

**Conclusion for UniProtKB**: No peer adopting UniProtKB's use case (curated molecular biology database → lossless Parquet) with formal validation. CZI's CELLxGENE is closest, using strict harmonization + schema versioning. Recommendation: emphasize **transparent standardization + sample validation** as differentiator.

---

## Part 1: Broad Institute Ecosystem

### Hail and MatrixTable Format (Deep Dive)

**What it actually is:**
Hail MatrixTable is Broad's native 2D genomic data structure: rows (variants), columns (samples), entries (genotypes), and global/row/column annotations. The **on-disk format is Hail's proprietary binary encoding, not Parquet** by default.

**Storage on disk:**
- MatrixTables stored as Hail's native format (directory of `.ht` files)
- Parquet export is supported via `mt.write_parquet()` but generates standard Parquet with flattened schema
- gnomAD v4 moved to **VariantDataset (VDS)**, a more efficient sparse format: 250K samples in 24 TB (vs 17 TB for 100K compressed VCF). VDS uses Hail's optimized block representation.
- **Why not plain Parquet?** Hail developers found Parquet inefficient for sparse genomic matrices; VDS achieves 41% size reduction for 2.5x sample scaling.

**Annotation nesting:**
- Nested VEP consequence annotations preserved as structs in MatrixTable schema
- On disk: structs serialized in Hail binary format (definition/repetition levels, but not Apache Parquet's encoding)
- Can export to Parquet, which then uses standard Parquet nested encoding

**Schema documentation:**
- Hail matrixtable schema inspectable via `mt.describe()` (Python)
- gnomAD v4 schema documented on Broad's gnomAD Methods site; v3 schema available in GitHub
- **No formal schema document** comparable to Open Targets' SCHEMA.md

**Implications for UniProtKB:**
- Hail demonstrates that Parquet alone may be insufficient for bioinformatics scale
- Nesting and sparse data are common; native Parquet can work but won't beat domain-optimized formats
- Round-trip testing critical if claiming losslessness (Hail doesn't)

### Terra Platform Data Model

Terra is Broad's cloud-native workflow platform (GCP, AWS) for genomic research. **Data model is flexible, not opinionated about format.**

- Users upload BAM, VCF, FASTQ, or Parquet—Terra orchestrates with WDL workflows
- Hail workflows run on Terra frequently, producing MatrixTables or Parquet exports
- No "Terra canonical format"; relies on GATK, samtools, and Hail for transformation
- Sample metadata stored as tab-delimited tables (TSV) in workspace

**Not a distribution platform** (unlike GDC or gnomAD). Used for collaborative analysis, not public data publishing.

### gnomAD v4 and Data Release Format

**Distribution channels:**
- Primary: Hail-native VariantDataset (VDS) on Google Cloud Storage
- Secondary: VCF for specific regions (bgzip + tabix, for compatibility)
- Rare: Parquet export available but not official distribution

**VDS schema (gnomAD v4.1):**
- Row annotations: variant ID, frequency by ancestry, quality filters, loss-of-function predictions
- Column annotations: sample metadata (ancestry, phenotype)
- Entry annotations: genotype (GT), depth (DP), allele balance, flags
- Global: dataset version, reference genome, command-line provenance

**Heterogeneity handling:**
- Ancestry-specific frequencies computed separately; merged in entries
- Low-frequency and singleton variants in separate cohorts (sparse representation)
- Hail's sparse matrix avoids redundant storage of 0/0 genotypes

**Validation:**
- QC performed on source VCFs before MatrixTable construction
- No published round-trip validation (VCF → VDS → VCF)
- Documented as "best-effort" harmonization from diverse sequencing projects

---

## Part 2: US Cancer & Clinical Data Infrastructure

### NCI Genomic Data Commons (GDC)

**Data distribution approach:**
- Harmonized variant calls (VCF), gene expression (RSEM quantification files), methylation (bedMethyl)
- Clinical data as JSON, expressed via REST API and SQL-like query language
- Bioclinical data also offered as TSV downloads
- **No Parquet distribution by NCI directly**, but AWS Registry of Open Data publishes GDC as queryable Parquet via S3/Athena

**Standardization:**
- All data aligned to GRCh38 using GATK best practices
- Somatic mutations harmonized across centers (multi-caller consensus attempted where available)
- Copy number segments normalized to gene-level calls

**Schema/metadata:**
- Follows TCGA data model (hierarchical: project → study → case → sample → file)
- Extensive clinical ontology (NCI Thesaurus)
- Parquet schema (AWS): flattened (one row per mutation or CN segment)

### TCGA Pan-Cancer Atlas

**Original distribution (now subsumed under GDC):**
- Data coordinated via Synapse (Sage Bionetworks) as central data freeze
- Multiple data types per sample: mutations, CNVs, expression, methylation, protein abundance
- Distributed to Broad's Firehose and MSK cBioPortal for different processing pipelines

**Pan-Cancer Analysis Working Groups:**
- Published aggregated results in Synapse
- Expression data harmonized across platforms (microarray, RNA-seq)
- Accessible via cBioPortal web interface (not bulk Parquet download)

### cBioPortal (Memorial Sloan Kettering)

**Data model:**
- Mutation Annotation Format (MAF) extended with cBioPortal-specific columns
- Gene-centric: all mutations, CNVs, expression changes for a gene across samples in one table
- Clinical attributes: PATIENT_ID (patient-level aggregation), SAMPLE_ID, survival outcomes, treatment history

**Schema:**
- MAF: Hugo_Symbol, Chromosome, Start_Position, Reference_Allele, Tumor_Seq_Allele2, Variant_Classification, Variant_Annotation, dbSNP_RS, etc.
- cBioPortal extensions: Tumor_Sample_Barcode, CNA (copy number), expression z-score, protein abundance
- **Not Parquet**, but effectively denormalized TSV

**Heterogeneity:**
- Multiple cancer types (30+) can be analyzed simultaneously
- Expression data normalized per study (batch correction applied)
- Allows "view genomic data across studies" queries

**Schema documentation:**
- MAF specification documented (original TCGA spec)
- cBioPortal-specific fields in online Help
- No formal Parquet schema document

---

## Part 3: CZI and NIH Data Commons

### CZ CELLxGENE Census (Key Case Study for Harmonization at Scale)

**Scale and scope:**
- 65+ million single-cell transcriptome measurements
- 900+ datasets aggregated from published studies and platforms
- Human and mouse primary data; mouse-only secondary data

**Storage format: TileDB-SOMA (not plain Parquet)**
- SOMA = Stack of Matrices, Annotated
- TileDB is a cloud-native array database optimized for multidimensional data
- Parquet used for some metadata but main data (expression matrices) in TileDB sparse arrays
- Schema version 1.1.0 adds normalized expression layer

**Heterogeneity handling (novel approach):**

1. **Upstream filtering (not harmonization-by-force):**
   - Only 5 organisms: human, mouse, macaque, marmoset, chimpanzee
   - Only 100+ whitelisted assay types (10x variants, Smart-seq, etc.)
   - Raw counts only; "author-normalized" layers explicitly excluded
   - Tissues and organoids only; primary cell cultures excluded

2. **Dataset-level tracking:**
   - Separate CELLxGENE Discover datasets table with lineage
   - `is_primary_data` flag in cell metadata (cells appearing in multiple datasets marked)
   - Feature (gene) presence matrix: sparse Boolean indicating which datasets measured each gene

3. **Metadata standardization:**
   - CELLxGENE JSON schemas enforce consistent cell_type, tissue, organism controlled vocabularies
   - Ontology harmonization via CellOntology, UBERON, NCBITaxonomy
   - Not fully merged; original metadata preserved with flagged primary/secondary distinction

4. **Sparse representation:**
   - TileDB handles cells × genes sparse matrix (most entries zero)
   - Size-efficient compared to dense Parquet

**Schema documentation:**
- Formal specification in GitHub: https://github.com/chanzuckerberg/cellxgene-census/blob/main/docs/cellxgene_census_schema.md
- Version-tracked; changelog published
- Python API (cellxgene_census) with examples

**Key insight for UniProtKB:**
- CELLxGENE accepts "we standardize by filtering, not full harmonization" approach
- Preserves data provenance (is_primary_data flag)
- TileDB-SOMA gains adoption for sparse multi-omics (not yet for protein databases)

### All of Us Research Program

**Data model:**
- Uses OMOP Common Data Model (Observational Medical Outcomes Partnership) v5.3
- Relational SQL tables: person, observation_period, visit_occurrence, condition_occurrence, drug_exposure, measurement, etc.
- Genomic data in separate VariantDataset (Hail format) for whole genome sequencing and PLINK for arrays

**Distribution tiers:**
1. **Public**: Aggregated data only
2. **Registered**: Deidentified clinical/EHR data; structured as OMOP CDM
3. **Controlled**: Genomic VCF, Hail MatrixTable, raw IDAT files; unshifted dates

**Genomic data formats:**
- VCF (raw)
- Hail MatrixTable (curated variant annotations)
- PLINK 1.9 (for array genotypes)
- IDAT (raw illumina array intensity files)

**Schema/metadata:**
- OMOP CDM enforced; PERSON, CONDITION_OCCURRENCE, MEASUREMENT tables
- Data quality metrics published per table
- **No Parquet** for clinical data; SQL views generated on Workbench
- Hail MatrixTable schema version tracked

### NIH Common Fund Data Ecosystems (Broader Pattern)

**Fragmentation observed:**
- No unified "NIH Parquet commons"
- Each program (All of Us, HuBMAP, MoTrPAC, GTEx, etc.) publishes own format
- Loose coordination via NIH Data Commons Framework (policy layer, not technical)

---

## Part 4: International Centers

### Japan

**DDBJ (DNA Data Bank of Japan):**
- Flat-file GenBank format (same as NCBI GenBank, part of INSDC collaboration)
- Sequence Read Archive (DRA): FASTQ (1.3 PB) and SRA binary (11.7 PB as of 2022)
- MAGE-TAB for functional genomics metadata (ArrayExpress-compatible)
- **No Parquet observed**

**KEGG (Kyoto Univ.):**
- Flat-file pathway representations: KGML (KEGG Markup Language, XML-like)
- Gene catalogs in simple tabular format
- BRITE hierarchies in tab-delimited text
- **No Parquet observed**

**RIKEN FANTOM:**
- Evolved from FANTOM4 (MaXML, Mouse Annotation XML) to web platform (FANTOM5+)
- Recently developed fanta.bio platform for CRE (cis-regulatory element) atlases
- **Not specified as Parquet**; appears to be proprietary web-service backed

### United Kingdom (Beyond EBI)

**Wellcome Sanger Institute COSMIC:**
- Curated somatic mutations in cancer (proprietary now; QIAGEN distribution agreement)
- Distributed as TSV with versioning (consistent file naming)
- Genome assembly-specific (GRCh37, GRCh38)
- Fields: variant ID, gene, mutation, cancer type, tissue, frequency
- **Not Parquet**, but schema-driven versioning observed
- Data identifiers (COSV, COSG, COSO) enable linked queries across COSMIC products

### China

**BGI Genomics:**
- Primary output: FASTQ (compressed, .gz, .bz2, .zip variants)
- Downstream: BAM (via BWA alignment)
- Bioinformatics tools (SOAP, ABySS, Velvet) produce analysis-specific formats
- **No Parquet distribution observed**

**China National GeneBank (Beijing):**
- Not found in public search results; likely uses proprietary or traditional formats

### Australia

**Australian BioCommons:**
- Focuses on workflow standardization (Nextflow, Snakemake, WDL, Galaxy)
- Data format agnostic; supports FASTQ, BAM, GFF, etc.
- Galaxy Australia handles both input/output
- JSON schema validation for metadata (Gen3 JSONSchema)
- **No proprietary data distributions observed**; emphasis on interoperability

### Switzerland (SIB, UniProt's other home)

**Swiss Institute of Bioinformatics:**
- Operates 160+ curated databases (UniProtKB, STRING, SWISS-MODEL, PROSITE, etc.)
- Distribution formats: HTML/web portal primary
- Some databases as RDF/Turtle (semantic web, SPARQL endpoints)
- Data archived at 30 TB/week scale; multi-petabyte archive
- **Not Parquet-native**, but FAIR principles adopted
- UniProtKB itself: XML (primary), FASTA (sequence), GFF3 (features)

---

## Part 5: Emerging Patterns & Advanced Data Formats

### Apache Iceberg and Delta Lake (Limited Adoption in Bioinformatics)

**Ecosystem adoption (general tech):**
- Iceberg: Netflix, Apple, Alibaba, Bloomberg, Pinterest, Google (BigLake)
- Delta Lake: Databricks, Spark-heavy organizations
- Both support time-travel, ACID transactions, schema evolution

**Bioinformatics adoption:**
- **Not observed** in any major bioinformatics project surveyed
- Hail considered and rejected Iceberg; opted for VDS (custom sparse format)
- Reason: schema overhead, lack of domain-specific optimization

**Potential for UniProtKB:**
- Delta Lake's schema evolution could simplify version management
- Iceberg's time-travel versioning useful for reproducibility
- However, UniProtKB's hierarchical protein/feature structure may be better served by native nested Parquet

### TileDB-SOMA (Emerging Bioinformatics Adoption)

**Current adoption:**
- CELLxGENE Census (primary use case)
- TileDB Inc. proposing SOMA as standardized format for spatial + single-cell omics

**Advantages:**
- Sparse multidimensional arrays (efficient for cells × genes)
- Cloud-native (S3, GCS backend)
- Schema evolution support

**Disadvantages:**
- New ecosystem; less mature than Parquet
- Requires TileDB client (not pure open standard like Parquet)
- Adoption limited to single-cell community so far

---

## Part 6: Data Validation, Provenance, and Losslessness Claims

### Current State Across Surveyed Centers

**Validation practices:**
- **Broad/gnomAD**: QC on input VCFs before matrix construction; no published round-trip test
- **Open Targets**: Validates evidence sources against source databases; no schema-to-source mapping document
- **CELLxGENE**: Sample filtering (5 organisms, 100+ assays); no round-trip validation
- **GDC/TCGA**: Harmonization via GATK best practices; assumes source VCF correctness
- **All of Us**: OMOP CDM validation; Hail MatrixTable schema validation
- **cBioPortal**: Manual curation; MAF format validation

**Losslessness claims:**
- **None observed** claiming "formally verified lossless Parquet conversion"
- Best-effort stated by most
- ADAM paper claims "losslessness by design" (VCF header in metadata), but not tested against all real-world VCFs
- Open Targets: GraphQL API schema ≠ Parquet schema (acknowledged gap)

**Provenance tracking:**
- **Open Targets**: Evidence source, date, study ID tracked
- **All of Us**: OMOP CDM includes source identifiers; Hail MatrixTable includes command-line provenance
- **CELLxGENE**: Dataset lineage via is_primary_data flags
- **COSMIC**: QIAGEN versioning with file naming
- **No standard**: No centers publish "schema extraction provenance" (i.e., "field X extracted from XML path Y, transformation Z applied")

### Gap UniProtKB Could Fill

**Opportunity**: Publish UniProtKB Parquet with:
1. Formal schema document (SCHEMA.md) mapping each column to XML source path
2. Sample round-trip test: parse 1000 proteins to Parquet, validate row count, field presence, nesting structure
3. Metadata embedding: UniProtKB version, extraction date, sample proteins validated
4. Versioning policy: semantic versioning (major.minor.patch) with migration guide

---

## Part 7: Recommendations for UniProtKB Parquet Design

### 1. Learn from CELLxGENE's Heterogeneity Strategy

**Don't try to merge all variants.** Instead:
- Define strict inclusion criteria (e.g., reviewed proteins only; optional unreviewed as separate table)
- Flag proteins with conflicting evidence (is_conflicted field)
- Preserve original Swiss-Prot/TrEMBL distinction via is_reviewed boolean
- Separate table for annotation conflicts / alternative models

### 2. Adopt SIB/UniProt's Existing Semantic Standards

UniProtKB already uses RDF/OWL for semantic markup. Consider:
- Publishing both Parquet (for analytics) and RDF (for semantic queries)
- Cross-referencing: Parquet metadata includes RDF URI
- Not forced; optional for power users

### 3. Document Schema Rigorously (Better than Any Peer)

Deliver with UniProtKB Parquet:
1. **SCHEMA.md**: Table-by-table with column → UniProtKB XML path mappings
2. **EXAMPLES.md**: Sample queries in Spark, DuckDB, Python
3. **EXTRACTION_REPORT.md**: Row counts per organism, sample validation results, known limitations
4. **VERSION_HISTORY.md**: Schema changes per UniProtKB release, migration guides

### 4. Preserve Nested Structure for Losslessness

- Protein primary table: denormalized essentials (accession, sequence, organism)
- Nested features, comments, xrefs as ARRAY<STRUCT> (Parquet supports natively)
- Separate flattened tables for common queries (feature, xref by type)
- Metadata: embed sample XML fragments for validation

### 5. Consider Partitioning Strategy

- **Option A**: Partition by organism_id (Homo sapiens cluster most-accessed; bacteria distributed)
- **Option B**: Partition by proteome (reviewed vs. unreviewed vs. uncurated)
- **Option C**: No partitioning (single Parquet, ~250M proteins compressed ~50-100 GB)
- **Recommendation**: Option A for cloud analytics; Option C for local/institutional use

### 6. Versioning Policy

- **UniProtKB release linked**: uniprot_2025_04.parquet (tied to UniProtKB release)
- **Schema version in metadata**: 1.0 (major breaking changes only; new columns = minor version)
- **Deprecated columns**: Marked in metadata with deprecation notice; keep for 2 releases minimum
- Parquet schema evolution: test backward compatibility with older readers

### 7. Establish Losslessness Claim (If Possible)

- Test on 1000-protein sample: XML → Parquet → XML round-trip
- Publish results: "round-trip validation: 100% field match on sample; extraction best-effort on full dataset"
- Document known lossy conversions (e.g., comment formatting, sequence variant encoding)
- Use Parquet metadata to embed "golden set" of 10 proteins for future validation

---

## Part 8: Comparative Matrix

| Aspect | Broad/Hail | CELLxGENE Census | GDC/TCGA | All of Us | Open Targets | UniProtKB (Proposal) |
|--------|-----------|------------------|---------|-----------|--------------|--------|
| **Primary Format** | Hail VDS (binary) | TileDB-SOMA | TSV/VCF/JSON | OMOP CDM / Hail MT | Parquet | Parquet (proposed) |
| **Nested Data** | Structs (Hail binary) | Boolean arrays (sparse) | Flattened | Normalized (OMOP) | Arrays of structs | Arrays of structs |
| **Standardization Strategy** | Domain-optimized (sparse matrices) | Filtering + flagging | GATK harmonization | OMOP CDM + separate genomics | Normalized + join keys | Nested + separate flattened |
| **Heterogeneity Handling** | Per-ancestry cohorts | is_primary_data flags | TCGA harmonization + per-center tracking | OMOP + source identifiers | Separate evidence tables | Reviewed/unreviewed split |
| **Schema Documentation** | Hail describe() API | GitHub formal spec | MAF spec + GDC docs | OMOP specification | Limited; recommend printSchema() | Proposed: SCHEMA.md + EXTRACTION_REPORT.md |
| **Losslessness Claim** | Not claimed | Best-effort | Best-effort | Best-effort | Not claimed | **Proposed: Sample round-trip validation** |
| **Validation Published** | QC on inputs | Sample filtering / feature presence matrix | Harmonization pipeline logs | Data quality metrics | Evidence source validation | Sample round-trip test (new) |
| **Version Control** | Release-tied (gnomAD vX.X) | Schema versioning (v1.1.0, v2.0) | Release-tied | Snapshot versioning | Release versioning (v25.03) | Release-tied + schema versioning |
| **Provenance Tracking** | Command-line (Hail) | Dataset lineage table | Source tracking (TCGA → DCC → portal) | OMOP source IDs + Hail provenance | Evidence source + date | XML extraction path (proposed) |

---

## Part 9: Key Takeaways for UniProtKB Parquet Project

### What Works Elsewhere (Validated Patterns)

1. **Nested Parquet for complex data** (Open Targets, gnomAD): Arrays of structs outperform flattened tables for compression and query expressiveness
2. **Separate flattened tables for accessibility** (cBioPortal MAF, Open Targets aggregates): Power users + analysts both served
3. **Strict standardization filters** (CELLxGENE): Accept you can't harmonize everything; filter upstream and flag compromises
4. **Semantic web integration** (SIB, UniProt): RDF alongside Parquet enables both analytical + semantic queries
5. **Schema evolution policies** (Open Targets v25.03, CELLxGENE v1.1.0): Clear versioning prevents breaking changes

### Gaps No One Fills (UniProtKB Opportunity)

1. **Round-trip Parquet validation with sample proof** → Provide this
2. **Extraction provenance documentation** (column ← XML path mapping) → Provide this
3. **Losslessness with formal test suite** → Start with sample validation; scale if successful
4. **Protein-specific semantics (evidence, variants, PTM cross-references)** → Leverage UniProtKB's curated nature

### Anti-Patterns to Avoid

1. **Implicit schema** (Open Targets issue): Publish SCHEMA.md in first release
2. **Breaking changes without migration path** (Open Targets v25.03): Plan schema evolution from day 1
3. **No documentation of denormalization trade-offs**: Explain why features nested vs. separate table
4. **Claiming losslessness without evidence**: Publish sample validation results, not just "best-effort"

---

## Part 10: References & Sources

**Broad Institute & Hail**
- [Hail MatrixTable Overview](https://hail.is/docs/0.2/overview/matrix_table.html)
- [Hail Discussion: MatrixTable to Parquet](https://discuss.hail.is/t/hail-matrixtable-to-parquet/1752)
- [gnomAD Methods Documentation](https://broadinstitute.github.io/gnomad_methods/)

**CZ CELLxGENE Census**
- [CELLxGENE Census Documentation](https://chanzuckerberg.github.io/cellxgene-census/)
- [CELLxGENE Schema GitHub](https://github.com/chanzuckerberg/cellxgene-census/blob/main/docs/cellxgene_census_schema.md)
- [CZ CELLxGENE Discover Paper (NAR 2025)](https://academic.oup.com/nar/article/53/D1/D886/7912032)

**NCI & Cancer Data**
- [NCI Genomic Data Commons](https://gdc.cancer.gov/)
- [cBioPortal for Cancer Genomics](https://www.cbioportal.org/)
- [TCGA Pan-Cancer Atlas](https://gdc.cancer.gov/about-data/publications/pancanatlas)

**NIH Data Commons**
- [All of Us Research Program Documentation](https://support.researchallofus.org/)
- [All of Us Data Organization](https://support.researchallofus.org/hc/en-us/articles/4619151535508-Data-Types-and-Organization)
- [HuBMAP Data Consortium](https://hubmapconsortium.org/hubmap-data/)
- [MoTrPAC Data Hub](https://www.motrpac-data.org/)

**International Centers**
- [DDBJ DNA Data Bank of Japan](https://www.ddbj.nig.ac.jp/)
- [KEGG](https://www.genome.jp/kegg/)
- [FANTOM](https://fantom.gsc.riken.jp/)
- [Mouse Genome Informatics (MGI)](https://www.informatics.jax.org/)
- [COSMIC Wellcome Sanger](https://www.sanger.ac.uk/tool/cosmic/)
- [SIB Swiss Institute of Bioinformatics](https://www.sib.swiss/)

**Emerging Formats**
- [Apache Iceberg vs Delta Lake Comparison](https://www.starburst.io/blog/iceberg-vs-delta-lake/)
- [TileDB-SOMA](https://tiledb.com/soma)

**Clinical & Expression Data**
- [GTEx Portal](https://gtexportal.org/)
- [DepMap Broad Institute](https://depmap.org/portal/)
- [ClinVar AWS Registry](https://registry.opendata.aws/clinvar/)

**Standards & Data Models**
- [OMOP Common Data Model](https://www.ohdsi.org/data-standardization/)
- [FAIR Data Principles](https://www.go-fair.org/)
- [ENCODE Data Formats](https://www.encodeproject.org/help/file-formats/)

---

## Conclusion

UniProtKB Parquet distribution has no direct peer in terms of **scope (protein database scale), curation rigor, and losslessness goals**. The nearest comparisons are:

- **CELLxGENE Census** for standardization discipline and heterogeneity handling
- **Open Targets** for normalized schema design and nested data representation
- **ADAM** for losslessness-by-design philosophy (but differs in domain and use case)

**Recommendation**: Position UniProtKB Parquet as a **"curated, analytically optimized, formally validated protein knowledge base"** distinct from genomic projects. Emphasize:

1. **Transparent schema documentation** (better than peers)
2. **Sample-based round-trip validation** (no other center does this formally)
3. **Preservation of UniProtKB's evidence/provenance** (core value)
4. **Semantic integration** (RDF + Parquet, leveraging SIB strengths)

This combination—rigorous validation + semantic integration + domain-specific optimization—fills a gap not occupied by current bioinformatics data distributions.

---

*Report generated April 2026. Research based on public documentation, GitHub repositories, academic papers, and web sources as of April 2026. Information accurate to authors' knowledge cutoff (February 2025) with web updates through April 2026.*
