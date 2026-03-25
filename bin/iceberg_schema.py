"""
Apache Iceberg schema definition for the UniProtKB data lake.

Two tables:
  - entries: one row per protein (the primary table for 90% of queries)
  - features: one row per positional annotation (unnested for feature-level analysis)

Design principles:
  - Frequently filtered/grouped fields are top-level columns (fast, Iceberg data skipping)
  - Full nested structures preserved alongside for power users (zero cost if not selected
    thanks to Parquet column pruning)
  - Sorted by taxid for locality — queries for a single organism skip most files
  - No partition columns — Iceberg handles data skipping via per-file column statistics

Usage:
    from iceberg_schema import ENTRIES_SCHEMA, FEATURES_SCHEMA

    catalog.create_table("uniprot.entries", schema=ENTRIES_SCHEMA, sort_order=ENTRIES_SORT_ORDER)
    catalog.create_table("uniprot.features", schema=FEATURES_SCHEMA, sort_order=FEATURES_SORT_ORDER)
"""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField


# ─── Reusable nested types ────────────────────────────────────────────

EvidenceType = StructType(
    NestedField(field_id=901, name="evidenceCode", field_type=StringType(), required=False),
    NestedField(field_id=902, name="source", field_type=StringType(), required=False),
    NestedField(field_id=903, name="id", field_type=StringType(), required=False),
)

EvidenceListType = ListType(element_id=900, element_type=EvidenceType, element_required=False)

NameValueType = StructType(
    NestedField(field_id=910, name="value", field_type=StringType(), required=False),
    NestedField(field_id=911, name="evidences", field_type=EvidenceListType, required=False),
)

NameValueListType = ListType(element_id=909, element_type=NameValueType, element_required=False)

DbXrefPropertyType = StructType(
    NestedField(field_id=920, name="key", field_type=StringType(), required=False),
    NestedField(field_id=921, name="value", field_type=StringType(), required=False),
)

DbXrefType = StructType(
    NestedField(field_id=930, name="database", field_type=StringType(), required=False),
    NestedField(field_id=931, name="id", field_type=StringType(), required=False),
    NestedField(field_id=932, name="properties",
                field_type=ListType(element_id=933, element_type=DbXrefPropertyType, element_required=False),
                required=False),
    NestedField(field_id=934, name="evidences", field_type=EvidenceListType, required=False),
    NestedField(field_id=935, name="isoformId", field_type=StringType(), required=False),
)


# ─── Full nested types for power users ────────────────────────────────

OrganismType = StructType(
    NestedField(field_id=100, name="scientificName", field_type=StringType(), required=False),
    NestedField(field_id=101, name="commonName", field_type=StringType(), required=False),
    NestedField(field_id=102, name="taxonId", field_type=LongType(), required=False),
    NestedField(field_id=103, name="lineage",
                field_type=ListType(element_id=104, element_type=StringType(), element_required=False),
                required=False),
    NestedField(field_id=105, name="synonyms",
                field_type=ListType(element_id=106, element_type=StringType(), element_required=False),
                required=False),
    NestedField(field_id=107, name="evidences", field_type=EvidenceListType, required=False),
)

OrganismHostType = StructType(
    NestedField(field_id=110, name="scientificName", field_type=StringType(), required=False),
    NestedField(field_id=111, name="commonName", field_type=StringType(), required=False),
    NestedField(field_id=112, name="taxonId", field_type=LongType(), required=False),
    NestedField(field_id=113, name="synonyms",
                field_type=ListType(element_id=114, element_type=StringType(), element_required=False),
                required=False),
)

EcNumberType = StructType(
    NestedField(field_id=150, name="value", field_type=StringType(), required=False),
    NestedField(field_id=151, name="evidences", field_type=EvidenceListType, required=False),
)

ProteinNameType = StructType(
    NestedField(field_id=160, name="fullName", field_type=NameValueType, required=False),
    NestedField(field_id=161, name="shortNames", field_type=NameValueListType, required=False),
    NestedField(field_id=162, name="ecNumbers",
                field_type=ListType(element_id=163, element_type=EcNumberType, element_required=False),
                required=False),
)

ProteinDescriptionType = StructType(
    NestedField(field_id=170, name="recommendedName", field_type=ProteinNameType, required=False),
    NestedField(field_id=171, name="alternativeNames",
                field_type=ListType(element_id=172, element_type=ProteinNameType, element_required=False),
                required=False),
    NestedField(field_id=173, name="submissionNames",
                field_type=ListType(element_id=174, element_type=ProteinNameType, element_required=False),
                required=False),
    NestedField(field_id=175, name="flag", field_type=StringType(), required=False),
    NestedField(field_id=176, name="contains",
                field_type=ListType(element_id=177, element_type=ProteinNameType, element_required=False),
                required=False),
    NestedField(field_id=178, name="includes",
                field_type=ListType(element_id=179, element_type=ProteinNameType, element_required=False),
                required=False),
)

GeneType = StructType(
    NestedField(field_id=200, name="geneName", field_type=NameValueType, required=False),
    NestedField(field_id=201, name="synonyms", field_type=NameValueListType, required=False),
    NestedField(field_id=202, name="orfNames", field_type=NameValueListType, required=False),
    NestedField(field_id=203, name="orderedLocusNames", field_type=NameValueListType, required=False),
)

KeywordType = StructType(
    NestedField(field_id=210, name="id", field_type=StringType(), required=False),
    NestedField(field_id=211, name="name", field_type=StringType(), required=False),
    NestedField(field_id=212, name="category", field_type=StringType(), required=False),
    NestedField(field_id=213, name="evidences", field_type=EvidenceListType, required=False),
)

# Comments are polymorphic (different fields per commentType).
# Store as a struct with the union of all possible fields.
# Rare fields are null for most entries — Parquet handles this efficiently.
CommentType = StructType(
    NestedField(field_id=300, name="commentType", field_type=StringType(), required=False),
    NestedField(field_id=301, name="texts",
                field_type=ListType(element_id=302, element_type=NameValueType, element_required=False),
                required=False),
    NestedField(field_id=303, name="molecule", field_type=StringType(), required=False),
    NestedField(field_id=304, name="note", field_type=StringType(), required=False),
    # Subcellular location
    NestedField(field_id=305, name="subcellularLocations",
                field_type=ListType(element_id=306, element_type=StructType(
                    NestedField(field_id=307, name="location", field_type=NameValueType, required=False),
                    NestedField(field_id=308, name="topology", field_type=NameValueType, required=False),
                    NestedField(field_id=309, name="orientation", field_type=NameValueType, required=False),
                ), element_required=False),
                required=False),
    # Disease
    NestedField(field_id=310, name="disease", field_type=StructType(
        NestedField(field_id=311, name="diseaseId", field_type=StringType(), required=False),
        NestedField(field_id=312, name="diseaseAccession", field_type=StringType(), required=False),
        NestedField(field_id=313, name="acronym", field_type=StringType(), required=False),
        NestedField(field_id=314, name="description", field_type=StringType(), required=False),
    ), required=False),
    # Catalytic activity / reaction
    NestedField(field_id=320, name="reaction", field_type=StructType(
        NestedField(field_id=321, name="name", field_type=StringType(), required=False),
        NestedField(field_id=322, name="ecNumber", field_type=StringType(), required=False),
        NestedField(field_id=323, name="evidences", field_type=EvidenceListType, required=False),
    ), required=False),
    # Interaction
    NestedField(field_id=330, name="interactions",
                field_type=ListType(element_id=331, element_type=StructType(
                    NestedField(field_id=332, name="interactantOne", field_type=StructType(
                        NestedField(field_id=333, name="uniProtKBAccession", field_type=StringType(), required=False),
                        NestedField(field_id=334, name="geneName", field_type=StringType(), required=False),
                        NestedField(field_id=335, name="intActId", field_type=StringType(), required=False),
                    ), required=False),
                    NestedField(field_id=336, name="interactantTwo", field_type=StructType(
                        NestedField(field_id=337, name="uniProtKBAccession", field_type=StringType(), required=False),
                        NestedField(field_id=338, name="geneName", field_type=StringType(), required=False),
                        NestedField(field_id=339, name="intActId", field_type=StringType(), required=False),
                    ), required=False),
                    NestedField(field_id=340, name="numberOfExperiments", field_type=IntegerType(), required=False),
                    NestedField(field_id=341, name="organismDiffer", field_type=BooleanType(), required=False),
                ), element_required=False),
                required=False),
    # Alternative products (isoforms)
    NestedField(field_id=350, name="isoforms",
                field_type=ListType(element_id=351, element_type=StructType(
                    NestedField(field_id=352, name="isoformIds",
                                field_type=ListType(element_id=353, element_type=StringType(), element_required=False),
                                required=False),
                    NestedField(field_id=354, name="name", field_type=NameValueType, required=False),
                    NestedField(field_id=355, name="isoformSequenceStatus", field_type=StringType(), required=False),
                ), element_required=False),
                required=False),
    # Catch-all evidences
    NestedField(field_id=360, name="evidences", field_type=EvidenceListType, required=False),
)

# Feature (nested version — kept in entries table as array)
FeatureType = StructType(
    NestedField(field_id=400, name="type", field_type=StringType(), required=False),
    NestedField(field_id=401, name="location", field_type=StructType(
        NestedField(field_id=402, name="start", field_type=StructType(
            NestedField(field_id=403, name="value", field_type=IntegerType(), required=False),
            NestedField(field_id=404, name="modifier", field_type=StringType(), required=False),
        ), required=False),
        NestedField(field_id=405, name="end", field_type=StructType(
            NestedField(field_id=406, name="value", field_type=IntegerType(), required=False),
            NestedField(field_id=407, name="modifier", field_type=StringType(), required=False),
        ), required=False),
        NestedField(field_id=408, name="sequence", field_type=StringType(), required=False),
    ), required=False),
    NestedField(field_id=409, name="description", field_type=StringType(), required=False),
    NestedField(field_id=410, name="featureId", field_type=StringType(), required=False),
    NestedField(field_id=411, name="evidences", field_type=EvidenceListType, required=False),
    NestedField(field_id=412, name="alternativeSequence", field_type=StructType(
        NestedField(field_id=413, name="originalSequence", field_type=StringType(), required=False),
        NestedField(field_id=414, name="alternativeSequences",
                    field_type=ListType(element_id=415, element_type=StringType(), element_required=False),
                    required=False),
    ), required=False),
    NestedField(field_id=416, name="ligand", field_type=StructType(
        NestedField(field_id=417, name="name", field_type=StringType(), required=False),
        NestedField(field_id=418, name="id", field_type=StringType(), required=False),
        NestedField(field_id=419, name="label", field_type=StringType(), required=False),
        NestedField(field_id=420, name="note", field_type=StringType(), required=False),
    ), required=False),
    NestedField(field_id=421, name="ligandPart", field_type=StructType(
        NestedField(field_id=422, name="name", field_type=StringType(), required=False),
        NestedField(field_id=423, name="id", field_type=StringType(), required=False),
        NestedField(field_id=424, name="note", field_type=StringType(), required=False),
    ), required=False),
)

# Citation / reference
CitationType = StructType(
    NestedField(field_id=500, name="id", field_type=StringType(), required=False),
    NestedField(field_id=501, name="citationType", field_type=StringType(), required=False),
    NestedField(field_id=502, name="title", field_type=StringType(), required=False),
    NestedField(field_id=503, name="authors",
                field_type=ListType(element_id=504, element_type=StringType(), element_required=False),
                required=False),
    NestedField(field_id=505, name="journal", field_type=StringType(), required=False),
    NestedField(field_id=506, name="publicationDate", field_type=StringType(), required=False),
    NestedField(field_id=507, name="volume", field_type=StringType(), required=False),
    NestedField(field_id=508, name="firstPage", field_type=StringType(), required=False),
    NestedField(field_id=509, name="lastPage", field_type=StringType(), required=False),
    NestedField(field_id=510, name="citationCrossReferences",
                field_type=ListType(element_id=511, element_type=StructType(
                    NestedField(field_id=512, name="database", field_type=StringType(), required=False),
                    NestedField(field_id=513, name="id", field_type=StringType(), required=False),
                ), element_required=False),
                required=False),
    NestedField(field_id=514, name="submissionDatabase", field_type=StringType(), required=False),
)

ReferenceType = StructType(
    NestedField(field_id=520, name="referenceNumber", field_type=IntegerType(), required=False),
    NestedField(field_id=521, name="citation", field_type=CitationType, required=False),
    NestedField(field_id=522, name="referencePositions",
                field_type=ListType(element_id=523, element_type=StringType(), element_required=False),
                required=False),
    NestedField(field_id=524, name="referenceComments",
                field_type=ListType(element_id=525, element_type=StructType(
                    NestedField(field_id=526, name="type", field_type=StringType(), required=False),
                    NestedField(field_id=527, name="value", field_type=StringType(), required=False),
                ), element_required=False),
                required=False),
    NestedField(field_id=528, name="evidences", field_type=EvidenceListType, required=False),
)

GeneLocationType = StructType(
    NestedField(field_id=600, name="geneEncodingType", field_type=StringType(), required=False),
    NestedField(field_id=601, name="value", field_type=StringType(), required=False),
    NestedField(field_id=602, name="evidences", field_type=EvidenceListType, required=False),
)


# ─── ENTRIES TABLE ────────────────────────────────────────────────────
#
# One row per protein. The primary table for 90% of queries.
#
# Top-level columns: frequently filtered/grouped fields, directly
# accessible by Iceberg data skipping and simple WHERE clauses.
#
# Nested columns: full structures for power users. Zero cost if not
# selected (Parquet column pruning).

ENTRIES_SCHEMA = Schema(
    # ── Identity ──────────────────────────────────────────────────
    NestedField(field_id=1, name="acc", field_type=StringType(), required=True,
                doc="Primary accession (e.g. P12345)"),
    NestedField(field_id=2, name="id", field_type=StringType(), required=False,
                doc="Entry name (e.g. P53_HUMAN)"),
    NestedField(field_id=3, name="reviewed", field_type=BooleanType(), required=True,
                doc="true = Swiss-Prot (reviewed), false = TrEMBL (unreviewed)"),
    NestedField(field_id=4, name="secondary_accs",
                field_type=ListType(element_id=5, element_type=StringType(), element_required=False),
                required=False,
                doc="Previous/secondary accessions"),

    # ── Organism (flattened) ──────────────────────────────────────
    NestedField(field_id=10, name="taxid", field_type=LongType(), required=False,
                doc="NCBI taxonomy ID (e.g. 9606 for human)"),
    NestedField(field_id=11, name="organism_name", field_type=StringType(), required=False,
                doc="Scientific name (e.g. Homo sapiens)"),
    NestedField(field_id=12, name="organism_common", field_type=StringType(), required=False,
                doc="Common name (e.g. Human)"),
    NestedField(field_id=13, name="lineage",
                field_type=ListType(element_id=14, element_type=StringType(), element_required=False),
                required=False,
                doc="Taxonomic lineage [Eukaryota, Metazoa, ...]"),

    # ── Gene & protein (flattened) ────────────────────────────────
    NestedField(field_id=20, name="gene_name", field_type=StringType(), required=False,
                doc="Primary gene name (e.g. TP53)"),
    NestedField(field_id=21, name="protein_name", field_type=StringType(), required=False,
                doc="Recommended full protein name"),
    NestedField(field_id=22, name="ec_numbers",
                field_type=ListType(element_id=23, element_type=StringType(), element_required=False),
                required=False,
                doc="EC numbers for enzymes (e.g. [2.7.11.1])"),
    NestedField(field_id=24, name="protein_existence", field_type=StringType(), required=False,
                doc="Evidence level (e.g. 1: Evidence at protein level)"),
    NestedField(field_id=25, name="annotation_score", field_type=DoubleType(), required=False,
                doc="Annotation quality score (1-5)"),

    # ── Sequence (flattened) ──────────────────────────────────────
    NestedField(field_id=30, name="sequence", field_type=StringType(), required=False,
                doc="Amino acid sequence"),
    NestedField(field_id=31, name="seq_length", field_type=IntegerType(), required=False,
                doc="Sequence length"),
    NestedField(field_id=32, name="seq_mass", field_type=IntegerType(), required=False,
                doc="Molecular weight in Da"),
    NestedField(field_id=33, name="seq_md5", field_type=StringType(), required=False,
                doc="MD5 checksum of the sequence"),
    NestedField(field_id=34, name="seq_crc64", field_type=StringType(), required=False,
                doc="CRC64 checksum of the sequence"),

    # ── Cross-reference shortcuts ─────────────────────────────────
    NestedField(field_id=40, name="go_ids",
                field_type=ListType(element_id=41, element_type=StringType(), element_required=False),
                required=False,
                doc="Gene Ontology IDs (e.g. [GO:0005634, GO:0006915])"),
    NestedField(field_id=42, name="xref_dbs",
                field_type=ListType(element_id=43, element_type=StringType(), element_required=False),
                required=False,
                doc="Unique database names from cross-references (e.g. [PDB, Pfam])"),
    NestedField(field_id=44, name="keyword_ids",
                field_type=ListType(element_id=45, element_type=StringType(), element_required=False),
                required=False,
                doc="Keyword IDs (e.g. [KW-0181, KW-0378])"),
    NestedField(field_id=46, name="keyword_names",
                field_type=ListType(element_id=47, element_type=StringType(), element_required=False),
                required=False,
                doc="Keyword names (e.g. [Complete proteome, Hydrolase])"),

    # ── Versioning ────────────────────────────────────────────────
    NestedField(field_id=50, name="first_public", field_type=DateType(), required=False,
                doc="Date of first public release"),
    NestedField(field_id=51, name="last_modified", field_type=DateType(), required=False,
                doc="Date of last annotation update"),
    NestedField(field_id=52, name="last_seq_modified", field_type=DateType(), required=False,
                doc="Date of last sequence update"),
    NestedField(field_id=53, name="entry_version", field_type=IntegerType(), required=False,
                doc="Entry version number"),
    NestedField(field_id=54, name="seq_version", field_type=IntegerType(), required=False,
                doc="Sequence version number"),

    # ── Counts (from extraAttributes — useful for quick filtering) ──
    NestedField(field_id=60, name="feature_count", field_type=IntegerType(), required=False,
                doc="Total number of features"),
    NestedField(field_id=61, name="xref_count", field_type=IntegerType(), required=False,
                doc="Total number of cross-references"),
    NestedField(field_id=62, name="comment_count", field_type=IntegerType(), required=False,
                doc="Total number of comments"),
    NestedField(field_id=63, name="uniparc_id", field_type=StringType(), required=False,
                doc="UniParc archive identifier"),

    # ── Full nested structures (zero cost if not SELECTed) ────────
    NestedField(field_id=70, name="organism", field_type=OrganismType, required=False,
                doc="Full organism record including evidences"),
    NestedField(field_id=71, name="protein_desc", field_type=ProteinDescriptionType, required=False,
                doc="Full protein description with all names, EC, flags"),
    NestedField(field_id=72, name="genes",
                field_type=ListType(element_id=73, element_type=GeneType, element_required=False),
                required=False,
                doc="All gene names, synonyms, ORF names, ordered locus names"),
    NestedField(field_id=74, name="keywords",
                field_type=ListType(element_id=75, element_type=KeywordType, element_required=False),
                required=False,
                doc="Full keyword objects with categories and evidences"),
    NestedField(field_id=76, name="comments",
                field_type=ListType(element_id=77, element_type=CommentType, element_required=False),
                required=False,
                doc="Functional annotations (FUNCTION, SUBUNIT, DISEASE, etc.)"),
    NestedField(field_id=78, name="xrefs",
                field_type=ListType(element_id=79, element_type=DbXrefType, element_required=False),
                required=False,
                doc="Full cross-references (PDB, Pfam, InterPro, GO, etc.)"),
    NestedField(field_id=80, name="references",
                field_type=ListType(element_id=81, element_type=ReferenceType, element_required=False),
                required=False,
                doc="Literature citations"),
    NestedField(field_id=82, name="features",
                field_type=ListType(element_id=83, element_type=FeatureType, element_required=False),
                required=False,
                doc="Positional features (also available unnested in features table)"),
    NestedField(field_id=84, name="organism_hosts",
                field_type=ListType(element_id=85, element_type=OrganismHostType, element_required=False),
                required=False,
                doc="Host organisms (for viruses/parasites)"),
    NestedField(field_id=86, name="gene_locations",
                field_type=ListType(element_id=87, element_type=GeneLocationType, element_required=False),
                required=False,
                doc="Gene encoding locations (e.g. mitochondrion, plasmid)"),
)


# ─── FEATURES TABLE ──────────────────────────────────────────────────
#
# One row per positional annotation, unnested from entries.features.
# Designed for feature-level queries like "find all signal peptides
# in human" without needing UNNEST or JOINs.
#
# Denormalized: taxid, reviewed, organism_name, seq_length included
# so most feature queries don't need to join back to entries.

FEATURES_SCHEMA = Schema(
    NestedField(field_id=1, name="acc", field_type=StringType(), required=True,
                doc="Primary accession of the parent entry"),
    NestedField(field_id=2, name="reviewed", field_type=BooleanType(), required=True,
                doc="true = Swiss-Prot, false = TrEMBL"),
    NestedField(field_id=3, name="taxid", field_type=LongType(), required=False,
                doc="NCBI taxonomy ID (denormalized from entry)"),
    NestedField(field_id=4, name="organism_name", field_type=StringType(), required=False,
                doc="Scientific name (denormalized from entry)"),
    NestedField(field_id=5, name="seq_length", field_type=IntegerType(), required=False,
                doc="Sequence length (denormalized — useful for coverage calculations)"),

    NestedField(field_id=10, name="type", field_type=StringType(), required=False,
                doc="Feature type (Signal, Chain, Domain, Active site, ...)"),
    NestedField(field_id=11, name="start_pos", field_type=IntegerType(), required=False,
                doc="Start position (1-based, inclusive)"),
    NestedField(field_id=12, name="end_pos", field_type=IntegerType(), required=False,
                doc="End position (1-based, inclusive)"),
    NestedField(field_id=13, name="start_modifier", field_type=StringType(), required=False,
                doc="Position modifier (EXACT, OUTSIDE, UNSURE, UNKNOWN)"),
    NestedField(field_id=14, name="end_modifier", field_type=StringType(), required=False,
                doc="Position modifier (EXACT, OUTSIDE, UNSURE, UNKNOWN)"),
    NestedField(field_id=15, name="description", field_type=StringType(), required=False,
                doc="Feature description"),
    NestedField(field_id=16, name="feature_id", field_type=StringType(), required=False,
                doc="Feature identifier (e.g. PRO_..., VAR_...)"),

    NestedField(field_id=20, name="evidence_codes",
                field_type=ListType(element_id=21, element_type=StringType(), element_required=False),
                required=False,
                doc="ECO evidence codes (e.g. [ECO:0000269, ECO:0000305])"),

    # Variant / alternative sequence data
    NestedField(field_id=30, name="original_sequence", field_type=StringType(), required=False,
                doc="Original sequence (for variants/conflicts)"),
    NestedField(field_id=31, name="alternative_sequences",
                field_type=ListType(element_id=32, element_type=StringType(), element_required=False),
                required=False,
                doc="Alternative sequence(s)"),

    # Ligand binding data
    NestedField(field_id=40, name="ligand_name", field_type=StringType(), required=False,
                doc="Ligand name (for binding sites)"),
    NestedField(field_id=41, name="ligand_id", field_type=StringType(), required=False,
                doc="Ligand ChEBI ID"),
    NestedField(field_id=42, name="ligand_label", field_type=StringType(), required=False,
                doc="Ligand label"),
    NestedField(field_id=43, name="ligand_note", field_type=StringType(), required=False,
                doc="Ligand note"),
)


# ─── Sort Orders ──────────────────────────────────────────────────────
#
# Sorted by taxid so that queries for a single organism benefit from
# Iceberg file-level min/max statistics (data skipping).

ENTRIES_SORT_ORDER = SortOrder(
    SortField(source_id=10, transform=IdentityTransform()),   # taxid
)

FEATURES_SORT_ORDER = SortOrder(
    SortField(source_id=3, transform=IdentityTransform()),    # taxid
)


# ─── Convenience: column name lists for common query patterns ─────────

# Minimal entry summary (fast, small)
ENTRY_SUMMARY_COLUMNS = [
    "acc", "id", "reviewed", "taxid", "organism_name",
    "gene_name", "protein_name", "seq_length",
]

# Entry with sequence
ENTRY_WITH_SEQ_COLUMNS = ENTRY_SUMMARY_COLUMNS + ["sequence", "seq_mass", "seq_md5"]

# Entry with functional annotation shortcuts
ENTRY_FUNCTIONAL_COLUMNS = ENTRY_SUMMARY_COLUMNS + [
    "ec_numbers", "go_ids", "keyword_names", "protein_existence", "annotation_score",
]
