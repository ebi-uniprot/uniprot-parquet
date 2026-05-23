# Deduplication Proposal: Residual Nested Columns + Round-Trip Validation

**Status:** Revised after review — ready for second read
**Author:** Pipeline design discussion
**Date:** 2026-04-14
**Scope:** UniProtKB Parquet lake layout (branch `static-lake`)

### Revision history

- **v1** (initial draft) — proposed residual + `g` with per-array positional
  zipping for evidence splits.
- **v2** (this document) — incorporates review feedback:
  - **Rejected** positional zipping for evidences and reference comments.
    Full struct lists now live in the residual; convenience projections are
    pure duplicates of short fields.
  - **Promoted** `keywords.category` → new `keyword_categories` convenience
    column on `entries`.
  - **Promoted** `feature.location.sequence` → new `location_sequence`
    convenience column on `features`.
  - **Committed to both phases.** Phase 1 is the safety net, Phase 2 delivers
    the storage ROI — ship both.
  - **`g` is Python, in `tests/`**, optimised for maintainability.
  - **Schema evolution policy: default to convenience.**
  - **Single-release flip**, no deprecation window — JSONL serves as the
    fallback for any legacy consumer.

---

## TL;DR

The current lake stores each nested struct (`feature`, `comment`, `reference`, and
six structs inside `entries`) alongside convenience columns that extract flat,
queryable fields from the same data. Measurements show that roughly **40% of the
compressed lake** is data duplicated between these two representations. At full
UniProtKB scale (~248M entries) that is ~900 GB of redundant bytes in a ~2.4 TB
artifact.

This document evaluates **trimming the nested columns down to "residuals"** —
only fields not already captured by convenience columns — and introducing a
test-only reconstruction function `g` that proves `g(f(x)) == x` for every
release. The `xref` column has already been removed under the same reasoning
(it was a clean case with no residual).

The proposal is genuinely worth considering **because the lake is destined for
public distribution via EBI FTP/HTTP** alongside the canonical JSONL. In that
context, duplication has real bandwidth and download-time costs. For a purely
cluster-resident artifact, the current layout would be fine as-is.

---

## Context

Three facts drove this discussion:

1. The lake is static, read-heavy, and optimised for analytical SQL. Each entry
   is exploded into five sorted Parquet tables (entries, features, xrefs,
   comments, publications) with flat convenience columns for fast querying.
2. Each child table (and six fields inside `entries`) also carries a nested
   struct column that duplicates the full original JSON sub-tree. This was
   added to guarantee information completeness — no source field is silently
   dropped if UniProt adds new content.
3. The lake will be **published on EBI FTP/HTTP alongside the source JSONL**.
   Both artifacts will be available; users pick whichever format matches their
   workflow. Most users prefer flat/TSV-style access over raw JSON.

The combination of (2) and (3) exposes an inefficiency: the lake is
redistributing bytes that are also present in the JSONL, in duplicate, to every
downloader.

## Current state

After the `xref` column removal (already shipped), the lake has the following
nested columns:

| Table          | Nested column    | Size (sample) | % of table |
|----------------|------------------|---------------|------------|
| `entries`      | `organism`       | 64 KB         | 1.6%       |
| `entries`      | `protein_desc`   | 466 KB        | 11.9%      |
| `entries`      | `genes`          | 107 KB        | 2.8%       |
| `entries`      | `keywords`       | 269 KB        | 6.9%       |
| `entries`      | `organism_hosts` | 16 KB         | 0.4%       |
| `entries`      | `gene_locations` | 4 KB          | 0.1%       |
| `features`     | `feature`        | 3.7 MB        | 52.8%      |
| `xrefs`        | —                | (removed)     | —          |
| `comments`     | `comment`        | 4.1 MB        | 97.0%      |
| `publications` | `reference`      | 5.9 MB        | 50.0%      |

Sample is 4,591 diverse UniProtKB entries.

Per-table duplication (how much of each nested struct is also in convenience
columns):

| Table          | Duplication | Notes                                                 |
|----------------|-------------|-------------------------------------------------------|
| `features`     | ~60%        | location/ligand/evidences mostly duplicated           |
| `publications` | ~70%        | citation fields mostly duplicated                     |
| `entries`      | ~10%        | most convenience columns come from outside the 6 structs |
| `comments`     | ~1%         | comment struct is polymorphic and largely residual    |

## The proposal

Replace each full nested column with a **residual struct** that contains only
the fields not extracted by convenience columns. Anything the convenience
columns already capture is removed from the struct. The lake remains losslessly
reconstructible when combined with the residual, but at roughly 17% less
compressed size.

Introduce a **test-only reconstruction function `g`** that takes Parquet rows
(entries + joined child tables) and returns the original JSON shape. Use it to
validate the pipeline during CI and release candidates:

```
f(JSONL)     → Parquet
g(Parquet)   → JSONL
assert g(f(x)) == x
```

`g` is a pipeline correctness tool. It is **not** a user-facing API, because
users who want the original JSON will download the JSONL artifact instead.

### Residual field inventory (proposed)

**`features.feature`** →
  `{location.sequence, evidences[{source, id}], featureCrossReferences, ligandPart}`

**`publications.reference`** →
  `{citation.{bookName, editors, publisher, address, institute, patentNumber},
    evidences[{source, id}], referenceComments.evidences}`

**`comments.comment`** → keep as-is (1% duplication, not worth refactoring)

**`entries.organism`** → `{synonyms, evidences}`
**`entries.protein_desc`** → `{recommendedName.shortNames, alternativeNames.shortNames,
    cdAntigenNames, submissionNames, allergenName, innNames, contains, includes, evidences}`
**`entries.genes`** → `{orfNames, orderedLocusNames, evidences}`
**`entries.keywords`** → `{category, evidences}`
**`entries.organism_hosts`**, **`entries.gene_locations`** → keep as-is
  (zero duplication; no convenience columns extract them)

## Why the change is worth considering

**Public distribution changes the cost model.** A cluster-resident lake pays
storage once. A public artifact pays download bandwidth every time a user pulls
it, and mirror storage at every site that copies it. At EBI scale and across a
multi-year lifecycle, 400 GB of redundant bytes per release is not trivial.

**The lossless property is preserved.** `g(f(x)) == x` is enforced by CI. The
residual columns are directly queryable in SQL just like today — the user
experience for analytical queries is identical. The only change is that the
nested column no longer duplicates fields you can get more cheaply from flat
columns.

**The formal `(f, g)` framing is a publishable correctness property.** For a
bioinformatics data product that researchers will cite and build on, "we
validate round-trip equality against the source on every release" is a
stronger statement than "we store everything twice in case you want it."

**Most users prefer flat/TSV-style access anyway.** The nested column's "full
original JSON in one read" property is less valuable than initially assumed.
Users who want raw JSON pull the JSONL; users who want to query pull the
Parquet. The two artifacts play complementary roles.

## Why the change might not be worth it

**Engineering cost is real.** Writing `g`, maintaining a round-trip test, and
making per-field "convenience or residual?" decisions on every UniProt schema
change is permanent overhead. Today the pipeline has the nice property that
new fields are picked up automatically by the nested struct with zero code
changes.

**Storage is cheap and query performance is unaffected.** The current
duplication costs nothing at query time (Parquet column pruning means unused
columns aren't read). 2.4 TB is small for modern cluster storage. If
distribution bandwidth turns out not to be a bottleneck in practice, the
optimisation targets a non-problem.

**A subtle class of bugs becomes possible.** A bug in `g` that mis-reconstructs
a rare field could go undetected if the fixture doesn't exercise that code
path. The round-trip test has to be kept comprehensive; gaps in coverage
become silent correctness holes.

**"Residual" becomes a schema concept users need to understand.** Today a user
looking at the `features` table sees "here are the flat columns, and here's
`feature` with everything else." After the refactor: "here are the flat
columns, and here's `feature` with *the fields not already flat above*." That's
a subtle distinction that has to be documented.

## Alternatives considered

### Do nothing

Keep the current layout. Accept the duplication as the price of simplicity and
automatic schema evolution. Best if EBI distribution bandwidth turns out not
to be a real constraint.

### Drop nested columns entirely from child tables; use views for ergonomics

Replace `feature`, `reference`, etc. with SQL views that project struct-style
access from the flat convenience columns. This saves even more storage than
residual trimming but **loses information completeness** — any field not
captured by convenience columns is gone. Risky for a published artifact.

### Ship two layouts: "minimal" and "full"

Publish both a lean (convenience-only) and a complete (current layout) version
of the lake. Users pick what they need. Doubles the artifact volume EBI hosts
but gives users choice. Complex to maintain two pipelines and two
documentation surfaces.

### Residual trim + `g` *(this proposal)*

Middle ground. Single published layout that's smaller than "full" but still
provably complete via round-trip validation against the JSONL.

## Impact analysis

### Storage and bandwidth

| Metric                          | Current | After trim | Δ        |
|--------------------------------|---------|------------|----------|
| Sample lake (4,591 entries)    | 38.1 MB | ~31.5 MB   | −17%     |
| Projected full lake (~248M)    | ~2.4 TB | ~2.0 TB    | −400 GB  |
| Per-download savings            | —       | ~400 GB    | —        |

Savings scale with download volume over time.

### Query performance

No meaningful impact. Residual fields remain queryable via struct path syntax
(`SELECT feature.location.sequence FROM features`). Convenience columns are
unchanged. Parquet column pruning means unused data is never read.

### Maintenance

- `g` becomes ongoing code that must be updated when UniProt changes schema.
- CI gains a round-trip test (fast on fixtures, ~minutes on SLURM for full-scale).
- Every new UniProt field requires a "convenience or residual?" decision.

### User experience

- Users doing SQL queries: no change.
- Users wanting raw JSON: download the JSONL (as before).
- Users who relied on the nested column as a "full JSON escape hatch" in
  Parquet: affected, but this audience is small given (a) the JSONL is
  published alongside and (b) most users prefer flat/TSV access.

### Paper / EBI submission

Strengthens the correctness claim: "validated against the source JSONL via
round-trip testing on every release." Easier to defend than "stored twice for
redundancy."

## Implementation phases

### Phase 1 — Build `g`, validate current layout (low risk)

1. Write `reconstruct_entry(entry_row, features, xrefs, comments, publications) → dict`
   as a pure Python function in `tests/` or `bin/`.
2. Add `tests/test_roundtrip.py` that runs `f` on fixtures, then `g` on the
   result, asserts deep-equality against the original JSONL entries.
3. Wire the test into CI for fixtures.
4. Add a release-candidate script that runs `g(f(x)) == x` on the full
   UniProtKB release before publishing. Embarrassingly parallel via SLURM.
5. Ship Phase 1 as its own PR. No schema changes; lake layout unchanged.

**Value delivered by Phase 1 alone:** a provable lossless guarantee for the
current layout, without any storage refactor. Could be the stopping point if
the team decides the storage savings aren't worth the further complexity.

### Phase 2 — Refactor transform to emit residuals (higher risk)

1. Decide which fields belong in convenience vs residual per table
   (proposal: section above).
2. Rewrite `_build_features_sql`, `_build_publications_sql`, and the entries
   struct constructors to emit residual structs.
3. Update `TABLE_META`, `COLUMN_DESCRIPTIONS`, and `validate_lake.py` field
   completeness check.
4. Update `g` to combine convenience + residual correctly.
5. Re-run round-trip test; verify green.
6. Benchmark final storage vs baseline.

### Phase 3 — Publish, document, deprecate old layout

1. Ship the new layout as the next release.
2. Write `SCHEMA.md` explaining the convenience/residual split.
3. Note in the datapackage descriptor that the JSONL is the archival format
   and the Parquet is the analytical format.

Phase 1 de-risks Phase 2 entirely. Running Phase 1 first means you prove the
validation infrastructure works before changing the storage layout — any
failure after Phase 2 is unambiguously "the refactor broke something" rather
than "something in `g` was wrong all along."

## Decisions from review

The following decisions were made during review and are now incorporated into
the schema appendix below.

1. **EBI bandwidth savings justify the refactor.** 400 GB per release × every
   downloader × every mirror × every release is material. Proceed with Phase 2.
2. **Promote two residual fields to convenience:**
   - `keywords.category` → new convenience column `keyword_categories` on
     `entries` (highly valuable for `GROUP BY` / `WHERE`).
   - `feature.location.sequence` → new convenience column `location_sequence`
     on `features` (sequence strings are core to UniProt workflows).
   - `organism.synonyms` stays in residual (taxid / organism_name cover
     almost all filtering use cases).
3. **Both phases, no stopping at Phase 1.** Phase 1 is the safety net; Phase 2
   is the ROI. Ship both.
4. **`g` is Python in `tests/`.** Optimise for maintainability. Execution time
   of minutes (or a few hours single-threaded) is fine — round-trip is only a
   CI / release-candidate gate. No Rust/Cython.
5. **Schema evolution policy: default to convenience.** When UniProt adds a
   field, ask "will a researcher filter/group/join on this in the next year?"
   If yes → convenience column. If no (deep or obscure metadata) → residual.
   Write this into `CONTRIBUTING.md`.
6. **Flip in one release, no deprecation window.** The JSONL is published
   alongside the Parquet and serves as the lossless escape hatch for anyone
   dependent on the old structure. Call it a major version bump (Lake Schema
   v2.0) and rip the band-aid off.

## Critical technical note: no positional-zip splitting

An earlier draft of the schema split the `evidences` array by putting
`evidenceCode` values in a convenience list and `{source, id}` pairs in the
residual struct, relying on positional alignment to reconstruct them. **This
approach was rejected during review** as dangerously brittle:

- Any downstream tool that handles NULLs differently can shift the arrays out
  of phase.
- A user who filters one array but not the other silently corrupts data.
- Writing `g` to safely zip `list[string]` and `list[struct]` with correct
  handling of empty arrays, nulls, and filter-induced misalignment is
  error-prone.

**Revised rule: never split a logical array across convenience and residual
columns.** If a field inside an array (like `evidenceCode`) is valuable enough
to expose as a convenience column, copy it as a flat list, but leave the
**entire** `[{evidenceCode, source, id}]` struct in the residual. The storage
cost of duplicating a few short repeating strings is negligible versus the
data-corruption risk of positional zipping.

This rule applies everywhere in the schema: `evidences` in features and
publications, evidence sub-lists inside the entries residuals, and any future
array whose elements have fields we want both as flat convenience and as full
residual.

## Summary

| Question                                                 | Answer                                                         |
|----------------------------------------------------------|----------------------------------------------------------------|
| Does the current layout have real duplication?          | Yes, ~40% of lake bytes; ~400 GB at full scale.                |
| Does it cost anything today?                            | Storage only. Query performance is unaffected.                 |
| Does it cost anything if published at EBI?              | Yes — download bandwidth, mirror storage, user download time.  |
| Is trimming to residuals technically feasible?          | Yes, with a per-table residual struct and reconstruction `g`.  |
| Does it affect users?                                   | Minimally — SQL unchanged; raw JSON users pull the JSONL.      |
| Does it affect pipeline maintainers?                    | Yes — `g` must be maintained; schema changes need a decision.  |
| Is the correctness claim stronger?                      | Yes — round-trip validation becomes a release-blocking test.   |
| Is the refactor reversible?                             | Yes — the JSONL is the source of truth; rebuild with any `f`.  |

**Recommended sequencing if proceeding:** Phase 1 first (build `g`, validate
current layout). Decide after Phase 1 whether the storage savings of Phase 2
are worth the refactor in light of real data on how much duplication
actually compresses and ships.

---

## Appendix: Final schema after deduplication

This is the complete Parquet schema that would result from applying the
proposal (Phase 2). Convenience columns are unchanged from the current layout.
Nested columns are replaced with residual structs containing only fields that
are not captured by convenience columns. Tables are sorted and partitioned as
today.

Notation: `col_name :: type` — `{...}` denotes a struct; `[...]` denotes a list.

### `entries`

One row per UniProtKB entry. Sort order: `from_reviewed DESC, taxid ASC, acc ASC`.

```
-- Keys and identity
acc                :: string          -- primary accession (PK)
id                 :: string          -- UniProtKB ID (e.g. HEMA_HUMAN)
reviewed           :: bool            -- true if Swiss-Prot
entry_type         :: string          -- "UniProtKB reviewed (Swiss-Prot)" | "UniProtKB unreviewed (TrEMBL)"
secondary_accs     :: list[string]    -- obsolete/merged accessions
taxid              :: int64           -- NCBI taxonomy ID

-- Organism (convenience)
organism_name      :: string          -- scientific name
organism_common    :: string          -- common name
lineage            :: list[string]    -- taxonomic lineage (root → species)

-- Genes (convenience)
gene_names         :: list[string]    -- primary gene names
gene_synonyms      :: list[string]    -- gene name synonyms

-- Protein description (convenience)
protein_name       :: string          -- recommendedName.fullName
alt_protein_names  :: list[string]    -- alternativeNames.fullName (flattened)
protein_flag       :: string          -- e.g. "Precursor", "Fragment"
ec_numbers         :: list[string]    -- EC numbers

-- Protein existence and score
protein_existence  :: string          -- "Evidence at protein level" etc.
annotation_score   :: double          -- UniProt annotation score

-- Sequence
sequence           :: string          -- amino acid sequence
seq_length         :: int32
seq_mass           :: int32           -- average molecular mass (Da)
seq_md5            :: string
seq_crc64          :: string

-- Cross-reference / keyword summary (convenience)
go_ids             :: list[string]    -- GO term IDs from xrefs
xref_dbs           :: list[string]    -- distinct databases referenced
keyword_ids        :: list[string]    -- keyword accessions
keyword_names      :: list[string]    -- keyword names
keyword_categories :: list[string]    -- PROMOTED: keyword categories (aligned by index with keyword_ids)

-- Audit / versioning
first_public       :: date32
last_modified      :: date32
last_seq_modified  :: date32
entry_version      :: int32
seq_version        :: int32

-- Child table counts
feature_count      :: int32
xref_count         :: int32
comment_count      :: int32
reference_count    :: int32

-- Other
uniparc_id         :: string
extra_attributes   :: struct{         -- preserved as-is (complex, stable)
                       countByCommentType :: struct{...}
                       countByFeatureType :: struct{...}
                       uniParcId          :: string
                     }

-- Residual structs (NEW: trimmed to non-duplicated fields only)
-- Note: wherever a convenience column exposes a scalar/list extracted from
-- a struct, the full owning struct remains intact in the residual. Never
-- split an array across layers (see "positional-zip" rule in the main doc).

organism_residual       :: struct{
                             synonyms  :: list[string]
                             evidences :: list[struct{evidenceCode, source, id}]
                           }

protein_desc_residual   :: struct{
                             recommendedName_full :: struct{        -- whole sub-struct, un-split
                               fullName   :: struct{value, evidences}
                               shortNames :: list[struct{value, evidences}]
                               ecNumbers  :: list[struct{value, evidences}]
                             }
                             alternativeNames_full :: list[struct{  -- whole sub-struct
                               fullName   :: struct{value, evidences}
                               shortNames :: list[struct{value, evidences}]
                               ecNumbers  :: list[struct{value, evidences}]
                             }]
                             cdAntigenNames   :: list[struct{value, evidences}]
                             submissionNames  :: list[struct{fullName :: struct{value, evidences}}]
                             allergenName     :: struct{value, evidences}
                             innNames         :: list[struct{value, evidences}]
                             contains         :: list[struct{...}]   -- full "contains" subtree
                             includes         :: list[struct{...}]   -- full "includes" subtree
                           }

-- Full residual lists (not parallel arrays — each row of the outer list is
-- a complete gene/keyword record, self-contained, reconstructible without
-- any positional-zip against convenience columns).

genes_full              :: list[struct{                 -- one entry per gene, fully self-contained
                             geneName          :: struct{value, evidences}
                             synonyms          :: list[struct{value, evidences}]
                             orfNames          :: list[struct{value, evidences}]
                             orderedLocusNames :: list[struct{value, evidences}]
                           }]

keywords_full           :: list[struct{                 -- one entry per keyword, fully self-contained
                             id        :: string        -- duplicated into convenience keyword_ids
                             name      :: string        -- duplicated into convenience keyword_names
                             category  :: string        -- duplicated into convenience keyword_categories
                             evidences :: list[struct{evidenceCode, source, id}]
                           }]

organism_hosts          :: list[struct{              -- UNCHANGED (no convenience)
                             scientificName :: string
                             commonName     :: string
                             synonyms       :: list[string]
                             taxonId        :: int64
                           }]

gene_locations          :: list[struct{              -- UNCHANGED (no convenience)
                             geneEncodingType :: string
                             value            :: string
                             evidences        :: list[struct{evidenceCode, source, id}]
                           }]
```

### `features`

One row per sequence feature. Sort order: `acc ASC, start_pos ASC`.

```
-- Keys
acc                :: string          -- FK → entries.acc
from_reviewed      :: bool
taxid              :: int64

-- Entry context (convenience, denormalised)
organism_name      :: string
seq_length         :: int32

-- Core feature fields (convenience)
type                   :: string      -- "Chain", "Domain", "Active site", ...
start_pos              :: int32
end_pos                :: int32
start_modifier         :: string      -- "EXACT" | "OUTSIDE" | "UNKNOWN" | ...
end_modifier           :: string
description            :: string
feature_id             :: string
evidence_codes         :: list[string]   -- evidenceCode values only (for fast filters)
location_sequence      :: string         -- PROMOTED: location.sequence (core to many workflows)
original_sequence      :: string
alternative_sequences  :: list[string]
ligand_name            :: string
ligand_id              :: string
ligand_label           :: string
ligand_note            :: string

-- Residual struct (NEW)
feature_residual   :: struct{
                       evidences_full         :: list[struct{       -- full struct, no positional zipping
                                                   evidenceCode :: string
                                                   source       :: string
                                                   id           :: string
                                                 }]
                       featureCrossReferences :: list[struct{
                                                   database :: string
                                                   id       :: string
                                                 }]
                       ligandPart             :: struct{
                                                   name :: string
                                                   id   :: string
                                                   note :: string
                                                 }
                     }
```

### `xrefs`

One row per cross-reference to an external database. Sort order:
`from_reviewed DESC, taxid ASC, acc ASC`. **No nested column** (already trimmed
— the UniProt xref schema is flat and fully captured by convenience columns).

```
-- Keys
acc                :: string          -- FK → entries.acc
from_reviewed      :: bool
taxid              :: int64

-- Cross-reference fields (convenience = complete)
database           :: string          -- "PDB", "Ensembl", "GO", "InterPro", ...
id                 :: string          -- identifier in external DB
properties         :: list[struct{key, value}]  -- database-specific
isoform_id         :: string          -- if isoform-specific (nullable)
evidences          :: list[struct{evidenceCode, source, id}]
```

### `comments`

One row per comment annotation. Sort order: `acc ASC, comment_type ASC`.
Retained with its full nested column because comments are polymorphic and
~97% of the comment content is not captured by convenience columns
(redesigning comments into a fully flat schema would explode the column count
into hundreds of type-specific fields).

```
-- Keys
acc                :: string          -- FK → entries.acc
from_reviewed      :: bool
taxid              :: int64

-- Basic comment identity (convenience)
comment_type       :: string          -- "FUNCTION", "SUBCELLULAR LOCATION", ...
text_value         :: string          -- primary text for simple comment types (may be null)

-- Full comment struct (retained — see DEDUPLICATION_PROPOSAL.md)
comment            :: struct{...}     -- full polymorphic comment object as stored in source JSON
```

### `publications`

One row per literature reference. Sort order: `acc ASC, reference_number ASC`.

```
-- Keys
acc                :: string          -- FK → entries.acc
from_reviewed      :: bool
taxid              :: int64

-- Citation fields (convenience)
reference_number   :: int32
citation_type      :: string          -- "journal article", "book", "patent", ...
citation_id        :: string          -- PubMed ID for journal articles
title              :: string
authors            :: list[string]
authoring_group    :: list[string]
publication_date   :: string          -- as-is string (UniProt uses varied formats)
journal            :: string
volume             :: string
first_page         :: string
last_page          :: string
submission_database :: string
citation_xrefs     :: list[struct{database, id}]

-- Reference annotations (convenience)
reference_positions :: list[string]   -- e.g. "[1-50]", "VARIANT 340"
reference_comments  :: list[struct{type, value}]

-- Evidence codes (convenience)
evidences          :: list[string]    -- evidenceCode values only (for fast filters)

-- Residual struct (NEW)
reference_residual :: struct{
                       citation_extras :: struct{
                         bookName        :: string
                         editors         :: list[string]
                         publisher       :: string
                         address         :: string
                         institute       :: string
                         patentNumber    :: string
                       }
                       evidences_full       :: list[struct{       -- complete evidence list
                                                evidenceCode :: string
                                                source       :: string
                                                id           :: string
                                              }]
                       referenceComments_full :: list[struct{     -- complete reference comments,
                                                    type      :: string         -- self-contained
                                                    value     :: string
                                                    evidences :: list[struct{evidenceCode, source, id}]
                                                  }]
                     }
```

### Schema summary

| Table          | Convenience cols | Residual / full-struct columns                                   | Change from current            |
|----------------|------------------|------------------------------------------------------------------|--------------------------------|
| `entries`      | 38               | `organism_residual`, `protein_desc_residual`, `genes_full`, `keywords_full`, `organism_hosts`, `gene_locations` | 6 nested → 4 residual/full + 2 unchanged; `keyword_categories` promoted |
| `features`     | 19               | `feature_residual`                                               | `feature` → residual; `location_sequence` promoted |
| `xrefs`        | 8                | —                                                                | unchanged (already done)       |
| `comments`     | 5                | `comment`                                                        | unchanged                      |
| `publications` | 19               | `reference_residual`                                             | `reference` → residual         |

Convenience column count changes vs current:
- `entries`: +1 (`keyword_categories`)
- `features`: +1 (`location_sequence`)
- All others: unchanged.

### Notes on the schema

- **Evidence handling (no positional zipping).** Every residual that contains
  evidences stores the **complete** `[{evidenceCode, source, id}]` struct.
  Convenience columns that expose `evidence_codes` / `evidences` as flat
  `list[string]` are pure projections — `g` never reads them, it reads the
  full evidence list from the residual. The small duplication of short
  repeating code strings is the price of eliminating the zipping hazard.
- **"Full" residual lists (genes, keywords, referenceComments)** are
  self-contained: every element of the outer list is a complete record, not
  a fragment that needs to be re-aligned with a convenience list. `g`
  reconstructs from these directly.
- **Null safety**: every residual field is nullable. For entries where the
  source JSON has no residual content (e.g., a feature with no cross-references
  or ligandPart), the residual struct is fully null and costs almost nothing
  after Parquet compression.
- **`comment` column is kept intact** because residual trimming there saves
  ~1% while adding significant complexity to `g`.
- **Schema evolution policy**: when UniProt adds a field, ask "will a
  researcher filter / group / join on this in the next year?" If yes, flatten
  it into a convenience column. If it is deep or obscure metadata, drop it
  into the residual. Default is convenience. Write this policy into
  `CONTRIBUTING.md`.
- **Never split arrays across layers.** If a field inside a list of structs
  is valuable as a flat convenience list, copy it — do not "zip" the
  remainder back with the convenience list during reconstruction. `g` reads
  the full struct list from the residual.
