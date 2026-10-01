-- UniProtKB Parquet Data Lake — DuckDB Setup Views
--
-- Creates virtual views over the six lake tables so downstream queries
-- can use plain table names (entries, features, xrefs, comments, publications,
-- accession_map) without repeating read_parquet() globs.
--
-- Requires all six tables: DuckDB binds a view at creation, so a glob that
-- matches no files fails here.  For an entries-only copy of the lake use
-- uniprot_parquet.connect() (which reads manifest.json and stubs the missing
-- tables) or create only the views you have.  Globs also need a listable
-- filesystem; over plain HTTP use uniprot_parquet.connect(), which builds
-- the views from the manifest's explicit file lists.
--
-- Usage (local lake):
--
--   import duckdb
--   con = duckdb.connect()
--   base = '/path/to/2026_01/lake'
--   with open('setup_views.sql') as f:
--       con.sql(f.read().replace('${BASE}', base))
--
-- Usage (remote — EBI FTP via httpfs):
--
--   import duckdb
--   con = duckdb.connect()
--   con.sql("INSTALL httpfs; LOAD httpfs;")
--   base = 'https://ftp.ebi.ac.uk/pub/databases/uniprot/knowledgebase/parquet/2026_01/lake'
--   with open('setup_views.sql') as f:
--       con.sql(f.read().replace('${BASE}', base))
--
-- After setup, query with plain table names:
--   con.sql("SELECT * FROM entries WHERE organism_id = 9606 LIMIT 5")
--

-- ── Base views ─────────────────────────────────────────────────────────
-- Every table is Hive-partitioned as <table>/review_status=swissprot|trembl/.
-- The views read '<table>/*/*.parquet' with hive_partitioning = true and
-- REPLACE the stored `reviewed` column with (review_status = 'swissprot'):
-- the two are equal by construction (validator check 19), and deriving
-- `reviewed` from the path lets DuckDB prune whole files on
-- `WHERE reviewed = true` before opening any TrEMBL footer (verified with
-- EXPLAIN ANALYZE, 2026-09-17: 1 file read vs 2 without the partition key).
-- `review_status` itself is EXCLUDEd so the view schema equals the stored schema.
-- These hide the read_parquet() glob and give you clean table names.
-- Parquet predicate pushdown and column pruning work through views —
-- DuckDB pushes filters down to the row-group level automatically.

CREATE OR REPLACE VIEW entries AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/entries/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW features AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/features/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW xrefs AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/xrefs/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW comments AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/comments/*/*.parquet', hive_partitioning = true);
-- Named "publications" to match UniProt's entry page terminology.
-- ("references" is also a reserved word in SQL.)
CREATE OR REPLACE VIEW publications AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/publications/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW accession_map AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/accession_map/*/*.parquet', hive_partitioning = true);


-- ── Macros for common query patterns ───────────────────────────────────
-- These are parameterised table functions.  Call them like:
--   SELECT * FROM protein_card('P04637');
--   SELECT * FROM organism_features(9606, 'Domain');

-- Full annotation card for a single protein: entry + features + xrefs
-- Returns one row per feature/xref combination for a single accession.
-- Use this when you want everything about one protein at a glance.
CREATE OR REPLACE MACRO protein_card(target_acc) AS TABLE (
    SELECT
        e.accession, e.gene_primary, e.protein_name_primary, e.organism_name, e.organism_id,
        e.reviewed, e.length, e.proteinExistence, e.annotationScore,
        e.go_id, e.keyword, e.ec,
        e.feature_count, e.xref_count, e.comment_count, e.reference_count
    FROM entries e
    WHERE e.accession = target_acc
);


-- All features of a given type for an organism (every features column).
-- Example: SELECT * FROM organism_features(9606, 'Domain');
CREATE OR REPLACE MACRO organism_features(target_taxid, feature_type) AS TABLE (
    SELECT f.*
    FROM features f
    WHERE f.organism_id = target_taxid AND f.type = feature_type
    ORDER BY f.accession, f.start_pos
);


-- Cross-references for an organism, filtered to specific databases.
-- Example: SELECT * FROM organism_xrefs(9606, ['PDB', 'AlphaFoldDB']);
CREATE OR REPLACE MACRO organism_xrefs(target_taxid, databases) AS TABLE (
    SELECT x.* EXCLUDE (reviewed, organism_id, xref_index)
    FROM xrefs x
    WHERE x.organism_id = target_taxid AND list_contains(databases, x.database)
    ORDER BY x.accession, x.database
);


-- Functional annotations (comments) for an organism.
-- commentType values are plain strings: 'FUNCTION', 'SUBCELLULAR LOCATION', etc.
-- Example: SELECT * FROM organism_comments(9606, 'FUNCTION');
CREATE OR REPLACE MACRO organism_comments(target_taxid, ctype) AS TABLE (
    SELECT c.accession, c.commentType, c.text_value
    FROM comments c
    WHERE c.organism_id = target_taxid AND c.commentType = ctype
    ORDER BY c.accession
);


-- Join entries with features for an organism — filter first, join second.
-- Returns one row per feature, with entry-level columns attached.
-- Example: SELECT * FROM entries_with_features(9606) WHERE type = 'Signal';
CREATE OR REPLACE MACRO entries_with_features(target_taxid) AS TABLE (
    SELECT e.accession, e.gene_primary, e.protein_name_primary, e.reviewed,
           f.* EXCLUDE (accession, reviewed, organism_id, organism_name, length)
    FROM entries e
    JOIN features f ON f.accession = e.accession AND f.organism_id = e.organism_id
    WHERE e.organism_id = target_taxid
    ORDER BY e.accession, f.start_pos
);


-- Join entries with xrefs for an organism + specific databases.
-- Example: SELECT * FROM entries_with_xrefs(9606, ['PDB', 'Ensembl']);
CREATE OR REPLACE MACRO entries_with_xrefs(target_taxid, databases) AS TABLE (
    SELECT e.accession, e.gene_primary, e.protein_name_primary, e.reviewed,
           x.* EXCLUDE (accession, reviewed, organism_id, xref_index)
    FROM entries e
    JOIN xrefs x ON x.accession = e.accession AND x.organism_id = e.organism_id
    WHERE e.organism_id = target_taxid AND list_contains(databases, x.database)
    ORDER BY e.accession, x.database
);


-- Unnest isoforms for a single protein.
-- Parses the ALTERNATIVE PRODUCTS comment to extract isoform IDs,
-- names, sequence status, and variant sequence identifiers.
-- Useful for proteomics/mass-spec workflows that need to map
-- peptides to specific isoforms.
-- Example: SELECT * FROM unnest_isoforms('P04637');
CREATE OR REPLACE MACRO unnest_isoforms(target_acc) AS TABLE (
    SELECT
        i.accession,
        e.gene_primary,
        iso.name.value                  AS isoform_name,
        unnest(iso.isoformIds)          AS isoform_id,
        iso.isoformSequenceStatus       AS sequence_status,
        iso.sequenceIds                 AS variant_sequence_ids
    FROM (
        SELECT
            c.accession,
            unnest(from_json(to_json(c)->'$.isoforms', '[{"name":{"value":"VARCHAR"},"isoformIds":["VARCHAR"],"isoformSequenceStatus":"VARCHAR","sequenceIds":["VARCHAR"]}]')) AS iso
        FROM comments c
        WHERE c.commentType = 'ALTERNATIVE PRODUCTS'
          AND c.accession = target_acc
    ) i
    JOIN entries e ON e.accession = i.accession
);
