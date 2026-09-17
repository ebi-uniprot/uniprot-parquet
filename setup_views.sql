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
--   con.sql("SELECT * FROM entries WHERE taxid = 9606 LIMIT 5")
--

-- ── Base views ─────────────────────────────────────────────────────────
-- Every table is Hive-partitioned as <table>/review_status=swissprot|trembl/
-- (plan Part D).  The views read '<table>/*/*.parquet' with hive_partitioning
-- = true and REPLACE the stored `reviewed` column with (review_status =
-- 'swissprot'): the two are equal by construction (validator check 19), and
-- deriving `reviewed` from the path lets DuckDB prune whole files on
-- `WHERE reviewed = true` before opening any TrEMBL footer (verified with
-- EXPLAIN ANALYZE, D.3 spike, 2026-09-17: 1 file read vs 2 without the
-- partition key).  `review_status` itself is EXCLUDEd so the view schema
-- equals the stored schema.
-- These hide the read_parquet() glob and give you clean table names.
-- Parquet predicate pushdown and column pruning work through views —
-- DuckDB pushes filters down to the row-group level automatically.

CREATE OR REPLACE VIEW entries AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/entries/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW features AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/features/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW xrefs AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed)
    FROM read_parquet('${BASE}/xrefs/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW comments AS SELECT * EXCLUDE (review_status) REPLACE ((review_status = 'swissprot') AS reviewed, comment::JSON AS comment)
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
        e.acc,
        e.gene_name,
        e.protein_name,
        e.organism_name,
        e.taxid,
        e.reviewed,
        e.seq_length,
        e.protein_existence,
        e.annotation_score,
        e.go_ids,
        e.keyword_names,
        e.ec_numbers,
        e.feature_count,
        e.xref_count,
        e.comment_count,
        e.reference_count
    FROM entries e
    WHERE e.acc = target_acc
);


-- All features of a given type for an organism.
-- Example: SELECT * FROM organism_features(9606, 'Domain');
CREATE OR REPLACE MACRO organism_features(target_taxid, feature_type) AS TABLE (
    SELECT
        f.acc,
        f.type,
        f.start_pos,
        f.end_pos,
        f.description,
        f.feature_id,
        f.evidence_codes
    FROM features f
    WHERE f.taxid = target_taxid
      AND f.type = feature_type
    ORDER BY f.acc, f.start_pos
);


-- Cross-references for an organism, filtered to specific databases.
-- Example: SELECT * FROM organism_xrefs(9606, ['PDB', 'AlphaFoldDB']);
CREATE OR REPLACE MACRO organism_xrefs(target_taxid, databases) AS TABLE (
    SELECT
        x.acc,
        x.database,
        x.id,
        x.properties
    FROM xrefs x
    WHERE x.taxid = target_taxid
      AND list_contains(databases, x.database)
    ORDER BY x.acc, x.database
);


-- Functional annotations (comments) for an organism.
-- comment_type values are plain strings: 'FUNCTION', 'SUBCELLULAR LOCATION', etc.
-- Example: SELECT * FROM organism_comments(9606, 'FUNCTION');
CREATE OR REPLACE MACRO organism_comments(target_taxid, ctype) AS TABLE (
    SELECT
        c.acc,
        c.comment_type,
        c.text_value
    FROM comments c
    WHERE c.taxid = target_taxid
      AND c.comment_type = ctype
    ORDER BY c.acc
);


-- Join entries with features for an organism — filter first, join second.
-- Returns one row per feature, with entry-level columns attached.
-- Example: SELECT * FROM entries_with_features(9606) WHERE type = 'Signal';
CREATE OR REPLACE MACRO entries_with_features(target_taxid) AS TABLE (
    SELECT
        e.acc,
        e.gene_name,
        e.protein_name,
        e.reviewed,
        f.type,
        f.start_pos,
        f.end_pos,
        f.description,
        f.feature_id
    FROM entries e
    JOIN features f ON f.acc = e.acc AND f.taxid = e.taxid
    WHERE e.taxid = target_taxid
    ORDER BY e.acc, f.start_pos
);


-- Join entries with xrefs for an organism + specific databases.
-- Example: SELECT * FROM entries_with_xrefs(9606, ['PDB', 'Ensembl']);
CREATE OR REPLACE MACRO entries_with_xrefs(target_taxid, databases) AS TABLE (
    SELECT
        e.acc,
        e.gene_name,
        e.protein_name,
        e.reviewed,
        x.database,
        x.id,
        x.properties
    FROM entries e
    JOIN xrefs x ON x.acc = e.acc AND x.taxid = e.taxid
    WHERE e.taxid = target_taxid
      AND list_contains(databases, x.database)
    ORDER BY e.acc, x.database
);


-- Unnest isoforms for a single protein.
-- Parses the ALTERNATIVE PRODUCTS comment to extract isoform IDs,
-- names, sequence status, and variant sequence identifiers.
-- Useful for proteomics/mass-spec workflows that need to map
-- peptides to specific isoforms.
-- Example: SELECT * FROM unnest_isoforms('P04637');
CREATE OR REPLACE MACRO unnest_isoforms(target_acc) AS TABLE (
    SELECT
        i.acc,
        e.gene_name,
        iso.name.value                  AS isoform_name,
        unnest(iso.isoformIds)          AS isoform_id,
        iso.isoformSequenceStatus       AS sequence_status,
        iso.sequenceIds                 AS variant_sequence_ids
    FROM (
        SELECT
            acc,
            unnest(
                from_json(
                    comment->'$.isoforms',
                    '[{"name":{"value":"VARCHAR"},"isoformIds":["VARCHAR"],"isoformSequenceStatus":"VARCHAR","sequenceIds":["VARCHAR"]}]'
                )
            ) AS iso
        FROM comments
        WHERE comment_type = 'ALTERNATIVE PRODUCTS'
          AND acc = target_acc
    ) i
    JOIN entries e ON e.acc = i.acc
);
