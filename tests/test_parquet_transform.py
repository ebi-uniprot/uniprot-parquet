"""Tests for the Parquet transform pipeline against small.json.gz.

These tests run the full pipeline once (via the parquet_lake session fixture)
and verify row counts, column schemas, data integrity, and sort order.
"""

import json
import os

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest


# ─── Expected values from small.json.gz ──────────────────────────────
# The fixture contains a mix of Swiss-Prot (reviewed) and TrEMBL (unreviewed)
# entries to exercise both code paths in the pipeline.
EXPECTED_ENTRIES = 50
EXPECTED_FEATURES = 1325
EXPECTED_REVIEWED = 30
EXPECTED_UNREVIEWED = 20


def open_table(lake_dir, table_name):
    """Open a Parquet dataset from the lake directory."""
    return ds.dataset(os.path.join(lake_dir, table_name), format="parquet")


@pytest.fixture(scope="session")
def lake_dir(parquet_lake):
    return parquet_lake["lake_dir"]


@pytest.fixture(scope="session")
def entries_ds(lake_dir):
    return open_table(lake_dir, "entries")


@pytest.fixture(scope="session")
def features_ds(lake_dir):
    return open_table(lake_dir, "features")


@pytest.fixture(scope="session")
def xrefs_ds(lake_dir):
    return open_table(lake_dir, "xrefs")


@pytest.fixture(scope="session")
def comments_ds(lake_dir):
    return open_table(lake_dir, "comments")


@pytest.fixture(scope="session")
def publications_ds(lake_dir):
    return open_table(lake_dir, "publications")


# ─── Row counts ──────────────────────────────────────────────────────


class TestRowCounts:
    def test_entries_count(self, entries_ds):
        assert entries_ds.count_rows() == EXPECTED_ENTRIES

    def test_features_count(self, features_ds):
        assert features_ds.count_rows() == EXPECTED_FEATURES

    def test_feature_count_consistency(self, entries_ds, features_ds):
        """sum(entries.feature_count) should equal features row count."""
        fc_sum = 0
        for batch in entries_ds.to_batches(columns=["feature_count"]):
            s = pc.sum(batch.column("feature_count")).as_py()
            if s is not None:
                fc_sum += s
        assert fc_sum == features_ds.count_rows()

    def test_xrefs_count_positive(self, xrefs_ds):
        assert xrefs_ds.count_rows() > 0, "Expected xrefs rows"

    def test_xrefs_count_consistency(self, entries_ds, xrefs_ds):
        """sum(entries.xref_count) should equal xrefs row count."""
        xref_sum = 0
        for batch in entries_ds.to_batches(columns=["xref_count"]):
            s = pc.sum(batch.column("xref_count")).as_py()
            if s is not None:
                xref_sum += s
        assert xref_sum == xrefs_ds.count_rows()

    def test_comments_count_positive(self, comments_ds):
        assert comments_ds.count_rows() > 0, "Expected comments rows"

    def test_comments_count_consistency(self, entries_ds, comments_ds):
        """sum(entries.comment_count) should equal comments row count."""
        comment_sum = 0
        for batch in entries_ds.to_batches(columns=["comment_count"]):
            s = pc.sum(batch.column("comment_count")).as_py()
            if s is not None:
                comment_sum += s
        assert comment_sum == comments_ds.count_rows()

    def test_publications_count_positive(self, publications_ds):
        assert publications_ds.count_rows() > 0, "Expected publications rows"

    def test_publications_count_consistency(self, entries_ds, publications_ds):
        """sum(entries.reference_count) should equal publications row count."""
        ref_sum = 0
        for batch in entries_ds.to_batches(columns=["reference_count"]):
            s = pc.sum(batch.column("reference_count")).as_py()
            if s is not None:
                ref_sum += s
        assert ref_sum == publications_ds.count_rows()


# ─── Manifest ──────────────────────────────────────────────────────


class TestManifest:
    def test_manifest_exists(self, lake_dir):
        assert os.path.exists(os.path.join(lake_dir, "manifest.json"))

    def test_manifest_release(self, lake_dir):
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        assert manifest["release"] == "test_2026"

    def test_manifest_row_counts(self, lake_dir):
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        assert manifest["tables"]["entries"]["row_count"] == EXPECTED_ENTRIES
        assert manifest["tables"]["features"]["row_count"] == EXPECTED_FEATURES

    def test_manifest_files_match_disk(self, lake_dir):
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        for table_name, info in manifest["tables"].items():
            table_dir = os.path.join(lake_dir, table_name)
            actual = sorted(f for f in os.listdir(table_dir) if f.endswith(".parquet"))
            expected = sorted(info["files"])
            assert actual == expected, f"{table_name}: disk {actual} != manifest {expected}"


# ─── Schema checks ──────────────────────────────────────────────────


class TestSchema:
    def test_entries_required_columns(self, entries_ds):
        names = set(entries_ds.schema.names)
        required = {
            "acc", "id", "reviewed", "taxid", "organism_name", "gene_names",
            "gene_synonyms", "protein_name", "alt_protein_names",
            "sequence", "seq_length", "go_ids", "xref_dbs",
            "feature_count", "xref_count", "comment_count", "reference_count",
            "entry_type", "extra_attributes",
        }
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_features_required_columns(self, features_ds):
        names = set(features_ds.schema.names)
        required = {
            "acc", "from_reviewed", "taxid", "organism_name", "seq_length",
            "type", "start_pos", "end_pos", "description", "feature",
        }
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_xrefs_required_columns(self, xrefs_ds):
        names = set(xrefs_ds.schema.names)
        required = {
            "acc", "from_reviewed", "taxid", "database", "id", "properties",
            "isoform_id", "evidences",
        }
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_comments_required_columns(self, comments_ds):
        names = set(comments_ds.schema.names)
        required = {"acc", "from_reviewed", "taxid", "comment_type", "text_value", "comment"}
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_publications_required_columns(self, publications_ds):
        names = set(publications_ds.schema.names)
        required = {
            "acc", "from_reviewed", "taxid", "citation_type", "citation_id",
            "title", "authors", "publication_date",
            "reference_number", "evidences", "reference",
        }
        missing = required - names
        assert not missing, f"Missing columns: {missing}"


# ─── Data integrity ─────────────────────────────────────────────────


class TestDataIntegrity:
    def test_reviewed_split(self, entries_ds):
        arrow = entries_ds.to_table(columns=["reviewed"])
        reviewed = pc.sum(pc.cast(arrow.column("reviewed"), "int64")).as_py()
        total = arrow.num_rows

        assert reviewed == EXPECTED_REVIEWED
        assert total - reviewed == EXPECTED_UNREVIEWED

    def test_accessions_not_null(self, entries_ds):
        arrow = entries_ds.to_table(columns=["acc"])
        assert arrow.column("acc").null_count == 0

    def test_taxid_populated(self, entries_ds):
        arrow = entries_ds.to_table(columns=["taxid"])
        assert arrow.column("taxid").null_count == 0

    def test_features_have_parent_acc(self, features_ds):
        arrow = features_ds.to_table(columns=["acc"])
        assert arrow.column("acc").null_count == 0

    def test_features_have_type(self, features_ds):
        arrow = features_ds.to_table(columns=["type"])
        assert arrow.column("type").null_count == 0

    def test_xrefs_have_database(self, xrefs_ds):
        arrow = xrefs_ds.to_table(columns=["database"])
        assert arrow.column("database").null_count == 0

    def test_xrefs_have_id(self, xrefs_ds):
        arrow = xrefs_ds.to_table(columns=["id"])
        assert arrow.column("id").null_count == 0

    def test_comments_have_type(self, comments_ds):
        arrow = comments_ds.to_table(columns=["comment_type"])
        assert arrow.column("comment_type").null_count == 0

    def test_publications_have_citation_type(self, publications_ds):
        arrow = publications_ds.to_table(columns=["citation_type"])
        assert arrow.column("citation_type").null_count == 0

    def test_publications_have_acc(self, publications_ds):
        arrow = publications_ds.to_table(columns=["acc"])
        assert arrow.column("acc").null_count == 0

    def test_sample_accession_present(self, entries_ds):
        """Check that known accessions from small.json.gz are in the table."""
        arrow = entries_ds.to_table(columns=["acc"])
        accs = set(arrow.column("acc").to_pylist())
        # One reviewed and one unreviewed accession from the fixture
        assert "P02538" in accs, "Expected reviewed accession P02538"
        assert "A0A096MIX7" in accs, "Expected unreviewed accession A0A096MIX7"

    def test_entry_type_not_null(self, entries_ds):
        """entry_type preserves the original entryType string for lossless round-trip."""
        arrow = entries_ds.to_table(columns=["entry_type"])
        assert arrow.column("entry_type").null_count == 0

    def test_extra_attributes_populated(self, entries_ds):
        """extra_attributes preserves countByCommentType/countByFeatureType/uniParcId."""
        arrow = entries_ds.to_table(columns=["extra_attributes"])
        assert arrow.column("extra_attributes").null_count < arrow.num_rows

    def test_features_have_feature_struct(self, features_ds):
        """The feature column preserves the full original nested structure."""
        arrow = features_ds.to_table(columns=["feature"])
        assert arrow.column("feature").null_count == 0

    def test_publications_have_reference_number(self, publications_ds):
        """reference_number preserves the ordinal position from the original JSON."""
        arrow = publications_ds.to_table(columns=["reference_number"])
        assert arrow.column("reference_number").null_count == 0

    def test_publications_have_reference_struct(self, publications_ds):
        """The reference column preserves the full original nested structure."""
        arrow = publications_ds.to_table(columns=["reference"])
        assert arrow.column("reference").null_count == 0

    def test_comments_have_comment_struct(self, comments_ds):
        """The comment column preserves the full original nested structure."""
        arrow = comments_ds.to_table(columns=["comment"])
        assert arrow.column("comment").null_count == 0


# ─── Sort order: reviewed DESC, taxid ASC, acc ASC ───────────────
# small.json.gz contains both reviewed (Swiss-Prot) and unreviewed
# (TrEMBL) entries, so these tests exercise the full three-column
# sort including the reviewed DESC boundary.


class TestSortOrder:
    def test_entries_sorted_by_reviewed_desc_taxid_asc_acc_asc(self, entries_ds):
        """Verify entries are sorted by (reviewed DESC, taxid ASC, acc ASC)."""
        arrow = entries_ds.to_table(columns=["reviewed", "taxid", "acc"])
        reviewed = arrow.column("reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        accs = arrow.column("acc").to_pylist()
        keys = [(not r, t, a) for r, t, a in zip(reviewed, taxids, accs)]
        assert keys == sorted(keys), "Entries not sorted by (reviewed DESC, taxid ASC, acc ASC)"

    def test_features_sorted_by_from_reviewed_desc_taxid_asc_acc_asc_start_pos(self, features_ds):
        arrow = features_ds.to_table(columns=["from_reviewed", "taxid", "acc", "start_pos"])
        reviewed = arrow.column("from_reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        accs = arrow.column("acc").to_pylist()
        start_pos = [p if p is not None else float('inf') for p in arrow.column("start_pos").to_pylist()]
        keys = [(not r, t, a, s) for r, t, a, s in zip(reviewed, taxids, accs, start_pos)]
        assert keys == sorted(keys), "Features not sorted by (from_reviewed DESC, taxid ASC, acc ASC, start_pos ASC)"

    def test_xrefs_sorted_by_from_reviewed_desc_taxid_asc_acc_asc(self, xrefs_ds):
        arrow = xrefs_ds.to_table(columns=["from_reviewed", "taxid", "acc"])
        reviewed = arrow.column("from_reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        accs = arrow.column("acc").to_pylist()
        keys = [(not r, t, a) for r, t, a in zip(reviewed, taxids, accs)]
        assert keys == sorted(keys), "Xrefs not sorted by (from_reviewed DESC, taxid ASC, acc ASC)"

    def test_comments_sorted_by_from_reviewed_desc_taxid_asc_acc_asc(self, comments_ds):
        arrow = comments_ds.to_table(columns=["from_reviewed", "taxid", "acc"])
        reviewed = arrow.column("from_reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        accs = arrow.column("acc").to_pylist()
        keys = [(not r, t, a) for r, t, a in zip(reviewed, taxids, accs)]
        assert keys == sorted(keys), "Comments not sorted by (from_reviewed DESC, taxid ASC, acc ASC)"

    def test_publications_sorted_by_from_reviewed_desc_taxid_asc_acc_asc(self, publications_ds):
        arrow = publications_ds.to_table(columns=["from_reviewed", "taxid", "acc"])
        reviewed = arrow.column("from_reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        accs = arrow.column("acc").to_pylist()
        keys = [(not r, t, a) for r, t, a in zip(reviewed, taxids, accs)]
        assert keys == sorted(keys), "Publications not sorted by (from_reviewed DESC, taxid ASC, acc ASC)"
