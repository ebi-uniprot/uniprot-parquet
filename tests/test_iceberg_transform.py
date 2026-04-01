"""Tests for the Iceberg transform pipeline against small.json.gz.

These tests run the full pipeline once (via the iceberg_lake session fixture)
and verify row counts, column types, data integrity, and Iceberg metadata.
"""

import pytest
from pyiceberg.catalog.sql import SqlCatalog


# ─── Expected values from small.json.gz ──────────────────────────────
EXPECTED_ENTRIES = 44
EXPECTED_FEATURES = 1451
EXPECTED_REVIEWED = 44
EXPECTED_UNREVIEWED = 0


@pytest.fixture(scope="session")
def catalog(iceberg_lake):
    return SqlCatalog(
        "uniprot",
        **{"uri": iceberg_lake["catalog_uri"], "warehouse": iceberg_lake["warehouse"]},
    )


@pytest.fixture(scope="session")
def entries_table(catalog):
    return catalog.load_table("uniprotkb.entries")


@pytest.fixture(scope="session")
def features_table(catalog):
    return catalog.load_table("uniprotkb.features")


@pytest.fixture(scope="session")
def xrefs_table(catalog):
    return catalog.load_table("uniprotkb.xrefs")


@pytest.fixture(scope="session")
def comments_table(catalog):
    return catalog.load_table("uniprotkb.comments")


# ─── Row counts ──────────────────────────────────────────────────────


class TestRowCounts:
    def test_entries_count(self, entries_table):
        snapshot = entries_table.current_snapshot()
        assert snapshot is not None
        count = int(snapshot.summary["total-records"])
        assert count == EXPECTED_ENTRIES

    def test_features_count(self, features_table):
        snapshot = features_table.current_snapshot()
        assert snapshot is not None
        count = int(snapshot.summary["total-records"])
        assert count == EXPECTED_FEATURES

    def test_feature_count_consistency(self, entries_table, features_table):
        """sum(entries.feature_count) should equal features row count."""
        import pyarrow.compute as pc

        fc_sum = 0
        for batch in entries_table.scan(
            selected_fields=("feature_count",)
        ).to_arrow_batch_reader():
            s = pc.sum(batch.column("feature_count")).as_py()
            if s is not None:
                fc_sum += s

        features_count = int(
            features_table.current_snapshot().summary["total-records"]
        )
        assert fc_sum == features_count

    def test_xrefs_count_positive(self, xrefs_table):
        snapshot = xrefs_table.current_snapshot()
        assert snapshot is not None
        count = int(snapshot.summary["total-records"])
        assert count > 0, "Expected xrefs rows"

    def test_xrefs_count_consistency(self, entries_table, xrefs_table):
        """sum(entries.xref_count) should equal xrefs row count."""
        import pyarrow.compute as pc

        xref_sum = 0
        for batch in entries_table.scan(
            selected_fields=("xref_count",)
        ).to_arrow_batch_reader():
            s = pc.sum(batch.column("xref_count")).as_py()
            if s is not None:
                xref_sum += s

        xrefs_count = int(
            xrefs_table.current_snapshot().summary["total-records"]
        )
        assert xref_sum == xrefs_count

    def test_comments_count_positive(self, comments_table):
        snapshot = comments_table.current_snapshot()
        assert snapshot is not None
        count = int(snapshot.summary["total-records"])
        assert count > 0, "Expected comments rows"

    def test_comments_count_consistency(self, entries_table, comments_table):
        """sum(entries.comment_count) should equal comments row count."""
        import pyarrow.compute as pc

        comment_sum = 0
        for batch in entries_table.scan(
            selected_fields=("comment_count",)
        ).to_arrow_batch_reader():
            s = pc.sum(batch.column("comment_count")).as_py()
            if s is not None:
                comment_sum += s

        comments_count = int(
            comments_table.current_snapshot().summary["total-records"]
        )
        assert comment_sum == comments_count


# ─── Single snapshot per table ───────────────────────────────────────


class TestSnapshots:
    def test_entries_single_snapshot(self, entries_table):
        """The entire entries write should produce exactly one snapshot
        (plus possibly one for set_properties)."""
        from pyiceberg.table.snapshots import Operation

        snapshots = list(entries_table.metadata.snapshots)
        append_snapshots = [
            s for s in snapshots
            if s.summary and s.summary.operation == Operation.APPEND
        ]
        assert len(append_snapshots) == 1, (
            f"Expected 1 append snapshot, got {len(append_snapshots)}"
        )

    def test_features_single_snapshot(self, features_table):
        from pyiceberg.table.snapshots import Operation

        snapshots = list(features_table.metadata.snapshots)
        append_snapshots = [
            s for s in snapshots
            if s.summary and s.summary.operation == Operation.APPEND
        ]
        assert len(append_snapshots) == 1, (
            f"Expected 1 append snapshot, got {len(append_snapshots)}"
        )

    def test_xrefs_single_snapshot(self, xrefs_table):
        from pyiceberg.table.snapshots import Operation

        snapshots = list(xrefs_table.metadata.snapshots)
        append_snapshots = [
            s for s in snapshots
            if s.summary and s.summary.operation == Operation.APPEND
        ]
        assert len(append_snapshots) == 1

    def test_comments_single_snapshot(self, comments_table):
        from pyiceberg.table.snapshots import Operation

        snapshots = list(comments_table.metadata.snapshots)
        append_snapshots = [
            s for s in snapshots
            if s.summary and s.summary.operation == Operation.APPEND
        ]
        assert len(append_snapshots) == 1


# ─── Schema checks ──────────────────────────────────────────────────


class TestSchema:
    def test_entries_required_columns(self, entries_table):
        schema = entries_table.schema()
        names = {f.name for f in schema.fields}
        required = {
            "acc", "id", "reviewed", "taxid", "organism_name", "gene_name",
            "protein_name", "sequence", "seq_length", "go_ids", "xref_dbs",
            "feature_count", "xref_count", "comment_count",
        }
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_features_required_columns(self, features_table):
        schema = features_table.schema()
        names = {f.name for f in schema.fields}
        required = {
            "acc", "reviewed", "taxid", "organism_name", "seq_length",
            "type", "start_pos", "end_pos", "description",
        }
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_xrefs_required_columns(self, xrefs_table):
        schema = xrefs_table.schema()
        names = {f.name for f in schema.fields}
        required = {"acc", "reviewed", "taxid", "database", "id", "properties"}
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_comments_required_columns(self, comments_table):
        schema = comments_table.schema()
        names = {f.name for f in schema.fields}
        required = {"acc", "reviewed", "taxid", "comment_type", "text_value", "comment"}
        missing = required - names
        assert not missing, f"Missing columns: {missing}"


# ─── Data integrity ─────────────────────────────────────────────────


class TestDataIntegrity:
    def test_reviewed_split(self, entries_table):
        import pyarrow.compute as pc

        arrow = entries_table.scan(selected_fields=("reviewed",)).to_arrow()
        reviewed = pc.sum(pc.cast(arrow.column("reviewed"), "int64")).as_py()
        total = arrow.num_rows

        assert reviewed == EXPECTED_REVIEWED
        assert total - reviewed == EXPECTED_UNREVIEWED

    def test_accessions_not_null(self, entries_table):
        arrow = entries_table.scan(selected_fields=("acc",)).to_arrow()
        assert arrow.column("acc").null_count == 0

    def test_taxid_populated(self, entries_table):
        """All entries in small.json.gz have organisms with taxonIds."""
        arrow = entries_table.scan(selected_fields=("taxid",)).to_arrow()
        assert arrow.column("taxid").null_count == 0

    def test_features_have_parent_acc(self, features_table):
        arrow = features_table.scan(selected_fields=("acc",)).to_arrow()
        assert arrow.column("acc").null_count == 0

    def test_features_have_type(self, features_table):
        arrow = features_table.scan(selected_fields=("type",)).to_arrow()
        assert arrow.column("type").null_count == 0

    def test_xrefs_have_database(self, xrefs_table):
        arrow = xrefs_table.scan(selected_fields=("database",)).to_arrow()
        assert arrow.column("database").null_count == 0

    def test_xrefs_have_id(self, xrefs_table):
        arrow = xrefs_table.scan(selected_fields=("id",)).to_arrow()
        assert arrow.column("id").null_count == 0

    def test_comments_have_type(self, comments_table):
        arrow = comments_table.scan(selected_fields=("comment_type",)).to_arrow()
        assert arrow.column("comment_type").null_count == 0

    def test_sample_accession_present(self, entries_table):
        """Check that a known accession from small.json.gz is in the table."""
        arrow = entries_table.scan(selected_fields=("acc",)).to_arrow()
        accs = set(arrow.column("acc").to_pylist())
        assert "A0A1L5YRA2" in accs

    def test_release_property(self, entries_table):
        assert entries_table.properties.get("uniprot.release") == "test_2026"


# ─── Sort order: reviewed DESC, taxid ASC ─────────────────────────
# Note: small.json.gz contains only reviewed (Swiss-Prot) entries, so
# the reviewed DESC ordering is trivially satisfied.  The production
# validator (validate_iceberg.py) tests the full two-column sort on
# datasets with both reviewed and unreviewed entries.


class TestSortOrder:
    def test_entries_sorted_by_reviewed_desc_taxid_asc(self, entries_table):
        """Verify entries are sorted by (reviewed DESC, taxid ASC)."""
        arrow = entries_table.scan(selected_fields=("reviewed", "taxid")).to_arrow()
        reviewed = arrow.column("reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        pairs = [(not r, t) for r, t in zip(reviewed, taxids)]
        assert pairs == sorted(pairs), "Entries not sorted by (reviewed DESC, taxid ASC)"

    def test_features_sorted_by_reviewed_desc_taxid_asc(self, features_table):
        arrow = features_table.scan(selected_fields=("reviewed", "taxid")).to_arrow()
        reviewed = arrow.column("reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        pairs = [(not r, t) for r, t in zip(reviewed, taxids)]
        assert pairs == sorted(pairs), "Features not sorted by (reviewed DESC, taxid ASC)"

    def test_xrefs_sorted_by_reviewed_desc_taxid_asc(self, xrefs_table):
        arrow = xrefs_table.scan(selected_fields=("reviewed", "taxid")).to_arrow()
        reviewed = arrow.column("reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        pairs = [(not r, t) for r, t in zip(reviewed, taxids)]
        assert pairs == sorted(pairs), "Xrefs not sorted by (reviewed DESC, taxid ASC)"

    def test_comments_sorted_by_reviewed_desc_taxid_asc(self, comments_table):
        arrow = comments_table.scan(selected_fields=("reviewed", "taxid")).to_arrow()
        reviewed = arrow.column("reviewed").to_pylist()
        taxids = arrow.column("taxid").to_pylist()
        pairs = [(not r, t) for r, t in zip(reviewed, taxids)]
        assert pairs == sorted(pairs), "Comments not sorted by (reviewed DESC, taxid ASC)"
