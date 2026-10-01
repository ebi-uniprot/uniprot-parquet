"""Tests for the Parquet transform pipeline.

These tests run the full pipeline once (via the parquet_lake session fixture)
and verify row counts, column schemas, data integrity, and sort order.

The fixture is a diverse ~5K-entry dataset fetched from UniProtKB REST API
(viruses, fragments, multi-gene, isoforms, rare comment types, etc.).
Expected values are derived from the fixture at test time, not hardcoded.
"""

import gzip
import json
import os
import re

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from conftest import (DIVERSE_JSON_GZ, SMALL_JSON_GZ, _rows_by_acc, group_by_acc,  # noqa: F401
                      open_table, run_bin, table_files, write_jsonl_zst)
from parquet_transform import CHILD_TABLES



# ─── Derive expected values from the fixture JSON ──────────────────
# Computed once per session so tests don't depend on a specific fixture file.

@pytest.fixture(scope="session")
def expected(fixture_json_gz):
    """Compute expected row counts and accessions from the source JSON."""
    with gzip.open(fixture_json_gz, "rt") as f:
        data = json.load(f)
    entries = data["results"]

    reviewed = [e for e in entries if e.get("entryType", "").startswith("UniProtKB reviewed")]
    unreviewed = [e for e in entries if not e.get("entryType", "").startswith("UniProtKB reviewed")]

    total_features = sum(len(e.get("features", [])) for e in entries)
    total_xrefs = sum(len(e.get("uniProtKBCrossReferences", [])) for e in entries)
    total_comments = sum(len(e.get("comments", [])) for e in entries)
    total_refs = sum(len(e.get("references", [])) for e in entries)

    # Pick one reviewed and one unreviewed accession for spot-check
    reviewed_acc = reviewed[0]["primaryAccession"] if reviewed else None
    unreviewed_acc = unreviewed[0]["primaryAccession"] if unreviewed else None

    return {
        "entries": len(entries),
        "reviewed": len(reviewed),
        "unreviewed": len(unreviewed),
        "features": total_features,
        "xrefs": total_xrefs,
        "comments": total_comments,
        "publications": total_refs,
        "reviewed_acc": reviewed_acc,
        "unreviewed_acc": unreviewed_acc,
    }


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


@pytest.fixture(scope="session")
def accession_map_ds(lake_dir):
    return open_table(lake_dir, "accession_map")


# ─── Row counts ──────────────────────────────────────────────────────


class TestRowCounts:
    def test_entries_count(self, entries_ds, expected):
        assert entries_ds.count_rows() == expected["entries"]

    def test_features_count(self, features_ds, expected):
        assert features_ds.count_rows() == expected["features"]

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

    def test_accession_map_primary_count(self, entries_ds, accession_map_ds):
        """One is_primary row per entry."""
        flags = accession_map_ds.to_table(columns=["is_primary"]).column("is_primary")
        assert pc.sum(flags).as_py() == entries_ds.count_rows()

    def test_accession_map_secondary_count(self, entries_ds, accession_map_ds):
        """One non-primary row per secondary accession."""
        n_secs = sum(len(v or []) for v in entries_ds.to_table(
            columns=["secondaryAccessions"]).column("secondaryAccessions").to_pylist())
        flags = accession_map_ds.to_table(columns=["is_primary"]).column("is_primary")
        assert flags.length() - pc.sum(flags).as_py() == n_secs


# ─── Partitions ──────────────────────────────────────


class TestPartitions:
    PATH_RE = re.compile(r"^review_status=(swissprot|trembl)/([a-z_]+)_\d{5}\.parquet$")

    def test_only_partition_directories(self, lake_dir):
        """Every Parquet file lives in <table>/review_status=<side>/."""
        for table in EXPECTED_TABLES:
            table_dir = os.path.join(lake_dir, table)
            rels = [os.path.relpath(f, table_dir) for f in table_files(lake_dir, table)]
            assert rels, table
            for rel in rels:
                m = self.PATH_RE.match(rel)
                assert m and m.group(2) == table, f"{table}: unexpected path {rel}"
            assert {r.split("/")[0] for r in rels} == {"review_status=swissprot", "review_status=trembl"}

    def test_duckdb_hive_column_counts(self, lake_dir, expected):
        import duckdb
        rows = duckdb.sql(f"""
            SELECT review_status, count(*) FROM read_parquet('{lake_dir}/entries/*/*.parquet')
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        assert dict(rows) == {"swissprot": expected["reviewed"], "trembl": expected["unreviewed"]}
        cols = [r[0] for r in duckdb.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{lake_dir}/entries/*/*.parquet', hive_partitioning = false)"
        ).fetchall()]
        assert "review_status" not in cols and "reviewed" in cols
        n = duckdb.sql(f"SELECT count(*) FROM read_parquet('{lake_dir}/entries/**/*.parquet')").fetchone()[0]
        assert n == expected["entries"]
        n_sp = duckdb.sql(
            f"SELECT count(*) FROM read_parquet('{lake_dir}/entries/review_status=swissprot/*.parquet')"
        ).fetchone()[0]
        assert n_sp == expected["reviewed"]

    def test_polars_and_pyarrow_directory_reads(self, lake_dir, expected):
        import polars as pl
        assert pl.scan_parquet(os.path.join(lake_dir, "entries") + "/").select(pl.len()).collect().item() == expected["entries"]
        lf = pl.scan_parquet(os.path.join(lake_dir, "entries") + "/").filter(pl.col("review_status") == "swissprot")
        assert lf.select(pl.len()).collect().item() == expected["reviewed"]
        t = pq.read_table(os.path.join(lake_dir, "entries"))
        assert t.num_rows == expected["entries"] and "review_status" in t.column_names
        import pandas as pd
        df = pd.read_parquet(os.path.join(lake_dir, "entries"), columns=["accession", "reviewed"])
        assert len(df) == expected["entries"]

    def test_partition_side_matches_stored_flag(self, lake_dir):
        for table in EXPECTED_TABLES:
            for side, flag in (("swissprot", True), ("trembl", False)):
                for f in table_files(lake_dir, table):
                    if f"review_status={side}/" not in f:
                        continue
                    col = pq.read_table(f, columns=["reviewed"]).column("reviewed")
                    assert pc.all(pc.equal(col, flag)).as_py(), f"{f}: reviewed != {flag}"

    def test_manifest_partitioning_block(self, lake_dir, expected):
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        part = manifest["tables"]["entries"]["partitioning"]
        assert part["scheme"] == "hive"
        key = part["keys"][0]
        assert key["name"] == "review_status" and key["derived_from"] == "reviewed"
        assert key["values"] == ["swissprot", "trembl"]
        assert key["row_counts"] == {"swissprot": expected["reviewed"], "trembl": expected["unreviewed"]}
        assert "second partition level" in manifest["tables"]["accession_map"]["partitioning"]["note"]

    def test_mid_batch_flip(self, tmp_path):
        """A batch that straddles the Swiss-Prot/TrEMBL boundary is split correctly."""
        from conftest import SMALL_JSON_GZ, _json_gz_to_jsonl_zst, run_transform
        jsonl = _json_gz_to_jsonl_zst(SMALL_JSON_GZ, str(tmp_path / "small.jsonl.zst"))
        outdir = str(tmp_path / "lake")
        run_transform(jsonl, outdir, release="flip", extra_args=["--batch-size", "7"])
        with open(os.path.join(outdir, "manifest.json")) as f:
            manifest = json.load(f)
        for table, info in manifest["tables"].items():
            counts = info["partitioning"]["keys"][0]["row_counts"]
            assert set(counts) == {"swissprot", "trembl"}, table
            assert sum(counts.values()) == info["row_count"], table
        assert manifest["tables"]["entries"]["partitioning"]["keys"][0]["row_counts"] == {"swissprot": 31, "trembl": 21}


# ─── Manifest ──────────────────────────────────────────────────────


class TestManifest:
    def test_manifest_exists(self, lake_dir):
        assert os.path.exists(os.path.join(lake_dir, "manifest.json"))

    def test_manifest_release(self, lake_dir):
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        assert manifest["release"] == "test_2026"

    def test_manifest_row_counts(self, lake_dir, expected):
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        assert manifest["tables"]["entries"]["row_count"] == expected["entries"]
        assert manifest["tables"]["features"]["row_count"] == expected["features"]

    def test_versions_consistent(self, lake_dir):
        """manifest, datapackage and Parquet footers agree on the schema version."""
        from parquet_transform import MANIFEST_FORMAT_VERSION, SCHEMA_VERSION
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        assert manifest["version"] == MANIFEST_FORMAT_VERSION == 2
        assert manifest["schema_version"] == SCHEMA_VERSION == dp["version"]
        first = os.path.join(lake_dir, "entries", sorted(manifest["tables"]["entries"]["files"])[0])
        md = pq.read_schema(first).metadata
        assert md[b"schema_version"].decode() == manifest["schema_version"]
        assert md[b"uniprot_release"].decode() == manifest["release"]
        assert md[b"license"] == b"CC-BY-4.0"

    def test_license_file(self, lake_dir):
        path = os.path.join(lake_dir, "LICENSE")
        assert os.path.exists(path)
        with open(path) as f:
            text = f.read()
        assert text.startswith("This directory contains data derived from UniProtKB")
        assert "Creative Commons Attribution 4.0 International" in text

    def test_file_details_match_disk(self, lake_dir):
        """Every listed file exists with the recorded size, SHA-256 and a organism_id range."""
        import hashlib
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        for table, info in manifest["tables"].items():
            assert set(info["file_details"]) == set(info["files"]), table
            assert info["size_bytes"] == sum(d["size_bytes"] for d in info["file_details"].values())
            for rel, d in info["file_details"].items():
                path = os.path.join(lake_dir, table, rel)
                assert os.path.getsize(path) == d["size_bytes"], rel
                assert d["taxid_min"] is not None and d["taxid_min"] <= d["taxid_max"], rel
            rel = info["files"][0]
            with open(os.path.join(lake_dir, table, rel), "rb") as fh:
                assert hashlib.sha256(fh.read()).hexdigest() == info["file_details"][rel]["sha256"]

    def test_sha256sums_verifies(self, lake_dir):
        import shutil
        import subprocess
        if not shutil.which("sha256sum"):
            pytest.skip("sha256sum not available")
        result = subprocess.run(["sha256sum", "-c", "--quiet", "SHA256SUMS.txt"],
                                cwd=lake_dir, capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
        with open(os.path.join(lake_dir, "SHA256SUMS.txt")) as f:
            names = [line.split("  ", 1)[1].strip() for line in f]
        assert names[-3:] == ["manifest.json", "datapackage.json", "LICENSE"]
        assert "SHA256SUMS.txt" not in names

    def test_metalink_parses(self, lake_dir):
        import xml.etree.ElementTree as ET
        ns = {"m": "urn:ietf:params:xml:ns:metalink"}
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        n_parquet = sum(len(i["files"]) for i in manifest["tables"].values())
        root = ET.parse(os.path.join(lake_dir, "RELEASE.metalink")).getroot()
        files = root.findall("m:file", ns)
        assert len(files) == n_parquet + 3
        for fe in files:
            assert fe.find("m:hash[@type='sha-256']", ns).text and fe.find("m:size", ns).text.isdigit()
        # table directories hold Parquet only (directory readers choke on sidecars)
        for table in manifest["tables"]:
            assert not os.path.exists(os.path.join(lake_dir, table, "RELEASE.metalink"))

    def test_manifest_files_match_disk(self, lake_dir):
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        for table_name, info in manifest["tables"].items():
            table_dir = os.path.join(lake_dir, table_name)
            actual = sorted(os.path.relpath(f, table_dir) for f in table_files(lake_dir, table_name))
            expected_files = sorted(info["files"])
            assert actual == expected_files, f"{table_name}: disk {actual} != manifest {expected_files}"


# ─── Data Package (Frictionless) ──────────────────────────────────────


EXPECTED_TABLES = ["entries", "features", "xrefs", "comments", "publications", "accession_map"]
CHILD_INDEX = [(table, index) for table, (_, index, _) in CHILD_TABLES.items()]


class TestDataPackage:
    def test_datapackage_exists(self, lake_dir):
        assert os.path.exists(os.path.join(lake_dir, "datapackage.json"))

    def test_datapackage_valid_structure(self, lake_dir):
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        assert dp["name"] == "uniprot-parquet"
        assert "resources" in dp
        assert len(dp["resources"]) == len(EXPECTED_TABLES)

    def test_datapackage_all_tables_present(self, lake_dir):
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        names = {r["name"] for r in dp["resources"]}
        assert names == set(EXPECTED_TABLES)

    def test_datapackage_fields_have_descriptions(self, lake_dir):
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        for resource in dp["resources"]:
            for field in resource["schema"]["fields"]:
                assert field.get("description"), (
                    f"{resource['name']}.{field['name']} is missing a description"
                )

    def test_datapackage_fields_have_arrow_types(self, lake_dir):
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        for resource in dp["resources"]:
            for field in resource["schema"]["fields"]:
                assert "arrowType" in field, (
                    f"{resource['name']}.{field['name']} is missing arrowType"
                )
                assert "nullable" in field, (
                    f"{resource['name']}.{field['name']} is missing nullable"
                )

    def test_datapackage_foreign_keys(self, lake_dir):
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        resource_map = {r["name"]: r for r in dp["resources"]}
        # Child tables should have foreign keys to entries
        for child in ["features", "xrefs", "comments", "publications"]:
            fks = resource_map[child]["schema"]["foreignKeys"]
            fk_fields = {fk["fields"][0] for fk in fks}
            assert "accession" in fk_fields, f"{child} missing accession foreign key"
            assert "organism_id" in fk_fields, f"{child} missing organism_id foreign key"

    def test_accession_map_fk(self, lake_dir):
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        resource = {r["name"]: r for r in dp["resources"]}["accession_map"]
        fks = resource["schema"]["foreignKeys"]
        assert any(fk["fields"] == ["primary_accession"] and fk["reference"]["resource"] == "entries"
                   and fk["reference"]["fields"] == ["accession"] for fk in fks), fks

    def test_datapackage_matches_manifest(self, lake_dir):
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        for resource in dp["resources"]:
            name = resource["name"]
            m_table = manifest["tables"][name]
            # Field count must match
            assert len(resource["schema"]["fields"]) == len(m_table["columns"]), (
                f"{name}: datapackage has {len(resource['schema']['fields'])} fields "
                f"but manifest has {len(m_table['columns'])} columns"
            )
            # Row count must match
            assert resource["rowCount"] == m_table["row_count"]

    def test_licenses_are_the_data_license(self, lake_dir):
        """A Data Package's licenses apply to its data: CC BY 4.0 only (the
        pipeline code's MIT license is not the data's)."""
        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            dp = json.load(f)
        assert [lic["name"] for lic in dp["licenses"]] == ["CC-BY-4.0"]

    def test_datapackage_frictionless_spec(self, lake_dir):
        """Validate the descriptor against the official Frictionless Data Package spec."""
        from frictionless import Package

        with open(os.path.join(lake_dir, "datapackage.json")) as f:
            descriptor = json.load(f)
        report = Package.validate_descriptor(descriptor)
        assert report.valid, (
            f"datapackage.json failed Frictionless validation: "
            f"{report.stats['errors']} errors, {report.stats['warnings']} warnings"
        )


# ─── Schema checks ──────────────────────────────────────────────────


class TestSchema:
    def test_entries_required_columns(self, entries_ds):
        names = set(entries_ds.schema.names)
        required = {
            # shortcuts
            "accession", "id", "reviewed", "organism_id", "organism_name", "gene_primary",
            "gene_synonym", "protein_name_primary", "alt_protein_names", "length", "go_id",
            "xref_dbs", "feature_count", "xref_count", "comment_count", "reference_count",
            "go_terms", "xref_proteomes", "lit_pubmed_id", "division",
            "keyword_categories", "date_created",
            # source columns: the entry's keys
            "entryType", "primaryAccession", "uniProtkbId", "entryAudit", "organism",
            "proteinDescription", "sequence", "genes", "keywords", "extraAttributes",
        }
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_entries_hot_columns_first(self, entries_ds):
        """UniProt's default result columns, organism_id and the sequence are first
        and contiguous."""
        assert entries_ds.schema.names[:9] == [
            "accession", "id", "reviewed", "organism_id", "organism_name", "gene_primary",
            "protein_name_primary", "length", "sequence",
        ]
        assert entries_ds.schema.names[9] == "gene_synonym"

    def test_sequence_is_the_json_object(self, lake_dir, originals):
        """sequence is UniProt's object; reading sequence.value scans only
        that field of the column."""
        import duckdb
        path = os.path.join(lake_dir, "entries", "**", "*.parquet")
        accession, value = duckdb.sql(f"SELECT accession, sequence.value FROM read_parquet('{path}') LIMIT 1").fetchone()
        assert value == originals[accession]["sequence"]["value"]
        plan = duckdb.sql(f"EXPLAIN SELECT sequence.value FROM read_parquet('{path}')").fetchall()[0][1]
        assert "sequence.value" in plan, plan

    def test_field_metadata(self, lake_dir):
        """Every field of every table carries a non-empty description, a
        category and (but for derived shortcuts) its input path."""
        for table in EXPECTED_TABLES:
            first = table_files(lake_dir, table)[0]
            schema = pq.read_schema(first)
            for field in schema:
                md = field.metadata or {}
                assert md.get(b"description"), f"{table}.{field.name} has no description metadata"
                assert md.get(b"category") in (b"shortcut", b"source"), (table, field.name)
        md = pq.read_schema(table_files(lake_dir, "entries")[0]).field("organism_id").metadata
        assert md[b"source_path"] == b"organism.taxonId"
        assert md[b"category"] == b"shortcut"
        md = pq.read_schema(table_files(lake_dir, "features")[0]).field("ligand").metadata
        assert md[b"source_path"] == b"features.ligand"
        assert md[b"category"] == b"source"

    def test_entries_view_has_no_duplicate_columns(self, lake_dir):
        """The client view adds no column of its own (reviewed is replaced in place)."""
        import uniprot_parquet
        con = uniprot_parquet.connect(lake_dir)
        names = [r[0] for r in con.sql("DESCRIBE entries").fetchall()]
        assert names == open_table(lake_dir, "entries").schema.names

    def test_features_required_columns(self, features_ds):
        names = set(features_ds.schema.names)
        required = {
            "accession", "reviewed", "organism_id", "organism_name", "length", "feature_index",
            "start_pos", "end_pos", "ligand_id", "evidence_codes",
            "type", "location", "description", "featureId", "evidences", "ligand",
        }
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_xrefs_required_columns(self, xrefs_ds):
        names = set(xrefs_ds.schema.names)
        required = {"accession", "reviewed", "organism_id", "xref_index",
                    "database", "id", "properties", "isoformId", "evidences"}
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_comments_required_columns(self, comments_ds):
        names = set(comments_ds.schema.names)
        required = {"accession", "reviewed", "organism_id", "comment_index", "text_value",
                    "commentType", "texts", "note", "reaction", "isoforms", "subcellularLocations"}
        missing = required - names
        assert not missing, f"Missing columns: {missing}"

    def test_comments_text_value_is_string(self, comments_ds):
        assert str(comments_ds.schema.field("text_value").type) in ("string", "large_string")

    def test_publications_required_columns(self, publications_ds):
        names = set(publications_ds.schema.names)
        required = {"accession", "reviewed", "organism_id", "reference_index",
                    "referenceNumber", "citation", "referencePositions", "evidences"}
        missing = required - names
        assert not missing, f"Missing columns: {missing}"


# ─── Data integrity ─────────────────────────────────────────────────


def _describe(lake_dir, table):
    import duckdb
    path = os.path.join(lake_dir, table, "**", "*.parquet")
    return {r[0]: r[1] for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}


def _element_keys(entries):
    """{table: set of JSON keys} over ``entries``: an entries column per
    top-level key (child arrays excepted), a child table's per element key."""
    keys = {"entries": set()}
    for table, (array, _, _) in CHILD_TABLES.items():
        keys[table] = set()
    arrays = {array: table for table, (array, _, _) in CHILD_TABLES.items()}
    for e in entries:
        for k, v in e.items():
            if k in arrays:
                for element in v:
                    keys[arrays[k]].update(element)
            else:
                keys["entries"].add(k)
    return keys


class TestShortcutAndSourceColumns:
    """Shortcut columns keep their declared type in every build; every other
    column is a source column: one per JSON key, typed as the input."""

    def test_declared_types_match_column_types(self, lake_dir):
        """Every COLUMN_TYPES entry matches the built table (the validator's check, in-process)."""
        from parquet_transform import COLUMN_TYPES
        for (table, col), expected in COLUMN_TYPES.items():
            actual = _describe(lake_dir, table).get(col)
            assert actual == expected, f"{table}.{col}: {actual} != {expected}"

    def test_shortcut_types_identical_subset_vs_full(self, lake_dir, small_lake):
        """The 52-entry subset and the full fixture agree on every shortcut."""
        from parquet_transform import TABLE_META
        for table in EXPECTED_TABLES:
            full, subset = _describe(lake_dir, table), _describe(small_lake["lake_dir"], table)
            for col in TABLE_META[table]["columns"]["shortcut"]:
                assert subset[col] == full[col], f"{table}.{col}: {subset[col]} != {full[col]}"

    def test_source_columns_are_the_json_keys(self, lake_dir, fixture_json_gz):
        """Every table's columns are its shortcuts plus exactly the keys its
        JSON elements have in the input, under the same names."""
        from parquet_transform import TABLE_META
        with gzip.open(fixture_json_gz, "rt") as f:
            keys = _element_keys(json.load(f)["results"])
        for table in keys:
            names = open_table(lake_dir, table).schema.names
            shortcuts = TABLE_META[table]["columns"]["shortcut"]
            assert [n for n in names if n in shortcuts] == [s for s in shortcuts if s in names]
            assert set(shortcuts) <= set(names), table
            assert set(names) - set(shortcuts) == keys[table], table

    def test_manifest_categories(self, lake_dir):
        """manifest.json splits every table's columns into shortcut and source."""
        from parquet_transform import TABLE_META
        with open(os.path.join(lake_dir, "manifest.json")) as f:
            manifest = json.load(f)
        for table, info in manifest["tables"].items():
            cats = info["column_categories"]
            names = [c["name"] for c in info["columns"]]
            assert cats["shortcut"] == TABLE_META[table]["columns"]["shortcut"], table
            assert cats["source"] == [n for n in names if n not in cats["shortcut"]], table

    def test_absent_key_is_no_column(self, small_lake, lake_dir):
        """A key no entry of the input has is no column: the column set
        follows the input (the small fixture has no geneLocations)."""
        assert "geneLocations" not in _top_level_keys(SMALL_JSON_GZ), \
            "small.json.gz gained geneLocations; pick another absent field"
        assert "geneLocations" not in open_table(small_lake["lake_dir"], "entries").schema.names
        assert "geneLocations" in open_table(lake_dir, "entries").schema.names

    def test_conflicting_key_is_a_json_column(self, lake_dir):
        """comments.note differs in shape between comment types: its column
        is Parquet JSON, which DuckDB reads as JSON (->> needs no cast)."""
        import duckdb
        import pyarrow as pa
        assert isinstance(open_table(lake_dir, "comments").schema.field("note").type, pa.JsonType)
        path = os.path.join(lake_dir, "comments", "**", "*.parquet")
        kind, text = duckdb.sql(f"SELECT typeof(note), note->>'$.texts[0].value' FROM read_parquet('{path}') "
                                f"WHERE note IS NOT NULL LIMIT 1").fetchone()
        assert kind == "JSON" and text

    def test_tier_consistency(self):
        """COLUMN_TYPES declares exactly the shortcut columns."""
        from parquet_transform import COLUMN_TYPES, TABLE_META
        for table, col in COLUMN_TYPES:
            assert col in TABLE_META[table]["columns"]["shortcut"], (table, col)
        for table, meta in TABLE_META.items():
            for col in meta["columns"]["shortcut"]:
                assert (table, col) in COLUMN_TYPES, f"{table}.{col} has no declared type"


def _lake_rows(lake):
    """{table: {accession: [row, ...]}} for the entries and child tables of ``lake``."""
    return {t: _rows_by_acc(lake, t) for t in ("entries", *CHILD_TABLES)}


def _reconstruction_failures(entries, rows):
    """accession: first diff for every entry that does not rebuild exactly from ``rows``
    ({table: {accession: [row, ...]}})."""
    from reconstruct import reconstruct_entry, entries_match
    failures = []
    for e in entries:
        accession = e["primaryAccession"]
        rebuilt = reconstruct_entry(rows["entries"][accession][0], rows["features"].get(accession, []),
                                    rows["xrefs"].get(accession, []), rows["comments"].get(accession, []),
                                    rows["publications"].get(accession, []))
        ok, diff = entries_match(rebuilt, e)
        if not ok:
            failures.append(f"{accession}: {diff}")
    return failures


def _small_entries():
    with gzip.open(SMALL_JSON_GZ, "rt") as f:
        return json.load(f)["results"]


def _build_lake(entries, out, release):
    """Write ``entries`` as JSONL.zst under ``out`` and transform them;
    (lake dir, JSONL path)."""
    from conftest import run_transform

    jsonl = write_jsonl_zst(entries, str(out / f"{release}.jsonl.zst"))
    lake = str(out / "lake")
    run_transform(jsonl, lake, release=release)        # must not fail
    return lake, jsonl


@pytest.fixture(scope="module")
def additive_lake(tmp_path_factory, small_lake):
    """The small fixture with fields UniProt does not (yet) ship injected at
    every level (top level and nested, in entries and in every child table's
    elements), plus keys whose type differs between entries (inferred as
    JSON): one top-level, one in extraAttributes."""
    entries = _small_entries()
    first = lambda field: next(e for e in entries if e.get(field))  # noqa: E731
    e0 = entries[0]
    e0["brandNewTop"] = {"x": 1, "y": ["a"]}
    e0["entryAudit"]["newAuditField"] = "audit"
    e0["sequence"]["newSeqField"] = 7
    e0["organism"]["newOrgField"] = "org"
    e0.setdefault("extraAttributes", {})["newAttr"] = {"k": 1}
    e0.setdefault("organismHosts", [{"scientificName": "Homo sapiens", "taxonId": 9606}])
    e0["organismHosts"][0]["brandNew"] = "host"
    xref = first("uniProtKBCrossReferences")["uniProtKBCrossReferences"][0]
    xref["newXrefField"] = "xr"
    feature = first("features")["features"][0]
    feature["location"]["newLocField"] = "loc"
    feature["location"]["start"]["newStartField"] = 3
    reference = first("references")["references"][0]
    reference["citation"]["newCitationField"] = "cit"
    reference["newRefField"] = "ref"
    comment = first("comments")["comments"][0]
    comment["newCommentField"] = "com"
    entries[1]["weird"] = "a"
    entries[2]["weird"] = {"b": 1}
    entries[1].setdefault("extraAttributes", {})["weirdAttr"] = "a"
    entries[2].setdefault("extraAttributes", {})["weirdAttr"] = {"b": 1}

    lake, jsonl = _build_lake(entries, tmp_path_factory.mktemp("additive"), "additive_2026")
    return {"lake_dir": lake, "jsonl": jsonl, "entries": entries,
            "accs": {"e0": e0["primaryAccession"]},
            "previous_manifest": os.path.join(small_lake["lake_dir"], "manifest.json")}


class TestNewUniProtFields:
    """A field UniProt adds becomes a source column (a new key) or appears
    inside its struct (a new nested key): never dropped and never a build
    failure, and the lake still rebuilds every entry exactly."""

    def test_every_entry_reconstructs(self, additive_lake):
        lake = additive_lake["lake_dir"]
        rows = _lake_rows(lake)
        failures = _reconstruction_failures(additive_lake["entries"], rows)
        assert not failures, f"{len(failures)} entries differ; first: {failures[0]}"

    def test_new_keys_are_columns(self, additive_lake):
        lake, accession = additive_lake["lake_dir"], additive_lake["accs"]["e0"]
        e = _rows_by_acc(lake, "entries")[accession][0]
        assert e["brandNewTop"] == {"x": 1, "y": ["a"]}
        assert e["entryAudit"]["newAuditField"] == "audit"
        assert e["sequence"]["newSeqField"] == 7
        assert e["organism"]["newOrgField"] == "org"
        assert e["extraAttributes"]["newAttr"] == {"k": 1}
        assert e["organismHosts"][0]["brandNew"] == "host"

    def test_new_child_keys_are_columns(self, additive_lake):
        lake = additive_lake["lake_dir"]
        rows = {t: [r for rs in _rows_by_acc(lake, t).values() for r in rs] for t in CHILD_TABLES}
        assert "xr" in [r["newXrefField"] for r in rows["xrefs"]]
        assert "com" in [r["newCommentField"] for r in rows["comments"]]
        assert "ref" in [r["newRefField"] for r in rows["publications"]]
        assert "cit" in [r["citation"].get("newCitationField") for r in rows["publications"]]
        locs = [r["location"] for r in rows["features"]]
        assert any(loc.get("newLocField") == "loc" and loc["start"].get("newStartField") == 3
                   for loc in locs)

    def test_new_columns_are_described_as_new(self, additive_lake):
        """A new key gets a generated description, the source category and
        its input path."""
        md = pq.read_schema(table_files(additive_lake["lake_dir"], "xrefs")[0]).field("newXrefField").metadata
        assert b"not yet described" in md[b"description"]
        assert md[b"category"] == b"source"
        assert md[b"source_path"] == b"uniProtKBCrossReferences.newXrefField"

    def test_type_conflict_column_is_json(self, additive_lake):
        """A key whose type differs between entries is a JSON column, as is
        the object holding such a key (extraAttributes.weirdAttr)."""
        import duckdb
        import pyarrow as pa
        schema = open_table(additive_lake["lake_dir"], "entries").schema
        assert isinstance(schema.field("weird").type, pa.JsonType)
        assert isinstance(schema.field("extraAttributes").type, pa.JsonType)
        path = os.path.join(additive_lake["lake_dir"], "entries", "**", "*.parquet")
        assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{path}') "
                          f"WHERE weird->>'$.b' = '1'").fetchone()[0] == 1

    def test_shortcut_types_unchanged(self, additive_lake, small_lake):
        from parquet_transform import TABLE_META
        for table in EXPECTED_TABLES:
            added = _describe(additive_lake["lake_dir"], table)
            base = _describe(small_lake["lake_dir"], table)
            for col in TABLE_META[table]["columns"]["shortcut"]:
                assert added[col] == base[col], f"{table}.{col}"

    def test_validator_passes_and_reports_new_fields(self, additive_report):
        returncode, text = additive_report
        assert returncode == 0, text
        new_entries = next(line for line in text.splitlines()
                           if "entries: new UniProt fields (source columns without a description)" in line)
        assert set(new_entries.split("—")[1].strip().split(", ")) == {"brandNewTop", "weird"}
        assert "xrefs: new UniProt fields (source columns without a description)  —  newXrefField" in text
        # the schema diff against the unmodified small lake names each change
        assert "entries: column added: brandNewTop" in text
        assert "xrefs: column added: newXrefField" in text
        assert "entries: column retyped: organism  —  fields added: newOrgField" in text
        assert "shortcut schema changed" not in text
        assert "VALIDATION FAILED" not in text


def _validation_report(lake, jsonl, out_dir, *extra):
    """(exit code, report text) of validate_lake.py on ``lake``."""
    report = str(out_dir / "report.txt")
    result = run_bin("validate_lake.py", "--lake", lake, "--jsonl", jsonl,
                      "--spot-check-n", "100", "-o", report, *extra)
    with open(report) as f:
        return result.returncode, f.read()


@pytest.fixture(scope="module")
def additive_report(additive_lake, tmp_path_factory):
    """(exit code, report text) of validate_lake.py on the additive lake,
    diffed against the unmodified small lake."""
    return _validation_report(additive_lake["lake_dir"], additive_lake["jsonl"],
                              tmp_path_factory.mktemp("additive_report"),
                              "--previous-manifest", additive_lake["previous_manifest"])


# Strings DuckDB's JSON reader would otherwise infer as TIMESTAMP (in two
# notations), TIME, UUID and DATE.
TEMPORAL = {"stampZ": "2026-03-04T05:06:07Z", "stampOffset": "2026-03-04T05:06:07+02:00",
            "clock": "12:00:00", "uuid": "8f14e45f-ceea-467a-9575-6c8f5e0e4b8a",
            "day": "2026-03-04"}


def _inject_temporal(entries):
    """Put every TEMPORAL string, in every entry, at the top level, in
    organism, and in every cross-reference, feature location and citation,
    so each is inferred with that type rather than as VARCHAR."""
    for e in entries:
        e["auditStamp"] = TEMPORAL["stampZ"]
        e["organism"]["seen"] = dict(TEMPORAL)
        for x in e.get("uniProtKBCrossReferences", []):
            x["retrieved"] = TEMPORAL["day"]
        for f in e.get("features", []):
            f["location"]["checkedAt"] = TEMPORAL["stampOffset"]
        for r in e.get("references", []):
            r["citation"]["fetched"] = TEMPORAL["clock"]
    return entries


@pytest.fixture(scope="module")
def temporal_lake(tmp_path_factory):
    """The small fixture with date-, time- and UUID-shaped strings added at
    every level, in every entry."""
    entries = _inject_temporal(_small_entries())
    out = tmp_path_factory.mktemp("temporal")
    lake, jsonl = _build_lake(entries, out, "temporal_2026")
    return {"lake_dir": lake, "jsonl": jsonl, "entries": entries, "out": out}


class TestTemporalStrings:
    """A string is stored as the same string, whatever it looks like: DuckDB
    must not turn a timestamp-, time-, date- or UUID-shaped value into a
    typed value that reconstructs differently (a new UniProt field is just
    a new field; README §2)."""

    def test_staged_types_are_strings(self, tmp_path):
        entry = dict(TestOptionalPathGuards.MINIMAL, **TEMPORAL,
                     nested={**TEMPORAL, "items": [dict(TEMPORAL)]})
        _, read_clause, _ = TestOptionalPathGuards._stage([entry], tmp_path)
        staged = pq.read_schema(read_clause.split("'")[1])
        for name in TEMPORAL:
            assert str(staged.field(name).type) == "string", name
            assert str(staged.field("nested").type.field(name).type) == "string", name
            assert str(staged.field("nested").type.field("items").type.value_type
                       .field(name).type) == "string", name
        row = pq.read_table(read_clause.split("'")[1]).to_pylist()[0]
        assert {k: row[k] for k in TEMPORAL} == TEMPORAL
        assert {k: row["nested"]["items"][0][k] for k in TEMPORAL} == TEMPORAL

    def test_every_entry_reconstructs(self, temporal_lake):
        lake = temporal_lake["lake_dir"]
        rows = _lake_rows(lake)
        failures = _reconstruction_failures(temporal_lake["entries"], rows)
        assert not failures, f"{len(failures)} entries differ; first: {failures[0]}"

    def test_validator_passes(self, temporal_lake, tmp_path):
        returncode, text = _validation_report(temporal_lake["lake_dir"], temporal_lake["jsonl"],
                                              tmp_path)
        assert returncode == 0, text
        assert "ALL CHECKS PASSED" in text

    def test_sort_keeps_strings(self, temporal_lake):
        """SORT_JSONL writes the JSONL VALIDATE treats as ground truth, so a
        string it rewrites would pass validation unnoticed."""
        import zstandard as zstd
        out = temporal_lake["out"] / "sorted.jsonl.zst"
        result = run_bin("sort_jsonl.py", temporal_lake["jsonl"], "-o", out, "--memory-limit", "4GB")
        assert result.returncode == 0, result.stderr
        with open(out, "rb") as f:
            text = zstd.ZstdDecompressor().stream_reader(f).read().decode()
        sorted_entries = {e["primaryAccession"]: e for e in map(json.loads, text.splitlines())}
        assert len(sorted_entries) == len(temporal_lake["entries"])
        for e in sorted_entries.values():
            assert e["auditStamp"] == TEMPORAL["stampZ"]
            assert e["organism"]["seen"] == TEMPORAL
            assert {x["retrieved"] for x in e.get("uniProtKBCrossReferences", [])} <= {TEMPORAL["day"]}
            assert {f["location"]["checkedAt"] for f in e.get("features", [])} <= {TEMPORAL["stampOffset"]}
            assert {r["citation"]["fetched"] for r in e.get("references", [])} <= {TEMPORAL["clock"]}


@pytest.fixture(scope="module")
def case_dup_jsonl(tmp_path_factory):
    """The small fixture as JSONL with organism keys "ab" and "AB"."""
    entries = _small_entries()
    entries[0]["organism"]["ab"] = 1
    entries[1]["organism"]["AB"] = 2
    path = tmp_path_factory.mktemp("case_dup") / "in.jsonl"
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))
    return path


def test_duckdb_out_of_memory_exits_75(small_jsonl, tmp_path):
    """A DuckDB out-of-memory error exits 75 (EX_TEMPFAIL): the workflow
    retries it with twice the memory."""
    result = run_bin("parquet_transform.py", small_jsonl, "--outdir", tmp_path / "lake",
                     "--memory-limit", "50MB", "--threads", "2")
    assert result.returncode == 75, result.stderr[-2000:]
    assert "DuckDB ran out of memory" in result.stderr


def test_unknown_entry_type_is_refused(tmp_path):
    """A reworded entryType would put every Swiss-Prot entry in the trembl
    partition with reviewed = false: the build stops, naming the value."""
    entries = _small_entries()
    for e in entries:
        if e["entryType"] == "UniProtKB reviewed (Swiss-Prot)":
            e["entryType"] = "UniProtKB reviewed (SwissProt)"
    jsonl = write_jsonl_zst(entries, str(tmp_path / "in.jsonl.zst"))
    result = run_bin("parquet_transform.py", jsonl, "--outdir", tmp_path / "lake",
                     "--memory-limit", "4GB")
    assert result.returncode != 0
    assert "UniProtKB reviewed (SwissProt)" in result.stderr
    assert not os.path.exists(tmp_path / "lake" / "entries")


class TestCaseDuplicateKeys:
    """Sibling keys differing only in case cannot share a DuckDB struct: the
    build stops with a message naming the key, not DuckDB's internal error."""

    def test_refused_with_clear_message(self, case_dup_jsonl, tmp_path):
        result = run_bin("parquet_transform.py", case_dup_jsonl, "--outdir", tmp_path / "lake",
                         "--memory-limit", "4GB")
        assert result.returncode != 0
        assert "differ only in case" in result.stderr
        assert '"AB"' in result.stderr

    def test_sort_carries_both_keys(self, case_dup_jsonl, tmp_path):
        """SORT_JSONL moves lines without parsing them into a struct, so the
        JSONL deliverable keeps both keys (the transform then refuses)."""
        import zstandard as zstd
        out = tmp_path / "sorted.jsonl.zst"
        result = run_bin("sort_jsonl.py", case_dup_jsonl, "-o", out, "--memory-limit", "4GB")
        assert result.returncode == 0, result.stderr
        with open(out, "rb") as f:
            lines = zstd.ZstdDecompressor().stream_reader(f).read().decode().splitlines()
        assert sorted(lines) == sorted(case_dup_jsonl.read_text().splitlines())


@pytest.fixture(scope="module")
def dotted_lake(tmp_path_factory):
    """The small fixture with keys that themselves contain '.' (top level,
    nested in organism, and in a cross-reference) or a space."""
    entries = _small_entries()
    e0 = entries[0]
    e0["x.y"] = 1
    e0["with space"] = 2
    e0["organism"]["a.b"] = "nested"
    xref = next(e for e in entries if e.get("uniProtKBCrossReferences"))
    xref["uniProtKBCrossReferences"][0]["p.q"] = "xr"
    lake, jsonl = _build_lake(entries, tmp_path_factory.mktemp("dotted"), "dotted_2026")
    return {"lake_dir": lake, "jsonl": jsonl, "entries": entries,
            "accs": {"e0": e0["primaryAccession"]}}


class TestDottedKeys:
    """A key containing '.' or a space is a field like any other: a column (or
    struct field) with exactly that name, never read as nesting."""

    def test_every_entry_reconstructs(self, dotted_lake):
        lake = dotted_lake["lake_dir"]
        rows = _lake_rows(lake)
        failures = _reconstruction_failures(dotted_lake["entries"], rows)
        assert not failures, f"{len(failures)} entries differ; first: {failures[0]}"

    def test_dotted_keys_are_columns(self, dotted_lake):
        lake, accession = dotted_lake["lake_dir"], dotted_lake["accs"]["e0"]
        e = _rows_by_acc(lake, "entries")[accession][0]
        assert e["x.y"] == 1 and e["with space"] == 2
        assert e["organism"]["a.b"] == "nested"
        assert "xr" in [r["p.q"] for rs in _rows_by_acc(lake, "xrefs").values() for r in rs]


class TestOptionalPathGuards:
    """Every SQL builder binds against a subset that lacks optional paths, and a
    non-UniProtKB input is refused with a clear message (not a BinderException)."""

    # A single entry carrying only the paths every UniProtKB entry has.
    MINIMAL = {
        "primaryAccession": "P00001", "uniProtkbId": "TEST_HUMAN",
        "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "organism": {"taxonId": 9606, "scientificName": "Homo sapiens"},
        "sequence": {"value": "MKV", "length": 3, "molWeight": 360, "md5": "x", "crc64": "y"},
        "entryAudit": {"firstPublicDate": "2000-01-01", "lastAnnotationUpdateDate": "2000-01-02",
                       "lastSequenceUpdateDate": "2000-01-03", "entryVersion": 1, "sequenceVersion": 1},
        "proteinExistence": "1: Evidence at protein level", "annotationScore": 1.0,
    }
    # Optional top-level groups, dropped one at a time from the real fixture.
    OPTIONAL_GROUPS = ["genes", "secondaryAccessions", "proteinDescription", "extraAttributes",
                       "keywords", "uniProtKBCrossReferences", "features", "comments",
                       "references", "organismHosts", "geneLocations"]

    @staticmethod
    def _builders():
        from parquet_transform import (_build_entries_sql, _build_features_sql, _build_xrefs_sql,
                                       _build_comments_sql, _build_publications_sql,
                                       _build_accession_map_sql)
        return {"entries": _build_entries_sql, "features": _build_features_sql,
                "xrefs": _build_xrefs_sql, "comments": _build_comments_sql,
                "publications": _build_publications_sql, "accession_map": _build_accession_map_sql}

    @staticmethod
    def _stage(entries, tmp_path):
        """JSONL → staged Parquet → (con, read_clause, schema_paths), as main() does."""
        import duckdb
        from parquet_transform import build_read_clause, stage_to_parquet, discover_schema_paths
        jsonl = tmp_path / "in.jsonl"
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")
        con = duckdb.connect()
        staged = str(tmp_path / "staged.parquet")
        read_clause, _ = stage_to_parquet(con, build_read_clause(con, str(jsonl)), staged)
        return con, read_clause, discover_schema_paths(staged)

    def _build_all(self, entries, tmp_path):
        from parquet_transform import (check_curated_sources, check_required_paths,
                                       discover_json_paths, empty_child_arrays, inferred_types)
        con, read_clause, paths = self._stage(entries, tmp_path)
        check_required_paths(paths)
        types = inferred_types(con, read_clause)
        json_paths = discover_json_paths(types)
        check_curated_sources(types, empty_child_arrays(con, read_clause, types))
        from parquet_transform import _as_json_columns, json_source_columns
        return {name: _as_json_columns(
                    con.sql(build(paths, json_paths).format(read_clause=read_clause)).arrow().read_all(),
                    json_source_columns(name, paths, json_paths))
                for name, build in self._builders().items()}

    def test_minimal_entry_builds_every_table(self, tmp_path):
        tables = self._build_all([self.MINIMAL], tmp_path)
        assert tables["entries"].num_rows == 1 and tables["accession_map"].num_rows == 1
        for child in ("features", "xrefs", "comments", "publications"):
            assert tables[child].num_rows == 0, child
        row = tables["entries"].to_pylist()[0]
        assert row["accession"] == "P00001" and row["division"] == "human"
        assert row["protein_name_primary"] is None and row["gene_primary"] is None
        assert row["feature_count"] == 0 and row["keywordid"] == [] and row["go_id"] == []
        # the source columns are exactly the entry's keys
        from parquet_transform import TABLE_META
        assert (set(tables["entries"].column_names) - set(TABLE_META["entries"]["columns"]["shortcut"])
                == set(self.MINIMAL))
        assert row["organism"] == self.MINIMAL["organism"]
        # a zero-row child table still has the keys every element carries
        assert {"type", "location"} <= set(tables["features"].column_names)
        assert "commentType" in tables["comments"].column_names

    @pytest.mark.parametrize("group", OPTIONAL_GROUPS)
    def test_fixture_without_optional_group_builds(self, group, tmp_path):
        """Builds and still rebuilds every entry exactly."""
        from collections import defaultdict
        with gzip.open(SMALL_JSON_GZ, "rt") as f:
            entries = json.load(f)["results"][:20]
        for e in entries:
            e.pop(group, None)
        from reconstruct import rows_from_arrow
        tables = self._build_all(entries, tmp_path)
        assert tables["entries"].num_rows == len(entries)
        rows = {}
        for name, tbl in tables.items():
            rows[name] = defaultdict(list)
            for r in rows_from_arrow(tbl):
                rows[name][r["accession"]].append(r)
        failures = _reconstruction_failures(entries, rows)
        assert not failures, f"without {group}: {failures[:3]}"

    @pytest.mark.parametrize("ctype", ["SUBCELLULAR LOCATION", "INTERACTION"])
    def test_comments_without_any_texts_build(self, ctype, tmp_path):
        """A subset whose only comments carry no top-level ``texts`` gives a
        NULL text_value, not a BinderException (README §2: an absent optional
        curated source is a typed NULL)."""
        with gzip.open(SMALL_JSON_GZ, "rt") as f:
            entries = json.load(f)["results"][:20]
        for e in entries:
            e["comments"] = [c for c in e.get("comments", []) if c["commentType"] == ctype]
        assert any(e["comments"] for e in entries), ctype
        tables = self._build_all(entries, tmp_path)
        comments = tables["comments"]
        assert comments.num_rows == sum(len(e["comments"]) for e in entries)
        assert comments.column("text_value").null_count == comments.num_rows
        assert str(comments.schema.field("text_value").type) == "string"
        failures = _reconstruction_failures(entries, _rows_from_tables(tables))
        assert not failures, failures[:3]

    def test_missing_required_path_is_refused_clearly(self, tmp_path):
        from parquet_transform import check_required_paths
        _, _, paths = self._stage([{k: v for k, v in self.MINIMAL.items() if k != "entryAudit"}], tmp_path)
        with pytest.raises(RuntimeError, match="entryAudit.entryVersion"):
            check_required_paths(paths)
        with pytest.raises(RuntimeError, match="features.type"):
            check_required_paths(set(self.MINIMAL) | {"features", "features.description"}
                                 | {"organism.taxonId", "organism.scientificName"}
                                 | {f"sequence.{k}" for k in self.MINIMAL["sequence"]}
                                 | {f"entryAudit.{k}" for k in self.MINIMAL["entryAudit"]})



def _xrefs(entries):
    return (x for e in entries for x in e.get("uniProtKBCrossReferences", []))


def _features(entries):
    return (f for e in entries for f in e.get("features", []))


def _rows_from_tables(tables):
    """{table: {accession: [row, ...]}} from in-memory Arrow tables."""
    return {name: group_by_acc(tbl) for name, tbl in tables.items()}


class TestUpstreamShapeChanges:
    """A field UniProt ships in a new shape: the lake still rebuilds every
    entry exactly, or the build stops with a message naming the field."""

    @staticmethod
    def _build(entries, tmp_path):
        return TestOptionalPathGuards()._build_all(entries, tmp_path)

    @pytest.mark.parametrize("table", ["features", "xrefs", "comments", "publications"])
    def test_child_array_empty_in_every_entry(self, table, tmp_path):
        """An array present but empty in every entry is inferred with no
        element fields: it is treated like an absent one (a zero-row table
        with the keys every element carries), not refused as "not a
        UniProtKB dump"."""
        from parquet_transform import CHILD_TABLES, _EMPTY_CHILD_KEYS, TABLE_META
        array, _, count = CHILD_TABLES[table]
        entries = _small_entries()
        for e in entries:
            e[array] = []
        tables = self._build(entries, tmp_path)
        assert tables[table].num_rows == 0
        shortcuts = set(TABLE_META[table]["columns"]["shortcut"])
        assert [c for c in tables[table].column_names if c not in shortcuts] == list(_EMPTY_CHILD_KEYS[array])
        assert set(tables["entries"].column(count).to_pylist()) == {0}
        failures = _reconstruction_failures(entries, _rows_from_tables(tables))
        assert not failures, failures[:3]

    def test_entry_without_extra_attributes_stays_null(self, tmp_path):
        """Not an all-NULL struct (which reconstructs as extraAttributes: {})."""
        entries = _small_entries()
        e0 = next(e for e in entries if e.get("extraAttributes"))
        del e0["extraAttributes"]
        rows = _rows_from_tables(self._build(entries, tmp_path))
        assert rows["entries"][e0["primaryAccession"]][0]["extraAttributes"] is None
        failures = _reconstruction_failures(entries, rows)
        assert not failures, failures[:3]

    @pytest.mark.parametrize("field, value", [
        ("geneLocations", "Mitochondrion"),     # not valid JSON text
        ("organismHosts", "12"),                # valid JSON text, but a string
    ])
    def test_scalar_source_stays_a_string(self, field, value, tmp_path):
        """A source UniProt ships as a plain string is a string column, and
        rebuilds as the string, never decoded as JSON."""
        entries = _small_entries()
        for e in entries:
            e.pop(field, None)
        entries[0][field] = value
        tables = self._build(entries, tmp_path)
        assert str(tables["entries"].schema.field(field).type) == "string"
        rows = _rows_from_tables(tables)
        assert rows["entries"][entries[0]["primaryAccession"]][0][field] == value
        failures = _reconstruction_failures(entries, rows)
        assert not failures, failures[:3]

    @pytest.mark.parametrize("path", ["location.start.value", "ligand.id"])
    def test_curated_source_with_conflicting_types_is_refused(self, path, tmp_path):
        """A shortcut cannot hold a field inferred as JSON: the build stops
        instead of storing every value with its JSON quoting."""
        *parents, leaf = path.split(".")
        entries = _small_entries()
        features = [f for e in entries for f in e.get("features", [])]
        for feature, value in zip(features, ("a string", {"value": "an object"})):
            for part in parents:
                feature = feature.setdefault(part, {})
            feature[leaf] = value
        with pytest.raises(RuntimeError, match=rf"features\.{re.escape(path)}"):
            self._build(entries, tmp_path)

    def test_discover_json_paths_sees_json_arrays(self, tmp_path):
        """An array whose elements differ in type is inferred as JSON[]: its
        path is JSON-typed like a bare JSON field, at any depth."""
        from parquet_transform import discover_json_paths, inferred_types
        entry = dict(TestOptionalPathGuards.MINIMAL, tags=[1, "a"], nested={"xs": [{"a": 1}, "b"]})
        con, read_clause, _ = TestOptionalPathGuards._stage([entry], tmp_path)
        assert {"tags", "nested.xs"} <= discover_json_paths(inferred_types(con, read_clause))

    @pytest.mark.parametrize("table, column, mutate", [
        ("xrefs", "evidences",
         lambda es: next(x for x in _xrefs(es) if x.get("evidences")).update(evidences=["ECO:0000269"])),
        ("features", "featureCrossReferences",
         lambda es: next(f for f in _features(es) if f.get("featureCrossReferences"))
         ["featureCrossReferences"].append("rs1")),
    ])
    def test_array_with_conflicting_element_types_rebuilds(self, table, column, mutate, tmp_path):
        """A source column holding an array whose elements differ in type
        (JSON[]) is stored as JSON, and every entry still rebuilds."""
        import pyarrow as pa
        entries = _small_entries()
        mutate(entries)
        tables = self._build(entries, tmp_path)
        assert isinstance(tables[table].schema.field(column).type, pa.JsonType)
        failures = _reconstruction_failures(entries, _rows_from_tables(tables))
        assert not failures, failures[:3]

    @pytest.mark.parametrize("label, mutate", [
        ("accession_map.accession",
         lambda es: next(e for e in es if e.get("secondaryAccessions"))["secondaryAccessions"].append(12345)),
        ("entries.division",
         lambda es: es[0]["organism"]["lineage"].append(12345)),
    ])
    def test_curated_array_with_conflicting_element_types_is_refused(self, label, mutate, tmp_path):
        """A shortcut reading a list cannot take JSON[] elements (each would
        keep its JSON quoting): the build stops."""
        entries = _small_entries()
        mutate(entries)
        with pytest.raises(RuntimeError, match=rf"{re.escape(label)} "):
            self._build(entries, tmp_path)

    @pytest.mark.parametrize("column, mutate", [
        ("go_terms",
         lambda es: next(p for x in _xrefs(es) if x["database"] == "PDB"
                         for p in x.get("properties", []) if p["key"] == "Resolution").update(value=2.5)),
        ("ec",
         lambda es: next(n for e in es
                         for n in ((e.get("proteinDescription") or {}).get("recommendedName") or {})
                         .get("ecNumbers", [])).update(value=3)),
        ("protein_name_primary",
         lambda es: next(n for e in es for n in (e.get("proteinDescription") or {})
                         .get("submissionNames", []))["fullName"].update(value={"text": "an object"})),
    ])
    def test_derived_curated_source_with_conflicting_types_is_refused(self, column, mutate, tmp_path):
        """A derived curated column (no single COLUMN_SOURCES path) is checked
        against every path it reads, not silently filled with JSON text."""
        entries = _small_entries()
        mutate(entries)
        with pytest.raises(RuntimeError, match=rf"entries\.{column} "):
            self._build(entries, tmp_path)

    def test_every_curated_column_has_its_sources_checked(self):
        """check_curated_sources can only refuse a source it knows about."""
        from parquet_transform import COLUMN_SOURCES, DERIVED_SOURCES, TABLE_META
        unlisted = [(t, c) for t, meta in TABLE_META.items() for c in meta["columns"]["shortcut"]
                    if (t, c) not in COLUMN_SOURCES and (t, c) not in DERIVED_SOURCES]
        assert not unlisted

    @pytest.mark.parametrize("field, value, column", [
        ("ligand", "plain-string-ligand", "ligand_id"),
        ("evidences", "plain-string", "evidence_codes"),
    ])
    def test_curated_source_under_conflicting_parent_is_refused(self, field, value, column, tmp_path):
        """A curated field whose parent object is inferred as JSON has no
        discoverable path: the build stops instead of writing a typed NULL."""
        entries = _small_entries()
        next(f for f in _features(entries) if field not in f)[field] = value
        with pytest.raises(RuntimeError, match=rf"features\.{column} "):
            self._build(entries, tmp_path)

    @pytest.mark.parametrize("retype", [
        lambda title: {"value": title, "lang": "en"},
        lambda title: [title],
    ], ids=["object", "list"])
    def test_curated_source_retyped_in_every_entry_is_refused(self, retype, tmp_path):
        """A scalar shortcut source that is an object (or a list) in *every*
        entry is inferred as STRUCT (or LIST), not JSON; a CAST to VARCHAR
        would store its text rendering, so the build stops."""
        entries = _small_entries()
        ligands = [f["ligand"] for f in _features(entries) if "id" in (f.get("ligand") or {})]
        assert ligands
        for ligand in ligands:
            ligand["id"] = retype(ligand["id"])
        with pytest.raises(RuntimeError, match=r"features\.ligand_id "):
            self._build(entries, tmp_path)

    def test_feature_with_null_start_rebuilds(self, tmp_path):
        """A null location.start is normalised away, not rebuilt as {}."""
        entries = _small_entries()
        next(_features(entries))["location"]["start"] = None
        failures = _reconstruction_failures(entries, _rows_from_tables(self._build(entries, tmp_path)))
        assert not failures, failures[:3]

    @pytest.mark.parametrize("field, value, column", [
        ("keywords", "KW-0001", "keywordid"),
        ("genes", "ABC", "gene_primary"),
    ])
    def test_scalar_passthrough_read_by_curated_column_is_refused(self, field, value, column,
                                                                   tmp_path):
        """A source a shortcut reads becomes a plain value: the build
        stops, and says so (not 'a different type')."""
        entries = _small_entries()
        for e in entries:
            e.pop(field, None)
        entries[0][field] = value
        with pytest.raises(RuntimeError, match=rf"entries\.{column} ") as err:
            self._build(entries, tmp_path)
        assert "plain value" in str(err.value)
        assert "different type in different entries" not in str(err.value)


def test_conflicting_new_xref_field_is_a_json_column(tmp_path):
    """A new cross-reference key whose type differs between two xrefs is a
    JSON column of xrefs, and every entry still rebuilds."""
    import pyarrow as pa
    entries = _small_entries()
    owner = next(e for e in entries if len(e.get("uniProtKBCrossReferences", [])) >= 2)
    owner["uniProtKBCrossReferences"][0]["weirdXref"] = "a"
    owner["uniProtKBCrossReferences"][1]["weirdXref"] = {"b": 1}
    tables = TestOptionalPathGuards()._build_all(entries, tmp_path)
    assert isinstance(tables["xrefs"].schema.field("weirdXref").type, pa.JsonType)
    failures = _reconstruction_failures(entries, _rows_from_tables(tables))
    assert not failures, failures[:3]


def _top_level_keys(json_gz):
    import gzip, json
    with gzip.open(json_gz, "rt") as f:
        data = json.load(f)
    keys = set()
    for e in data["results"]:
        keys.update(e.keys())
    return keys


class TestDataIntegrity:
    def test_reviewed_split(self, entries_ds, expected):
        arrow = entries_ds.to_table(columns=["reviewed"])
        reviewed = pc.sum(pc.cast(arrow.column("reviewed"), "int64")).as_py()
        total = arrow.num_rows

        assert reviewed == expected["reviewed"]
        assert total - reviewed == expected["unreviewed"]

    def test_accessions_not_null(self, entries_ds):
        arrow = entries_ds.to_table(columns=["accession"])
        assert arrow.column("accession").null_count == 0

    def test_taxid_populated(self, entries_ds):
        arrow = entries_ds.to_table(columns=["organism_id"])
        assert arrow.column("organism_id").null_count == 0

    def test_features_have_parent_acc(self, features_ds):
        arrow = features_ds.to_table(columns=["accession"])
        assert arrow.column("accession").null_count == 0

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
        arrow = comments_ds.to_table(columns=["commentType"])
        assert arrow.column("commentType").null_count == 0

    def test_publications_have_citation(self, publications_ds):
        arrow = publications_ds.to_table(columns=["citation"])
        assert arrow.column("citation").null_count == 0

    def test_publications_have_acc(self, publications_ds):
        arrow = publications_ds.to_table(columns=["accession"])
        assert arrow.column("accession").null_count == 0

    def test_sample_accession_present(self, entries_ds, expected):
        """Check that at least one reviewed and one unreviewed accession are present."""
        arrow = entries_ds.to_table(columns=["accession"])
        accs = set(arrow.column("accession").to_pylist())
        if expected["reviewed_acc"]:
            assert expected["reviewed_acc"] in accs, f"Expected reviewed accession {expected['reviewed_acc']}"
        if expected["unreviewed_acc"]:
            assert expected["unreviewed_acc"] in accs, f"Expected unreviewed accession {expected['unreviewed_acc']}"

    def test_entry_type_not_null(self, entries_ds):
        """entryType keeps the original string (reviewed is derived from it)."""
        arrow = entries_ds.to_table(columns=["entryType"])
        assert arrow.column("entryType").null_count == 0

    def test_extra_attributes_populated(self, entries_ds):
        arrow = entries_ds.to_table(columns=["extraAttributes"])
        assert arrow.column("extraAttributes").null_count < arrow.num_rows

    def test_publications_have_reference_number(self, publications_ds):
        arrow = publications_ds.to_table(columns=["referenceNumber"])
        assert arrow.column("referenceNumber").null_count == 0


# ─── Sort order: reviewed DESC, organism_id ASC, accession ASC ───────────────
# The fixture contains both reviewed (Swiss-Prot) and unreviewed
# (TrEMBL) entries to exercise the full three-column sort.


class TestSortOrder:
    def test_entries_sorted_by_reviewed_desc_taxid_asc_acc_asc(self, entries_ds):
        """Verify entries are sorted by (reviewed DESC, organism_id ASC, accession ASC)."""
        arrow = entries_ds.to_table(columns=["reviewed", "organism_id", "accession"])
        reviewed = arrow.column("reviewed").to_pylist()
        taxids = arrow.column("organism_id").to_pylist()
        accs = arrow.column("accession").to_pylist()
        keys = [(not r, t, a) for r, t, a in zip(reviewed, taxids, accs)]
        assert keys == sorted(keys), "Entries not sorted by (reviewed DESC, organism_id ASC, accession ASC)"

    @pytest.mark.parametrize("table,index", CHILD_INDEX)
    def test_child_sorted_by_entry_order_then_index(self, lake_dir, table, index):
        """Child tables: (reviewed DESC, organism_id ASC, accession ASC, <x>_index ASC)."""
        arrow = open_table(lake_dir, table).to_table(columns=["reviewed", "organism_id", "accession", index])
        keys = list(zip((not r for r in arrow.column("reviewed").to_pylist()),
                        arrow.column("organism_id").to_pylist(), arrow.column("accession").to_pylist(),
                        arrow.column(index).to_pylist()))
        assert keys == sorted(keys), f"{table} not sorted by (reviewed DESC, organism_id, accession, {index})"

    def test_accession_map_sorted(self, accession_map_ds):
        arrow = accession_map_ds.to_table(columns=["reviewed", "accession", "primary_accession"])
        keys = list(zip((not r for r in arrow.column("reviewed").to_pylist()),
                        arrow.column("accession").to_pylist(), arrow.column("primary_accession").to_pylist()))
        assert keys == sorted(keys), "accession_map not sorted by (reviewed DESC, accession ASC, primary_accession ASC)"


class TestArrayPositions:
    """*_index is each element's position in the entry's source array."""


    @pytest.mark.parametrize("table,index", CHILD_INDEX)
    def test_index_is_zero_to_n_per_accession(self, lake_dir, table, index):
        from collections import defaultdict
        arrow = open_table(lake_dir, table).to_table(columns=["accession", index])
        by_acc = defaultdict(list)
        for accession, i in zip(arrow.column("accession").to_pylist(), arrow.column(index).to_pylist()):
            by_acc[accession].append(i)
        bad = [accession for accession, ids in by_acc.items() if sorted(ids) != list(range(len(ids)))]
        assert not bad, f"{table}.{index} is not 0..n-1 for {len(bad)} accessions, e.g. {bad[:3]}"

    @pytest.mark.parametrize("table,index", CHILD_INDEX)
    def test_index_matches_source_position(self, lake_dir, fixture_json_gz, table, index):
        """The row at (accession, i) is the i-th element of the source array (spot-checked
        on the entry with the longest array)."""
        with gzip.open(fixture_json_gz, "rt") as f:
            entries = json.load(f)["results"]
        field = CHILD_TABLES[table][0]
        entry = max(entries, key=lambda e: len(e.get(field, [])))
        accession = entry["primaryAccession"]
        arrow = open_table(lake_dir, table).to_table(filter=ds.field("accession") == accession)
        rows = sorted(arrow.to_pylist(maps_as_pydicts="strict"), key=lambda r: r[index])
        probe = {"features": ("type", lambda e: e["type"]),
                 "xrefs": ("id", lambda e: e["id"]),
                 "comments": ("commentType", lambda e: e["commentType"]),
                 "publications": ("referenceNumber", lambda e: e["referenceNumber"])}[table]
        assert [r[probe[0]] for r in rows] == [probe[1](e) for e in entry[field]]


# ─── Zero-row child tables (tiny subsets) ──────────────────────────

@pytest.fixture(scope="module")
def empty_child_lake(tmp_path_factory):
    """A lake built from entries stripped of comments and references: the
    comments and publications tables are zero-row and must still publish one
    empty schema-valid Parquet file each."""
    import orjson
    import zstandard as zstd
    from conftest import _json_gz_to_jsonl_zst, run_transform  # noqa: F401

    out_dir = tmp_path_factory.mktemp("emptychild")
    with gzip.open(SMALL_JSON_GZ, "rt") as f:
        entries = json.load(f)["results"]
    jsonl = str(out_dir / "stripped.jsonl.zst")
    cctx = zstd.ZstdCompressor(level=3)
    with open(jsonl, "wb") as fout:
        with cctx.stream_writer(fout) as w:
            for e in entries:
                e.pop("comments", None)
                e.pop("references", None)
                w.write(orjson.dumps(e) + b"\n")
    lake = str(out_dir / "lake")
    run_transform(jsonl, lake, release="empty_2026")
    return {"lake_dir": lake, "jsonl": jsonl}


class TestEmptyChildTables:
    def test_zero_row_table_publishes_empty_file(self, empty_child_lake):
        import duckdb
        lake = empty_child_lake["lake_dir"]
        for table in ("comments", "publications"):
            files = table_files(lake, table)
            assert files, f"{table}: zero-row table published no Parquet file"
            assert all(pq.read_metadata(f).num_rows == 0 for f in files)
            n = duckdb.sql(
                f"SELECT count(*) FROM read_parquet('{lake}/{table}/**/*.parquet')"
            ).fetchone()[0]
            assert n == 0
            assert open_table(lake, table).count_rows() == 0

    def test_zero_row_table_in_manifest(self, empty_child_lake):
        with open(os.path.join(empty_child_lake["lake_dir"], "manifest.json")) as f:
            m = json.load(f)
        for table in ("comments", "publications"):
            t = m["tables"][table]
            assert t["row_count"] == 0
            assert t["files"], f"{table}: manifest lists no files"
            assert t["columns"], f"{table}: manifest lost the schema"

    def test_validator_passes_on_zero_row_tables(self, empty_child_lake, tmp_path):
        import subprocess
        import sys
        bin_dir = os.path.join(os.path.dirname(__file__), "..", "bin")
        env = os.environ.copy()
        env["PYTHONPATH"] = bin_dir + ":" + env.get("PYTHONPATH", "")
        report = str(tmp_path / "report.txt")
        result = subprocess.run(
            [sys.executable, os.path.join(bin_dir, "validate_lake.py"),
             "--lake", empty_child_lake["lake_dir"],
             "--jsonl", empty_child_lake["jsonl"],
             "--spot-check-n", "5", "-o", report],
            env=env, capture_output=True, text=True)
        detail = ""
        if result.returncode != 0 and os.path.exists(report):
            with open(report) as f:
                detail = f.read()
        assert result.returncode == 0, f"validator failed:\n{result.stderr}\n{detail}"


class TestSortKeepsLines:
    """SORT_JSONL reorders lines and changes nothing else: each line of
    sorted.jsonl.zst is a line of its input, byte for byte (README: "the same
    data in its original JSON form"), so VALIDATE's ground truth is the
    dump's data rather than DuckDB's re-serialisation of it."""

    @staticmethod
    def _sorted_lines(lines, tmp_path):
        import zstandard as zstd
        src = tmp_path / "in.jsonl"
        src.write_text("".join(line + "\n" for line in lines))
        out = tmp_path / "sorted.jsonl.zst"
        result = run_bin("sort_jsonl.py", src, "-o", out, "--memory-limit", "4GB")
        assert result.returncode == 0, result.stderr
        with open(out, "rb") as f:
            return zstd.ZstdDecompressor().stream_reader(f).read().decode().splitlines()

    def test_lines_are_unchanged(self, tmp_path):
        entries = _small_entries()
        # Key sets differ between entries, and a field holds 2 in one entry
        # and 2.5 in another (inferred DOUBLE, so a rewrite gives 2.0).
        entries[0]["score"] = 2
        entries[1]["score"] = 2.5
        entries[2]["extra"] = {"b": 1, "a": [{}, {"k": None}]}
        lines = [json.dumps(e) for e in entries]
        lines[3] = lines[3].replace('"primaryAccession": ', '"primaryAccession" :  ')
        out = self._sorted_lines(lines, tmp_path)
        assert sorted(out) == sorted(lines)

    def test_order(self, tmp_path):
        entries = _small_entries()
        out = self._sorted_lines([json.dumps(e) for e in reversed(entries)], tmp_path)
        keys = [("Swiss-Prot" not in e["entryType"], e["organism"]["taxonId"], e["primaryAccession"])
                for e in map(json.loads, out)]
        assert len(keys) == len(entries) and keys == sorted(keys)


# ─── Inference, batch and derived-column edge cases ──────────────────


def _trembl_shaped_entries(copies=600, new_keys=6):
    """The small fixture plus ``copies`` TrEMBL entries whose
    proteinDescription holds only submissionNames, with ``new_keys`` keys
    UniProt might add, one entry each: the key mix of a TrEMBL-dominated dump,
    where most proteinDescription keys are rare."""
    entries = _small_entries()
    trembl = [e for e in entries if "TrEMBL" in e["entryType"]
              and list(e.get("proteinDescription", {})) == ["submissionNames"]]
    extra = []
    for i in range(copies):
        e = json.loads(json.dumps(trembl[i % len(trembl)]))
        e["primaryAccession"] = f"A{i:05d}"
        extra.append(e)
    for i in range(new_keys):
        extra[i]["proteinDescription"][f"newName{i}"] = {"value": f"new {i}"}
    return entries + extra


def _inferred(entries, tmp_path, **options):
    """{top-level column: DuckDBPyType} DuckDB's JSON reader infers for ``entries``."""
    import duckdb
    jsonl = tmp_path / "infer.jsonl"
    with open(jsonl, "w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")
    opts = "".join(f", {k}={v}" for k, v in options.items())
    rel = duckdb.sql(f"SELECT * FROM read_json_auto('{jsonl}', format='newline_delimited', "
                     f"sample_size=-1{opts}) LIMIT 0")
    return dict(zip(rel.columns, rel.types))


class TestSparseObjectsStayStructs:
    """An object most of whose keys are rare (proteinDescription in a
    TrEMBL-dominated dump) is one DuckDB's default inference turns into a MAP,
    which hides every curated source inside it.  The read clause keeps it a
    STRUCT; only comments is read as a MAP."""

    def test_default_inference_would_be_a_map(self, tmp_path):
        """Precondition: without the fix this input loses proteinDescription."""
        types = _inferred(_trembl_shaped_entries(), tmp_path)
        assert types["proteinDescription"].id == "map"

    def test_curated_columns_are_populated(self, tmp_path):
        entries = _trembl_shaped_entries()
        rows = _rows_from_tables(TestOptionalPathGuards()._build_all(entries, tmp_path))
        for e in entries:
            pd = e.get("proteinDescription") or {}
            name = ((pd.get("recommendedName") or {}).get("fullName")
                    or ((pd.get("submissionNames") or [{}])[0]).get("fullName") or {}).get("value")
            row = rows["entries"][e["primaryAccession"]][0]
            assert row["protein_name_primary"] == name, e["primaryAccession"]
        assert any(r[0]["ec"] for r in rows["entries"].values())
        failures = _reconstruction_failures(entries, rows)
        assert not failures, failures[:3]

    def test_comments_are_structs_with_a_column_per_key(self, tmp_path):
        """comments is read as a STRUCT list, like every other object: each
        comment key is a column of comments, and a narrow subset's columns
        are that subset's keys."""
        import duckdb
        from parquet_transform import build_read_clause
        entries = _small_entries()[:20]
        for e in entries:
            e["comments"] = [c for c in e.get("comments", []) if c["commentType"] == "FUNCTION"]
        jsonl = tmp_path / "in.jsonl"
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")
        assert "MAP(" not in build_read_clause(duckdb.connect(), str(jsonl))
        tables = TestOptionalPathGuards()._build_all(entries, tmp_path)
        keys = {k for e in entries for c in e["comments"] for k in c}
        assert set(tables["comments"].column_names) == {
            "accession", "reviewed", "organism_id", "comment_index", "text_value"} | keys
        failures = _reconstruction_failures(entries, _rows_from_tables(tables))
        assert not failures, failures[:3]

    def test_curated_source_under_a_map_is_refused(self):
        import duckdb
        from parquet_transform import check_curated_sources, inferred_types
        types = inferred_types(duckdb.connect(),
                               "(SELECT NULL::MAP(VARCHAR, JSON) AS proteinDescription)")
        with pytest.raises(RuntimeError, match=r"entries\.protein_name_primary ← proteinDescription\."):
            check_curated_sources(types)


class TestLargeArrowBuffers:
    """DuckDB hands over each batch with 64-bit offsets, so a batch holding
    more than 2 GiB of strings in one column cannot overflow; the writer casts
    it back to the regular types, so the files are unchanged."""

    def test_init_duckdb_uses_large_buffers(self, tmp_path):
        from parquet_transform import init_duckdb
        con = init_duckdb("1GB", 1, str(tmp_path))
        assert con.sql("SELECT current_setting('arrow_large_buffer_size')").fetchone()[0] is True

    def test_batch_is_split_and_cast_to_regular_types(self, monkeypatch):
        import duckdb
        import parquet_transform as pt
        con = duckdb.connect()
        con.sql("SET arrow_large_buffer_size=true")
        tbl = con.sql("""SELECT i, repeat('x', i) AS s, [repeat('y', i), NULL] AS l,
                                {'a': repeat('z', i)} AS st, MAP {'k': repeat('v', i)} AS m,
                                to_json({'n': i}) AS j, NULL::VARCHAR AS nul
                         FROM range(200) t(i)""").arrow().read_all()
        assert "large_string" in str(tbl.schema)                    # precondition
        monkeypatch.setattr(pt, "MAX_CHUNK_BYTES", 2000)
        out = pt._to_regular_types(tbl)
        assert "large" not in str(out.schema), out.schema
        assert out.column("s").num_chunks > 1
        assert out.to_pylist() == tbl.to_pylist()
        assert [f.metadata for f in out.schema] == [f.metadata for f in tbl.schema]

    def test_lake_files_have_regular_types(self, lake_dir):
        for table in EXPECTED_TABLES:
            schema = pq.read_schema(next(iter(sorted(
                os.path.join(dp, f) for dp, _, fs in os.walk(os.path.join(lake_dir, table))
                for f in fs if f.endswith(".parquet")))))
            assert "large_" not in str(schema), table


class TestDerivedColumnEdgeCases:
    """Curated columns derived from several source values."""

    def test_non_numeric_pubmed_id_builds(self, tmp_path):
        """A malformed PubMed id is kept (after the numeric ones) rather than
        failing the entries table."""
        entries = _small_entries()
        e = next(e for e in entries if e.get("references"))
        e["references"] = e["references"][:1]
        e["references"][0]["citation"]["citationCrossReferences"] = [
            {"database": "PubMed", "id": i} for i in ("10", "2", "abc")]
        rows = _rows_from_tables(TestUpstreamShapeChanges._build(entries, tmp_path))
        assert rows["entries"][e["primaryAccession"]][0]["lit_pubmed_id"] == ["2", "10", "abc"]
        failures = _reconstruction_failures(entries, rows)
        assert not failures, failures[:3]

    def test_gene_primary_keeps_one_slot_per_gene(self, tmp_path):
        """gene_primary keeps one slot per gene: NULL for a gene with no
        geneName, as in UniProt's genes array."""
        entries = _small_entries()
        e = entries[0]
        e["genes"] = [{"orfNames": [{"value": "ORF1"}]}, {"geneName": {"value": "X"}}]
        rows = _rows_from_tables(TestUpstreamShapeChanges._build(entries, tmp_path))
        row = rows["entries"][e["primaryAccession"]][0]
        assert row["gene_primary"] == [None, "X"]


@pytest.fixture(scope="module")
def renamed(tmp_path_factory):
    """The small fixture with features.ligand.id renamed in every feature."""
    from conftest import run_transform
    entries = _small_entries()
    for f in _features(entries):
        if "id" in (f.get("ligand") or {}):
            f["ligand"]["chebiId"] = f["ligand"].pop("id")
    out = tmp_path_factory.mktemp("renamed")
    jsonl = write_jsonl_zst(entries, str(out / "renamed.jsonl.zst"))
    lake = str(out / "lake")
    result = run_transform(jsonl, lake, release="renamed_2026")
    return {"lake": lake, "jsonl": jsonl, "stderr": result.stderr, "out": out}


class TestAbsentCuratedSources:
    """An optional shortcut source missing from the input (renamed upstream,
    say) is a typed NULL column, and both the transform log and the
    validation report say so."""

    def test_transform_warns(self, renamed):
        assert "WARNING: shortcut sources absent from input" in renamed["stderr"]
        assert "features.ligand_id ← features.ligand.id" in renamed["stderr"]

    def test_validator_warns(self, renamed):
        returncode, text = _validation_report(renamed["lake"], renamed["jsonl"], renamed["out"])
        assert returncode == 0, text
        assert "[WARN] features.ligand_id has no values" in text


def test_files_roll_over_inside_a_batch(small_jsonl, tmp_path):
    """A batch is written in row-group slices and the file size checked
    after each, so a file is at most one row group over target however large
    the batch, and the target decides the file count."""
    import pyarrow.parquet as pq
    from conftest import table_files

    def build(target):
        lake = str(tmp_path / f"lake_{target}")
        result = run_bin("parquet_transform.py", small_jsonl, "--outdir", lake, "--memory-limit", "3GB",
                         "--batch-size", "4000", "--row-group-size", "250",
                         "--target-file-bytes", str(target), "--only", "entries")
        assert result.returncode == 0, result.stderr[-2000:]
        return table_files(lake, "entries")

    counts = {}
    for target in (256_000, 512_000):
        files = build(target)
        counts[target] = len(files)
        assert len(files) > 2, files            # more than one file per partition
        for path in files:
            # The writer rolls over after the row group that reaches the
            # target, so every row group but the last starts below it.
            meta = pq.ParquetFile(path).metadata
            last = meta.row_group(meta.num_row_groups - 1)
            start = min(c.dictionary_page_offset or c.data_page_offset
                        for c in (last.column(j) for j in range(meta.num_columns)))
            assert start < target, (path, start)
    assert counts[256_000] > counts[512_000], counts
