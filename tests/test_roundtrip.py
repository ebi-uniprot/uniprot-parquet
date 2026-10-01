"""Roundtrip equivalence, piece by piece: JSONL → Parquet → compare.

Loads the original JSON entries, runs the pipeline, then checks each part of
each entry (accessions, scalars, child arrays, derived columns) against the
lake, so a failure names the part that broke.  Child arrays are compared
set-wise here; tests/test_reconstruct.py rebuilds whole entries from the six
tables and compares them exactly, array order included.
"""

import json
from collections import defaultdict
from typing import Any

import pytest

# ─── Helpers ────────────────────────────────────────────────────────────
from conftest import load_original_entries, open_table  # noqa: F401
from reconstruct import CHILD_ARRAYS, _element, normalize_value  # shared with the validator

# ─── Fixtures ───────────────────────────────────────────────────────────
# originals, lake, lake_entries, lake_features, lake_xrefs, lake_comments and
# lake_publications are module-scoped fixtures in conftest.py (shared with
# tests/test_reconstruct.py).
Entries = dict[str, dict[str, Any]]           # originals, lake_entries
Rows = dict[str, list[dict[str, Any]]]        # lake_features, lake_xrefs, ...


# ─── Tests: Completeness ───────────────────────────────────────────────


class TestAccessionMap:
    """Every secondary accession resolves to its entry through accession_map."""

    def test_secondaries_resolve(self, originals: Entries, lake: str) -> None:
        import duckdb
        rows = duckdb.sql("SELECT accession, primary_accession, is_primary "
                          f"FROM read_parquet('{lake}/accession_map/**/*.parquet')").fetchall()
        secondary = {(a, p) for a, p, is_p in rows if not is_p}
        primary = {a for a, p, is_p in rows if is_p and a == p}
        for accession, orig in originals.items():
            assert accession in primary, f"{accession}: no primary row"
            for sec in orig.get("secondaryAccessions") or []:
                assert (sec, accession) in secondary, f"{accession}: secondary {sec} does not resolve"


class TestAccessionCompleteness:
    """Every accession in the original data must appear in the lake."""

    def test_all_accessions_present(self, originals: Entries, lake_entries: Entries) -> None:
        original_accs = set(originals.keys())
        lake_accs = set(lake_entries.keys())
        missing = original_accs - lake_accs
        assert not missing, f"Accessions missing from lake: {missing}"

    def test_no_extra_accessions(self, originals: Entries, lake_entries: Entries) -> None:
        original_accs = set(originals.keys())
        lake_accs = set(lake_entries.keys())
        extra = lake_accs - original_accs
        assert not extra, f"Unexpected accessions in lake: {extra}"

    def test_entry_count_matches(self, originals: Entries, lake_entries: Entries) -> None:
        assert len(originals) == len(lake_entries)


# ─── Tests: Entry-level field equivalence ──────────────────────────────


class TestEntryFields:
    """For each entry, verify that scalar and nested fields survived."""

    def test_source_columns_equal_the_json(self, originals: Entries, lake_entries: Entries) -> None:
        """Every top-level key of an entry (child arrays aside) is a column
        holding the JSON value as shipped."""
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            for key, value in orig.items():
                if key in CHILD_ARRAYS:
                    continue
                assert normalize_value(lake[key]) == normalize_value(value), f"{accession}: {key}"

    def test_uniprot_id_preserved(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            assert lake["id"] == orig["uniProtkbId"], (
                f"{accession}: id mismatch"
            )

    def test_taxid_preserved(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            assert lake["organism_id"] == orig["organism"]["taxonId"], (
                f"{accession}: organism_id {lake['organism_id']} != {orig['organism']['taxonId']}"
            )

    def test_organism_name_preserved(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            assert lake["organism_scientific_name"] == orig["organism"]["scientificName"], (
                f"{accession}: organism_scientific_name mismatch"
            )

    def test_seq_length_preserved(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            assert lake["length"] == orig["sequence"]["length"], (
                f"{accession}: length {lake['length']} != {orig['sequence']['length']}"
            )

    def test_reviewed_flag_correct(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            expected = "Swiss-Prot" in orig["entryType"]
            assert lake["reviewed"] == expected, (
                f"{accession}: reviewed {lake['reviewed']} != expected {expected}"
            )

    def test_entry_audit_dates(self, originals: Entries, lake_entries: Entries) -> None:
        """The DATE shortcuts are entryAudit's date strings."""
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            audit = orig["entryAudit"]
            assert str(lake["date_created"]) == audit["firstPublicDate"], f"{accession}: date_created"
            assert str(lake["date_modified"]) == audit["lastAnnotationUpdateDate"], f"{accession}: date_modified"
            assert str(lake["date_sequence_modified"]) == audit["lastSequenceUpdateDate"], (
                f"{accession}: date_sequence_modified")


# ─── Tests: Child table counts ─────────────────────────────────────────


class TestChildTableCounts:
    """Row counts in child tables must match the original array lengths."""

    def test_feature_counts(self, originals: Entries, lake_features: Rows) -> None:
        for accession, orig in originals.items():
            orig_count = len(orig.get("features") or [])
            lake_count = len(lake_features.get(accession, []))
            assert orig_count == lake_count, (
                f"{accession}: features count {lake_count} != original {orig_count}"
            )

    def test_xref_counts(self, originals: Entries, lake_xrefs: Rows) -> None:
        for accession, orig in originals.items():
            orig_count = len(orig.get("uniProtKBCrossReferences") or [])
            lake_count = len(lake_xrefs.get(accession, []))
            assert orig_count == lake_count, (
                f"{accession}: xrefs count {lake_count} != original {orig_count}"
            )

    def test_comment_counts(self, originals: Entries, lake_comments: Rows) -> None:
        for accession, orig in originals.items():
            orig_count = len(orig.get("comments") or [])
            lake_count = len(lake_comments.get(accession, []))
            assert orig_count == lake_count, (
                f"{accession}: comments count {lake_count} != original {orig_count}"
            )

    def test_publication_counts(self, originals: Entries, lake_publications: Rows) -> None:
        for accession, orig in originals.items():
            orig_count = len(orig.get("references") or [])
            lake_count = len(lake_publications.get(accession, []))
            assert orig_count == lake_count, (
                f"{accession}: publications count {lake_count} != original {orig_count}"
            )


# ─── Tests: Child table content (lossless nested structs) ─────────────


class TestFeatureContent:
    """A features row's source columns are the original feature."""

    def test_all_features_present(self, originals: Entries, lake_features: Rows) -> None:
        for accession, orig in originals.items():
            orig_features = orig.get("features") or []
            lake_rows = lake_features.get(accession, [])

            # Each row's source columns, compared order-independently
            orig_set = {
                json.dumps(normalize_value(f), sort_keys=True, default=str)
                for f in orig_features
            }
            lake_set = {
                json.dumps(normalize_value(_element(row, "features")), sort_keys=True, default=str)
                for row in lake_rows
            }
            missing = orig_set - lake_set
            assert not missing, (
                f"{accession}: {len(missing)} features missing from lake"
            )


class TestXrefContent:
    """Cross-references are preserved at the field level."""

    def test_all_xrefs_present(self, originals: Entries, lake_xrefs: Rows) -> None:
        for accession, orig in originals.items():
            orig_xrefs = orig.get("uniProtKBCrossReferences") or []
            lake_rows = lake_xrefs.get(accession, [])

            # Build comparable tuples: (database, id)
            orig_keys = {(x["database"], x["id"]) for x in orig_xrefs}
            lake_keys = {(row["database"], row["id"]) for row in lake_rows}
            missing = orig_keys - lake_keys
            assert not missing, (
                f"{accession}: {len(missing)} xrefs missing from lake, e.g. {list(missing)[:3]}"
            )


class TestCommentContent:
    """A comments row's source columns are the original comment."""

    def test_all_comments_present(self, originals: Entries, lake_comments: Rows) -> None:
        for accession, orig in originals.items():
            orig_comments = orig.get("comments") or []
            lake_rows = lake_comments.get(accession, [])

            # Each row's source columns, compared order-independently
            orig_set = {
                json.dumps(normalize_value(c), sort_keys=True, default=str)
                for c in orig_comments
            }
            lake_set = {
                json.dumps(normalize_value(_element(row, "comments")), sort_keys=True, default=str)
                for row in lake_rows
            }
            missing = orig_set - lake_set
            assert not missing, (
                f"{accession}: {len(missing)} comments missing from lake"
            )

    def test_text_value_matches_texts(self, originals: Entries, lake_comments: Rows) -> None:
        """text_value is the double-newline join of texts[*].value, else NULL."""
        for accession, orig in originals.items():
            by_type: defaultdict[str, list[str | None]] = defaultdict(list)
            for row in lake_comments.get(accession, []):
                by_type[row["commentType"]].append(row["text_value"])
            for c in orig.get("comments") or []:
                lake_values = by_type[c["commentType"]]
                if "texts" in c:
                    expected = "\n\n".join(t["value"] for t in c["texts"])
                    assert expected in lake_values, (
                        f"{accession}: {c['commentType']} text_value not found"
                    )
                else:
                    assert all(v is None for v in lake_values), (
                        f"{accession}: {c['commentType']} has no texts but text_value is set"
                    )


class TestPublicationContent:
    """A publications row's source columns are the original reference."""

    def test_all_publications_present(self, originals: Entries, lake_publications: Rows) -> None:
        for accession, orig in originals.items():
            orig_refs = orig.get("references") or []
            lake_rows = lake_publications.get(accession, [])

            # Each row's source columns, compared order-independently
            orig_set = {
                json.dumps(normalize_value(r), sort_keys=True, default=str)
                for r in orig_refs
            }
            lake_set = {
                json.dumps(normalize_value(_element(row, "publications")), sort_keys=True, default=str)
                for row in lake_rows
            }
            missing = orig_set - lake_set
            assert not missing, (
                f"{accession}: {len(missing)} references missing from lake"
            )


# ─── Tests: Convenience column derivation correctness ─────────────────


class TestConvenienceColumns:
    """Convenience columns must be correctly derived from the source data."""

    def test_gene_names_match(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            orig_genes = [
                g["geneName"]["value"]
                for g in (orig.get("genes") or [])
                if g.get("geneName", {}).get("value")
            ]
            # The SQL list_transform produces NULL for genes without geneName;
            # filter those out for comparison.
            lake_genes = [g for g in (lake.get("gene_primary") or []) if g is not None]
            assert set(orig_genes) == set(lake_genes), (
                f"{accession}: gene_primary mismatch: {orig_genes} vs {lake_genes}"
            )

    def test_go_ids_match(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            orig_go = {
                x["id"]
                for x in (orig.get("uniProtKBCrossReferences") or [])
                if x["database"] == "GO"
            }
            lake_go = set(lake.get("go_id") or [])
            assert orig_go == lake_go, (
                f"{accession}: go_id mismatch"
            )

    def test_go_terms_match_go_ids(self, originals: Entries, lake_entries: Entries) -> None:
        for accession in originals:
            lake = lake_entries[accession]
            go_terms = lake.get("go_terms") or []
            go_id = lake.get("go_id") or []
            assert {t["id"] for t in go_terms} == set(go_id), f"{accession}: go_terms ids"
            assert len(go_terms) == len(go_id), f"{accession}: go_terms length"
            for t in go_terms:
                assert t["aspect"] in {"P", "F", "C", None}, f"{accession}: aspect {t['aspect']!r}"
                if t["aspect"] is not None:
                    assert t["term"], f"{accession}: empty term"

    def test_pubmed_ids_match(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            ids: set[str] = set()
            for r in orig.get("references") or []:
                for c in (r.get("citation") or {}).get("citationCrossReferences") or []:
                    if c["database"] == "PubMed":
                        ids.add(c["id"])
            expected = sorted(ids, key=int)
            assert (lake_entries[accession].get("lit_pubmed_id") or []) == expected, f"{accession}: lit_pubmed_id"

    @staticmethod
    def _ec_numbers(node: Any) -> set[str]:
        """Every ecNumbers[].value anywhere under ``node`` (every naming
        block, including those of includes[] and contains[])."""
        if isinstance(node, list):
            return {v for x in node for v in TestConvenienceColumns._ec_numbers(x)}
        if not isinstance(node, dict):
            return set()
        found = {x["value"] for x in node.get("ecNumbers") or []}
        for key, value in node.items():
            if key != "ecNumbers":
                found |= TestConvenienceColumns._ec_numbers(value)
        return found

    def test_ec_matches_every_naming_block(self, originals: Entries, lake_entries: Entries) -> None:
        wrong = [acc for acc, orig in originals.items()
                 if set(lake_entries[acc].get("ec") or [])
                 != self._ec_numbers(orig.get("proteinDescription"))]
        assert not wrong, f"{len(wrong)} entries' ec differs, e.g. {wrong[:5]}"

    def test_ec_includes_contained_chains(self, originals: Entries, lake_entries: Entries) -> None:
        """P0DTD1 (SARS-CoV-2 pp1ab) has EC numbers only on its chains."""
        if "P0DTD1" not in originals:
            pytest.skip("P0DTD1 not in this fixture")
        assert "3.4.22.69" in lake_entries["P0DTD1"]["ec"]

    def test_proteome_ids_match(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            expected = sorted({
                x["id"] for x in (orig.get("uniProtKBCrossReferences") or [])
                if x["database"] == "Proteomes"
            })
            assert (lake_entries[accession].get("proteome_id") or []) == expected, f"{accession}: proteome_id"

    def test_division_values(self, originals: Entries, lake_entries: Entries) -> None:
        allowed = {"archaea", "bacteria", "fungi", "human", "invertebrates", "mammals",
                   "plants", "rodents", "vertebrates", "viruses", "unclassified"}
        for accession in originals:
            lake = lake_entries[accession]
            assert lake["division"] in allowed, f"{accession}: division {lake['division']!r}"
            if lake["organism_id"] == 9606:
                assert lake["division"] == "human", accession

    def test_xref_dbs_match(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            orig_dbs = {
                x["database"]
                for x in (orig.get("uniProtKBCrossReferences") or [])
            }
            lake_dbs = set(lake.get("xref_dbs") or [])
            assert orig_dbs == lake_dbs, (
                f"{accession}: xref_dbs mismatch"
            )

    def test_keyword_ids_match(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            orig_kw = [kw["id"] for kw in (orig.get("keywords") or [])]
            lake_kw = lake.get("keywordid") or []
            assert set(orig_kw) == set(lake_kw), (
                f"{accession}: keywordid mismatch"
            )

    def test_keyword_names_match(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            orig_kw = [kw["name"] for kw in (orig.get("keywords") or [])]
            lake_kw = lake.get("keyword") or []
            assert set(orig_kw) == set(lake_kw), (
                f"{accession}: keyword mismatch"
            )

    def test_feature_count_matches_source(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            assert lake["feature_count"] == len(orig.get("features") or []), (
                f"{accession}: feature_count mismatch"
            )

    def test_xref_count_matches_source(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            assert lake["xref_count"] == len(orig.get("uniProtKBCrossReferences") or []), (
                f"{accession}: xref_count mismatch"
            )

    def test_comment_count_matches_source(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            assert lake["comment_count"] == len(orig.get("comments") or []), (
                f"{accession}: comment_count mismatch"
            )

    def test_reference_count_matches_source(self, originals: Entries, lake_entries: Entries) -> None:
        for accession, orig in originals.items():
            lake = lake_entries[accession]
            assert lake["reference_count"] == len(orig.get("references") or []), (
                f"{accession}: reference_count mismatch"
            )


class TestShortcutValues:
    """Every shortcut column of every table equals an independent Python
    computation from the JSON entry (bin/shortcut_reference.py, which the
    validator's check 21 also uses)."""

    def test_reference_covers_every_shortcut(self) -> None:
        from parquet_transform import TABLE_META
        from shortcut_reference import expected_shortcuts
        entry = {"primaryAccession": "P1", "secondaryAccessions": ["Q1"],
                 "entryType": "UniProtKB reviewed (Swiss-Prot)",
                 "features": [{}], "uniProtKBCrossReferences": [{}],
                 "comments": [{}], "references": [{}]}
        rows = expected_shortcuts(entry)
        missing = {table: sorted(set(meta["columns"]["shortcut"]) - set(rows[table][0]))
                   for table, meta in TABLE_META.items()}
        assert not any(missing.values()), f"shortcuts without a reference value: {missing}"
        extra = {table: sorted(set(rows[table][0]) - set(meta["columns"]["shortcut"]))
                 for table, meta in TABLE_META.items()}
        assert not any(extra.values()), f"reference values that are not shortcuts: {extra}"

    def test_every_shortcut_matches_reference(self, originals: Entries, lake: str, lake_entries: Entries,
                                              lake_features: Rows, lake_xrefs: Rows, lake_comments: Rows,
                                              lake_publications: Rows) -> None:
        from shortcut_reference import compare_rows, expected_shortcuts, lake_order
        amap: dict[str, list[dict[str, Any]]] = {}
        for row in open_table(lake, "accession_map").to_table().to_pylist():
            amap.setdefault(row["primary_accession"], []).append(row)
        lake_rows = {"entries": {a: [r] for a, r in lake_entries.items()},
                     "features": lake_features, "xrefs": lake_xrefs,
                     "comments": lake_comments, "publications": lake_publications,
                     "accession_map": amap}
        problems: list[str] = []
        for accession, orig in originals.items():
            for table, rows in expected_shortcuts(orig).items():
                got = lake_order(table, lake_rows[table].get(accession, []))
                problems += compare_rows(table, accession, rows, got)
        assert not problems, f"{len(problems)} shortcut mismatches, e.g. {problems[:5]}"
