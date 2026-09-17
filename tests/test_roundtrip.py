"""Roundtrip equivalence test: JSONL → Parquet → reconstruct → compare.

The ultimate data-lake integrity test: load the original JSON entries,
run the pipeline, then reconstruct each entry from the five Parquet
tables and verify that every piece of data survived the round trip.

Equivalence is measured set-wise (order-independent) since the pipeline
sorts rows differently from the input order.
"""

import gzip
import json
import os
from collections import defaultdict

import pyarrow.dataset as ds
import pytest

from reconstruct import deep_sort, normalize_value  # shared with the validator


# ─── Helpers ────────────────────────────────────────────────────────────


from conftest import load_original_entries, open_table  # noqa: F401


# ─── Fixtures ───────────────────────────────────────────────────────────
# originals, lake, lake_entries, lake_features, lake_xrefs, lake_comments and
# lake_publications are session fixtures in conftest.py (shared with
# tests/test_reconstruct.py so the row dicts are built once).


# ─── Tests: Completeness ───────────────────────────────────────────────


class TestAccessionCompleteness:
    """Every accession in the original data must appear in the lake."""

    def test_all_accessions_present(self, originals, lake_entries):
        original_accs = set(originals.keys())
        lake_accs = set(lake_entries.keys())
        missing = original_accs - lake_accs
        assert not missing, f"Accessions missing from lake: {missing}"

    def test_no_extra_accessions(self, originals, lake_entries):
        original_accs = set(originals.keys())
        lake_accs = set(lake_entries.keys())
        extra = lake_accs - original_accs
        assert not extra, f"Unexpected accessions in lake: {extra}"

    def test_entry_count_matches(self, originals, lake_entries):
        assert len(originals) == len(lake_entries)


# ─── Tests: Entry-level field equivalence ──────────────────────────────


class TestEntryFields:
    """For each entry, verify that scalar and nested fields survived."""

    def test_entry_type_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["entry_type"] == orig["entryType"], (
                f"{acc}: entry_type {lake['entry_type']!r} != {orig['entryType']!r}"
            )

    def test_uniprot_id_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["id"] == orig["uniProtkbId"], (
                f"{acc}: id mismatch"
            )

    def test_taxid_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["taxid"] == orig["organism"]["taxonId"], (
                f"{acc}: taxid {lake['taxid']} != {orig['organism']['taxonId']}"
            )

    def test_organism_name_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["organism_name"] == orig["organism"]["scientificName"], (
                f"{acc}: organism_name mismatch"
            )

    def test_sequence_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["sequence"] == orig["sequence"]["value"], (
                f"{acc}: sequence mismatch"
            )

    def test_seq_length_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["seq_length"] == orig["sequence"]["length"], (
                f"{acc}: seq_length {lake['seq_length']} != {orig['sequence']['length']}"
            )

    def test_seq_mass_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["seq_mass"] == orig["sequence"]["molWeight"], (
                f"{acc}: seq_mass mismatch"
            )

    def test_seq_md5_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["seq_md5"] == orig["sequence"]["md5"], (
                f"{acc}: seq_md5 mismatch"
            )

    def test_annotation_score_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["annotation_score"] == orig["annotationScore"], (
                f"{acc}: annotation_score mismatch"
            )

    def test_protein_existence_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["protein_existence"] == orig["proteinExistence"], (
                f"{acc}: protein_existence mismatch"
            )

    def test_reviewed_flag_correct(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            expected = "Swiss-Prot" in orig["entryType"]
            assert lake["reviewed"] == expected, (
                f"{acc}: reviewed {lake['reviewed']} != expected {expected}"
            )

    def test_secondary_accessions_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_sec = orig.get("secondaryAccessions") or []
            lake_sec = lake.get("secondary_accs") or []
            assert set(orig_sec) == set(lake_sec), (
                f"{acc}: secondary_accs mismatch: {orig_sec} vs {lake_sec}"
            )

    def test_entry_audit_dates_preserved(self, originals, lake_entries):
        """entryAudit dates and versions survive the flattening."""
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            audit = orig["entryAudit"]
            # Dates are stored as DATE type, compare as strings
            assert str(lake["first_public"]) == audit["firstPublicDate"], (
                f"{acc}: first_public mismatch"
            )
            assert str(lake["last_modified"]) == audit["lastAnnotationUpdateDate"], (
                f"{acc}: last_modified mismatch"
            )
            assert lake["entry_version"] == audit["entryVersion"], (
                f"{acc}: entry_version mismatch"
            )
            assert lake["seq_version"] == audit["sequenceVersion"], (
                f"{acc}: seq_version mismatch"
            )


# ─── Tests: Nested struct preservation ─────────────────────────────────


class TestNestedStructs:
    """The full nested structures stored in the entries table must match originals."""

    def test_organism_struct_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_org = normalize_value(orig["organism"])
            lake_org = normalize_value(lake["organism"])
            assert deep_sort(orig_org) == deep_sort(lake_org), (
                f"{acc}: organism struct mismatch"
            )

    def test_protein_desc_struct_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_pd = normalize_value(orig.get("proteinDescription"))
            lake_pd = normalize_value(lake.get("protein_desc"))
            assert deep_sort(orig_pd) == deep_sort(lake_pd), (
                f"{acc}: proteinDescription struct mismatch"
            )

    def test_genes_struct_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_genes = normalize_value(orig.get("genes") or [])
            lake_genes = normalize_value(lake.get("genes") or [])
            assert deep_sort(orig_genes) == deep_sort(lake_genes), (
                f"{acc}: genes struct mismatch"
            )

    def test_keywords_struct_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_kw = normalize_value(orig.get("keywords") or [])
            lake_kw = normalize_value(lake.get("keywords") or [])
            assert deep_sort(orig_kw) == deep_sort(lake_kw), (
                f"{acc}: keywords struct mismatch"
            )

    def test_extra_attributes_preserved(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_ea = normalize_value(orig.get("extraAttributes"))
            lake_ea = normalize_value(lake.get("extra_attributes"))
            assert deep_sort(orig_ea) == deep_sort(lake_ea), (
                f"{acc}: extraAttributes mismatch"
            )


# ─── Tests: Child table counts ─────────────────────────────────────────


class TestChildTableCounts:
    """Row counts in child tables must match the original array lengths."""

    def test_feature_counts(self, originals, lake_features):
        for acc, orig in originals.items():
            orig_count = len(orig.get("features") or [])
            lake_count = len(lake_features.get(acc, []))
            assert orig_count == lake_count, (
                f"{acc}: features count {lake_count} != original {orig_count}"
            )

    def test_xref_counts(self, originals, lake_xrefs):
        for acc, orig in originals.items():
            orig_count = len(orig.get("uniProtKBCrossReferences") or [])
            lake_count = len(lake_xrefs.get(acc, []))
            assert orig_count == lake_count, (
                f"{acc}: xrefs count {lake_count} != original {orig_count}"
            )

    def test_comment_counts(self, originals, lake_comments):
        for acc, orig in originals.items():
            orig_count = len(orig.get("comments") or [])
            lake_count = len(lake_comments.get(acc, []))
            assert orig_count == lake_count, (
                f"{acc}: comments count {lake_count} != original {orig_count}"
            )

    def test_publication_counts(self, originals, lake_publications):
        for acc, orig in originals.items():
            orig_count = len(orig.get("references") or [])
            lake_count = len(lake_publications.get(acc, []))
            assert orig_count == lake_count, (
                f"{acc}: publications count {lake_count} != original {orig_count}"
            )


# ─── Tests: Child table content (lossless nested structs) ─────────────


class TestFeatureContent:
    """The 'feature' column preserves the full original feature struct."""

    def test_all_features_present(self, originals, lake_features):
        for acc, orig in originals.items():
            orig_features = orig.get("features") or []
            lake_rows = lake_features.get(acc, [])

            # Compare the preserved 'feature' structs (order-independent)
            orig_set = {
                json.dumps(deep_sort(normalize_value(f)), sort_keys=True, default=str)
                for f in orig_features
            }
            lake_set = {
                json.dumps(deep_sort(normalize_value(row["feature"])), sort_keys=True, default=str)
                for row in lake_rows
            }
            missing = orig_set - lake_set
            assert not missing, (
                f"{acc}: {len(missing)} features missing from lake"
            )


class TestXrefContent:
    """Cross-references are preserved at the field level."""

    def test_all_xrefs_present(self, originals, lake_xrefs):
        for acc, orig in originals.items():
            orig_xrefs = orig.get("uniProtKBCrossReferences") or []
            lake_rows = lake_xrefs.get(acc, [])

            # Build comparable tuples: (database, id)
            orig_keys = {(x["database"], x["id"]) for x in orig_xrefs}
            lake_keys = {(row["database"], row["id"]) for row in lake_rows}
            missing = orig_keys - lake_keys
            assert not missing, (
                f"{acc}: {len(missing)} xrefs missing from lake, e.g. {list(missing)[:3]}"
            )


class TestCommentContent:
    """The 'comment' column preserves the full original comment as JSON."""

    def test_all_comments_present(self, originals, lake_comments):
        for acc, orig in originals.items():
            orig_comments = orig.get("comments") or []
            lake_rows = lake_comments.get(acc, [])

            # Parse the stored JSON and compare (order-independent)
            orig_set = {
                json.dumps(deep_sort(normalize_value(c)), sort_keys=True, default=str)
                for c in orig_comments
            }
            lake_set = set()
            for row in lake_rows:
                comment = row["comment"]
                if isinstance(comment, str):
                    comment = json.loads(comment)
                lake_set.add(
                    json.dumps(deep_sort(normalize_value(comment)), sort_keys=True, default=str)
                )
            missing = orig_set - lake_set
            assert not missing, (
                f"{acc}: {len(missing)} comments missing from lake"
            )

    def test_text_value_matches_texts(self, originals, lake_comments):
        """text_value is the double-newline join of texts[*].value, else NULL."""
        for acc, orig in originals.items():
            by_type = defaultdict(list)
            for row in lake_comments.get(acc, []):
                by_type[row["comment_type"]].append(row["text_value"])
            for c in orig.get("comments") or []:
                lake_values = by_type[c["commentType"]]
                if "texts" in c:
                    expected = "\n\n".join(t["value"] for t in c["texts"])
                    assert expected in lake_values, (
                        f"{acc}: {c['commentType']} text_value not found"
                    )
                else:
                    assert all(v is None for v in lake_values), (
                        f"{acc}: {c['commentType']} has no texts but text_value is set"
                    )


class TestPublicationContent:
    """The 'reference' column preserves the full original reference struct."""

    def test_all_publications_present(self, originals, lake_publications):
        for acc, orig in originals.items():
            orig_refs = orig.get("references") or []
            lake_rows = lake_publications.get(acc, [])

            # Compare the preserved 'reference' structs (order-independent)
            orig_set = {
                json.dumps(deep_sort(normalize_value(r)), sort_keys=True, default=str)
                for r in orig_refs
            }
            lake_set = {
                json.dumps(deep_sort(normalize_value(row["reference"])), sort_keys=True, default=str)
                for row in lake_rows
            }
            missing = orig_set - lake_set
            assert not missing, (
                f"{acc}: {len(missing)} references missing from lake"
            )


# ─── Tests: Convenience column derivation correctness ─────────────────


class TestConvenienceColumns:
    """Convenience columns must be correctly derived from the source data."""

    def test_gene_names_match(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_genes = [
                g["geneName"]["value"]
                for g in (orig.get("genes") or [])
                if g.get("geneName", {}).get("value")
            ]
            # The SQL list_transform produces NULL for genes without geneName;
            # filter those out for comparison.
            lake_genes = [g for g in (lake.get("gene_names") or []) if g is not None]
            assert set(orig_genes) == set(lake_genes), (
                f"{acc}: gene_names mismatch: {orig_genes} vs {lake_genes}"
            )

    def test_go_ids_match(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_go = {
                x["id"]
                for x in (orig.get("uniProtKBCrossReferences") or [])
                if x["database"] == "GO"
            }
            lake_go = set(lake.get("go_ids") or [])
            assert orig_go == lake_go, (
                f"{acc}: go_ids mismatch"
            )

    def test_gene_name_matches(self, originals, lake_entries):
        for acc in originals:
            lake = lake_entries[acc]
            assert lake["gene_name"] == (lake["gene_names"] or [None])[0], f"{acc}: gene_name"

    def test_go_terms_match_go_ids(self, originals, lake_entries):
        for acc in originals:
            lake = lake_entries[acc]
            go_terms = lake.get("go_terms") or []
            go_ids = lake.get("go_ids") or []
            assert {t["id"] for t in go_terms} == set(go_ids), f"{acc}: go_terms ids"
            assert len(go_terms) == len(go_ids), f"{acc}: go_terms length"
            for t in go_terms:
                assert t["aspect"] in {"P", "F", "C", None}, f"{acc}: aspect {t['aspect']!r}"
                if t["aspect"] is not None:
                    assert t["term"], f"{acc}: empty term"

    def test_pubmed_ids_match(self, originals, lake_entries):
        for acc, orig in originals.items():
            ids = set()
            for r in orig.get("references") or []:
                for c in (r.get("citation") or {}).get("citationCrossReferences") or []:
                    if c["database"] == "PubMed":
                        ids.add(c["id"])
            expected = sorted(ids, key=int)
            assert (lake_entries[acc].get("pubmed_ids") or []) == expected, f"{acc}: pubmed_ids"

    def test_proteome_ids_match(self, originals, lake_entries):
        for acc, orig in originals.items():
            expected = sorted({
                x["id"] for x in (orig.get("uniProtKBCrossReferences") or [])
                if x["database"] == "Proteomes"
            })
            assert (lake_entries[acc].get("proteome_ids") or []) == expected, f"{acc}: proteome_ids"

    def test_division_values(self, originals, lake_entries):
        allowed = {"archaea", "bacteria", "fungi", "human", "invertebrates", "mammals",
                   "plants", "rodents", "vertebrates", "viruses", "unclassified"}
        for acc in originals:
            lake = lake_entries[acc]
            assert lake["division"] in allowed, f"{acc}: division {lake['division']!r}"
            if lake["taxid"] == 9606:
                assert lake["division"] == "human", acc

    def test_xref_dbs_match(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_dbs = {
                x["database"]
                for x in (orig.get("uniProtKBCrossReferences") or [])
            }
            lake_dbs = set(lake.get("xref_dbs") or [])
            assert orig_dbs == lake_dbs, (
                f"{acc}: xref_dbs mismatch"
            )

    def test_keyword_ids_match(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_kw = [kw["id"] for kw in (orig.get("keywords") or [])]
            lake_kw = lake.get("keyword_ids") or []
            assert set(orig_kw) == set(lake_kw), (
                f"{acc}: keyword_ids mismatch"
            )

    def test_keyword_names_match(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            orig_kw = [kw["name"] for kw in (orig.get("keywords") or [])]
            lake_kw = lake.get("keyword_names") or []
            assert set(orig_kw) == set(lake_kw), (
                f"{acc}: keyword_names mismatch"
            )

    def test_feature_count_matches_source(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["feature_count"] == len(orig.get("features") or []), (
                f"{acc}: feature_count mismatch"
            )

    def test_xref_count_matches_source(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["xref_count"] == len(orig.get("uniProtKBCrossReferences") or []), (
                f"{acc}: xref_count mismatch"
            )

    def test_comment_count_matches_source(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["comment_count"] == len(orig.get("comments") or []), (
                f"{acc}: comment_count mismatch"
            )

    def test_reference_count_matches_source(self, originals, lake_entries):
        for acc, orig in originals.items():
            lake = lake_entries[acc]
            assert lake["reference_count"] == len(orig.get("references") or []), (
                f"{acc}: reference_count mismatch"
            )
