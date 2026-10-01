"""g(f(x)) == x: bin/reconstruct.py rebuilds every fixture entry from the lake,
array order included."""

import random

import pyarrow as pa
import pytest

from reconstruct import (reconstruct_entry, entries_match, normalize_value, first_diff,
                         rows_from_arrow)


def test_entry_is_its_source_columns():
    """A row's source columns are its JSON element; shortcut columns and the
    Hive partition column are never read, and NULL columns are left out."""
    entry = {"accession": "P1", "reviewed": True, "gene_primary": ["X"], "review_status": "swissprot",
             "primaryAccession": "P1", "entryType": "UniProtKB reviewed (Swiss-Prot)",
             "sequence": {"value": "MKV", "length": 3, "md5": None}, "geneLocations": None}
    feature = {"accession": "P1", "feature_index": 0, "start_pos": 1, "evidence_codes": [],
               "type": "Chain", "location": {"start": {"value": 1}, "end": {"value": 3}}}
    assert reconstruct_entry(entry, [feature]) == {
        "primaryAccession": "P1", "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "sequence": {"value": "MKV", "length": 3},
        "features": [{"type": "Chain", "location": {"start": {"value": 1}, "end": {"value": 3}}}]}


def test_rows_from_arrow_decodes_json_columns():
    """arrow.json columns (and those named, for DuckDB results) are decoded;
    a string column that happens to hold JSON-looking text is not."""
    json_col = pa.ExtensionArray.from_storage(pa.json_(), pa.array(['{"b": 1}', None, '"a"']))
    tbl = pa.table({"j": json_col, "s": ["12", "x", None], "t": ['[1]', None, None]})
    assert rows_from_arrow(tbl) == [{"j": {"b": 1}, "s": "12", "t": "[1]"},
                                   {"j": None, "s": "x", "t": None},
                                   {"j": "a", "s": None, "t": None}]
    assert rows_from_arrow(tbl, json_columns={"t"})[0]["t"] == [1]


def test_array_order_is_compared():
    """Reordered arrays do not match; reordered object keys do."""
    assert entries_match({"a": [1, 2], "b": {"x": 1, "y": 2}}, {"b": {"y": 2, "x": 1}, "a": [1, 2]})[0]
    ok, diff = entries_match({"a": [1, 2]}, {"a": [2, 1]})
    assert not ok and diff.startswith("$.a[0]")


def test_empty_child_array_matches_absent():
    """An empty child-table array and an absent one are both zero child rows,
    so they compare equal; other empty arrays, and non-empty ones, do not."""
    assert entries_match({"accession": "P1"}, {"accession": "P1", "features": [], "references": []})[0]
    assert entries_match({"accession": "P1", "comments": []}, {"accession": "P1"})[0]
    assert not entries_match({"accession": "P1"}, {"accession": "P1", "features": [{"type": "Chain"}]})[0]
    assert not entries_match({"accession": "P1"}, {"accession": "P1", "keywords": []})[0]


def test_swapped_positions_do_not_match(originals, lake_entries, lake_features, lake_xrefs,
                                        lake_comments, lake_publications):
    """Swapping two features' positions breaks reconstruction: array order is
    part of the lossless guarantee (the validator's check 16 uses the same
    reconstruct_entry / entries_match)."""
    for accession, rows in lake_features.items():
        pair = next(((a, b) for a in rows for b in rows
                     if a["feature_index"] < b["feature_index"]
                     and (a["type"], a["start_pos"]) != (b["type"], b["start_pos"])), None)
        if pair:
            break
    else:
        pytest.skip("no entry with two distinct features")
    a, b = pair
    swapped = [dict(r, feature_index=b["feature_index"]) if r is a else
               dict(r, feature_index=a["feature_index"]) if r is b else r for r in rows]
    others = [t.get(accession, []) for t in (lake_xrefs, lake_comments, lake_publications)]
    assert entries_match(reconstruct_entry(lake_entries[accession], rows, *others), originals[accession])[0]
    ok, _ = entries_match(reconstruct_entry(lake_entries[accession], swapped, *others), originals[accession])
    assert not ok, f"{accession}: swapping feature positions went unnoticed"


def test_child_rows_are_placed_by_index(originals, lake_entries, lake_features, lake_xrefs,
                                        lake_comments, lake_publications):
    """Rows handed over in any order rebuild arrays in source order (*_index)."""
    rng = random.Random(0)
    accession = max(originals, key=lambda a: len(lake_features.get(a, [])))
    shuffled = [list(t.get(accession, [])) for t in (lake_features, lake_xrefs, lake_comments, lake_publications)]
    for rows in shuffled:
        rng.shuffle(rows)
    rebuilt = reconstruct_entry(lake_entries[accession], *shuffled)
    ok, diff = entries_match(rebuilt, originals[accession])
    assert ok, f"{accession}: {diff}"


@pytest.mark.parametrize("source", [{}, {"FUNCTION": 2, "SUBUNIT": 1}])
def test_map_columns_round_trip_through_arrow(source):
    """An empty Parquet MAP must come back as {} — the source's {} — not [] (the
    validator's reconstruction check would otherwise report 'type list != dict')."""
    tbl = pa.table({"m": pa.array([source], type=pa.map_(pa.string(), pa.int64()))})
    row = tbl.to_pylist(maps_as_pydicts="strict")[0]
    assert normalize_value(row["m"]) == source
    assert first_diff(normalize_value(row["m"]), source) is None


def test_every_entry_reconstructs(originals, lake_entries, lake_features, lake_xrefs,
                                  lake_comments, lake_publications):
    failures = []
    for accession, orig in originals.items():
        rebuilt = reconstruct_entry(
            lake_entries[accession],
            lake_features.get(accession, []),
            lake_xrefs.get(accession, []),
            lake_comments.get(accession, []),
            lake_publications.get(accession, []),
        )
        ok, diff = entries_match(rebuilt, orig)
        if not ok:
            failures.append(f"{accession}: {diff}")
    assert not failures, f"{len(failures)} entries differ; first: {failures[0]}"


def test_reconstruction_has_no_extra_keys(originals, lake_entries):
    """Top-level keys of the rebuilt entry are a subset of the original's."""
    for accession, orig in list(originals.items())[:50]:
        rebuilt = reconstruct_entry(lake_entries[accession])
        extra = set(rebuilt) - set(orig)
        assert not extra, f"{accession}: unexpected keys {extra}"
