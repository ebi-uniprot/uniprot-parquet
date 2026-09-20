"""g(f(x)) == x: bin/reconstruct.py rebuilds every fixture entry from the lake (plan A11)."""

import pyarrow as pa
import pytest

from reconstruct import reconstruct_entry, entries_match, normalize_value, first_diff


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
    for acc, orig in originals.items():
        rebuilt = reconstruct_entry(
            lake_entries[acc],
            lake_features.get(acc, []),
            lake_xrefs.get(acc, []),
            lake_comments.get(acc, []),
            lake_publications.get(acc, []),
        )
        ok, diff = entries_match(rebuilt, orig)
        if not ok:
            failures.append(f"{acc}: {diff}")
    assert not failures, f"{len(failures)} entries differ; first: {failures[0]}"


def test_reconstruction_has_no_extra_keys(originals, lake_entries):
    """Top-level keys of the rebuilt entry are a subset of the original's."""
    for acc, orig in list(originals.items())[:50]:
        rebuilt = reconstruct_entry(lake_entries[acc])
        extra = set(rebuilt) - set(orig)
        assert not extra, f"{acc}: unexpected keys {extra}"
