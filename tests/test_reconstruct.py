"""g(f(x)) == x: bin/reconstruct.py rebuilds every fixture entry from the lake (plan A11)."""

import pytest

from reconstruct import reconstruct_entry, entries_match


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
