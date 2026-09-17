#!/usr/bin/env python3
"""
reconstruct.py — rebuild a UniProtKB JSON entry from its lake rows (plan A11).

``reconstruct_entry`` is the inverse ``g`` of the transform ``f``: given one
``entries`` row and the child rows for the same accession, it returns the
UniProtKB JSON entry.  ``tests/test_reconstruct.py`` and the validator's
``check_reconstruction`` assert ``g(f(x)) == x`` for every fixture entry and
for a sample of every release, so the lake is provably lossless.

Comparison is order-independent inside arrays (``deep_sort``): the child
tables carry no position index, so array order is not reproduced.  This is
a known limitation recorded in ``PLAN_SCHEMA_V2.md`` (Results log, Step 10).

``deep_sort`` and ``normalize_value`` live here so the validator and the test
suite normalise identically.
"""

import json


# ─── Normalisation (shared by tests and validator) ──────────────────────


def deep_sort(obj):
    """Recursively sort dict keys and list elements for order-independent comparison."""
    if isinstance(obj, dict):
        return {k: deep_sort(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        sorted_items = [deep_sort(item) for item in obj]
        try:
            return sorted(sorted_items, key=lambda x: json.dumps(x, sort_keys=True, default=str))
        except TypeError:
            return sorted_items
    return obj


def normalize_value(val):
    """Normalise a value for comparison: drop None-valued dict fields (Parquet
    unions add nullable fields the source never had), keep numbers, stringify
    everything else (dates, etc.)."""
    if val is None:
        return None
    if isinstance(val, dict):
        return {k: normalize_value(v) for k, v in val.items() if v is not None}
    if isinstance(val, list):
        return [normalize_value(v) for v in val]
    if isinstance(val, (int, float)):
        return val
    return str(val)


def first_diff(a, b, path="$"):
    """Return the first differing key path between two normalised objects, or None."""
    if type(a) is not type(b) and not (isinstance(a, (int, float)) and isinstance(b, (int, float))):
        return f"{path}: type {type(a).__name__} != {type(b).__name__}"
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                return f"{path}.{k}: missing on left"
            if k not in b:
                return f"{path}.{k}: missing on right"
            d = first_diff(a[k], b[k], f"{path}.{k}")
            if d:
                return d
        return None
    if isinstance(a, list):
        if len(a) != len(b):
            return f"{path}: list length {len(a)} != {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            d = first_diff(x, y, f"{path}[{i}]")
            if d:
                return d
        return None
    if a != b:
        return f"{path}: {a!r} != {b!r}"
    return None


# ─── Reconstruction ─────────────────────────────────────────────────────


def _date(v):
    return None if v is None else str(v)


def reconstruct_entry(entry_row, feature_rows=(), xref_rows=(), comment_rows=(),
                      publication_rows=()) -> dict:
    """Rebuild the UniProtKB JSON entry from lake rows.

    ``entry_row`` is one ``entries`` row as ``{column: python value}``; the
    other arguments are lists of child rows for the same accession (any
    order).  Keys whose value is None are dropped, as are None-valued fields
    inside structs (``normalize_value``).
    """
    e = entry_row
    out = {
        "entryType": e.get("entry_type"),
        "primaryAccession": e.get("acc"),
        "secondaryAccessions": e.get("secondary_accs"),
        "uniProtkbId": e.get("id"),
        "entryAudit": {
            "firstPublicDate": _date(e.get("first_public")),
            "lastAnnotationUpdateDate": _date(e.get("last_modified")),
            "lastSequenceUpdateDate": _date(e.get("last_seq_modified")),
            "entryVersion": e.get("entry_version"),
            "sequenceVersion": e.get("seq_version"),
        },
        "annotationScore": e.get("annotation_score"),
        "organism": e.get("organism"),
        "organismHosts": e.get("organism_hosts"),
        "proteinExistence": e.get("protein_existence"),
        "proteinDescription": e.get("protein_desc"),
        "genes": e.get("genes"),
        "geneLocations": e.get("gene_locations"),
        "keywords": e.get("keywords"),
        "sequence": {
            "value": e.get("sequence"),
            "length": e.get("seq_length"),
            "molWeight": e.get("seq_mass"),
            "md5": e.get("seq_md5"),
            "crc64": e.get("seq_crc64"),
        },
        "extraAttributes": e.get("extra_attributes"),
    }
    if feature_rows:
        out["features"] = [r["feature"] for r in feature_rows]
    if xref_rows:
        out["uniProtKBCrossReferences"] = [
            {"database": r["database"], "id": r["id"], "properties": r.get("properties"),
             "isoformId": r.get("isoform_id"), "evidences": r.get("evidences")}
            for r in xref_rows
        ]
    if comment_rows:
        out["comments"] = [
            json.loads(r["comment"]) if isinstance(r["comment"], str) else r["comment"]
            for r in comment_rows
        ]
    if publication_rows:
        out["references"] = [r["reference"] for r in publication_rows]
    return normalize_value({k: v for k, v in out.items() if v is not None})


def entries_match(reconstructed, original):
    """(bool, first differing path) for a reconstructed vs original entry."""
    a = deep_sort(normalize_value(reconstructed))
    b = deep_sort(normalize_value(original))
    if a == b:
        return True, None
    return False, first_diff(a, b)
