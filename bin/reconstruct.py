#!/usr/bin/env python3
"""
reconstruct.py — rebuild a UniProtKB JSON entry from its lake rows.

``reconstruct_entry`` is the inverse ``g`` of the transform ``f``: given one
``entries`` row and the child rows for the same accession, it returns the
UniProtKB JSON entry.  ``tests/test_reconstruct.py`` and the validator's
``check_reconstruction`` assert ``g(f(x)) == x`` for every fixture entry and
for a sample of every release, so the lake is provably lossless.

A row's source columns (every column that is not a shortcut) are its JSON
element, key for key: the entry, feature, cross-reference, comment or
reference as UniProt ships it.  Shortcut columns are never read.  Child rows
are placed by their ``*_index`` column, so array order is part of the
comparison.  Dict key order is ignored (dict ``==``), and an empty
``features`` / ``uniProtKBCrossReferences`` / ``comments`` / ``references``
array counts as absent: both are stored as zero child rows
(``drop_empty_child_arrays``).

A source column whose type differs between entries is stored as JSON text
(Parquet's JSON type, ``arrow.json`` in Arrow): ``rows_from_arrow`` decodes
it.  Convert every Arrow table with it before reconstructing.

``normalize_value`` and ``entries_match`` live here so the validator and the
test suite normalise identically.
"""

import json

import pyarrow as pa

from parquet_transform import CHILD_TABLES, PARTITION_KEY, TABLE_META


# ─── Normalisation (shared by tests and validator) ──────────────────────


def normalize_value(val):
    """Normalise a value for comparison: drop None-valued dict fields (a
    STRUCT holds every key any entry has, NULL where this one has none),
    keep numbers, stringify everything else.

    Callers must convert Arrow rows with ``rows_from_arrow`` (or
    ``to_pylist(maps_as_pydicts="strict")``) so MAP columns arrive as dicts:
    the default list-of-tuples form makes an empty MAP indistinguishable from
    an empty JSON array."""
    if val is None:
        return None
    if isinstance(val, dict):
        return {k: normalize_value(v) for k, v in val.items() if v is not None}
    if isinstance(val, list):
        return [normalize_value(v) for v in val]
    if isinstance(val, (int, float)):
        return val
    return str(val)


# Top-level arrays stored as child-table rows: an empty one and an absent
# one are both zero rows, so the two are not told apart.
CHILD_ARRAYS = tuple(array for array, _, _ in CHILD_TABLES.values())


def drop_empty_child_arrays(entry: dict) -> dict:
    """``entry`` without its empty child-table arrays (see CHILD_ARRAYS)."""
    return {k: v for k, v in entry.items() if not (k in CHILD_ARRAYS and v == [])}


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


# ─── Arrow rows ─────────────────────────────────────────────────────────


def rows_from_arrow(table: pa.Table, json_columns=()) -> list[dict]:
    """``table``'s rows as dicts, MAPs as dicts and every JSON column
    decoded: those typed arrow.json (as read from the Parquet files) and
    those named in ``json_columns`` (a query result, where DuckDB hands JSON
    over as plain strings)."""
    decode = {f.name for f in table.schema if isinstance(f.type, pa.JsonType)} | set(json_columns)
    rows = table.to_pylist(maps_as_pydicts="strict")
    if decode:
        for row in rows:
            for name in decode & row.keys():
                if row[name] is not None:
                    row[name] = json.loads(row[name])
    return rows


# ─── Reconstruction ─────────────────────────────────────────────────────


def _element(row: dict, table: str) -> dict:
    """The JSON element a row holds: its source columns, NULL ones left out."""
    skip = set(TABLE_META[table]["columns"]["shortcut"]) | {PARTITION_KEY}
    return {k: v for k, v in row.items() if k not in skip and v is not None}


def _in_order(rows, index_column):
    """Child rows in source-array order (by their *_index column when present)."""
    rows = list(rows)
    if rows and rows[0].get(index_column) is not None:
        rows.sort(key=lambda r: r[index_column])
    return rows


def reconstruct_entry(entry_row, feature_rows=(), xref_rows=(), comment_rows=(),
                      publication_rows=()) -> dict:
    """Rebuild the UniProtKB JSON entry from lake rows.

    ``entry_row`` is one ``entries`` row as ``{column: python value}``; the
    other arguments are lists of child rows for the same accession (any
    order; they are placed by their ``*_index`` column).  Rows come from
    ``rows_from_arrow``.  The entry is the entries row's source columns plus
    one array per child table; keys whose value is None are dropped, as are
    None-valued fields inside structs (``normalize_value``), and a child
    array with no rows is left out.
    """
    out = _element(entry_row, "entries")
    for table, rows in zip(CHILD_TABLES, (feature_rows, xref_rows, comment_rows, publication_rows)):
        array, index, _ = CHILD_TABLES[table]
        if rows:
            out[array] = [_element(r, table) for r in _in_order(rows, index)]
    return normalize_value(out)


class _Bool:
    """A JSON boolean for comparison: Python's True == 1, a JSON true is not a 1."""
    __slots__ = ("value",)

    def __init__(self, value: bool):
        self.value = value

    def __eq__(self, other):
        return isinstance(other, _Bool) and other.value == self.value

    def __repr__(self):
        return repr(self.value)


def _strict_bools(val):
    """``val`` with every bool wrapped in _Bool, so it never equals a number."""
    if isinstance(val, bool):
        return _Bool(val)
    if isinstance(val, dict):
        return {k: _strict_bools(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_strict_bools(v) for v in val]
    return val


def entries_match(reconstructed, original):
    """(bool, first differing path) for a reconstructed vs original entry.
    Numbers compare by value (5 == 5.0: a field mixing integers and decimals
    is stored as DOUBLE); a boolean equals only the same boolean."""
    a = _strict_bools(drop_empty_child_arrays(normalize_value(reconstructed)))
    b = _strict_bools(drop_empty_child_arrays(normalize_value(original)))
    if a == b:
        return True, None
    return False, first_diff(a, b)
