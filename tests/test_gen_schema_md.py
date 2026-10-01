"""Tests for bin/gen_schema_md.py."""

import re

import pyarrow as pa
import pytest

from gen_schema_md import _display_type
from parquet_transform import CHILD_TABLES

WIDE = pa.struct([(f"field_{i:02d}", pa.string()) for i in range(20)])
WIDE_SHORT = "struct<" + ", ".join(f"field_{i:02d}" for i in range(20)) + ">"


class TestDisplayType:
    """A nested type too long for a table cell is shortened to its top-level
    field names, never cut off mid-token, whoever wrote the list type."""

    def test_short_type_unchanged(self):
        assert _display_type("list<l: int32>") == "list<l: int32>"

    def test_struct(self):
        assert _display_type(str(WIDE)) == WIDE_SHORT

    @pytest.mark.parametrize("list_type, head", [
        (pa.list_(pa.field("l", WIDE)), "list"),              # DuckDB's child name
        (pa.list_(WIDE), "list"),                             # PyArrow's: item
        (pa.list_(pa.field("element", WIDE)), "list"),        # Parquet spec's
        (pa.large_list(WIDE), "large_list"),
        (pa.list_(pa.list_(WIDE)), "list<list"),
    ])
    def test_list_of_struct(self, list_type, head):
        closing = ">" * head.count("list")
        assert _display_type(str(list_type)) == f"{head}<{WIDE_SHORT}{closing}"


def _cells(row: str) -> list[str]:
    """A Markdown table row's cells: split on '|' not escaped as '\\|'."""
    return [c.strip() for c in re.split(r"(?<!\\)\|", row.strip())[1:-1]]


def _rendered_rows(columns: dict[str, list[str]]) -> dict[tuple[str, str], list[str]]:
    """render() a manifest with ``columns`` ({table: [column]}); each
    column's row as its cells, keyed (table, column)."""
    from gen_schema_md import render
    manifest = {"release": "r", "version": 1, "tables": {
        t: {"row_count": 1, "description": "", "columns": [{"name": c, "type": "int32"} for c in cols]}
        for t, cols in columns.items()}}
    rows, table = {}, None
    for line in render(manifest).splitlines():
        if line.startswith("## `"):
            table = line[4:-1]
        elif table and line.startswith("| `"):
            cells = _cells(line)
            rows[(table, cells[0].strip("`"))] = cells
    return rows


class TestColumnRows:
    """Every column row has exactly the table's five cells."""

    def test_source_with_pipe_is_escaped(self):
        """accession_map.accession reads 'primaryAccession | secondaryAccessions'."""
        cells = _rendered_rows({"accession_map": ["accession"]})[("accession_map", "accession")]
        assert len(cells) == 5, cells
        assert "primaryAccession" in cells[3] and "secondaryAccessions" in cells[3]

    @pytest.mark.parametrize("table, column",
                             [(table, index) for table, (_, index, _) in CHILD_TABLES.items()])
    def test_array_position_is_derived(self, table, column):
        """An array position has no source path in the input."""
        cells = _rendered_rows({table: [column]})[(table, column)]
        assert len(cells) == 5, cells
        assert cells[3] == "derived"
        assert "position" in cells[4].lower()


def test_column_sources_are_paths():
    """A COLUMN_SOURCES value is '<top level>' or ' | '-joined dotted input
    paths; a column with no input path belongs in DERIVED_SOURCES."""
    from parquet_transform import COLUMN_SOURCES
    for key, source in COLUMN_SOURCES.items():
        if source == "<top level>":
            continue
        for path in source.split(" | "):
            assert re.fullmatch(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*", path), (key, source)
