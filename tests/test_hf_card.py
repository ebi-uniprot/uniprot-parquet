"""huggingface/prepare_card.py: the dataset card filled in from a release's
manifest, and refused when it does not fit the release or the Hub."""

import json
import pathlib
import subprocess
import sys

import pytest
from conftest import PROJECT_ROOT

SCRIPT = pathlib.Path(PROJECT_ROOT) / "huggingface" / "prepare_card.py"
CARD = pathlib.Path(PROJECT_ROOT) / "huggingface" / "dataset_card.md"
TABLES = ("entries", "features", "xrefs", "comments", "publications", "accession_map")


def _manifest(release: str = "2026_04", schema: str = "0.1.0", file_bytes: int = 300_000_000,
              files_per_table: int = 2) -> dict[str, object]:
    tables = {}
    for i, name in enumerate(TABLES):
        details = {f"review_status=trembl/{name}_{n:05d}.parquet": {"size_bytes": file_bytes, "sha256": "x"}
                   for n in range(1, files_per_table + 1)}
        tables[name] = {"row_count": 154_222_205 * (i + 1), "size_bytes": file_bytes * files_per_table,
                        "file_details": details}
    return {"release": release, "schema_version": schema, "tables": tables}


def _prepare(tmp_path: pathlib.Path, manifest: dict[str, object],
             card: str | None = None) -> tuple[subprocess.CompletedProcess[str], pathlib.Path]:
    lake = tmp_path / "lake"
    lake.mkdir(exist_ok=True)
    (lake / "manifest.json").write_text(json.dumps(manifest))
    card_path = tmp_path / "card_in.md"
    card_path.write_text(CARD.read_text() if card is None else card)
    out = tmp_path / "card.md"
    result = subprocess.run([sys.executable, str(SCRIPT), str(card_path), str(lake), "-o", str(out)],
                            capture_output=True, text=True)
    return result, out


def test_card_is_filled_from_the_manifest(tmp_path: pathlib.Path) -> None:
    result, out = _prepare(tmp_path, _manifest())
    assert result.returncode == 0, result.stderr
    card = out.read_text()
    assert "TODO" not in card
    assert "| `entries` | One row per protein | 154,222,205 | 600.0 MB |" in card
    assert "| `accession_map` | One row per primary or secondary accession | 925,333,230 | 600.0 MB |" in card
    assert "size_categories:\n- 1B<n<10B\n" in card          # 3.2 billion rows in all
    # nothing else changes
    assert card.splitlines()[:12] == CARD.read_text().splitlines()[:12]
    assert "12 Parquet files" in result.stderr


def test_card_for_another_release_is_refused(tmp_path: pathlib.Path) -> None:
    result, out = _prepare(tmp_path, _manifest(release="2027_01"))
    assert result.returncode == 1 and "the lake is 2027_01" in result.stderr
    assert not out.exists()


def test_schema_out_of_beta_is_refused(tmp_path: pathlib.Path) -> None:
    result, _ = _prepare(tmp_path, _manifest(schema="1.0.0"))
    assert result.returncode == 1 and "calls the lake a beta" in result.stderr


def test_tables_unlike_the_lakes_are_refused(tmp_path: pathlib.Path) -> None:
    manifest = _manifest()
    del manifest["tables"]["xrefs"]  # type: ignore[attr-defined]
    result, _ = _prepare(tmp_path, manifest)
    assert result.returncode == 1 and "are not the lake's" in result.stderr


@pytest.mark.parametrize(("file_bytes", "files_per_table", "why"), [
    (51 * 10**9, 2, "over the Hub's 50 GB per file"),
    (1, 10_001, "over the Hub's 10,000 per folder"),
])
def test_hub_limits_are_checked(tmp_path: pathlib.Path, file_bytes: int, files_per_table: int, why: str) -> None:
    result, _ = _prepare(tmp_path, _manifest(file_bytes=file_bytes, files_per_table=files_per_table))
    assert result.returncode == 1 and why in result.stderr


def test_a_todo_left_is_refused(tmp_path: pathlib.Path) -> None:
    card = CARD.read_text().replace("Built and validated by", "TODO: Built and validated by")
    result, _ = _prepare(tmp_path, _manifest(), card=card)
    assert result.returncode == 1 and "a TODO is left" in result.stderr
