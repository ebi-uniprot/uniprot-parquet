"""huggingface/hf_verify.py's comparison of a release on the Hub with its
local copy (the Hub's listing stood in for: no network)."""

import os
import pathlib
import subprocess
import sys

from conftest import PROJECT_ROOT

sys.path.insert(0, os.path.join(PROJECT_ROOT, "huggingface"))
from hf_verify import HubFile, compare, git_blob_id, known_sha256, local_files, sha256_of


def _release(tmp_path: pathlib.Path) -> pathlib.Path:
    out = tmp_path / "2026_04"
    (out / "lake" / "entries").mkdir(parents=True)
    (out / "lake" / "entries" / "e_00001.parquet").write_bytes(b"PAR1" + bytes(range(256)) * 10)
    (out / "lake" / "manifest.json").write_text('{"release": "2026_04"}\n')
    (out / "provenance.json").write_text("{}\n")
    (out / "sorted.jsonl.zst").write_bytes(b"not uploaded")
    (out / ".RELEASE_COMPLETE.pending").write_text("hidden")
    sha = sha256_of(out / "lake" / "entries" / "e_00001.parquet")
    (out / "lake" / "SHA256SUMS.txt").write_text(f"{sha}  entries/e_00001.parquet\n")
    return out


def _as_on_hub(local: dict[str, pathlib.Path]) -> dict[str, HubFile]:
    """The listing the Hub would give: Parquet in LFS, the rest as git blobs."""
    hub = {}
    for p, path in local.items():
        data = path.read_bytes()
        lfs = sha256_of(path) if p.endswith(".parquet") else None
        hub[p] = HubFile(p, len(data), "pointer" if lfs else git_blob_id(data), lfs)
    return hub


def test_git_blob_id_is_gits() -> None:
    data = b"hello\n"
    git = subprocess.run(["git", "hash-object", "--stdin"], input=data, capture_output=True, check=True)
    assert git_blob_id(data) == git.stdout.decode().strip()


def test_uploaded_files_are_all_but_the_jsonl_and_hidden_ones(tmp_path: pathlib.Path) -> None:
    local = local_files(_release(tmp_path), "2026_04")
    assert sorted(local) == ["2026_04/lake/SHA256SUMS.txt", "2026_04/lake/entries/e_00001.parquet",
                             "2026_04/lake/manifest.json", "2026_04/provenance.json"]


def test_an_exact_copy_passes(tmp_path: pathlib.Path) -> None:
    out = _release(tmp_path)
    local = local_files(out, "2026_04")
    assert compare(local, _as_on_hub(local), known_sha256(out, "2026_04")) == []


def test_missing_extra_and_changed_files_are_found(tmp_path: pathlib.Path) -> None:
    out = _release(tmp_path)
    local = local_files(out, "2026_04")
    hub = _as_on_hub(local)
    del hub["2026_04/provenance.json"]
    hub["2026_04/stray.txt"] = HubFile("2026_04/stray.txt", 1, "x", None)
    m = hub["2026_04/lake/manifest.json"]
    hub[m.path] = HubFile(m.path, m.size, git_blob_id(b'{"release": "2026_03"}\n'), None)
    p = hub["2026_04/lake/entries/e_00001.parquet"]
    hub[p.path] = HubFile(p.path, p.size, p.blob_id, "0" * 64)
    problems = compare(local, hub, known_sha256(out, "2026_04"))
    assert problems == ["MISSING on the Hub: 2026_04/provenance.json",
                        "EXTRA on the Hub: 2026_04/stray.txt",
                        "SHA-256 2026_04/lake/entries/e_00001.parquet: the Hub's differs",
                        "CONTENT 2026_04/lake/manifest.json: the Hub's differs"]


def test_a_truncated_upload_is_found_by_size(tmp_path: pathlib.Path) -> None:
    out = _release(tmp_path)
    local = local_files(out, "2026_04")
    hub = _as_on_hub(local)
    p = hub["2026_04/lake/entries/e_00001.parquet"]
    hub[p.path] = HubFile(p.path, p.size - 1, p.blob_id, p.lfs_sha256)
    assert compare(local, hub, {}) == [f"SIZE {p.path}: Hub {p.size - 1:,} bytes, local {p.size:,}"]
