#!/usr/bin/env python3
"""
Check an uploaded release against the local copy it was uploaded from, file by
file, then tag it.

    python huggingface/hf_verify.py uniprot/uniprotkb-parquet 2026_04 $OUT [--card $SCRATCH/card_2026_04.md] [--tag]

Every file under OUT that hf_upload.sh uploads (all but sorted.jsonl.zst) must
be at <release>/ in the repo with the same content, and the repo's <release>/
must hold nothing else.  Nothing is downloaded: the Hub's file listing gives
each large (LFS) file's SHA-256, compared with lake/SHA256SUMS.txt (Parquet,
which verify_release.py --full re-hashed) or with the local file's; and each
small file's git blob id, compared with the local file's.  --card checks the
repo's README.md is that card.  --tag then tags the repo's current revision
<release> (the card's examples pin it), refusing to move an existing tag.

Exit 0 if everything matches (and the tag was made), 1 otherwise.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

EXCLUDED = {"sorted.jsonl.zst"}   # over the Hub's per-file limit (hf_upload.sh --exclude)


@dataclass(frozen=True)
class HubFile:
    path: str
    size: int
    blob_id: str
    lfs_sha256: str | None   # set for a file the Hub stores in LFS


def git_blob_id(data: bytes) -> str:
    """The id git (and the Hub) gives a small file's content."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def local_files(out: Path, release: str) -> dict[str, Path]:
    """The files hf_upload.sh uploads, by their path in the repo."""
    files = {}
    for path in sorted(out.rglob("*")):
        rel = path.relative_to(out)
        if path.is_file() and rel.as_posix() not in EXCLUDED and not any(p.startswith(".") for p in rel.parts):
            files[f"{release}/{rel.as_posix()}"] = path
    return files


def known_sha256(out: Path, release: str) -> dict[str, str]:
    """SHA-256 from lake/SHA256SUMS.txt, by path in the repo (no re-hashing of 85 GB)."""
    sums = out / "lake" / "SHA256SUMS.txt"
    known = {}
    for line in sums.read_text().splitlines() if sums.is_file() else []:
        sha, name = line.split(maxsplit=1)
        known[f"{release}/lake/{name.lstrip('*')}"] = sha
    return known


def compare(local: dict[str, Path], hub: dict[str, HubFile], known: dict[str, str]) -> list[str]:
    """Problems: files missing from the Hub, extra on it, or with other content."""
    problems = [f"MISSING on the Hub: {p}" for p in sorted(local.keys() - hub.keys())]
    problems += [f"EXTRA on the Hub: {p}" for p in sorted(hub.keys() - local.keys())]
    for p in sorted(local.keys() & hub.keys()):
        f, path = hub[p], local[p]
        if f.size != path.stat().st_size:
            problems.append(f"SIZE {p}: Hub {f.size:,} bytes, local {path.stat().st_size:,}")
        elif f.lfs_sha256 is not None:
            if f.lfs_sha256 != (known.get(p) or sha256_of(path)):
                problems.append(f"SHA-256 {p}: the Hub's differs")
        elif f.blob_id != git_blob_id(path.read_bytes()):
            problems.append(f"CONTENT {p}: the Hub's differs")
    return problems


def hub_files(api: Any, repo: str, path: str, revision: str) -> dict[str, HubFile]:
    files = {}
    for f in api.list_repo_tree(repo, repo_type="dataset", path_in_repo=path,
                                recursive=True, revision=revision):
        if hasattr(f, "blob_id"):   # a file, not a folder
            lfs = getattr(f, "lfs", None)
            files[f.path] = HubFile(f.path, f.size, f.blob_id, lfs.sha256 if lfs else None)
    return files


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("repo", help="e.g. uniprot/uniprotkb-parquet")
    ap.add_argument("release", help="e.g. 2026_04: the repo's folder, and the tag")
    ap.add_argument("out", type=Path, help="the local release directory that was uploaded")
    ap.add_argument("--card", type=Path, help="the card uploaded as README.md")
    ap.add_argument("--revision", default="main")
    ap.add_argument("--tag", action="store_true", help="tag the revision <release> if everything matches")
    a = ap.parse_args()

    from huggingface_hub import HfApi
    api = HfApi()
    info = api.repo_info(a.repo, repo_type="dataset", revision=a.revision)
    print(f"{a.repo}@{a.revision}: commit {info.sha}, {'private' if info.private else 'public'}")

    local = local_files(a.out, a.release)
    hub = hub_files(api, a.repo, a.release, a.revision)
    problems = compare(local, hub, known_sha256(a.out, a.release))
    if a.card:
        readme = hub_files(api, a.repo, "", a.revision).get("README.md")
        if readme is None:
            problems.append("MISSING on the Hub: README.md")
        elif readme.blob_id != git_blob_id(a.card.read_bytes()):
            problems.append(f"CONTENT README.md: not {a.card}")
    for p in problems:
        print(p)
    if problems:
        print(f"FAILED: {len(problems)} problems")
        return 1
    n_lfs = sum(1 for f in hub.values() if f.lfs_sha256)
    print(f"OK: {len(local)} files match ({n_lfs} by SHA-256, {len(local) - n_lfs} by git blob id)"
          + (", and README.md is the card" if a.card else ""))

    if a.tag:
        tags = {t.name for t in api.list_repo_refs(a.repo, repo_type="dataset").tags}
        if a.release in tags:
            print(f"tag {a.release} exists already: not moved")
            return 1
        api.create_tag(a.repo, tag=a.release, revision=info.sha, repo_type="dataset",
                       tag_message=f"UniProtKB {a.release}")
        print(f"tagged {info.sha} {a.release}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
