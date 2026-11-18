#!/usr/bin/env python3
"""
The dataset card for a release, filled in from its lake/manifest.json, with
the checks an upload needs first.

    python huggingface/prepare_card.py huggingface/dataset_card.md $OUT/lake -o $SCRATCH/card_2026_04.md
    cd huggingface && CARD=$SCRATCH/card_2026_04.md RELEASE=2026_04 OUT=$OUT sbatch hf_upload.sh

(Outside the checkout: an untracked file there marks a build's provenance dirty.)

Fills each table's Rows and Size (the TODOs) and the front matter's
size_categories from the manifest, so no number is typed by hand.  Refuses
(exit 1, nothing written) when:
  - the card is for another release than the lake (its viewer paths, its text);
  - the card's tables and the lake's differ;
  - schema_version is not 0.x (the card calls the lake a beta);
  - a file is over the Hub's 50 GB limit, a folder holds over 10,000 files, or
    the release over 100,000 (https://huggingface.co/docs/hub/storage-limits);
  - a TODO is left.

Standard library only.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

MAX_FILE_BYTES = 50 * 10**9
MAX_FILES_PER_FOLDER = 10_000
MAX_FILES = 100_000
# The Hub's size_categories, by total rows
SIZE_CATEGORIES = [(10**3, "n<1K"), (10**4, "1K<n<10K"), (10**5, "10K<n<100K"), (10**6, "100K<n<1M"),
                   (10**7, "1M<n<10M"), (10**8, "10M<n<100M"), (10**9, "100M<n<1B"), (10**10, "1B<n<10B"),
                   (10**11, "10B<n<100B"), (10**12, "100B<n<1T")]


class CardError(Exception):
    pass


def size_category(rows: int) -> str:
    return next((name for limit, name in SIZE_CATEGORIES if rows < limit), "n>1T")


def human_bytes(n: int) -> str:
    """Decimal units, as the Hub shows sizes."""
    for unit, scale in (("TB", 10**12), ("GB", 10**9), ("MB", 10**6), ("kB", 10**3)):
        if n >= scale:
            return f"{n / scale:.1f} {unit}"
    return f"{n} B"


def hub_limits(manifest: dict[str, Any], release: str) -> list[str]:
    """Problems with the Hub's file and folder limits, from the manifest's file list."""
    problems = []
    folders: Counter[str] = Counter()
    n_files = 0
    for table, info in manifest["tables"].items():
        for rel, details in info.get("file_details", {}).items():
            n_files += 1
            folders[f"{release}/lake/{table}/{rel.rsplit('/', 1)[0]}"] += 1
            if details["size_bytes"] > MAX_FILE_BYTES:
                problems.append(f"{table}/{rel}: {human_bytes(details['size_bytes'])}, over the Hub's 50 GB per file")
    for folder, n in folders.items():
        if n > MAX_FILES_PER_FOLDER:
            problems.append(f"{folder}: {n:,} files, over the Hub's 10,000 per folder")
    if n_files > MAX_FILES:
        problems.append(f"{n_files:,} files, over the Hub's 100,000 per repo")
    return problems


def prepare(card: str, manifest: dict[str, Any]) -> tuple[str, list[str]]:
    """The filled card and a summary, or CardError."""
    release = manifest["release"]
    tables = manifest["tables"]
    viewer = set(re.findall(r"path: (\S+)/lake/", card))
    if viewer != {release}:
        raise CardError(f"the card's viewer paths are for {sorted(viewer)}, the lake is {release}")
    if f"release **{release}**" not in card:
        raise CardError(f"the card's text does not name release **{release}**")
    if not str(manifest.get("schema_version", "")).startswith("0."):
        raise CardError(f"schema_version {manifest.get('schema_version')}: the card calls the lake a beta (0.x)")

    rows_in_card = re.findall(r"^\| `(\w+)` \|", card, flags=re.M)
    if set(rows_in_card) != set(tables):
        raise CardError(f"the card's tables {sorted(rows_in_card)} are not the lake's {sorted(tables)}")

    def fill(m: re.Match[str]) -> str:
        info = tables[m.group(1)]
        return f"| `{m.group(1)}` | {m.group(2)} | {info['row_count']:,} | {human_bytes(info['size_bytes'])} |"
    filled = re.sub(r"^\| `(\w+)` \| ([^|]+?) \| [^|]*? \| [^|]*? \|$", fill, card, flags=re.M)

    total_rows = sum(int(t["row_count"]) for t in tables.values())
    category = size_category(total_rows)
    filled, n = re.subn(r"(size_categories:\n- )\S+", rf"\g<1>{category}", filled, count=1)
    if n != 1:
        raise CardError("the card has no size_categories")

    problems = hub_limits(manifest, release)
    if problems:
        raise CardError("; ".join(problems))
    if "TODO" in filled:
        raise CardError("a TODO is left: " + next(line for line in filled.splitlines() if "TODO" in line))

    files = sum(len(t.get("file_details", {})) for t in tables.values())
    biggest = max((d["size_bytes"] for t in tables.values() for d in t.get("file_details", {}).values()), default=0)
    total_bytes = sum(int(t["size_bytes"]) for t in tables.values())
    summary = [f"release {release}, schema_version {manifest['schema_version']}",
               f"{len(tables)} tables, {total_rows:,} rows ({category}), {files} Parquet files, "
               f"{human_bytes(total_bytes)}; largest file {human_bytes(biggest)}"]
    summary += [f"  {name}: {info['row_count']:,} rows, {human_bytes(info['size_bytes'])}"
                for name, info in tables.items()]
    return filled, summary


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("card", type=Path, help="huggingface/dataset_card.md")
    ap.add_argument("lake", type=Path, help="the release's lake/ (holds manifest.json)")
    ap.add_argument("-o", "--output", type=Path, required=True)
    a = ap.parse_args()
    try:
        filled, summary = prepare(a.card.read_text(), json.loads((a.lake / "manifest.json").read_text()))
    except CardError as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 1
    a.output.write_text(filled)
    print("\n".join(summary), file=sys.stderr)
    print(f"→ {a.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
