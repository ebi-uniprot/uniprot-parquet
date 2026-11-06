"""Check an uploaded release against its SHA256SUMS.txt, then tag it.

    OUT=$SCRATCH/uniprot_parquet/2026_03
    python huggingface/hf_verify.py uniprot/uniprotkb-parquet 2026_03 $OUT/lake/SHA256SUMS.txt [--tag]

Compares the SHA-256 the Hub stores for each LFS file (every Parquet file)
with the lake's checksums; nothing is downloaded.
"""
import sys
from pathlib import Path

from huggingface_hub import HfApi

repo, release, sums = sys.argv[1:4]
api = HfApi()

want = {}
for line in Path(sums).read_text().splitlines():
    sha, name = line.split(maxsplit=1)
    want[f"{release}/lake/{name.lstrip('*')}"] = sha

have = {}
for f in api.list_repo_tree(repo, repo_type="dataset", path_in_repo=release, recursive=True):
    if getattr(f, "lfs", None):
        have[f.path] = f.lfs.sha256

parquet = {p: s for p, s in want.items() if p.endswith(".parquet")}
missing = sorted(p for p in parquet if p not in have)
wrong = sorted(p for p in parquet if p in have and have[p] != parquet[p])
for p in missing:
    print("MISSING", p)
for p in wrong:
    print("MISMATCH", p)
if missing or wrong:
    sys.exit(1)
print(f"OK: {len(parquet)} Parquet files match SHA256SUMS.txt")

if "--tag" in sys.argv:
    api.create_tag(repo, tag=release, repo_type="dataset")
    print(f"tagged {release}")
