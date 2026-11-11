#!/bin/bash
# Upload a finished release to a Hugging Face dataset repo (created private),
# as <release>/ in the repo, then the dataset card as README.md.
#   cd huggingface && RELEASE=2026_04 sbatch hf_upload.sh
#   (OUT=<release dir> if the release is not under $SCRATCH/uniprot_parquet)
# Needs `hf auth login` (a token with write access to the repo) beforehand.
# Resumable: if the job dies or times out, submit it again; files already
# committed are skipped.
#SBATCH -p datamover
#SBATCH -c 4
#SBATCH --mem 16G
#SBATCH -t 2-00:00:00
#SBATCH -J hf-upload
#SBATCH -o hf-upload-%j.out
set -euo pipefail

: "${RELEASE:?set RELEASE, e.g. RELEASE=2026_04}"
HF_REPO=${HF_REPO:-uniprot/uniprotkb-parquet}
OUT=${OUT:-$SCRATCH/uniprot_parquet/$RELEASE}
CARD=${CARD:-$SLURM_SUBMIT_DIR/dataset_card.md}

test -f "$OUT/RELEASE_COMPLETE" || { echo "no RELEASE_COMPLETE in $OUT" >&2; exit 1; }
test -f "$CARD" || { echo "no dataset card at $CARD" >&2; exit 1; }
# The card names the release in its viewer paths and examples, and its numbers are filled in per release
grep -q "path: $RELEASE/lake/" "$CARD" || { echo "$CARD is not for $RELEASE" >&2; exit 1; }
! grep -n TODO "$CARD" || { echo "fill in the TODOs in $CARD (from $OUT/lake/manifest.json)" >&2; exit 1; }

# sorted.jsonl.zst (~100 GB) is over the Hub's per-file limit
hf upload "$HF_REPO" "$OUT" "$RELEASE" --repo-type dataset --private \
    --exclude sorted.jsonl.zst --commit-message "UniProtKB $RELEASE"
hf upload "$HF_REPO" "$CARD" README.md --repo-type dataset \
    --commit-message "Dataset card for $RELEASE"
