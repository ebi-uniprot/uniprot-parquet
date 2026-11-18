#!/bin/bash
# Upload a finished release to a Hugging Face dataset repo, as <release>/ in
# the repo, then its dataset card as README.md.
#
#   python huggingface/prepare_card.py huggingface/dataset_card.md $OUT/lake -o $SCRATCH/card_$RELEASE.md
#   cd huggingface && RELEASE=2026_04 OUT=$OUT CARD=$SCRATCH/card_$RELEASE.md sbatch hf_upload.sh
#
# Then check the upload and tag it:
#   python huggingface/hf_verify.py $HF_REPO $RELEASE $OUT --card $SCRATCH/card_$RELEASE.md --tag
#
# Environment:
#   RELEASE     e.g. 2026_04                                                   (required)
#   CARD        the card prepare_card.py filled in for RELEASE                 (required)
#   OUT         the release directory [$SCRATCH/uniprot_parquet/$RELEASE]
#   HF_REPO     [uniprot/uniprotkb-parquet]
#   VISIBILITY  private or public: the repo's, if this creates it [private]
#
# Needs the hf CLI (huggingface_hub >= 2.4: a large folder goes up in several
# commits, and a rerun resumes) and `hf auth login` with a token that can
# write to HF_REPO.  Resumable: if the job dies or times out, submit it again.
#SBATCH -p datamover
#SBATCH -c 4
#SBATCH --mem 16G
#SBATCH -t 2-00:00:00
#SBATCH -J hf-upload
#SBATCH -o hf-upload-%j.out
set -euo pipefail

: "${RELEASE:?set RELEASE, e.g. RELEASE=2026_04}"
: "${CARD:?set CARD to the card prepare_card.py filled in (python huggingface/prepare_card.py ...)}"
HF_REPO=${HF_REPO:-uniprot/uniprotkb-parquet}
OUT=${OUT:-$SCRATCH/uniprot_parquet/$RELEASE}
VISIBILITY=${VISIBILITY:-private}
# bin/ (parts_input.py), from the submit directory (huggingface/ or the checkout) or this script's
BIN=""
for d in "${SLURM_SUBMIT_DIR:-.}/../bin" "${SLURM_SUBMIT_DIR:-.}/bin" "$(dirname "$0")/../bin"; do
    if [[ -f "$d/parts_input.py" ]]; then BIN=$d; break; fi
done

die() { echo "hf_upload.sh: $*" >&2; exit 1; }
command -v hf >/dev/null || die "no hf CLI (pip install 'huggingface_hub>=2.4')"
[[ -n "$BIN" ]] || die "cannot find bin/parts_input.py: submit from the checkout or its huggingface/"
hf auth whoami >/dev/null 2>&1 || die "not logged in (hf auth login)"
case $VISIBILITY in private) VIS=--private ;; public) VIS=--no-private ;; *) die "VISIBILITY is private or public" ;; esac

test -f "$OUT/RELEASE_COMPLETE" || die "no RELEASE_COMPLETE in $OUT"
test -f "$CARD" || die "no dataset card at $CARD"
# The card names the release in its viewer paths and examples; prepare_card.py filled in its numbers
grep -q "path: $RELEASE/lake/" "$CARD" || die "$CARD is not for $RELEASE"
! grep -n TODO "$CARD" || die "$CARD still has TODOs: python huggingface/prepare_card.py ..."
# provenance.json is published: a dat2jsonl record must name files, not the build machine's paths
python3 - "$OUT/provenance.json" "$BIN" <<'EOF' || die "$OUT/provenance.json records internal paths: bin/parts_input.py public"
import json, sys
sys.path.insert(0, sys.argv[2])
from parts_input import public_source
source = json.load(open(sys.argv[1])).get("inputs", {}).get("source", {})
sys.exit(0 if not str(source.get("kind", "")).startswith("dat2jsonl") or public_source(source) == source else 1)
EOF

echo "uploading $OUT to $HF_REPO ($VISIBILITY if new) as $RELEASE/"
# sorted.jsonl.zst (~100 GB) is over the Hub's per-file limit
hf upload "$HF_REPO" "$OUT" "$RELEASE" --repo-type dataset $VIS \
    --exclude sorted.jsonl.zst --exclude ".*" --commit-message "UniProtKB $RELEASE"
hf upload "$HF_REPO" "$CARD" README.md --repo-type dataset \
    --commit-message "Dataset card for $RELEASE"
echo "done: now python huggingface/hf_verify.py $HF_REPO $RELEASE $OUT --card $CARD --tag"
