#!/usr/bin/env bash
# ─── UniProtKB → Iceberg Data Lake Pipeline ─────────────────────
# Usage:
#   ./run_lake.sh                                         # test mode
#   ./run_lake.sh prod                                    # production
#   ./run_lake.sh prod --input /path/to/UniProtKB.json.gz
#   ./run_lake.sh prod --outdir /scratch/results

set -euo pipefail

MODE="${1:-test_small}"
shift || true

# ─── Parse optional flags ─────────────────────────────────────────
INPUTFILE_OVERRIDE=""
OUTDIR_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --input)  INPUTFILE_OVERRIDE="$2"; shift 2 ;;
        --outdir) OUTDIR_OVERRIDE="$2";    shift 2 ;;
        *)        echo "Unknown flag: $1"; exit 1  ;;
    esac
done

# ─── Defaults ─────────────────────────────────────────────────────
case "$MODE" in
    test_small)
        INPUTFILE="test_json/small.json.gz"
        OUTDIR="$(pwd)/datalake/uniprot_lake_test_small"
        MEMORY="8GB"
        PROFILE="local"
        ;;
    test_med)
        INPUTFILE="test_json/med.json.gz"
        OUTDIR="$(pwd)/datalake/uniprot_lake_test_med"
        MEMORY="8GB"
        PROFILE="local"
        ;;
    subset)
        INPUTFILE="~/uniprot100k.json.gz"
        OUTDIR="$(pwd)/datalake"
        MEMORY="16GB"
        PROFILE="short"
        ;;
    prod)
        INPUTFILE="UniProtKB.json.gz"
        OUTDIR="$(pwd)/datalake"
        MEMORY="16GB"
        PROFILE="prod"
        ;;
    *)
        echo "Usage: $0 {test_small|test_med|prod|subset} [--input FILE] [--outdir DIR]"
        exit 1
        ;;
esac

# Apply overrides
[[ -n "$INPUTFILE_OVERRIDE" ]] && INPUTFILE="$INPUTFILE_OVERRIDE"
[[ -n "$OUTDIR_OVERRIDE" ]]    && OUTDIR="$OUTDIR_OVERRIDE"

echo "╔═══════════════════════════════════════════════════════╗"
echo "║  UniProtKB → Iceberg Data Lake  v4.0                  ║"
echo "╠═══════════════════════════════════════════════════════╣"
echo "║  Mode:      $MODE"
echo "║  Input:     $INPUTFILE"
echo "║  Output:    $OUTDIR"
echo "║  Memory:    $MEMORY"
echo "║  Profile:   $PROFILE"
echo "╚═══════════════════════════════════════════════════════╝"
echo ""

COMMAND="nextflow run upjson2lake.nf \
    --inputfile ${INPUTFILE} \
    --outdir ${OUTDIR} \
    --memory_limit ${MEMORY} \
    -profile ${PROFILE} \
    -ansi-log true \
    -resume"

echo "Running: $COMMAND"
echo ".-- BEGUN $(date) --."
eval $COMMAND
echo "'-- ENDED $(date) --'"
