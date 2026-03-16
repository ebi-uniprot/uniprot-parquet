#!/usr/bin/env bash
# ─── UniProtKB → Parquet Datalake Pipeline ────────────────────────
# Usage:
#   ./run_lake.sh                                         # test mode
#   ./run_lake.sh prod                                    # production
#   ./run_lake.sh prod --input /path/to/UniProtKB.json.gz
#   ./run_lake.sh prod --outdir /scratch/results
#   ./run_lake.sh prod --input /path/to/file.json.gz --outdir /scratch/results

set -euo pipefail

#MODE="${1:-test}"
#SUBSET_SIZE="${2:-}"

MODE="${1:-test}"
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
        BATCHSIZE=100
        MAXFORKS=4
        MEMORY="12GB"
        PROFILE="local"
        ;;
    test_med)
        INPUTFILE="test_json/med.json.gz"
        OUTDIR="$(pwd)/results/uniprot_lake_test_med"
        BATCHSIZE=500
        MAXFORKS=8
        MEMORY="16GB"
        PROFILE="local"
        ;;
    prod)
        INPUTFILE="~/uniprot100k.json.gz"
        OUTDIR="$(pwd)/datalake"
        BATCHSIZE=500
        MAXFORKS=50
        MEMORY="16GB"
        PROFILE="prod"
        ;;
    subset)
        INPUTFILE="UniProtKB.json.gz"
        OUTDIR="$(pwd)/datalake"
        BATCHSIZE=500000
        MAXFORKS=30
        MEMORY="8GB"
        PROFILE="short"
        ;;
    *)
        echo "Usage: $0 {test|prod|subset} [subset_size]"
        exit 1
        ;;
esac

echo "╔═══════════════════════════════════════════════════════╗"
echo "║  UniProtKB → Parquet Datalake                         ║"
echo "╠═══════════════════════════════════════════════════════╣"
echo "║  Mode:      $MODE"
echo "║  Input:     $INPUTFILE"
echo "║  Output:    $OUTDIR"
echo "║  Batch:     $BATCHSIZE"
echo "║  Max forks: $MAXFORKS"
echo "║  Memory:    $MEMORY"
echo "║  Profile:   $PROFILE"
echo "╚═══════════════════════════════════════════════════════╝"
echo ""

COMMAND="nextflow run upjson2lake.nf \
    --inputfile ${INPUTFILE} \
    --outdir ${OUTDIR} \
    --batchsize ${BATCHSIZE} \
    --maxforks ${MAXFORKS} \
    --memory_limit ${MEMORY} \
    -profile ${PROFILE} \
    -ansi-log true \
    -resume"

echo "Running: $COMMAND"
echo ".-- BEGUN $(date) --."
eval $COMMAND
echo "'-- ENDED $(date) --'"