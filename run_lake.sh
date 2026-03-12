#!/usr/bin/env bash
# ─── UniProtKB → Parquet Datalake Pipeline ────────────────────────
# Usage:
#   ./run_lake.sh                    # test mode (entries_test.json, local)
#   ./run_lake.sh prod               # production (UniProtKB.json.gz, slurm)
#   ./run_lake.sh subset 5000000     # first 5M records, slurm short queue

set -euo pipefail

MODE="${1:-test}"
SUBSET_SIZE="${2:-}"

# ─── Defaults ─────────────────────────────────────────────────────
case "$MODE" in
    test)
        INPUTFILE="entries_test.json"
        OUTDIR="$(pwd)/datalake"
        BATCHSIZE=100
        MAXFORKS=4
        MEMORY="12GB"
        PROFILE="local"
        ;;
    prod)
        INPUTFILE="UniProtKB.json.gz"
        OUTDIR="$(pwd)/datalake"
        BATCHSIZE=500000
        MAXFORKS=50
        MEMORY="8GB"
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
echo "║  UniProtKB → Parquet Datalake                        ║"
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
