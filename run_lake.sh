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
DUCKDB_TEMP_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --input)      INPUTFILE_OVERRIDE="$2";   shift 2 ;;
        --outdir)     OUTDIR_OVERRIDE="$2";      shift 2 ;;
        --duckdb-tmp) DUCKDB_TEMP_OVERRIDE="$2"; shift 2 ;;
        *)            echo "Unknown flag: $1"; exit 1  ;;
    esac
done

# ─── Defaults ─────────────────────────────────────────────────────
case "$MODE" in
    test_small)
        INPUTFILE="tests/fixtures/small.json.gz"
        OUTDIR="$(pwd)/datalake/uniprot_lake_test_small"
        MEMORY="8GB"
        RELEASE="test_small"
        PROFILE="local"
        ;;
    test_med)
        INPUTFILE="demo/input.json.gz"
        OUTDIR="$(pwd)/datalake/uniprot_lake_test_med"
        MEMORY="8GB"
        RELEASE="test_med"
        PROFILE="local"
        ;;
    subset)
        INPUTFILE="~/uniprot100k.json.gz"
        OUTDIR="$(pwd)/datalake"
        MEMORY="16GB"
        RELEASE="subset"
        PROFILE="short"
        ;;
    prod)
        INPUTFILE="UniProtKB.json.gz"
        OUTDIR="$(pwd)/datalake"
        MEMORY="96GB"
        RELEASE="2026_01"
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

W=55
BAR=$(printf '═%.0s' $(seq 1 $W))
printf "╔%s╗\n" "$BAR"
printf "║  %-$(( W - 2 ))s║\n" "UniProtKB → Iceberg Data Lake"
printf "╠%s╣\n" "$BAR"
printf "║  %-$(( W - 2 ))s║\n" "Mode:      $MODE"
printf "║  %-$(( W - 2 ))s║\n" "Release:   $RELEASE"
printf "║  %-$(( W - 2 ))s║\n" "Input:     $INPUTFILE"
printf "║  %-$(( W - 2 ))s║\n" "Output:    $OUTDIR"
printf "║  %-$(( W - 2 ))s║\n" "Memory:    $MEMORY"
printf "║  %-$(( W - 2 ))s║\n" "Profile:   $PROFILE"
printf "╚%s╝\n" "$BAR"
echo ""

# Ensure report directory exists (Nextflow won't create it)
mkdir -p "$OUTDIR/reports"

# Build command as an array to handle paths with spaces safely
NF_CMD=(
    nextflow run upjson2lake.nf
    --inputfile "$INPUTFILE"
    --outdir "$OUTDIR"
    --process_memory "$MEMORY"
    --release "$RELEASE"
    -profile "$PROFILE"
    -ansi-log true
    -resume
)

# Add DuckDB temp directory if specified
if [[ -n "$DUCKDB_TEMP_OVERRIDE" ]]; then
    NF_CMD+=(--duckdb_temp "$DUCKDB_TEMP_OVERRIDE")
fi

echo "Running: ${NF_CMD[*]}"
echo ".-- BEGUN $(date) --."
"${NF_CMD[@]}"
echo "'-- ENDED $(date) --'"
