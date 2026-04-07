#!/usr/bin/env bash
# ─── UniProtKB → Parquet Data Lake Pipeline ─────────────────────
# Usage:
#   ./run_lake.sh                                         # small: ~4K entries, local
#   ./run_lake.sh subset                                  # subset: 100K entries, SLURM short
#   ./run_lake.sh full --expected-count 248799253          # full UniProtKB, SLURM prod
#   ./run_lake.sh full --expected-count 248799253 --input /path/to/UniProtKB.json.gz
#   ./run_lake.sh full --expected-count 248799253 --outdir /scratch/results

# Prerequisites:
#   Activate your conda/mamba environment before running this script.
#   On SLURM, the active environment carries to compute nodes automatically.
#   No container or Nextflow conda directive is used — this is intentional:
#   it avoids conda resolution overhead on every run and version-pinning
#   differences between Nextflow's conda integration and your local env.
#   Add a Dockerfile/Singularity definition when moving to scheduled production.

set -euo pipefail

MODE="${1:-small}"
shift || true

# ─── Parse optional flags ─────────────────────────────────────────
INPUTFILE_OVERRIDE=""
OUTDIR_OVERRIDE=""
DUCKDB_TEMP_OVERRIDE=""
EXPECTED_COUNT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --input)          INPUTFILE_OVERRIDE="$2";   shift 2 ;;
        --outdir)         OUTDIR_OVERRIDE="$2";      shift 2 ;;
        --duckdb-tmp)     DUCKDB_TEMP_OVERRIDE="$2"; shift 2 ;;
        --expected-count) EXPECTED_COUNT="$2";        shift 2 ;;
        *)                echo "Unknown flag: $1"; exit 1  ;;
    esac
done

# ─── Defaults ─────────────────────────────────────────────────────
case "$MODE" in
    small)
        INPUTFILE="demo/input.json.gz"
        OUTDIR="$(pwd)/datalake"
        MEMORY="8GB"
        RELEASE="small"
        PROFILE="local"
        ;;
    subset)
        INPUTFILE="${HOME}/uniprot100k.json.gz"
        OUTDIR="$(pwd)/datalake"
        MEMORY="16GB"
        RELEASE="subset"
        PROFILE="short"
        ;;
    full)
        INPUTFILE="UniProtKB.json.gz"
        OUTDIR="$(pwd)/datalake"
        MEMORY="96GB"
        RELEASE="2026_01"
        PROFILE="prod"
        ;;
    *)
        echo "Usage: $0 {small|subset|full} [--input FILE] [--outdir DIR] [--duckdb-tmp DIR] [--expected-count N]"
        exit 1
        ;;
esac

# In full mode, --expected-count is mandatory to guard against silent data loss.
# Get the expected count from UniProt release statistics before launching.
if [[ "$MODE" == "full" && -z "$EXPECTED_COUNT" ]]; then
    echo "ERROR: --expected-count is required for full mode."
    echo "  Get the entry count from the UniProt release notes, then run:"
    echo "    $0 full --expected-count <N>"
    exit 1
fi

# Apply overrides
[[ -n "$INPUTFILE_OVERRIDE" ]] && INPUTFILE="$INPUTFILE_OVERRIDE"
[[ -n "$OUTDIR_OVERRIDE" ]]    && OUTDIR="$OUTDIR_OVERRIDE"

W=55
BAR=$(printf '═%.0s' $(seq 1 $W))
printf "╔%s╗\n" "$BAR"
printf "║  %-$(( W - 2 ))s║\n" "UniProtKB → Parquet Data Lake"
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

# Add expected entry count if specified (verified during STREAM_JSONL)
if [[ -n "$EXPECTED_COUNT" ]]; then
    NF_CMD+=(--expected_count "$EXPECTED_COUNT")
fi

echo "Running: ${NF_CMD[*]}"
echo ".-- BEGUN $(date) --."
"${NF_CMD[@]}"
echo "'-- ENDED $(date) --'"
