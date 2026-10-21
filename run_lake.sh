#!/usr/bin/env bash
# ─── UniProtKB → Parquet Data Lake Pipeline ─────────────────────
# Usage:
#   ./run_lake.sh                                         # small: 52-entry committed fixture, local
#   ./run_lake.sh --input demo/input.json.gz              # small: ~5K demo entries (after demo/run_demo.sh)
#   ./run_lake.sh subset                                  # subset: 100K entries, SLURM short
#   ./run_lake.sh full --expected-count 248799253 --release 2026_04 --previous-manifest datalake/2026_03/lake/manifest.json
#                                                         # full UniProtKB, SLURM prod
#   ./run_lake.sh full --expected-count 248799253 --first-release   # no previous release to diff against
#   ./run_lake.sh full ... --input /path/to/UniProtKB.json.gz --outdir /scratch/results --release 2026_04

# Prerequisites:
#   Activate your conda/mamba environment before running this script.
#   On SLURM, the active environment carries to compute nodes automatically.
#   No container or Nextflow conda directive is used — this is intentional:
#   it avoids conda resolution overhead on every run and version-pinning
#   differences between Nextflow's conda integration and your local env.
#   Add a Dockerfile/Singularity definition when moving to scheduled production.

set -euo pipefail

# Mode defaults to small and may be omitted when the first argument is a flag.
if [[ "${1:-}" == --* ]]; then
    MODE="small"
else
    MODE="${1:-small}"
    shift || true
fi

# ─── Parse optional flags ─────────────────────────────────────────
INPUTFILE_OVERRIDE=""
OUTDIR_OVERRIDE=""
DUCKDB_TEMP_OVERRIDE=""
EXPECTED_COUNT=""
RELEASE_OVERRIDE=""
PREVIOUS_MANIFEST=""
FIRST_RELEASE=""
MAX_TIME=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --input)          INPUTFILE_OVERRIDE="$2";   shift 2 ;;
        --outdir)         OUTDIR_OVERRIDE="$2";      shift 2 ;;
        --duckdb-tmp)     DUCKDB_TEMP_OVERRIDE="$2"; shift 2 ;;
        --expected-count) EXPECTED_COUNT="$2";        shift 2 ;;
        --release)        RELEASE_OVERRIDE="$2";      shift 2 ;;
        --previous-manifest) PREVIOUS_MANIFEST="$2";  shift 2 ;;
        --first-release)  FIRST_RELEASE=1;            shift ;;
        --max-time)       MAX_TIME="$2";              shift 2 ;;
        *)                echo "Unknown flag: $1"; exit 1  ;;
    esac
done

# ─── Defaults ─────────────────────────────────────────────────────
case "$MODE" in
    small)
        INPUTFILE="tests/fixtures/small.json.gz"   # committed: works on a fresh clone
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
        RELEASE="2026_03"
        PROFILE="prod"
        ;;
    *)
        echo "Usage: $0 {small|subset|full} [--input FILE] [--outdir DIR] [--duckdb-tmp DIR] [--expected-count N] [--release LABEL] [--previous-manifest FILE | --first-release] [--max-time DURATION]"
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

# In full mode, every release's validation report compares its schema with
# the previous release's (README §2): say which one, or that there is none.
if [[ -n "$PREVIOUS_MANIFEST" && -n "$FIRST_RELEASE" ]]; then
    echo "ERROR: --previous-manifest and --first-release contradict each other."
    exit 1
fi
if [[ "$MODE" == "full" && -z "$PREVIOUS_MANIFEST" && -z "$FIRST_RELEASE" ]]; then
    echo "ERROR: full mode needs --previous-manifest <previous release>/lake/manifest.json"
    echo "  (the validation report's schema diff), or --first-release if there is none."
    exit 1
fi

# Apply overrides
[[ -n "$INPUTFILE_OVERRIDE" ]] && INPUTFILE="$INPUTFILE_OVERRIDE"
[[ -n "$OUTDIR_OVERRIDE" ]]    && OUTDIR="$OUTDIR_OVERRIDE"
[[ -n "$RELEASE_OVERRIDE" ]]   && RELEASE="$RELEASE_OVERRIDE"

# The box widens to its longest row.  Rows are padded by hand: ${#row}
# counts characters (the arrow is one), printf's %-Ns width counts bytes.
TITLE="UniProtKB → Parquet Data Lake"
ROWS=("Mode:      $MODE" "Release:   $RELEASE" "Input:     $INPUTFILE"
      "Output:    $OUTDIR" "Memory:    $MEMORY" "Profile:   $PROFILE")
W=55
for row in "$TITLE" "${ROWS[@]}"; do
    if (( ${#row} + 3 > W )); then W=$(( ${#row} + 3 )); fi
done
box_row() { printf "║  %s%*s║\n" "$1" $(( W - 2 - ${#1} )) ""; }
BAR=$(printf '═%.0s' $(seq 1 $W))
printf "╔%s╗\n" "$BAR"
box_row "$TITLE"
printf "╠%s╣\n" "$BAR"
for row in "${ROWS[@]}"; do box_row "$row"; done
printf "╚%s╝\n" "$BAR"
echo ""

# Pre-flight: verify input file exists before submitting to SLURM
if [[ ! -f "$INPUTFILE" ]]; then
    echo "ERROR: Input file not found: $INPUTFILE" >&2
    echo "  (resolved from working directory: $(pwd))" >&2
    if [[ "$INPUTFILE" == demo/* ]]; then
        echo "  Run demo/run_demo.sh first to download the demo input, or pass --input." >&2
    fi
    exit 1
fi
if [[ -n "$PREVIOUS_MANIFEST" && ! -f "$PREVIOUS_MANIFEST" ]]; then
    echo "ERROR: Previous manifest not found: $PREVIOUS_MANIFEST" >&2
    echo "  (resolved from working directory: $(pwd))" >&2
    exit 1
fi
# The run republishes into $OUTDIR/$RELEASE while VALIDATE reads the previous
# manifest, so one from inside it would be diffed against itself.
if [[ -n "$PREVIOUS_MANIFEST" && -d "$OUTDIR/$RELEASE" ]]; then
    MANIFEST_REAL="$(cd "$(dirname "$PREVIOUS_MANIFEST")" && pwd -P)/$(basename "$PREVIOUS_MANIFEST")"
    RELEASE_REAL="$(cd "$OUTDIR/$RELEASE" && pwd -P)"
    if [[ "$MANIFEST_REAL" == "$RELEASE_REAL"/* ]]; then
        echo "ERROR: Previous manifest is inside the release being built ($RELEASE_REAL): $PREVIOUS_MANIFEST" >&2
        echo "  Pass the previous release's lake/manifest.json." >&2
        exit 1
    fi
fi

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

# Previous release's manifest: the validation report lists schema differences.
# Passed as given: Nextflow resolves a relative path against this directory.
if [[ -n "$PREVIOUS_MANIFEST" ]]; then
    NF_CMD+=(--previous_manifest "$PREVIOUS_MANIFEST")
fi

# Wall-time cap of a retried heavy process (default 72h): at most the
# queue's limit, or SLURM rejects the retry.
if [[ -n "$MAX_TIME" ]]; then
    NF_CMD+=(--max_time "$MAX_TIME")
fi

echo "Running: ${NF_CMD[*]}"
echo ".-- BEGUN $(date) --."
"${NF_CMD[@]}"
echo "'-- ENDED $(date) --'"

# Nextflow exits 0 even when the published copy fails verification (the check
# runs in onComplete, after the workflow succeeded): the marker is the signal.
if [[ ! -e "$OUTDIR/$RELEASE/RELEASE_COMPLETE" ]]; then
    echo "ERROR: $OUTDIR/$RELEASE/RELEASE_COMPLETE was not written: the release is not complete." >&2
    echo "  The log says why (RELEASE_COMPLETE not written: ...). Re-run the same command (-resume)." >&2
    exit 1
fi
