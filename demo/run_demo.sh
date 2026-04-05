#!/usr/bin/env bash
# ─── UniProtKB Parquet Data Lake Demo ───────────────────────────
#
# Downloads reviewed Drosophila melanogaster entries (~3,800 proteins)
# from UniProtKB and builds a complete Parquet data lake from scratch.
#
# Output (all inside demo/):
#   input.json.gz           Downloaded UniProtKB JSON (kept for re-runs)
#   lake/2026_01/           Parquet data lake + sorted JSONL + provenance
#
# Usage:
#   cd demo && ./run_demo.sh            # default: Drosophila reviewed
#   cd demo && ./run_demo.sh --clean    # wipe lake/ and rebuild
#
# The download is skipped if input.json.gz already exists.
# The pipeline is idempotent — re-running overwrites the lake.

set -euo pipefail
cd "$(dirname "$0")"

# ─── Configuration ────────────────────────────────────────────────
QUERY="(organism_id:7227)+AND+(reviewed:true)"   # D. melanogaster Swiss-Prot
RELEASE="2026_01"
INPUT="input.json.gz"
LAKE_DIR="lake"
MEMORY="8GB"

# ─── Parse flags ──────────────────────────────────────────────────
CLEAN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean) CLEAN=true; shift ;;
        *)       echo "Unknown flag: $1"; exit 1 ;;
    esac
done

if $CLEAN; then
    echo "Cleaning lake directory..."
    rm -rf "$LAKE_DIR" .nextflow work .nextflow.log*
fi

# ─── 1. Download ──────────────────────────────────────────────────
if [[ -f "$INPUT" ]]; then
    echo "Input already exists: $INPUT ($(du -h "$INPUT" | cut -f1) compressed)"
    echo "  (delete it to re-download)"
else
    URL="https://rest.uniprot.org/uniprotkb/stream?query=${QUERY}&format=json&compressed=true"
    echo "Downloading from UniProtKB REST API..."
    echo "  Query: $QUERY"
    echo "  URL:   $URL"
    curl --progress-bar -o "$INPUT" "$URL"
    echo "  Saved: $INPUT ($(du -h "$INPUT" | cut -f1) compressed)"
fi

# ─── 2. Validate download ────────────────────────────────────────
# Quick sanity check: is it valid gzip with a "results" key?
if ! gzip -t "$INPUT" 2>/dev/null; then
    echo "ERROR: $INPUT is not valid gzip. Delete it and re-run."
    exit 1
fi

ENTRY_COUNT=$(gzip -dc "$INPUT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(len(d.get('results', [])))
")
echo "  Entries: ${ENTRY_COUNT}"

if [[ "$ENTRY_COUNT" -eq 0 ]]; then
    echo "ERROR: Download contains 0 entries. Check the query."
    exit 1
fi

# ─── 3. Run pipeline ─────────────────────────────────────────────
echo ""
W=55
BAR=$(printf '═%.0s' $(seq 1 $W))
printf "╔%s╗\n" "$BAR"
printf "║  %-$(( W - 2 ))s║\n" "UniProtKB Parquet Data Lake Demo"
printf "║  %-$(( W - 2 ))s║\n" "Entries:  ${ENTRY_COUNT}"
printf "║  %-$(( W - 2 ))s║\n" "Release:  ${RELEASE}"
printf "╚%s╝\n" "$BAR"
echo ""

PIPELINE="../upjson2lake.nf"

# Ensure report directory exists (Nextflow won't create it)
mkdir -p "$LAKE_DIR/reports"

nextflow run "$PIPELINE" \
    --inputfile "$(pwd)/$INPUT" \
    --outdir "$(pwd)/$LAKE_DIR" \
    --release "$RELEASE" \
    --process_memory "$MEMORY" \
    -profile local \
    -ansi-log true \
    -resume

# ─── 4. Summary ──────────────────────────────────────────────────
echo ""
echo "Demo complete. Output:"
echo ""
RELEASE_DIR="$LAKE_DIR/$RELEASE"
if [[ -f "$RELEASE_DIR/validation_report.txt" ]]; then
    tail -3 "$RELEASE_DIR/validation_report.txt"
    echo ""
fi
echo "  Lake:         $RELEASE_DIR/lake/"
echo "  Sorted JSONL: $RELEASE_DIR/sorted.jsonl.zst"
if [[ -f "$RELEASE_DIR/provenance.json" ]]; then
    echo "  Provenance:   $RELEASE_DIR/provenance.json"
fi
echo ""
echo "Query with DuckDB:"
echo ""
cat <<EOF
import duckdb
con = duckdb.connect()
con.sql("SELECT * FROM read_parquet('$(pwd)/$RELEASE_DIR/lake/entries/*.parquet') LIMIT 5").show()
EOF
echo ""
