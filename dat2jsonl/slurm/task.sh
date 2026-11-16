#!/usr/bin/env bash
# One task of submit.sh's job array: the entries starting in one byte range of one .dat.
#   task.sh OUTDIR        (row SLURM_ARRAY_TASK_ID of OUTDIR/plan.tsv)
# Writes parts/part-NNNNN.jsonl.zst, then parts/part-NNNNN.stats.tsv, which marks it done.
set -euo pipefail

OUT=${1:?usage: task.sh OUTDIR}
TASK=${SLURM_ARRAY_TASK_ID:?not in a SLURM array task}
# shellcheck source=/dev/null
source "$OUT/run.env"

IFS=$'\t' read -r _ dat start end name < <(awk -F'\t' -v t="$TASK" 'NR > 1 && $1 == t' "$OUT/plan.tsv")
[[ -n "${name:-}" ]] || { echo "task $TASK is not in $OUT/plan.tsv" >&2; exit 1; }
part=$OUT/parts/$name.jsonl.zst
stats=$OUT/parts/$name.stats.tsv
if [[ -s "$stats" && -s "$part" ]]; then
  echo "$name: done already"
  exit 0
fi

# Java heap: three quarters of the job's memory (SLURM_MEM_PER_NODE is in MB)
HEAP=()
[[ -n "${SLURM_MEM_PER_NODE:-}" ]] && HEAP=("-Xmx$(( SLURM_MEM_PER_NODE * 3 / 4 ))m")

echo "$name: $dat [$start, $end) on $(hostname), ${SLURM_CPUS_PER_TASK:-1} CPUs"
java "${HEAP[@]}" -jar "$OUT/dat2jsonl.jar" \
    --dat "$dat" --range "$start:$end" \
    --keywords "$SRC/keywlist.txt" --diseases "$SRC/humdisease.txt" --subcell "$SRC/subcell.txt" \
    --go "$SRC/go/GO.dr_extended" --gaf-eco "$OUT/gaf-eco-mapping.txt" \
    --uniparc "$OUT/uniparc_active.txt" \
    --threads "${SLURM_CPUS_PER_TASK:-1}" --stats "$stats.tmp" \
  | zstd -q -f -T1 -o "$part.tmp"
mv "$part.tmp" "$part"
mv "$stats.tmp" "$stats"
