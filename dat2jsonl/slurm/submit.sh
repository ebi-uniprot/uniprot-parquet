#!/usr/bin/env bash
# Convert a whole UniProtKB release (.dat) to JSON lines on SLURM, with dat2jsonl.
#
#   SRC=/hps/software/users/martin/uniprot/uapi/source_data/2026_04/uniprot \
#   GAF_ECO=$FFC/gaf-eco-mapping.txt \
#   dat2jsonl/slurm/submit.sh OUTDIR
#
# Three stages, chained with SLURM dependencies:
#   1. uniparc   the uniparc_map lines the indexer keeps, once  -> OUTDIR/uniparc_active.txt
#   2. convert   a job array, one task per byte range of each .dat (CHUNK_GB each)
#                                                                -> OUTDIR/parts/part-NNNNN.jsonl.zst
#   3. check     the ranges tile every .dat, entry counts add up -> OUTDIR/CHECK.txt (+ COMPLETE)
#
# Run it from the environment that has java and zstd (micromamba activate dat2jsonl):
# sbatch hands that environment to the jobs.  Re-running it with the same OUTDIR resumes:
# finished parts are kept, the plan must be unchanged.
#
# Environment (defaults in brackets):
#   SRC            release directory holding the .dat, keywlist.txt, humdisease.txt,
#                  subcell.txt, go/GO.dr_extended, uniparc_map/            (required)
#   GAF_ECO        local gaf-eco-mapping.txt                               (required)
#   DATS           the flat files to convert [$SRC/uniprot_sprot.dat $SRC/uniprot_trembl.dat]
#   EXPECTED       the release's entry count, checked if given (REST's x-total-results)
#   CHUNK_GB       bytes of .dat per task, in GB [5]
#   CPUS MEM TIME  per conversion task [8, 16G, 2:00:00]
#   MAX_PARALLEL   tasks running at once [50]
#   PARTITION      SLURM partition [the cluster's default]
#   JAR            [dat2jsonl/target/dat2jsonl.jar]
set -euo pipefail

OUT=${1:?usage: SRC=... GAF_ECO=... submit.sh OUTDIR}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
: "${SRC:?set SRC to the release directory (source_data/<release>/uniprot)}"
: "${GAF_ECO:?set GAF_ECO to a local gaf-eco-mapping.txt}"
DATS=${DATS:-"$SRC/uniprot_sprot.dat $SRC/uniprot_trembl.dat"}
CHUNK_GB=${CHUNK_GB:-5}
CPUS=${CPUS:-8}
MEM=${MEM:-16G}
TIME=${TIME:-2:00:00}
MAX_PARALLEL=${MAX_PARALLEL:-50}
JAR=${JAR:-$HERE/../target/dat2jsonl.jar}
EXPECTED=${EXPECTED:-}
PART_OPT=()
[[ -n "${PARTITION:-}" ]] && PART_OPT=(-p "$PARTITION")

die() { echo "submit.sh: $*" >&2; exit 1; }
command -v java >/dev/null || die "no java on PATH (micromamba activate dat2jsonl)"
command -v zstd >/dev/null || die "no zstd on PATH (micromamba install -n dat2jsonl -c conda-forge zstd)"
command -v sbatch >/dev/null || die "no sbatch on PATH"
[[ -f "$JAR" ]] || die "no $JAR: build it first (mvn -B -q package -f dat2jsonl/pom.xml)"
for f in "$SRC/keywlist.txt" "$SRC/humdisease.txt" "$SRC/subcell.txt" "$SRC/go/GO.dr_extended" "$GAF_ECO"; do
  [[ -f "$f" ]] || die "no $f"
done
[[ -e "$SRC/uniparc_map" ]] || die "no $SRC/uniparc_map"
for d in $DATS; do
  [[ -f "$d" ]] || die "no $d"
  [[ "$d" != *.gz ]] || die "$d is compressed: byte ranges need an uncompressed .dat"
done

mkdir -p "$OUT/parts" "$OUT/logs"
OUT=$(cd "$OUT" && pwd)

# The plan: one row per task.  Byte ranges [start, end) tile each file; a task converts the
# entries that start in its range.
CHUNK=$(awk -v g="$CHUNK_GB" 'BEGIN { printf "%d", g * 1e9 }')
(( CHUNK >= 1000000 )) || die "CHUNK_GB=$CHUNK_GB: at least 0.001"
plan=$(
  printf 'task\tdat\tstart\tend\tpart\n'
  t=0
  for d in $DATS; do
    size=$(stat -L -c %s "$d")
    s=0
    while (( s < size )); do
      e=$(( s + CHUNK < size ? s + CHUNK : size ))
      t=$(( t + 1 ))
      printf '%d\t%s\t%d\t%d\tpart-%05d\n' "$t" "$d" "$s" "$e" "$t"
      s=$e
    done
  done
)
if [[ -f "$OUT/plan.tsv" ]]; then
  [[ "$plan" == "$(cat "$OUT/plan.tsv")" ]] || die "$OUT/plan.tsv differs from this run's plan (other DATS, CHUNK_GB or file sizes): use a new OUTDIR"
  echo "resuming $OUT"
else
  printf '%s\n' "$plan" > "$OUT/plan.tsv"
  # What made this output: frozen copies, so a rebuild or a moved file cannot change a rerun
  cp "$JAR" "$OUT/dat2jsonl.jar"
  cp "$GAF_ECO" "$OUT/gaf-eco-mapping.txt"
  {
    echo "SRC=$(printf '%q' "$SRC")"
    echo "EXPECTED=$(printf '%q' "$EXPECTED")"
  } > "$OUT/run.env"
  {
    echo "created: $(date -Is)"
    echo "SRC: $SRC"
    echo "jar sha256: $(sha256sum "$OUT/dat2jsonl.jar" | cut -d' ' -f1)"
    echo "gaf-eco-mapping.txt sha256: $(sha256sum "$OUT/gaf-eco-mapping.txt" | cut -d' ' -f1)"
    echo "java: $(java -version 2>&1 | grep -v 'Picked up' | head -1)"
    for d in $DATS; do echo "dat: $d $(stat -L -c '%s bytes, modified %y' "$d")"; done
    for f in keywlist.txt humdisease.txt subcell.txt go/GO.dr_extended; do
      echo "$f: $(stat -L -c '%s bytes, modified %y' "$SRC/$f")"
    done
  } > "$OUT/inputs.txt"
fi
N=$(( $(wc -l < "$OUT/plan.tsv") - 1 ))

# 1. uniparc
DEP=()
if [[ ! -s "$OUT/uniparc_active.txt" ]]; then
  J1=$(sbatch --parsable "${PART_OPT[@]}" -c 2 --mem 4G -t 2:00:00 -J d2j_uniparc \
        -o "$OUT/logs/uniparc-%j.out" \
        --wrap "java -jar $OUT/dat2jsonl.jar uniparc-active --uniparc $SRC/uniparc_map \
                > $OUT/uniparc_active.txt.tmp && mv $OUT/uniparc_active.txt.tmp $OUT/uniparc_active.txt")
  DEP=(-d "afterok:$J1")
  echo "uniparc:  job $J1"
fi

# 2. convert
J2=$(sbatch --parsable "${PART_OPT[@]}" "${DEP[@]}" --array "1-$N%$MAX_PARALLEL" \
      -c "$CPUS" --mem "$MEM" -t "$TIME" -J d2j_convert \
      -o "$OUT/logs/convert-%A_%a.out" "$HERE/task.sh" "$OUT")
echo "convert:  job $J2 ($N tasks, $MAX_PARALLEL at a time)"

# 3. check, whatever the tasks' outcome
J3=$(sbatch --parsable "${PART_OPT[@]}" -d "afterany:$J2" -c 1 --mem 1G -t 0:15:00 -J d2j_check \
      -o "$OUT/logs/check-%j.out" "$HERE/check.sh" "$OUT")
echo "check:    job $J3 -> $OUT/CHECK.txt"
