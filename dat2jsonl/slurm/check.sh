#!/usr/bin/env bash
# submit.sh's last stage: did the tasks convert every entry of every .dat, once?
#   check.sh OUTDIR    -> OUTDIR/CHECK.txt, and OUTDIR/COMPLETE if it passes
#
# Each task's stats give where its first entry starts and its last one ends.  The ranges
# cover a file exactly when, in file order, the first starts at 0, each ends where the next
# starts, and the last ends at the file's size; no .dat is read again.
set -euo pipefail

OUT=${1:?usage: check.sh OUTDIR}
# shellcheck source=/dev/null
source "$OUT/run.env"
rm -f "$OUT/COMPLETE"

sizes=$(awk -F'\t' 'NR > 1 {print $2}' "$OUT/plan.tsv" | sort -u | while read -r d; do
  printf '%s\t%s\n' "$d" "$(stat -L -c %s "$d")"
done)

{
  echo "dat2jsonl check: $OUT"
  echo "date: $(date -Is)"
  awk -F'\t' -v out="$OUT" -v expected="$EXPECTED" -v sizes="$sizes" '
    BEGIN {
      n = split(sizes, rows, "\n")
      for (i = 1; i <= n; i++) { split(rows[i], f, "\t"); size[f[1]] = f[2] }
    }
    NR > 1 {
      task = $1; dat = $2; part = $5
      stats = out "/parts/" part ".stats.tsv"
      if ((getline hdr < stats) <= 0 || (getline row < stats) <= 0) {
        missing = missing " " part; nmissing++; next
      }
      close(stats)
      split(row, s, "\t")
      # dat range_start range_end first_offset end_offset entries with_go with_uniparc first last secs
      first[task] = s[4]; endo[task] = s[5]; file[task] = dat
      entries[dat] += s[6]; total += s[6]; go += s[7]; uniparc += s[8]
      secs += s[11]; if (s[11] > maxsecs) { maxsecs = s[11]; slowest = part }
      if (!(dat in lasttask)) { order[++nd] = dat }
      if (!(dat in firsttask)) firsttask[dat] = task
      lasttask[dat] = task; ntasks++
    }
    END {
      ok = 1
      if (nmissing) { printf "MISSING %d parts:%s\n", nmissing, missing; ok = 0 }
      for (i = 1; i <= nd; i++) {
        d = order[i]; gaps = 0
        if (first[firsttask[d]] != 0) { printf "GAP %s: first entry at %s, not 0\n", d, first[firsttask[d]]; gaps++ }
        for (t = firsttask[d]; t < lasttask[d]; t++) {
          if (!(t in endo) || !((t + 1) in first)) continue
          if (endo[t] != first[t + 1]) { printf "GAP %s: task %d ends at %s, task %d starts at %s\n", d, t, endo[t], t + 1, first[t + 1]; gaps++ }
        }
        if (endo[lasttask[d]] != size[d]) { printf "GAP %s: last entry ends at %s, file is %s bytes\n", d, endo[lasttask[d]], size[d]; gaps++ }
        printf "%s: %d entries%s\n", d, entries[d], gaps ? "" : ", every byte covered once"
        if (gaps) ok = 0
      }
      printf "total: %d entries; GO evidences on %d; UniParc id on %d (%d without)\n", total, go, uniparc, total - uniparc
      printf "tasks: %d; %.0f CPU-task seconds in all; slowest %s, %.0f s\n", ntasks, secs, slowest, maxsecs
      if (expected != "") {
        if (total == expected) printf "expected %d entries: matches\n", expected
        else { printf "EXPECTED %d entries, got %d (%+d)\n", expected, total, total - expected; ok = 0 }
      }
      print ok && !nmissing ? "CHECK PASSED" : "CHECK FAILED"
      exit !(ok && !nmissing)
    }' "$OUT/plan.tsv"
} > "$OUT/CHECK.txt" && status=0 || status=$?

cat "$OUT/CHECK.txt"
if (( status == 0 )); then
  touch "$OUT/COMPLETE"
fi
exit "$status"
