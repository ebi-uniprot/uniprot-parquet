#!/usr/bin/env python3
"""
Check a whole dat2jsonl release against REST without a REST dump: a random sample.

  rest_sample.py sample OUTDIR WORKDIR [--n 1000000] [--seed 1] [--workers N]
      Pick about N entries at random from OUTDIR/parts/*.jsonl.zst (each entry kept
      with probability N / total, independently, so every part and byte range is
      sampled): WORKDIR/sample.jsonl and WORKDIR/accessions.txt.

  rest_sample.py fetch WORKDIR [--release 2026_04]
      Fetch those accessions from REST (/uniprotkb/accessions; up to 1,000 a request,
      fewer when long TrEMBL accessions would make the URL too long),
      as served: WORKDIR/rest/batch-NNNNN.json.gz.  Every response must say
      x-uniprot-release: RELEASE, else it stops.  Resumable.

Then compare, strictly:

  python bin/compare_json.py -e WORKDIR/rest/*.json.gz -a WORKDIR/sample.jsonl \\
      --workdir WORKDIR --report-json WORKDIR/report.json > WORKDIR/report.txt

No difference in n entries means, with 95% confidence, that fewer than 3/n of all
entries differ (the "rule of three").

Python 3.14 (compression.zstd); standard library only.
"""

from __future__ import annotations

import argparse
import gzip
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from compression import zstd
from multiprocessing import Pool
from pathlib import Path

API = "https://rest.uniprot.org/uniprotkb/accessions"
BATCH = 1000  # the endpoint's limit on accessions
MAX_URL = 7500  # the server refuses (414) a URL over about 8,200 characters
KEY = b'"primaryAccession":"'


def accession_of(line: bytes) -> str:
    i = line.index(KEY) + len(KEY)
    return line[i : line.index(b'"', i)].decode()


def total_entries(outdir: Path) -> int:
    total = 0
    for stats in sorted((outdir / "parts").glob("*.stats.tsv")):
        rows = stats.read_text().splitlines()
        total += int(rows[1].split("\t")[5])
    return total


def sample_part(job: tuple[str, str, float, int]) -> tuple[str, int, int]:
    part, out, rate, seed = job
    # One generator per part, seeded by its name: the same sample whatever the workers
    rng = random.Random(f"{seed}:{Path(part).name}")
    seen = kept = 0
    with zstd.open(part, "rb") as f, open(out, "wb") as o:
        for line in f:
            seen += 1
            if rng.random() < rate:
                o.write(line)
                kept += 1
    return part, seen, kept


def cmd_sample(a: argparse.Namespace) -> int:
    outdir, work = Path(a.outdir), Path(a.workdir)
    if not (outdir / "COMPLETE").exists():
        sys.exit(f"{outdir}/COMPLETE is missing: the check has not passed")
    parts = sorted((outdir / "parts").glob("part-*.jsonl.zst"))
    total = total_entries(outdir)
    rate = min(1.0, a.n / total)
    tmp = work / "sample_parts"
    tmp.mkdir(parents=True, exist_ok=True)
    jobs = [(str(p), str(tmp / (p.name.removesuffix(".zst"))), rate, a.seed) for p in parts]
    print(f"{len(parts)} parts, {total:,} entries; keeping each with p = {rate:.6g}", file=sys.stderr)
    t0 = time.time()
    seen = kept = 0
    with Pool(a.workers) as pool:
        for i, (part, s, k) in enumerate(pool.imap(sample_part, jobs), 1):
            seen += s
            kept += k
            print(f"  {i}/{len(parts)} {Path(part).name}: {k:,} of {s:,}", file=sys.stderr)
    if seen != total:
        sys.exit(f"parts hold {seen:,} entries, the stats say {total:,}")
    accs = []
    with open(work / "sample.jsonl", "wb") as o:
        for _, out, _, _ in jobs:
            with open(out, "rb") as f:
                for line in f:
                    o.write(line)
                    accs.append(accession_of(line))
            os.remove(out)
    tmp.rmdir()
    (work / "accessions.txt").write_text("\n".join(accs) + "\n")
    print(f"sampled {kept:,} of {seen:,} entries in {time.time() - t0:.0f} s -> {work}/sample.jsonl, "
          f"accessions.txt", file=sys.stderr)
    return 0


def get(url: str, release: str, tries: int = 6) -> bytes:
    """One response body, gzip-compressed once; checks the release it was served from."""
    # No Accept-Encoding: asked for gzip, REST sends gzip inside Content-Encoding gzip
    headers = {"User-Agent": "uniprot-parquet rest_sample.py"}
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                served = r.headers.get("x-uniprot-release")
                if served != release:
                    sys.exit(f"REST now serves release {served}, not {release}: stopping")
                body = bytes(r.read())
                while body[:2] == b"\x1f\x8b":  # gzip's magic: unwrap whatever came compressed
                    body = gzip.decompress(body)
                if not body.startswith(b'{"results":'):
                    sys.exit(f"not a JSON result for {url[:200]}...: {body[:200]!r}")
                return gzip.compress(body, 6)
        except urllib.error.HTTPError as e:
            if e.code < 500 and e.code != 429:
                sys.exit(f"HTTP {e.code} for {url[:200]}...: {e.read()[:300]!r}")
            err: Exception = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = e
        wait = 2 ** attempt * 5
        print(f"  {err}; retrying in {wait} s", file=sys.stderr)
        time.sleep(wait)
    sys.exit(f"giving up after {tries} tries: {url[:200]}...")


def url_for(batch: list[str]) -> str:
    # Commas unencoded: as %2C, the URI is three times as long per separator
    return API + "?" + urllib.parse.urlencode({"accessions": ",".join(batch), "format": "json"}, safe=",")


def batched(accs: list[str]) -> list[list[str]]:
    """Consecutive runs of up to BATCH accessions whose URL stays within MAX_URL."""
    batches: list[list[str]] = []
    cur: list[str] = []
    length = len(url_for([]))
    for acc in accs:
        if cur and (len(cur) == BATCH or length + 1 + len(acc) > MAX_URL):
            batches.append(cur)
            cur, length = [], len(url_for([]))
        length += len(acc) + (1 if cur else 0)
        cur.append(acc)
    if cur:
        batches.append(cur)
    assert all(len(url_for(b)) <= MAX_URL for b in batches)
    return batches


def cmd_fetch(a: argparse.Namespace) -> int:
    work = Path(a.workdir)
    accs = (work / "accessions.txt").read_text().split()
    rest = work / "rest"
    rest.mkdir(exist_ok=True)
    batches = batched(accs)
    print(f"{len(accs):,} accessions, {len(batches)} requests", file=sys.stderr)
    t0 = time.time()
    done = 0
    for i, batch in enumerate(batches, 1):
        out = rest / f"batch-{i:05d}.json.gz"
        if out.exists():
            continue
        url = url_for(batch)
        body = get(url, a.release)
        tmp = out.with_suffix(".tmp")
        tmp.write_bytes(body)
        tmp.rename(out)
        done += 1
        if done % 50 == 0 or i == len(batches):
            rate = done / (time.time() - t0)
            print(f"  {i}/{len(batches)} ({rate * 60:.0f} requests/min)", file=sys.stderr)
    print(f"done: {len(batches)} batches in {rest}", file=sys.stderr)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample", help="pick entries at random from a dat2jsonl output")
    s.add_argument("outdir")
    s.add_argument("workdir")
    s.add_argument("--n", type=int, default=1_000_000, help="entries to sample, about (default: 1,000,000)")
    s.add_argument("--seed", type=int, default=1)
    s.add_argument("--workers", type=int, default=max(1, (os.process_cpu_count() or 2) - 1),
                   help="processes (default: the CPUs this process may use, less one)")
    f = sub.add_parser("fetch", help="fetch the sampled accessions from REST")
    f.add_argument("workdir")
    f.add_argument("--release", default="2026_04", help="the release REST must serve (default: 2026_04)")
    a = ap.parse_args()
    return cmd_sample(a) if a.cmd == "sample" else cmd_fetch(a)


if __name__ == "__main__":
    sys.exit(main())
