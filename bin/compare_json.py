#!/usr/bin/env python3
"""
Compare two sets of UniProtKB entries in UniProt's JSON model, strictly.

Made to check JSONL built by UniProt's own code (uniprot-core, from the
release .dat) against the JSON the REST API serves: the lake keeps the JSON
as shipped, so the two must agree in every value, every JSON type and the
order of every object's keys.

Entries are paired by primaryAccession.  Every difference is counted under
its path, so the report says which fields differ and how often.  Numbers
are parsed exactly (Decimal), so 5 against 5.0 is a "type" difference and
nothing is lost to floating point.

Inputs (each side one or more files, mixed freely):
  - {"results": [...]}  REST search/stream output (.json, .json.gz, .json.zst)
  - JSON lines           one entry per line (.jsonl, .jsonl.gz, .jsonl.zst)
An accession repeated within one side keeps its first entry (test sets
fetched query by query overlap).

DIFF KINDS
  missing          a key in EXPECTED that ACTUAL lacks
  extra            a key in ACTUAL that EXPECTED lacks
  value            the values differ
  type             equal values as different JSON types (5 / 5.0, 1 / true, "5" / 5)
  number_format    equal numbers written differently (5.0 / 5.00)
  key_order        an object's keys in another order
  order            a list holds the same elements in another order
  only_in_expected a list element of EXPECTED with no counterpart in ACTUAL
  only_in_actual   a list element of ACTUAL with no counterpart in EXPECTED

Paths: list indices are dropped, and an element of comments, features,
uniProtKBCrossReferences or references is labelled by its type, e.g.
comments[FUNCTION].texts[].value or uniProtKBCrossReferences[GO].evidences[].id.

Usage:
    compare_json.py -e uniprotkb_reviewed.json.gz -a sprot.jsonl.zst
    compare_json.py -e testset/*.json -a trembl_testset.jsonl --report-json report.json

Exit status: 0 when every accession is on both sides and no entry differs, 1
otherwise.  Memory stays small: ACTUAL waits zlib-compressed in a temporary
SQLite file (--workdir) while EXPECTED is streamed against it.
"""

import argparse
import gzip
import json
import multiprocessing as mp
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterator
from decimal import Decimal
from typing import IO, Any

EXAMPLES_PER_PATH = 3
EXAMPLE_CHARS = 200

# ─── Reading ────────────────────────────────────────────────────────────


def open_text(path: str) -> IO[str]:
    """A text stream over a plain, .gz or .zst file (pigz / zstd when on PATH)."""
    if path.endswith(".gz"):
        tool = shutil.which("pigz")
        if tool is None:
            return gzip.open(path, "rt", encoding="utf-8")
    elif path.endswith(".zst"):
        tool = shutil.which("zstd")
        if tool is None:
            from compression import zstd  # Python 3.14+

            return zstd.open(path, "rt", encoding="utf-8")
    else:
        return open(path, encoding="utf-8")
    proc = subprocess.Popen([tool, "-dc", path], stdout=subprocess.PIPE)
    assert proc.stdout is not None
    return open(proc.stdout.fileno(), encoding="utf-8", closefd=True)


RESULTS_RE = re.compile(r'^\s*\{\s*"results"\s*:\s*\[')
ACC_RE = re.compile(r'"primaryAccession"\s*:\s*"([^"]+)"')


def iter_entries(path: str) -> Iterator[tuple[str, str]]:
    """(primary accession, entry text) for each entry of a REST JSON or JSONL file."""
    with open_text(path) as f:
        head = f.read(1 << 20)
        if RESULTS_RE.match(head):
            yield from _iter_results(f, head)
            return
        rest = ""
        while True:
            buf = rest + head
            lines = buf.split("\n")
            rest = lines.pop()
            for line in lines:
                if line.strip():
                    yield _accession(line, path), line
            head = f.read(1 << 22)
            if not head:
                break
        if rest.strip():
            yield _accession(rest, path), rest


def _accession(text: str, path: str) -> str:
    m = ACC_RE.search(text)
    if m is None:
        raise ValueError(f"{path}: an entry without primaryAccession: {text[:120]}")
    return m.group(1)


def _iter_results(f: IO[str], buf: str) -> Iterator[tuple[str, str]]:
    dec = json.JSONDecoder()
    i = buf.index("[") + 1
    while True:
        while i < len(buf) and buf[i] in " \n\r\t,":
            i += 1
        if i < len(buf) and buf[i] == "]":
            return
        try:
            obj, j = dec.raw_decode(buf, i)
        except json.JSONDecodeError:
            more = f.read(1 << 24)
            if not more:
                raise
            buf = buf[i:] + more
            i = 0
            continue
        yield obj["primaryAccession"], buf[i:j]
        i = j


def parse(text: str) -> Any:
    return json.loads(text, parse_float=Decimal)


# ─── Strict structural diff ─────────────────────────────────────────────

Diff = tuple[str, str, Any, Any]  # kind, path, expected value, actual value

LIST_LABEL: dict[str, Callable[[Any], str]] = {
    "comments": lambda c: str(c.get("commentType")) if isinstance(c, dict) else "?",
    "features": lambda f: str(f.get("type")) if isinstance(f, dict) else "?",
    "uniProtKBCrossReferences": lambda x: str(x.get("database")) if isinstance(x, dict) else "?",
    "references": lambda r: str((r.get("citation") or {}).get("citationType")) if isinstance(r, dict) else "?",
}


def _tagged(o: Any) -> Any:
    return {"$decimal": str(o)} if isinstance(o, Decimal) else str(o)


def strict(v: Any) -> str:
    """Text equal for two values only if they match in value, type and key order."""
    return json.dumps(v, ensure_ascii=False, default=_tagged)


def loose(v: Any) -> str:
    """Text equal for two values that match apart from key order."""
    return json.dumps(v, ensure_ascii=False, default=_tagged, sort_keys=True)


def short(v: Any) -> str:
    s = str(v) if isinstance(v, Decimal) else json.dumps(v, ensure_ascii=False, default=str)
    return s if len(s) <= EXAMPLE_CHARS else s[:EXAMPLE_CHARS] + "…"


def diff(e: Any, a: Any, path: str, out: list[Diff]) -> None:
    if isinstance(e, dict) and isinstance(a, dict):
        for k, ev in e.items():
            p = f"{path}.{k}" if path else k
            if k in a:
                diff(ev, a[k], p, out)
            else:
                out.append(("missing", p, ev, None))
        for k, av in a.items():
            if k not in e:
                out.append(("extra", f"{path}.{k}" if path else k, None, av))
        common_e = [k for k in e if k in a]
        common_a = [k for k in a if k in e]
        if common_e != common_a:
            out.append(("key_order", path or "(entry)", list(e), list(a)))
        return
    if isinstance(e, list) and isinstance(a, list):
        diff_list(e, a, path, out)
        return
    if type(e) is type(a):
        if e == a:
            if isinstance(e, Decimal) and str(e) != str(a):
                out.append(("number_format", path, e, a))
            return
        out.append(("value", path, e, a))
        return
    numeric = (int, Decimal)
    same = (isinstance(e, numeric) and isinstance(a, numeric) and not isinstance(e, bool)
            and not isinstance(a, bool) and e == a)
    if same or (e is not None and a is not None and str(e).lower() == str(a).lower()):
        out.append(("type", path, e, a))
    else:
        out.append(("value", path, e, a))


def diff_list(e: list[Any], a: list[Any], path: str, out: list[Diff]) -> None:
    key = path.rsplit(".", 1)[-1].split("[")[0]
    label = LIST_LABEL.get(key)
    if label is None:
        diff_seq(e, a, path + "[]", out)
        return
    ge: dict[str, list[Any]] = {}
    ga: dict[str, list[Any]] = {}
    for v in e:
        ge.setdefault(label(v), []).append(v)
    for v in a:
        ga.setdefault(label(v), []).append(v)
    le, la = [label(v) for v in e], [label(v) for v in a]
    if le != la and sorted(le) == sorted(la):
        out.append(("order", path + "[*]", le, la))
    for lab in list(ge) + [k for k in ga if k not in ge]:
        diff_seq(ge.get(lab, []), ga.get(lab, []), f"{path}[{lab}]", out)


def diff_seq(e: list[Any], a: list[Any], path: str, out: list[Diff]) -> None:
    se, sa = [strict(v) for v in e], [strict(v) for v in a]
    if se == sa:
        return
    if len(e) == len(a):
        if sorted(se) == sorted(sa):
            out.append(("order", path, None, None))
            return
        if sorted(map(loose, e)) != sorted(map(loose, a)):  # not merely key order inside: pair by position
            for x, y in zip(e, a, strict=True):
                diff(x, y, path, out)
            return
    # Pair elements equal apart from key order (so key_order is reported), then
    # what is left in order; the surplus of the longer side is only_in_*.
    pool: dict[str, list[int]] = {}
    for i, v in enumerate(a):
        pool.setdefault(loose(v), []).append(i)
    used: set[int] = set()
    rest_e = []
    for v in e:
        idx = pool.get(loose(v))
        if idx:
            i = idx.pop(0)
            used.add(i)
            diff(v, a[i], path, out)
        else:
            rest_e.append(v)
    rest_a = [v for i, v in enumerate(a) if i not in used]
    n = min(len(rest_e), len(rest_a))
    for x, y in zip(rest_e[:n], rest_a[:n], strict=True):
        diff(x, y, path, out)
    out.extend(("only_in_expected", path, v, None) for v in rest_e[n:])
    out.extend(("only_in_actual", path, None, v) for v in rest_a[n:])


def compare_entries(expected_text: str, actual_text: str) -> list[Diff]:
    out: list[Diff] = []
    diff(parse(expected_text), parse(actual_text), "", out)
    return out


# ─── Driver ─────────────────────────────────────────────────────────────


def work(item: tuple[str, str, bytes]) -> tuple[str, list[tuple[str, str, str, str]]]:
    acc, expected_text, actual_blob = item
    diffs = compare_entries(expected_text, zlib.decompress(actual_blob).decode())
    return acc, [(k, p, short(e), short(a)) for k, p, e, a in diffs]


class Report:
    def __init__(self) -> None:
        self.entries_with: Counter[tuple[str, str]] = Counter()
        self.occurrences: Counter[tuple[str, str]] = Counter()
        self.examples: dict[tuple[str, str], list[dict[str, str]]] = {}
        self.expected_entries = 0
        self.actual_entries = 0
        self.compared = 0
        self.identical = 0
        self.only_expected: list[str] = []
        self.only_actual: list[str] = []
        self.duplicates_expected = 0
        self.duplicates_actual = 0

    def add(self, acc: str, diffs: list[tuple[str, str, str, str]]) -> None:
        self.compared += 1
        if not diffs:
            self.identical += 1
            return
        seen = set()
        for kind, path, e, a in diffs:
            k = (kind, path)
            self.occurrences[k] += 1
            if k in seen:
                continue
            seen.add(k)
            self.entries_with[k] += 1
            ex = self.examples.setdefault(k, [])
            if len(ex) < EXAMPLES_PER_PATH:
                ex.append({"accession": acc, "expected": e, "actual": a})

    @property
    def ok(self) -> bool:
        return self.identical == self.compared and not self.only_expected and not self.only_actual


def eprint(*a: Any) -> None:
    print(*a, file=sys.stderr, flush=True)


def run(args: argparse.Namespace) -> Report:
    rep = Report()
    workdir = tempfile.mkdtemp(prefix="cmpjson_", dir=args.workdir)
    # The pool's feeder thread runs pairs(), so the connection crosses threads (never concurrently).
    db = sqlite3.connect(os.path.join(workdir, "actual.sqlite"), check_same_thread=False)
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    db.execute("CREATE TABLE a (acc TEXT PRIMARY KEY, blob BLOB, seen INTEGER DEFAULT 0)")
    t0 = time.time()
    try:
        batch: list[tuple[str, bytes]] = []
        for path in args.actual:
            for acc, text in iter_entries(path):
                rep.actual_entries += 1
                batch.append((acc, zlib.compress(text.encode(), 1)))
                if len(batch) >= 5000:
                    rep.duplicates_actual += _insert(db, batch)
        rep.duplicates_actual += _insert(db, batch)
        db.commit()
        eprint(f"actual: {rep.actual_entries:,} entries loaded ({time.time() - t0:.0f}s)")

        seen_expected: set[str] = set()

        def pairs() -> Iterator[tuple[str, str, bytes]]:
            for path in args.expected:
                for acc, text in iter_entries(path):
                    rep.expected_entries += 1
                    if acc in seen_expected:
                        rep.duplicates_expected += 1
                        continue
                    seen_expected.add(acc)
                    row = db.execute("SELECT blob FROM a WHERE acc = ?", (acc,)).fetchone()
                    if row is None:
                        rep.only_expected.append(acc)
                        continue
                    db.execute("UPDATE a SET seen = 1 WHERE acc = ?", (acc,))
                    yield acc, text, row[0]

        with mp.Pool(args.workers) as pool:
            for i, (acc, diffs) in enumerate(pool.imap_unordered(work, pairs(), chunksize=64), 1):
                rep.add(acc, diffs)
                if i % 100000 == 0:
                    eprint(f"  compared: {i:,} ({time.time() - t0:.0f}s)")
        rep.only_actual = [acc for (acc,) in db.execute("SELECT acc FROM a WHERE seen = 0 ORDER BY acc")]
        eprint(f"compared: {rep.compared:,} entries ({time.time() - t0:.0f}s)")
    finally:
        db.close()
        shutil.rmtree(workdir, ignore_errors=True)
    return rep


def _insert(db: sqlite3.Connection, batch: list[tuple[str, bytes]]) -> int:
    """Insert, keeping the first of a repeated accession; returns how many were repeats."""
    before = db.total_changes
    db.executemany("INSERT OR IGNORE INTO a (acc, blob) VALUES (?, ?)", batch)
    dup = len(batch) - (db.total_changes - before)
    batch.clear()
    return dup


def files_label(paths: list[str]) -> str:
    """The files, or for more than three, how many and where (the JSON report lists them all)."""
    if len(paths) <= 3:
        return " ".join(paths)
    dirs = sorted({os.path.dirname(p) or "." for p in paths})
    where = dirs[0] if len(dirs) == 1 else f"{len(dirs)} directories"
    return f"{len(paths):,} files in {where} ({os.path.basename(paths[0])} ... {os.path.basename(paths[-1])})"


def print_report(rep: Report, args: argparse.Namespace) -> None:
    p = print
    n = rep.compared or 1
    p("=" * 100)
    p("UniProtKB JSON: EXPECTED vs ACTUAL")
    p(f"  expected: {files_label(args.expected)}  ({rep.expected_entries:,} entries, "
      f"{rep.duplicates_expected:,} repeated accessions skipped)")
    p(f"  actual:   {files_label(args.actual)}  ({rep.actual_entries:,} entries, "
      f"{rep.duplicates_actual:,} repeated accessions skipped)")
    p("")
    p(f"Accessions only in expected: {len(rep.only_expected):,}  {rep.only_expected[:10]}")
    p(f"Accessions only in actual:   {len(rep.only_actual):,}  {rep.only_actual[:10]}")
    p(f"Entries compared: {rep.compared:,}; identical: {rep.identical:,} ({100 * rep.identical / n:.2f}%)")
    p("")
    if not rep.entries_with:
        p("No differences.")
    else:
        p("Differences  [entries, % of compared, occurrences, kind, path]:")
        for k in sorted(rep.entries_with, key=lambda q: (-rep.entries_with[q], q)):
            e = rep.entries_with[k]
            p(f"  {e:>9,} {100 * e / n:6.2f}% {rep.occurrences[k]:>11,}  {k[0]:<16} {k[1]}")
            for ex in rep.examples[k][: args.examples]:
                p(f"{'':30}{ex['accession']}: expected={ex['expected']}")
                p(f"{'':30}{'':{len(ex['accession'])}}  actual  ={ex['actual']}")
    p("")
    p("RESULT: " + ("identical" if rep.ok else "DIFFERENT"))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-e", "--expected", nargs="+", required=True, help="reference entries (e.g. REST JSON)")
    ap.add_argument("-a", "--actual", nargs="+", required=True, help="entries to check (e.g. uniprot-core JSONL)")
    ap.add_argument("--workers", type=int, default=max(1, (os.process_cpu_count() or 2) - 1),
                    help="worker processes (default: the CPUs this process may use, less one)")
    ap.add_argument("--workdir", default=None, help="where the temporary SQLite file goes")
    ap.add_argument("--examples", type=int, default=EXAMPLES_PER_PATH, help="examples printed per difference")
    ap.add_argument("--report-json", help="also write the report (with examples) as JSON here")
    args = ap.parse_args()

    rep = run(args)
    print_report(rep, args)
    if args.report_json:
        with open(args.report_json, "w") as f:
            json.dump({
                "expected": args.expected, "actual": args.actual,
                "expected_entries": rep.expected_entries, "actual_entries": rep.actual_entries,
                "compared": rep.compared, "identical": rep.identical,
                "only_in_expected": rep.only_expected, "only_in_actual": rep.only_actual,
                "differences": [
                    {"kind": k[0], "path": k[1], "entries": e, "occurrences": rep.occurrences[k],
                     "examples": rep.examples[k]}
                    for k, e in rep.entries_with.most_common()],
            }, f, indent=1, ensure_ascii=False)
    sys.exit(0 if rep.ok else 1)


if __name__ == "__main__":
    main()
