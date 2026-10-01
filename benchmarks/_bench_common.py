"""Shared helpers for the benchmark scripts (file size, zstd level, point lookup)."""

import glob
import http.server
import json
import os
import re
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, AnyStr, ClassVar, cast

if TYPE_CHECKING:
    import duckdb
    from _typeshed import SupportsRead, SupportsWrite

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BIN_DIR = os.path.join(REPO_ROOT, "bin")


def eprint(*a: Any, **k: Any) -> None:
    print(*a, file=sys.stderr, **k)


def human_size(nbytes: float) -> str:
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024 or unit == "TB":
            return f"{int(nbytes)} bytes" if unit == "bytes" else f"{nbytes:.2f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.2f} TB"


def time_query(con: duckdb.DuckDBPyConnection, sql: str, warmup: int = 1, runs: int = 5) -> dict[str, Any]:
    """Run a query several times; timing stats in ms (mirrors bench_baseline._time_query)."""
    for _ in range(warmup):
        con.sql(sql).fetchall()
    times: list[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        result = con.sql(sql).fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    return {"min_ms": round(min(times), 3), "median_ms": round(statistics.median(times), 3),
            "mean_ms": round(statistics.mean(times), 3), "runs": runs, "row_count": len(result)}


def build_lake(jsonl: str, outdir: str, only: str | None = None, extra: Sequence[str] = (), memory: str = "4GB",
               batch_size: int | None = None) -> float:
    """Run bin/parquet_transform.py; returns wall seconds."""
    cmd = [sys.executable, os.path.join(BIN_DIR, "parquet_transform.py"), jsonl,
           "--outdir", outdir, "--memory-limit", memory, "--release", "bench", *extra]
    if only:
        cmd += ["--only", only]
    if batch_size:
        cmd += ["--batch-size", str(batch_size)]
    env = dict(os.environ, PYTHONPATH=BIN_DIR + ":" + os.environ.get("PYTHONPATH", ""))
    t0 = time.perf_counter()
    subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
    return time.perf_counter() - t0


def table_files(lake: str, table: str) -> list[str]:
    return sorted(glob.glob(os.path.join(lake, table, "**", "*.parquet"), recursive=True))


def read_parquet_source(lake: str, table: str, base: str | None = None) -> str:
    """SQL source for a table: an explicit file list from manifest.json (works
    over plain HTTP, where DuckDB cannot expand a glob), rooted at `base`
    (a URL) or at `lake` (local)."""
    with open(os.path.join(lake, "manifest.json")) as f:
        rels = json.load(f)["tables"][table]["files"]
    root = base if base else lake
    return "[" + ", ".join(f"'{root}/{table}/{rel}'" for rel in rels) + "]"


def footer_bytes(files: Iterable[str]) -> int:
    import pyarrow.parquet as pq
    total: int = sum(pq.read_metadata(f).serialized_size for f in files)
    return total


def table_bytes(files: Iterable[str]) -> int:
    return sum(os.path.getsize(f) for f in files)


class _CountingHandler(http.server.SimpleHTTPRequestHandler):
    """Static file server with HTTP Range support (DuckDB httpfs reads byte
    ranges) that counts requests and bytes served."""
    stats: ClassVar[dict[str, int]] = {"requests": 0, "bytes": 0}
    lock = threading.Lock()
    protocol_version = "HTTP/1.1"      # keep-alive: DuckDB reuses one connection per file

    def log_message(self, *a: Any) -> None:
        pass

    def _count(self, n: int = 0) -> None:
        with self.lock:
            self.stats["requests"] += 1 if n == 0 else 0
            self.stats["bytes"] += n

    def do_HEAD(self) -> None:
        self._count()
        super().do_HEAD()

    def do_GET(self) -> None:
        self._count()
        path = self.translate_path(self.path)
        rng = self.headers.get("Range")
        if not rng or not os.path.isfile(path):
            return super().do_GET()
        size = os.path.getsize(path)
        m = re.match(r"bytes=(\d*)-(\d*)", rng)
        if not m:
            return super().do_GET()
        start = int(m.group(1)) if m.group(1) else max(size - int(m.group(2)), 0)
        end = int(m.group(2)) if m.group(1) and m.group(2) else size - 1
        end = min(end, size - 1)
        length = end - start + 1
        self.send_response(206)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(length))
        self.end_headers()
        with open(path, "rb") as f:
            f.seek(start)
            self.wfile.write(f.read(length))
        with self.lock:
            self.stats["bytes"] += length

    def copyfile(self, source: SupportsRead[AnyStr], outputfile: SupportsWrite[AnyStr]) -> None:
        data = source.read()
        with self.lock:
            self.stats["bytes"] += len(data)
        outputfile.write(data)


@contextmanager
def serve_directory(directory: str) -> Iterator[tuple[str, dict[str, int]]]:
    """Serve `directory` over HTTP on a free port; yields (base_url, stats dict)."""
    handler = cast(type[_CountingHandler],
                   type("H", (_CountingHandler,), {"stats": {"requests": 0, "bytes": 0},
                                                   "lock": threading.Lock()}))
    handler_cls = lambda *a, **k: handler(*a, directory=directory, **k)  # noqa: E731
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", handler.stats
    finally:
        server.shutdown()


def duckdb_http_connection() -> duckdb.DuckDBPyConnection:
    import duckdb
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    # No proxy for the local server (the sandbox sets http_proxy globally)
    con.sql("SET http_proxy = ''")
    return con


def write_results(prefix: str, results: dict[str, Any], out_dir: str, text: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    with open(os.path.join(out_dir, f"{prefix}_{ts}.json"), "w") as f:
        json.dump(results, f, indent=2)
    with open(os.path.join(out_dir, f"{prefix}_{ts}.txt"), "w") as f:
        f.write(text)
    with open(os.path.join(out_dir, f"{prefix}_latest.json"), "w") as f:
        json.dump(results, f, indent=2)
    with open(os.path.join(out_dir, f"{prefix}_latest.txt"), "w") as f:
        f.write(text)
    return os.path.join(out_dir, f"{prefix}_{ts}.json")
