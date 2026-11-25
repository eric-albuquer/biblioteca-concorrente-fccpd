"""Compara o relatório sequencial com 1, 2, 4 e até 6 processos."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import statistics
import time
import ctypes
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from benchmarks.analytics import analyze_partition, normalize_report
from benchmarks.generate_dataset import generate


def report_hash(report) -> str:
    payload = json.dumps(report, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def run_sequential(paths: list[Path]):
    return normalize_report([analyze_partition(path) for path in paths])


def run_parallel(paths: list[Path], workers: int):
    with ProcessPoolExecutor(max_workers=workers) as executor:
        return normalize_report(list(executor.map(analyze_partition, paths)))


def measure(function, repetitions: int):
    durations = []
    expected_hash = None
    for _ in range(repetitions):
        start = time.perf_counter()
        result = function()
        durations.append(time.perf_counter() - start)
        current_hash = report_hash(result)
        if expected_hash is not None and current_hash != expected_hash:
            raise RuntimeError("A mesma configuração produziu resultados diferentes.")
        expected_hash = current_hash
    return durations, expected_hash


def percentile(values: list[float], percentage: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentage
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def total_memory_bytes() -> int | None:
    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", ctypes.c_ulong),
                ("memory_load", ctypes.c_ulong),
                ("total_physical", ctypes.c_ulonglong),
                ("available_physical", ctypes.c_ulonglong),
                ("total_page_file", ctypes.c_ulonglong),
                ("available_page_file", ctypes.c_ulonglong),
                ("total_virtual", ctypes.c_ulonglong),
                ("available_virtual", ctypes.c_ulonglong),
                ("available_extended_virtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return status.total_physical
        return None
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None


def summary_row(mode: str, workers: int, durations: list[float], baseline: float):
    median = statistics.median(durations)
    speedup = baseline / median
    return {
        "mode": mode,
        "workers": workers,
        "median_seconds": median,
        "p95_seconds": percentile(durations, 0.95),
        "stdev_seconds": statistics.stdev(durations) if len(durations) > 1 else 0.0,
        "speedup": speedup,
        "efficiency": 1.0 if workers == 0 else speedup / workers,
    }


def write_markdown(path: Path, metadata: dict) -> None:
    lines = [
        "# Resultado do benchmark paralelo",
        "",
        f"- Eventos: {metadata['method']['total_events']:,}",
        f"- Repetições medidas: {metadata['method']['repetitions']}",
        f"- Python: {metadata['environment']['python']}",
        f"- CPUs lógicas: {metadata['environment']['logical_cpus']}",
        "",
        "| Modo | Processos | Mediana (s) | p95 (s) | Desvio (s) | Speedup | Eficiência |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for item in metadata["summary"]:
        lines.append(
            "| {mode} | {workers} | {median_seconds:.4f} | {p95_seconds:.4f} | "
            "{stdev_seconds:.4f} | {speedup:.2f}x | {efficiency:.1%} |".format(**item)
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("benchmark-data"))
    parser.add_argument("--output", type=Path, default=Path("benchmark-results"))
    parser.add_argument("--events-per-category", type=int, default=100_000)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--workers", type=int, nargs="*", default=[1, 2, 4, 6])
    args = parser.parse_args()

    paths = sorted(args.data.glob("*.ndjson"))
    if len(paths) != 6:
        paths = generate(args.data, args.events_per_category, seed=20261125)
    args.output.mkdir(parents=True, exist_ok=True)

    # Aquecimento fora das medições.
    analyze_partition(paths[0])
    sequential_durations, expected_hash = measure(
        lambda: run_sequential(paths), args.repetitions
    )
    rows = [
        {
            "mode": "sequential",
            "workers": 0,
            "repetition": index + 1,
            "seconds": duration,
        }
        for index, duration in enumerate(sequential_durations)
    ]

    summaries = []
    sequential_median = statistics.median(sequential_durations)
    summaries.append(summary_row("sequential", 0, sequential_durations, sequential_median))

    for workers in sorted(set(args.workers)):
        if workers < 1:
            continue
        durations, current_hash = measure(
            lambda workers=workers: run_parallel(paths, workers), args.repetitions
        )
        if current_hash != expected_hash:
            raise RuntimeError(
                f"Resultado paralelo com {workers} workers difere do sequencial."
            )
        rows.extend(
            {
                "mode": "parallel",
                "workers": workers,
                "repetition": index + 1,
                "seconds": duration,
            }
            for index, duration in enumerate(durations)
        )
        summaries.append(
            summary_row("parallel", workers, durations, sequential_median)
        )

    with (args.output / "raw.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    metadata = {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "total_memory_bytes": total_memory_bytes(),
        },
        "method": {
            "events_per_category": args.events_per_category,
            "categories": len(paths),
            "total_events": args.events_per_category * len(paths),
            "repetitions": args.repetitions,
            "warmup": 1,
        },
        "result_hash": expected_hash,
        "summary": summaries,
    }
    (args.output / "summary.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_markdown(args.output / "summary.md", metadata)
    print(json.dumps(metadata, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
