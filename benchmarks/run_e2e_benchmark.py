"""Benchmark ponta a ponta variando a quantidade de workers do Compose."""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import platform
import statistics
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID, uuid4

import psycopg

from biblioteca_distribuida.client import LibraryClient
from biblioteca_distribuida.config import CATEGORIES, Settings
from biblioteca_distribuida.messaging import command_queue, connect_with_retry


NONEXISTENT_LOAN = str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))


def percentile(values: list[float], percentage: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentage
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def compose(*arguments: str) -> None:
    subprocess.run(["docker", "compose", *arguments], check=True)


def wait_for_consumers(settings: Settings, expected: int, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        connection = connect_with_retry(settings.rabbitmq_url, attempts=1)
        try:
            channel = connection.channel()
            counts = [
                channel.queue_declare(
                    queue=command_queue(category), passive=True
                ).method.consumer_count
                for category in CATEGORIES
            ]
            if min(counts) >= expected:
                return
        finally:
            connection.close()
        time.sleep(0.5)
    raise TimeoutError(f"Workers não ficaram prontos: esperado={expected}.")


class TimedClient(LibraryClient):
    def __init__(self, client_id: str, settings: Settings):
        self.arrivals: dict[str, float] = {}
        super().__init__(client_id, settings)

    def _on_response(self, channel, method, properties, body: bytes) -> None:
        event = json.loads(body)
        self.arrivals[event["request_id"]] = time.perf_counter()
        super()._on_response(channel, method, properties, body)


def producer_run(
    producer_index: int,
    run_id: str,
    command_count: int,
    barrier: threading.Barrier,
    settings: Settings,
) -> list[float]:
    client = TimedClient(f"benchmark-e2e-{producer_index}", settings)
    sent: dict[str, float] = {}
    try:
        client.channel.queue_purge(client.queue)
        barrier.wait(timeout=60)
        for index in range(command_count):
            request_id = str(uuid4())
            sent[request_id] = time.perf_counter()
            client.return_loan(
                NONEXISTENT_LOAN,
                CATEGORIES[(producer_index + index) % len(CATEGORIES)],
                request_id=request_id,
            )
            client.connection.process_data_events(time_limit=0)

        deadline = time.monotonic() + 120
        while len(client.arrivals) < command_count and time.monotonic() < deadline:
            client.connection.process_data_events(time_limit=0.05)
        missing = set(sent) - set(client.arrivals)
        if missing:
            raise TimeoutError(f"{len(missing)} respostas não chegaram no benchmark.")
        return [client.arrivals[key] - started for key, started in sent.items()]
    finally:
        client.close()


def queue_depth(management_url: str, username: str, password: str) -> int:
    request = urllib.request.Request(f"{management_url.rstrip('/')}/api/queues/%2F")
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=2) as response:
        queues = json.load(response)
    names = {command_queue(category) for category in CATEGORIES}
    return sum(item.get("messages", 0) for item in queues if item["name"] in names)


def reset_requests(database_url: str) -> None:
    with psycopg.connect(database_url) as connection:
        connection.execute("TRUNCATE processed_requests")


def run_once(
    total_commands: int,
    producers: int,
    settings: Settings,
    database_url: str,
    management_url: str,
) -> dict[str, float]:
    reset_requests(database_url)
    counts = [total_commands // producers] * producers
    for index in range(total_commands % producers):
        counts[index] += 1
    barrier = threading.Barrier(producers + 1)
    run_id = uuid4().hex[:8]
    max_depth = 0

    with ThreadPoolExecutor(max_workers=producers) as executor:
        futures = [
            executor.submit(producer_run, index, run_id, count, barrier, settings)
            for index, count in enumerate(counts)
        ]
        deadline = time.monotonic() + 60
        while barrier.n_waiting < producers and time.monotonic() < deadline:
            time.sleep(0.05)
        started = time.perf_counter()
        barrier.wait(timeout=60)
        while not all(future.done() for future in futures):
            try:
                max_depth = max(
                    max_depth,
                    queue_depth(management_url, "biblioteca", "biblioteca"),
                )
            except Exception:
                pass
            time.sleep(0.05)
        latencies = [latency for future in futures for latency in future.result()]
    elapsed = time.perf_counter() - started
    return {
        "seconds": elapsed,
        "throughput_commands_s": total_commands / elapsed,
        "latency_median_ms": statistics.median(latencies) * 1000,
        "latency_p95_ms": percentile(latencies, 0.95) * 1000,
        "max_queue_depth": max_depth,
    }


def command_output(*arguments: str) -> str:
    return subprocess.run(
        list(arguments), check=True, capture_output=True, text=True
    ).stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("benchmark-e2e-results"))
    parser.add_argument("--commands", type=int, default=1200)
    parser.add_argument("--producers", type=int, default=8)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--workers", type=int, nargs="*", default=[1, 2, 4, 6])
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "E2E_DATABASE_URL",
            "postgresql://biblioteca:biblioteca@localhost:15432/biblioteca",
        ),
    )
    parser.add_argument(
        "--management-url",
        default=os.getenv("RABBITMQ_MANAGEMENT_URL", "http://localhost:15672"),
    )
    args = parser.parse_args()
    if args.commands < args.producers:
        parser.error("--commands deve ser maior ou igual a --producers")

    settings = Settings.from_env()
    args.output.mkdir(parents=True, exist_ok=True)
    rows = []
    summaries = []
    compose("stop", "worker-2")
    try:
        for worker_count in sorted(set(args.workers)):
            compose("up", "-d", "--scale", f"worker-1={worker_count}", "worker-1")
            wait_for_consumers(settings, worker_count)
            measurements = []
            for repetition in range(1, args.repetitions + 1):
                result = run_once(
                    args.commands,
                    args.producers,
                    settings,
                    args.database_url,
                    args.management_url,
                )
                row = {"workers": worker_count, "repetition": repetition, **result}
                rows.append(row)
                measurements.append(result)
                print(json.dumps(row, ensure_ascii=False))
            summaries.append(
                {
                    "workers": worker_count,
                    "throughput_median_commands_s": statistics.median(
                        item["throughput_commands_s"] for item in measurements
                    ),
                    "latency_median_ms": statistics.median(
                        item["latency_median_ms"] for item in measurements
                    ),
                    "latency_p95_ms": statistics.median(
                        item["latency_p95_ms"] for item in measurements
                    ),
                    "max_queue_depth": max(
                        item["max_queue_depth"] for item in measurements
                    ),
                }
            )
    finally:
        compose("up", "-d", "--scale", "worker-1=1", "worker-1", "worker-2")

    with (args.output / "raw.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    report = {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "logical_cpus": os.cpu_count(),
            "docker": command_output("docker", "version", "--format", "{{.Server.Version}}"),
            "docker_compose": command_output("docker", "compose", "version", "--short"),
        },
        "method": {
            "commands_per_repetition": args.commands,
            "parallel_producers": args.producers,
            "repetitions": args.repetitions,
            "workers": sorted(set(args.workers)),
            "command": "DEVOLVER empréstimo inexistente (caminho real com transação e resposta)",
        },
        "summary": summaries,
    }
    (args.output / "summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    lines = [
        "# Benchmark ponta a ponta",
        "",
        "| Workers | Vazão mediana (cmd/s) | Latência mediana (ms) | p95 (ms) | Fila máxima |",
        "|---:|---:|---:|---:|---:|",
    ]
    for item in summaries:
        lines.append(
            "| {workers} | {throughput_median_commands_s:.2f} | {latency_median_ms:.2f} | "
            "{latency_p95_ms:.2f} | {max_queue_depth} |".format(**item)
        )
    (args.output / "summary.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
