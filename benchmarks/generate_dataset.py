"""Gera histórico determinístico, separado pelas seis categorias."""

from __future__ import annotations

import argparse
import json
import random
from datetime import date, timedelta
from pathlib import Path

from biblioteca_distribuida.config import CATEGORIES


def generate(output: Path, events_per_category: int, seed: int) -> list[Path]:
    output.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    base = date(2020, 1, 1)
    for category_index, category in enumerate(CATEGORIES):
        randomizer = random.Random(seed + category_index)
        path = output / f"{category}.ndjson"
        paths.append(path)
        with path.open("w", encoding="utf-8", newline="\n") as stream:
            for index in range(events_per_category):
                borrowed = base + timedelta(days=randomizer.randrange(0, 2_000))
                duration = randomizer.randrange(1, 46)
                event = {
                    "event_id": f"{category_index}-{index}",
                    "client_id": f"client-{randomizer.randrange(0, 50_000):05d}",
                    "book_id": f"{category}-book-{randomizer.randrange(0, 500):03d}",
                    "borrowed_on": borrowed.isoformat(),
                    "returned_on": (borrowed + timedelta(days=duration)).isoformat(),
                }
                stream.write(json.dumps(event, separators=(",", ":")) + "\n")
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("benchmark-data"))
    parser.add_argument("--events-per-category", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=20261125)
    args = parser.parse_args()
    paths = generate(args.output, args.events_per_category, args.seed)
    print(f"Gerados {len(paths)} arquivos em {args.output}")


if __name__ == "__main__":
    main()
