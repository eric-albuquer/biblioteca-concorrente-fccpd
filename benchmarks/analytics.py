"""Relatório real de demanda e atraso sobre históricos de empréstimo."""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any


def analyze_partition(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    loans = 0
    overdue_loans = 0
    overdue_days = 0
    users: set[str] = set()
    title_counts: Counter[str] = Counter()

    with source.open("r", encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            borrowed = date.fromisoformat(event["borrowed_on"])
            returned = date.fromisoformat(event["returned_on"])
            duration = (returned - borrowed).days
            delay = max(0, duration - 14)
            loans += 1
            overdue_loans += int(delay > 0)
            overdue_days += delay
            users.add(event["client_id"])
            title_counts[event["book_id"]] += 1

    return {
        "category": source.stem,
        "loans": loans,
        "overdue_loans": overdue_loans,
        "overdue_days": overdue_days,
        "unique_users": len(users),
        "top_books": title_counts.most_common(10),
    }


def normalize_report(parts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(parts, key=lambda item: item["category"])
