import hashlib
import json
from pathlib import Path
from typing import Dict, Tuple

from .models import BaselineEntry, DynamicMetrics, Query


def fingerprint_query(query: Query) -> str:
    return hashlib.sha1(query.sql.encode("utf-8")).hexdigest()


def build_baseline_entry(metrics: DynamicMetrics) -> BaselineEntry:
    return BaselineEntry(
        p95_ms=metrics.p95_ms,
        rows_examined=metrics.rows_examined,
        chosen_key=metrics.chosen_key,
        plan_hash=metrics.plan_hash,
    )


def load_baseline(path: str) -> Dict[str, BaselineEntry]:
    p = Path(path)
    if not p.exists():
        return {}
    raw = json.loads(p.read_text())
    return {k: BaselineEntry(**v) for k, v in raw.items()}


def save_baseline(path: str, data: Dict[str, BaselineEntry]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    serializable = {k: v.model_dump() for k, v in data.items()}
    p.write_text(json.dumps(serializable, indent=2))


def compare_to_baseline(dynamic: DynamicMetrics, baseline: BaselineEntry, threshold: float = 0.2) -> Tuple[bool, str]:
    """Return (is_regression, reason)."""
    if dynamic.p95_ms is not None and baseline.p95_ms is not None:
        if dynamic.p95_ms > baseline.p95_ms * (1 + threshold):
            return True, f"p95_ms regression: {dynamic.p95_ms:.2f}ms > baseline {baseline.p95_ms:.2f}ms"
    if dynamic.rows_examined is not None and baseline.rows_examined is not None:
        if dynamic.rows_examined > baseline.rows_examined * (1 + threshold):
            return True, f"rows examined regression: {dynamic.rows_examined} > baseline {baseline.rows_examined}"
    return False, ""


__all__ = [
    "fingerprint_query",
    "build_baseline_entry",
    "load_baseline",
    "save_baseline",
    "compare_to_baseline",
]
