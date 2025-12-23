import json
import time
import hashlib
from typing import Any, Dict, Optional

import mysql.connector  # type: ignore

from .models import DynamicMetrics, Query


def _num(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _search_keys(obj: Any, keys: set[str]) -> Optional[Any]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys:
                return v
            found = _search_keys(v, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _search_keys(item, keys)
            if found is not None:
                return found
    return None


def _compute_plan_hash(plan: Dict[str, Any]) -> str:
    try:
        dumped = json.dumps(plan, sort_keys=True)
    except Exception:
        dumped = str(plan)
    return hashlib.sha1(dumped.encode("utf-8")).hexdigest()


def parse_explain_plan(plan: Dict[str, Any]) -> DynamicMetrics:
    metrics = DynamicMetrics()
    if not plan:
        return metrics
    exec_time = plan.get("execution_time_ms") or plan.get("execution_time")
    if isinstance(exec_time, dict):
        metrics.p95_ms = _num(exec_time.get("p95"))
        metrics.avg_ms = _num(exec_time.get("avg"))
    if metrics.p95_ms is None:
        p95_candidate = _search_keys(plan, {"p95_ms", "p95"})
        metrics.p95_ms = _num(p95_candidate)
    if metrics.avg_ms is None:
        avg_candidate = _search_keys(plan, {"avg_ms", "avg"})
        metrics.avg_ms = _num(avg_candidate)

    rows_candidate = _search_keys(plan, {"rows_examined_per_scan", "rows_examined", "rows"})
    metrics.rows_examined = int(rows_candidate) if rows_candidate is not None else None
    filesort_candidate = _search_keys(plan, {"used_filesort", "using_filesort"})
    if filesort_candidate is not None:
        metrics.using_filesort = bool(filesort_candidate)
    temp_candidate = _search_keys(plan, {"used_temp_table", "using_temporary"})
    if temp_candidate is not None:
        metrics.using_temp_table = bool(temp_candidate)
    key_candidate = _search_keys(plan, {"key", "possible_keys", "chosen_key"})
    if isinstance(key_candidate, str):
        metrics.chosen_key = key_candidate

    metrics.plan_json = plan
    metrics.plan_hash = _compute_plan_hash(plan)
    return metrics


def run_explain_analyze(query: Query, conn) -> DynamicMetrics:
    """Run EXPLAIN FORMAT=JSON and parse dynamic metrics."""
    sql = f"EXPLAIN FORMAT=JSON {query.sql}"
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    plan_raw = row[0] if row else None
    if isinstance(plan_raw, (bytes, bytearray)):
        plan_raw = plan_raw.decode("utf-8")
    if isinstance(plan_raw, str):
        try:
            plan = json.loads(plan_raw)
        except json.JSONDecodeError:
            plan = {}
    elif isinstance(plan_raw, dict):
        plan = plan_raw
    else:
        plan = {}
    return parse_explain_plan(plan)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    k = int(round((len(sorted_vals) - 1) * pct))
    return sorted_vals[k]


def run_latency_probe(query: Query, conn, runs: int = 5) -> DynamicMetrics:
    """Execute the query multiple times and measure latency."""
    durations: list[float] = []
    for _ in range(runs):
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(query.raw_sql or query.sql)
        try:
            cur.fetchall()
        except Exception:
            pass
        if query.kind in {"update", "delete", "insert"}:
            try:
                conn.rollback()
            except Exception:
                pass
        else:
            try:
                conn.commit()
            except Exception:
                pass
        cur.close()
        duration_ms = (time.perf_counter() - start) * 1000
        durations.append(duration_ms)
    avg = sum(durations) / len(durations) if durations else 0.0
    p95 = _percentile(durations, 0.95) if durations else 0.0
    return DynamicMetrics(p95_ms=p95, avg_ms=avg)


__all__ = [
    "run_explain_analyze",
    "run_latency_probe",
    "parse_explain_plan",
]
