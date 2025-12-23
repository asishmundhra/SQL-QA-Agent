import json
import time

from sqlqa.dynamic_runner import run_explain_analyze, run_latency_probe, parse_explain_plan
from sqlqa.models import Query


class _FakeCursor:
    def __init__(self, plan_json):
        self.plan_json = plan_json
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return [json.dumps(self.plan_json)]

    def fetchall(self):
        return []

    def close(self):
        pass


class _FakeConn:
    def __init__(self, plan_json):
        self.plan_json = plan_json
        self.closed = False

    def cursor(self):
        return _FakeCursor(self.plan_json)

    def rollback(self):
        pass

    def commit(self):
        pass

    def close(self):
        self.closed = True


def test_run_explain_analyze_parses_plan():
    plan = {
        "execution_time_ms": {"p95": 15, "avg": 10},
        "query_block": {"table": {"rows_examined_per_scan": 100, "key": "idx_users_email"}},
        "used_temp_table": True,
        "used_filesort": True,
    }
    conn = _FakeConn(plan)
    query = Query(id="q1", sql="SELECT * FROM users", raw_sql="SELECT * FROM users", source_path="x.sql", lineno=1, kind="select")
    metrics = run_explain_analyze(query, conn)
    assert metrics.p95_ms == 15
    assert metrics.rows_examined == 100
    assert metrics.using_temp_table is True
    assert metrics.using_filesort is True
    assert metrics.chosen_key == "idx_users_email"


def test_run_latency_probe_returns_metrics(monkeypatch):
    # Make perf_counter deterministic
    times = iter([0, 0.01, 0.02, 0.03, 0.04, 0.05])

    def fake_perf_counter():
        return next(times)

    monkeypatch.setattr(time, "perf_counter", fake_perf_counter)
    conn = _FakeConn({})
    query = Query(id="q2", sql="SELECT 1", raw_sql="SELECT 1", source_path="x.sql", lineno=1, kind="select")
    metrics = run_latency_probe(query, conn, runs=3)
    assert metrics.avg_ms >= 0
    assert metrics.p95_ms >= 0
