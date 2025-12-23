from sqlqa.models import DynamicMetrics, Query
from sqlqa.optimizer import optimize_query


def test_optimizer_generates_suggestions():
    sql = "SELECT * FROM users WHERE id = 1 OR id = 2 OR id = 3 ORDER BY created_at LIMIT 10 OFFSET 20"
    query = Query(id="q1", sql=sql, raw_sql=sql, source_path="x.sql", lineno=1, kind="select")
    dynamic = DynamicMetrics(p95_ms=800, rows_examined=20000, using_filesort=True, using_temp_table=True)
    suggestions = optimize_query(query, dynamic=dynamic, conn=None)
    kinds = {s.type for s in suggestions}
    ids = {s.suggestion_id for s in suggestions}
    assert "index_ddl" in kinds
    assert any("offset" in s.suggestion_id for s in suggestions)
    assert any("in" in s.suggestion_id for s in suggestions)
