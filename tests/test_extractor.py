from pathlib import Path

from sqlqa.config import load_config
from sqlqa.extractor import extract_queries


def test_extracts_sql_from_sample_repo():
    root = Path(__file__).parent.parent / "examples" / "sample_repo"
    config = load_config(Path(__file__).parent.parent / "examples" / "sql-policy.yaml")
    queries = extract_queries(str(root), config)
    assert queries, "Expected queries to be extracted"
    kinds = {q.kind for q in queries}
    assert "select" in kinds
    assert "delete" in kinds or "update" in kinds
    assert any("users" in q.raw_sql for q in queries)
