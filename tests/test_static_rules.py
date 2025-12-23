from sqlqa.config import load_config
from sqlqa.models import Query
from sqlqa.static_rules import analyze_query


def test_rules_trigger_for_bad_sql():
    config = load_config("examples/sql-policy.yaml")
    query = Query(
        id="test:1",
        sql="SELECT * FROM users WHERE LOWER(email) LIKE '%foo' AND id IN (1,2,3,4,5,6)",
        raw_sql="SELECT * FROM users WHERE LOWER(email) LIKE '%foo' AND id IN (1,2,3,4,5,6)",
        source_path="tests/data.sql",
        lineno=1,
        kind="select",
    )
    findings = analyze_query(query, config)
    rule_ids = {f.rule_id for f in findings}
    assert "select_star" in rule_ids
    assert "leading_wildcard_like" in rule_ids
    assert "non_sargable_predicate" in rule_ids
    assert "long_in_list" in rule_ids
