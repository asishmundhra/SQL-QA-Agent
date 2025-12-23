from typing import Callable, Iterable, List

import sqlglot
from sqlglot import expressions as exp

from .config import PolicyConfig
from .models import Query, QueryResult, StaticFinding


def _severity(config: PolicyConfig, rule_key: str) -> str:
    return getattr(config.severity, rule_key)


def rule_select_star(query: Query, config: PolicyConfig, expression: exp.Expression) -> List[StaticFinding]:
    findings: List[StaticFinding] = []
    if not config.static_rules.forbid_select_star:
        return findings
    if not isinstance(expression, exp.Select):
        return findings
    if any(isinstance(p, exp.Star) for p in expression.expressions):
        findings.append(
            StaticFinding(
                rule_id="select_star",
                severity=_severity(config, "select_star"),
                message="Query selects all columns with '*'.",
            )
        )
    return findings


def rule_update_delete_without_where(query: Query, config: PolicyConfig, expression: exp.Expression) -> List[StaticFinding]:
    findings: List[StaticFinding] = []
    if not config.static_rules.forbid_update_delete_without_where:
        return findings
    if isinstance(expression, (exp.Update, exp.Delete)) and expression.args.get("where") is None:
        findings.append(
            StaticFinding(
                rule_id="update_delete_without_where",
                severity=_severity(config, "update_delete_without_where"),
                message="UPDATE/DELETE statement without WHERE clause.",
            )
        )
    return findings


def rule_leading_wildcard_like(query: Query, config: PolicyConfig, expression: exp.Expression) -> List[StaticFinding]:
    findings: List[StaticFinding] = []
    if not config.static_rules.forbid_leading_wildcard_like:
        return findings
    for like in expression.find_all(exp.Like):
        pattern = like.args.get("expression") or like.args.get("pattern")
        if pattern is not None:
            pattern_sql = pattern.sql() if hasattr(pattern, "sql") else str(pattern)
            value = pattern_sql.strip().strip("'\"")
            if value.startswith("%"):
                findings.append(
                    StaticFinding(
                        rule_id="leading_wildcard_like",
                        severity=_severity(config, "leading_wildcard_like"),
                        message="LIKE pattern starts with a wildcard, which is non-sargable.",
                    )
                )
    return findings


def rule_non_sargable_predicate(query: Query, config: PolicyConfig, expression: exp.Expression) -> List[StaticFinding]:
    findings: List[StaticFinding] = []
    where_clause = expression.args.get("where")
    if not where_clause:
        return findings
    non_sargable_types = (exp.Lower, exp.Upper, exp.Date, exp.Substring)
    for node_type in non_sargable_types:
        for func in where_clause.find_all(node_type):
            if func.find(exp.Column):
                findings.append(
                    StaticFinding(
                        rule_id="non_sargable_predicate",
                        severity=_severity(config, "non_sargable_predicate"),
                        message="Non-sargable predicate using a function on a column.",
                    )
                )
                return findings
    return findings


def rule_long_in_list(query: Query, config: PolicyConfig, expression: exp.Expression) -> List[StaticFinding]:
    findings: List[StaticFinding] = []
    max_len = config.static_rules.max_in_list
    for in_expr in expression.find_all(exp.In):
        expressions = in_expr.args.get("expressions") or []
        if isinstance(expressions, list) and len(expressions) > max_len:
            findings.append(
                StaticFinding(
                    rule_id="long_in_list",
                    severity=_severity(config, "long_in_list"),
                    message=f"IN list has {len(expressions)} items (max {max_len}).",
                )
            )
    return findings


RULES: List[Callable[[Query, PolicyConfig, exp.Expression], List[StaticFinding]]] = [
    rule_select_star,
    rule_update_delete_without_where,
    rule_leading_wildcard_like,
    rule_non_sargable_predicate,
    rule_long_in_list,
]


def analyze_query(query: Query, config: PolicyConfig) -> List[StaticFinding]:
    try:
        expression = sqlglot.parse_one(query.sql, error_level="ignore")
    except Exception:
        expression = None
    if expression is None:
        return []
    findings: List[StaticFinding] = []
    for rule in RULES:
        findings.extend(rule(query, config, expression))
    return findings


def run_static_checks(queries: Iterable[Query], config: PolicyConfig) -> List[QueryResult]:
    results: List[QueryResult] = []
    for query in queries:
        findings = analyze_query(query, config)
        results.append(QueryResult(query=query, findings=findings))
    return results


__all__ = [
    "analyze_query",
    "run_static_checks",
    "rule_select_star",
    "rule_update_delete_without_where",
    "rule_leading_wildcard_like",
    "rule_non_sargable_predicate",
    "rule_long_in_list",
]
