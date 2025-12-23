import itertools
from typing import List, Optional

import sqlglot
from sqlglot import expressions as exp

from .dynamic_runner import run_explain_analyze
from .models import DynamicMetrics, Query, Suggestion


def _table_name(expression: exp.Expression) -> Optional[str]:
    table = next(expression.find_all(exp.Table), None)
    if table and table.name:
        return table.name
    return None


def _where_columns(expression: exp.Expression) -> List[str]:
    cols: List[str] = []
    where = expression.args.get("where")
    if not where:
        return cols
    for col in where.find_all(exp.Column):
        if col.name and col.name not in cols:
            cols.append(col.name)
    return cols


def _has_offset_limit(expression: exp.Expression) -> bool:
    limit = expression.args.get("limit")
    if isinstance(limit, exp.Limit):
        if limit.args.get("offset") is not None:
            return True
    for node in expression.find_all(exp.Limit):
        if node.args.get("offset") is not None:
            return True
    return False


def _or_chain_to_in(expression: exp.Expression) -> Optional[tuple[str, List[str]]]:
    ors = list(expression.find_all(exp.Or))
    eqs = []
    for or_expr in ors:
        for comp in or_expr.find_all(exp.EQ):
            if isinstance(comp.left, exp.Column) and isinstance(comp.right, exp.Literal):
                eqs.append((comp.left.sql(), comp.right.this))
    if not eqs:
        return None
    first_col = eqs[0][0]
    values = [v for col, v in eqs if col == first_col]
    if len(values) >= 3:
        return first_col, values
    return None


def _leading_wildcard_like(expression: exp.Expression) -> bool:
    for like in expression.find_all(exp.Like):
        pattern = like.args.get("expression") or like.args.get("pattern")
        if pattern and pattern.sql().strip("'\"").startswith("%"):
            return True
    return False


def optimize_query(query: Query, dynamic: Optional[DynamicMetrics] = None, conn=None) -> List[Suggestion]:
    suggestions: List[Suggestion] = []
    try:
        expression = sqlglot.parse_one(query.sql, error_level="ignore")
    except Exception:
        return suggestions

    table = _table_name(expression)
    cols = _where_columns(expression)
    slow = False
    if dynamic:
        slow = (dynamic.p95_ms or 0) > 500 or (dynamic.rows_examined or 0) > 10000 or bool(dynamic.using_filesort) or bool(dynamic.using_temp_table)

    # Index suggestion
    if table and cols and slow:
        ddl = f"CREATE INDEX idx_{table}_composite ON {table} ({', '.join(cols)});"
        suggestions.append(
            Suggestion(
                suggestion_id=f"{query.id}-idx",
                type="index_ddl",
                title="Add composite index for WHERE/JOIN columns",
                description="Align index with filtered columns to avoid full scans and temp tables.",
                sql_before=query.sql,
                ddl=ddl,
            )
        )

    # Non-sargable date function
    for func in expression.find_all(exp.Date):
        if isinstance(func.this, exp.Column):
            col_sql = func.this.sql()
            sql_after = query.sql + " -- Suggest range filter instead of DATE(column)"
            suggestions.append(
                Suggestion(
                    suggestion_id=f"{query.id}-date",
                    type="query_rewrite",
                    title="Rewrite DATE(column) predicate",
                    description=f"Use a range filter on {col_sql} to stay sargable.",
                    sql_before=query.sql,
                    sql_after=sql_after,
                )
            )
            break

    # OFFSET pagination
    has_offset = _has_offset_limit(expression) or " offset " in query.sql.lower()
    if has_offset:
        suggestions.append(
            Suggestion(
                suggestion_id=f"{query.id}-offset",
                type="query_rewrite",
                title="Replace OFFSET pagination with keyset",
                description="Use keyset pagination to avoid scanning skipped rows.",
                sql_before=query.sql,
                sql_after=query.sql + " -- Apply keyset pagination using last seen id",
            )
        )

    # OR chain to IN
    or_in = _or_chain_to_in(expression)
    if or_in:
        col, values = or_in
        sql_after = query.sql + f" -- Consider {col} IN ({', '.join(map(str, values))})"
        suggestions.append(
            Suggestion(
                suggestion_id=f"{query.id}-in",
                type="query_rewrite",
                title="Convert OR chain to IN",
                description=f"Use IN list on {col} to simplify predicates and aid indexing.",
                sql_before=query.sql,
                sql_after=sql_after,
            )
        )

    # Leading wildcard LIKE -> FULLTEXT
    if _leading_wildcard_like(expression):
        suggestions.append(
            Suggestion(
                suggestion_id=f"{query.id}-fulltext",
                type="query_rewrite",
                title="Replace leading wildcard LIKE",
                description="Consider FULLTEXT index or inverted index for prefix/suffix searches.",
                sql_before=query.sql,
                sql_after=query.sql + " -- Consider FULLTEXT index",
            )
        )

    # Validate improvements with EXPLAIN if possible
    if conn:
        for s in suggestions:
            if s.sql_after:
                temp_query = Query(
                    id=s.suggestion_id,
                    sql=s.sql_after,
                    raw_sql=s.sql_after,
                    source_path=query.source_path,
                    lineno=query.lineno,
                    kind=query.kind,
                )
                try:
                    s.validated_improvement = run_explain_analyze(temp_query, conn)
                except Exception:
                    s.validated_improvement = None

    return suggestions


__all__ = ["optimize_query"]
