from typing import List

from .models import QueryResult, SQLState


def compute_decision(results: List[QueryResult]) -> str:
    has_error = any(f.severity == "error" for r in results for f in r.findings)
    has_regression = any(r.baseline_regression for r in results)
    return "fail" if has_error or has_regression else "pass"


def _truncate(text: str, limit: int = 200) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def build_markdown_report(state: SQLState) -> str:
    total_queries = len(state.results)
    errors = sum(1 for r in state.results for f in r.findings if f.severity == "error")
    warnings = sum(1 for r in state.results for f in r.findings if f.severity == "warning")
    regressions = sum(1 for r in state.results if r.baseline_regression)

    lines: List[str] = []
    lines.append("# SQLQA Report")
    lines.append("")
    lines.append(f"- Queries scanned: {total_queries}")
    lines.append(f"- Errors: {errors}")
    lines.append(f"- Warnings: {warnings}")
    if regressions:
        lines.append(f"- Baseline regressions: {regressions}")
    lines.append("")

    for result in state.results:
        lines.append(f"## Query `{result.query.id}`")
        lines.append(f"- Source: `{result.query.source_path}` (line {result.query.lineno})")
        lines.append(f"- Kind: {result.query.kind}")
        raw = _truncate(result.query.raw_sql.replace("\n", " "))
        lines.append(f"- Raw SQL: `{raw}`")
        if result.dynamic:
            lines.append("- Dynamic metrics:")
            lines.append(
                f"  - p95_ms: {result.dynamic.p95_ms or 'n/a'}, avg_ms: {result.dynamic.avg_ms or 'n/a'}, rows_examined: {result.dynamic.rows_examined or 'n/a'}"
            )
            lines.append(
                f"  - filesort: {result.dynamic.using_filesort}, temp_table: {result.dynamic.using_temp_table}, key: {result.dynamic.chosen_key or 'n/a'}"
            )
        if result.baseline_regression:
            lines.append(f"- Baseline: regression detected -> {result.baseline_regression}")
        if result.findings:
            lines.append("- Findings:")
            for finding in result.findings:
                lines.append(f"  - [{finding.severity}] {finding.rule_id}: {finding.message}")
        else:
            lines.append("- Findings: none")
        if result.suggestions:
            lines.append("- Suggestions:")
            for s in result.suggestions:
                lines.append(f"  - ({s.type}) {s.title}: {s.description}")
                if s.ddl:
                    lines.append(f"    DDL: `{s.ddl}`")
                if s.sql_after:
                    lines.append(f"    Rewrite: `{_truncate(s.sql_after)}`")
                if s.validated_improvement and s.validated_improvement.p95_ms is not None:
                    lines.append(
                        f"    Validated p95_ms: {s.validated_improvement.p95_ms} rows_examined: {s.validated_improvement.rows_examined}"
                    )
        lines.append("")

    return "\n".join(lines)


__all__ = ["build_markdown_report", "compute_decision"]
