from typing import Dict, List, Optional, TypedDict

from urllib.parse import urlparse
import mysql.connector  # type: ignore

from langgraph.graph import END, StateGraph

from .config import PolicyConfig, load_config
from .extractor import extract_queries
from .models import BaselineEntry, DynamicMetrics, Query, QueryResult, SQLState
from .reporter import build_markdown_report, compute_decision
from .dynamic_runner import run_explain_analyze, run_latency_probe
from .optimizer import optimize_query
from .baseline import (
    load_baseline,
    save_baseline,
    fingerprint_query,
    build_baseline_entry,
    compare_to_baseline,
)
from .static_rules import run_static_checks


class GraphState(TypedDict, total=False):
    config: PolicyConfig
    queries: List[Query]
    results: List[QueryResult]
    decision: str
    report_md: str
    baseline: Dict[str, BaselineEntry]


def _connect_mysql(dsn: str):
    parsed = urlparse(dsn)
    return mysql.connector.connect(
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port or 3306,
        database=parsed.path.lstrip("/") or None,
    )


def build_graph(
    policy_path: str,
    repo_path: str = ".",
    dsn: Optional[str] = None,
    baseline_path: Optional[str] = None,
    save_baseline_flag: bool = False,
):
    """Create and compile the LangGraph for the SQLQA workflow."""

    def load_config_node(state: GraphState) -> GraphState:
        config = load_config(policy_path)
        return {"config": config}

    def extract_node(state: GraphState) -> GraphState:
        config = state["config"]
        queries = extract_queries(repo_path, config)
        return {"queries": queries}

    def static_checks_node(state: GraphState) -> GraphState:
        config = state["config"]
        queries = state.get("queries", [])
        results = run_static_checks(queries, config)
        return {"results": results}

    def dynamic_checks_node(state: GraphState) -> GraphState:
        if not dsn:
            return {}
        conn = _connect_mysql(dsn)
        results: List[QueryResult] = []
        for res in state.get("results", []):
            try:
                plan_metrics = run_explain_analyze(res.query, conn)
            except Exception:
                plan_metrics = DynamicMetrics()
            try:
                latency_metrics = run_latency_probe(res.query, conn, runs=3)
            except Exception:
                latency_metrics = DynamicMetrics()
            merged = DynamicMetrics(
                p95_ms=latency_metrics.p95_ms or plan_metrics.p95_ms,
                avg_ms=latency_metrics.avg_ms or plan_metrics.avg_ms,
                rows_examined=plan_metrics.rows_examined,
                using_filesort=plan_metrics.using_filesort,
                using_temp_table=plan_metrics.using_temp_table,
                chosen_key=plan_metrics.chosen_key,
                plan_json=plan_metrics.plan_json,
                plan_hash=plan_metrics.plan_hash,
            )
            res.dynamic = merged
            results.append(res)
        conn.close()
        return {"results": results}

    def optimizer_node(state: GraphState) -> GraphState:
        conn = _connect_mysql(dsn) if dsn else None
        results: List[QueryResult] = []
        for res in state.get("results", []):
            try:
                res.suggestions = optimize_query(res.query, res.dynamic, conn=conn)
            except Exception:
                res.suggestions = []
            results.append(res)
        if conn:
            conn.close()
        return {"results": results}

    def baseline_node(state: GraphState) -> GraphState:
        if not baseline_path:
            return {}
        baseline_data = load_baseline(baseline_path)
        for res in state.get("results", []):
            if res.dynamic:
                fp = fingerprint_query(res.query)
                existing = baseline_data.get(fp)
                if existing:
                    reg, reason = compare_to_baseline(res.dynamic, existing)
                    if reg:
                        res.baseline_regression = reason
                if save_baseline_flag:
                    baseline_data[fp] = build_baseline_entry(res.dynamic)
        if save_baseline_flag:
            save_baseline(baseline_path, baseline_data)
        return {"baseline": baseline_data}

    def decision_and_report_node(state: GraphState) -> GraphState:
        results = state.get("results", [])
        decision = compute_decision(results)
        sql_state = SQLState(
            config=state.get("config"),
            queries=state.get("queries", []),
            results=results,
            decision=decision,
            baseline=state.get("baseline", {}),
        )
        report = build_markdown_report(sql_state)
        return {"decision": decision, "report_md": report}

    graph = StateGraph(GraphState)
    graph.add_node("load_config", load_config_node)
    graph.add_node("extract", extract_node)
    graph.add_node("static_checks", static_checks_node)
    graph.add_node("dynamic_checks", dynamic_checks_node)
    graph.add_node("optimizer", optimizer_node)
    graph.add_node("baseline", baseline_node)
    graph.add_node("decision_report", decision_and_report_node)

    graph.set_entry_point("load_config")
    graph.add_edge("load_config", "extract")
    graph.add_edge("extract", "static_checks")
    graph.add_edge("static_checks", "dynamic_checks")
    graph.add_edge("dynamic_checks", "optimizer")
    graph.add_edge("optimizer", "baseline")
    graph.add_edge("baseline", "decision_report")
    graph.add_edge("decision_report", END)

    return graph.compile()


def run_sqlqa(
    policy_path: str,
    repo_path: str = ".",
    dsn: Optional[str] = None,
    baseline_path: Optional[str] = None,
    save_baseline_flag: bool = False,
) -> SQLState:
    """Execute the graph and return final SQLState."""
    app = build_graph(policy_path, repo_path, dsn=dsn, baseline_path=baseline_path, save_baseline_flag=save_baseline_flag)
    result_state = app.invoke({})
    return SQLState(
        config=result_state.get("config"),
        queries=result_state.get("queries", []),
        results=result_state.get("results", []),
        decision=result_state.get("decision"),
        report_md=result_state.get("report_md"),
        baseline=result_state.get("baseline", {}),
    )


__all__ = ["build_graph", "run_sqlqa", "GraphState"]
