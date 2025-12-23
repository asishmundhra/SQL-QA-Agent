from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from .config import PolicyConfig

QueryKind = Literal["select", "update", "delete", "insert", "other"]


class Query(BaseModel):
    id: str
    sql: str
    raw_sql: str
    source_path: str
    lineno: int
    kind: QueryKind = "other"


class StaticFinding(BaseModel):
    rule_id: str
    severity: str
    message: str


class DynamicMetrics(BaseModel):
    p95_ms: Optional[float] = None
    avg_ms: Optional[float] = None
    rows_examined: Optional[int] = None
    using_filesort: Optional[bool] = None
    using_temp_table: Optional[bool] = None
    chosen_key: Optional[str] = None
    plan_json: Optional[dict] = None
    plan_hash: Optional[str] = None


class Suggestion(BaseModel):
    suggestion_id: str
    type: Literal["query_rewrite", "index_ddl"]
    title: str
    description: str
    sql_before: Optional[str] = None
    sql_after: Optional[str] = None
    ddl: Optional[str] = None
    validated_improvement: Optional["DynamicMetrics"] = None


class BaselineEntry(BaseModel):
    p95_ms: Optional[float] = None
    rows_examined: Optional[int] = None
    chosen_key: Optional[str] = None
    plan_hash: Optional[str] = None


class QueryResult(BaseModel):
    query: Query
    findings: List[StaticFinding] = Field(default_factory=list)
    dynamic: Optional[DynamicMetrics] = None
    suggestions: List[Suggestion] = Field(default_factory=list)
    baseline_regression: Optional[str] = None


class SQLState(BaseModel):
    config: Optional[PolicyConfig] = None
    queries: List[Query] = Field(default_factory=list)
    results: List[QueryResult] = Field(default_factory=list)
    decision: Optional[str] = None
    report_md: Optional[str] = None
    baseline: Dict[str, BaselineEntry] = Field(default_factory=dict)


__all__ = [
    "Query",
    "StaticFinding",
    "QueryResult",
    "SQLState",
    "QueryKind",
    "DynamicMetrics",
    "Suggestion",
    "BaselineEntry",
]
