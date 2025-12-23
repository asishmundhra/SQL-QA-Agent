import ast
import fnmatch
import re
from pathlib import Path, PurePosixPath
from typing import Iterable, List

import sqlglot

from .config import PolicyConfig
from .models import Query, QueryKind

SQL_KEYWORDS = ("select", "update", "delete", "insert", "with")


def _should_include(path: Path, root: Path, include_patterns: Iterable[str], exclude_patterns: Iterable[str]) -> bool:
    """Check include/exclude globs in a cross-platform friendly way."""
    rel_posix_str = path.relative_to(root).as_posix()
    rel_native_str = str(path.relative_to(root))
    rel_posix = PurePosixPath(rel_posix_str)

    def _matches(patterns: Iterable[str]) -> bool:
        for pat in patterns:
            # try raw pattern
            if rel_posix.match(pat):
                return True
            if fnmatch.fnmatch(rel_posix_str, pat):
                return True
            if fnmatch.fnmatch(rel_native_str, pat):
                return True
            # allow patterns like "**/*.sql" to match top-level files
            if pat.startswith("**/"):
                trimmed = pat[3:]
                if fnmatch.fnmatch(rel_posix_str, trimmed):
                    return True
                if fnmatch.fnmatch(rel_native_str, trimmed):
                    return True
        return False

    if _matches(exclude_patterns):
        return False
    return _matches(include_patterns)


def _normalize_sql(sql: str) -> str:
    try:
        expression = sqlglot.parse_one(sql, error_level="ignore")
        if expression is None:
            return sql.strip()
        return expression.sql(pretty=False)
    except Exception:
        return sql.strip()


def _determine_kind(sql: str) -> QueryKind:
    lowered = sql.lstrip().lower()
    for kind in ("select", "update", "delete", "insert"):
        if lowered.startswith(kind):
            return kind  # type: ignore[return-value]
    return "other"


def _extract_from_sql_file(path: Path, start_index: int, root: Path) -> List[Query]:
    queries: List[Query] = []
    content = path.read_text()
    parts = [part.strip() for part in content.split(";") if part.strip()]
    for idx, raw in enumerate(parts):
        normalized = _normalize_sql(raw)
        qid = f"{path.relative_to(root)}:{idx + 1}"
        queries.append(
            Query(
                id=qid,
                sql=normalized,
                raw_sql=raw,
                source_path=str(path.relative_to(root)),
                lineno=1,
                kind=_determine_kind(raw),
            )
        )
    return queries


class _ExecuteStringVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.strings: List[tuple[str, int]] = []

    def visit_Call(self, node: ast.Call) -> None:
        func_name = ""
        if isinstance(node.func, ast.Attribute):
            func_name = node.func.attr.lower()
        elif isinstance(node.func, ast.Name):
            func_name = node.func.id.lower()
        if func_name in {"execute", "executemany", "text"} and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                self.strings.append((arg.value, getattr(arg, "lineno", node.lineno)))
        self.generic_visit(node)


def _looks_like_sql(text: str) -> bool:
    lowered = text.lower()
    return any(kw in lowered for kw in SQL_KEYWORDS)


def _extract_from_python_file(path: Path, start_index: int, root: Path) -> List[Query]:
    queries: List[Query] = []
    source = path.read_text()
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return queries

    visitor = _ExecuteStringVisitor()
    visitor.visit(tree)
    for idx, (raw, lineno) in enumerate(visitor.strings):
        if not _looks_like_sql(raw):
            continue
        normalized = _normalize_sql(raw)
        qid = f"{path.relative_to(root)}:{start_index + idx + 1}"
        queries.append(
            Query(
                id=qid,
                sql=normalized,
                raw_sql=raw,
                source_path=str(path.relative_to(root)),
                lineno=lineno,
                kind=_determine_kind(raw),
            )
        )
    # Simple heuristic for inline SQL strings not in execute() but present in code
    string_literals = re.findall(r'("""|\'\'\'|\'|")(.*?)(\1)', source, flags=re.DOTALL)
    for match in string_literals:
        raw = match[1]
        if not _looks_like_sql(raw):
            continue
        normalized = _normalize_sql(raw)
        qid = f"{path.relative_to(root)}:{len(queries) + 1}"
        queries.append(
            Query(
                id=qid,
                sql=normalized,
                raw_sql=raw,
                source_path=str(path.relative_to(root)),
                lineno=1,
                kind=_determine_kind(raw),
            )
        )
    return queries


def extract_queries(root_path: str, config: PolicyConfig) -> List[Query]:
    root = Path(root_path)
    include_patterns = config.targets.include_paths
    exclude_patterns = config.targets.exclude_paths
    queries: List[Query] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if not _should_include(path, root, include_patterns, exclude_patterns):
            continue
        if path.suffix == ".sql":
            queries.extend(_extract_from_sql_file(path, len(queries), root))
        elif path.suffix == ".py":
            queries.extend(_extract_from_python_file(path, len(queries), root))
    return queries


__all__ = ["extract_queries"]
