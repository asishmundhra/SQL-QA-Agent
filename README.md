# SQLQA

Framework-agnostic SQL quality checker orchestrated with LangGraph.

## Features
- Scans repositories for SQL in `.sql` files and Python `execute(...)` calls.
- Static rules: no `SELECT *`, `UPDATE/DELETE` must have `WHERE`, leading wildcard `LIKE`, non-sargable predicates, long `IN` lists.
- Optional dynamic checks (MySQL): EXPLAIN FORMAT=JSON + small latency probe.
- Optimizer suggestions: indexes and rewrites for slow/non-sargable queries.
- Baseline regression detection: compare current metrics to saved baseline.
- CLI via `sqlqa check --policy-path sql-policy.yaml --out report.md`.

## Architecture (flow)
![Architecture](archdiagram.jpg)
(https://drive.google.com/file/d/1_8TjLSWT0ZejePMVMRqOe_HopRLoZWI9/view?usp=sharing)

## Quickstart
1. Ensure Python 3.10+ is installed.
2. Create and activate a virtual env:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
3. Install the package (editable for development):
   ```bash
   pip install -e ".[dev]"
   ```
4. Run static-only sample:
   ```bash
   sqlqa check --policy-path examples/sql-policy.yaml --out report.md --repo-path examples/sample_repo
   cat report.md
   ```

### Dynamic checks (MySQL)
- Provide a MySQL DSN (e.g., `mysql://user:pass@localhost:3306/mydb`):
  ```bash
  sqlqa check --policy-path examples/sql-policy.yaml --out report.md --repo-path . --dsn "mysql://user:pass@localhost:3306/mydb"
  ```
- Save a baseline for regression detection:
  ```bash
  sqlqa baseline save --dsn "mysql://user:pass@localhost:3306/mydb" --policy-path examples/sql-policy.yaml --out .sqlqa/baseline.json
  ```

## Configuration
Use a YAML policy (see `examples/sql-policy.yaml`) to control include/exclude globs, static rule toggles, thresholds, and severity mapping.

## Development
- Run tests: `pytest`.
- Entry point: `sqlqa cli`.

## Notes
- Dynamic checks require a MySQL DSN; static checks work without a database.
