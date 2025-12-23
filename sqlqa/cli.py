from pathlib import Path

import typer

from .graph import run_sqlqa

app = typer.Typer(help="SQLQA static SQL quality checker.")
baseline_app = typer.Typer(help="Baseline operations.")


@app.command()
def check(
    policy_path: str = typer.Option(..., "--policy-path", "-p", help="Path to sql-policy.yaml"),
    out: str = typer.Option("sqlqa-report.md", "--out", "-o", help="Output markdown path"),
    repo_path: str = typer.Option(".", "--repo-path", "-r", help="Root of repository to scan"),
    dsn: str = typer.Option(None, "--dsn", help="MySQL DSN for dynamic checks (mysql://user:pass@host:port/db)"),
    baseline: str = typer.Option(None, "--baseline", help="Path to baseline JSON for regression checks"),
) -> None:
    """Run SQLQA analysis (static + optional dynamic) and write the report."""
    state = run_sqlqa(policy_path=policy_path, repo_path=repo_path, dsn=dsn, baseline_path=baseline)
    output_path = Path(out)
    output_path.write_text(state.report_md or "")
    typer.echo(f"Wrote report to {output_path}")
    raise typer.Exit(code=0 if state.decision == "pass" else 1)


@baseline_app.command("save")
def save_baseline(
    dsn: str = typer.Option(..., "--dsn", help="MySQL DSN for dynamic checks (mysql://user:pass@host:port/db)"),
    policy_path: str = typer.Option(..., "--policy-path", "-p", help="Path to sql-policy.yaml"),
    out: str = typer.Option(".sqlqa/baseline.json", "--out", "-o", help="Output baseline path"),
    repo_path: str = typer.Option(".", "--repo-path", "-r", help="Root of repository to scan"),
) -> None:
    """Run dynamic checks and persist a baseline file."""
    state = run_sqlqa(policy_path=policy_path, repo_path=repo_path, dsn=dsn, baseline_path=out, save_baseline_flag=True)
    typer.echo(f"Saved baseline for {len(state.results)} queries to {out}")


app.add_typer(baseline_app, name="baseline")


if __name__ == "__main__":
    app()
