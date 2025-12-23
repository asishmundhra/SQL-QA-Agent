from sqlqa.graph import run_sqlqa


def test_graph_runs_end_to_end(tmp_path):
    state = run_sqlqa(policy_path="examples/sql-policy.yaml", repo_path="examples/sample_repo")
    assert state.report_md
    assert state.decision in {"pass", "fail"}
    # Write report to ensure path handling works
    out_file = tmp_path / "report.md"
    out_file.write_text(state.report_md)
    assert out_file.exists()
