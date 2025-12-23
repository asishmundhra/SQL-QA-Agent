from sqlqa.baseline import build_baseline_entry, load_baseline, save_baseline, compare_to_baseline
from sqlqa.models import BaselineEntry, DynamicMetrics


def test_baseline_save_and_load(tmp_path):
    metrics = DynamicMetrics(p95_ms=100.0, rows_examined=500, chosen_key="idx_users")
    entry = build_baseline_entry(metrics)
    data = {"fp": entry}
    out = tmp_path / "baseline.json"
    save_baseline(out, data)
    loaded = load_baseline(out)
    assert "fp" in loaded
    assert loaded["fp"].p95_ms == 100.0
    assert loaded["fp"].chosen_key == "idx_users"


def test_compare_to_baseline_detects_regression():
    baseline = BaselineEntry(p95_ms=100.0, rows_examined=1000)
    dynamic = DynamicMetrics(p95_ms=130.0, rows_examined=1200)
    reg, reason = compare_to_baseline(dynamic, baseline, threshold=0.2)
    assert reg is True
    assert "p95_ms" in reason or "rows examined" in reason
