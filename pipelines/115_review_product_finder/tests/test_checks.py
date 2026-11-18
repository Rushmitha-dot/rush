from finder.checks import ERROR, WARN, failed, run_checks


def stats(**kw):
    base = {"rows_in": 100, "manifest_rows": 100, "outside_window": 0, "missing_key": 0, "quarantined": 0,
            "quarantine_reasons": {}, "unknown_product": 0}
    return base | kw


def names(results, sev):
    return {r.name for r in failed(results, sev)}


def test_clean_drop_passes():
    assert not failed(run_checks(stats(), [100, 90, 110], "daily"), ERROR)
    assert not failed(run_checks(stats(), [100, 90, 110], "daily"), WARN)


def test_misfiled_drop_blocks():
    assert "review_ts_in_window" in names(run_checks(stats(outside_window=3), [], "daily"), ERROR)


def test_quarantine_thresholds():
    r = run_checks(stats(quarantined=2), [], "daily")
    assert "quarantine_share" in names(r, WARN) and not failed(r, ERROR)
    assert "quarantine_share_max" in names(run_checks(stats(quarantined=6), [], "daily"), ERROR)


def test_volume_check_only_for_daily_with_history():
    assert "volume_vs_trailing_7" in names(run_checks(stats(rows_in=10, manifest_rows=10), [100, 100, 100],
                                                       "daily"), WARN)
    assert all(r.name != "volume_vs_trailing_7" for r in run_checks(stats(), [100, 100, 100], "history"))
    assert all(r.name != "volume_vs_trailing_7" for r in run_checks(stats(), [100], "daily"))


def test_row_count_must_match_manifest():
    assert "rows_match_manifest" in names(run_checks(stats(manifest_rows=101), [], "daily"), ERROR)
