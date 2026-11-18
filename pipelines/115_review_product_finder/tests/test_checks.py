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
    assert not failed(r, WARN), "two repeats in a small day shouldn't page anyone"
    r = run_checks(stats(rows_in=1000, manifest_rows=1000, quarantined=20), [], "daily")
    assert "quarantine_share" in names(r, WARN) and not failed(r, ERROR)
    bad = stats(quarantined=12, quarantine_reasons={"empty_text": 12})
    assert "rejected_rows_max" in names(run_checks(bad, [], "daily"), ERROR)


def test_duplicates_and_small_counts_never_block():
    dupes = stats(rows_in=15, manifest_rows=15, quarantined=6, quarantine_reasons={"duplicate": 6})
    assert not failed(run_checks(dupes, [], "daily"), ERROR)
    one_bad = stats(rows_in=15, manifest_rows=15, quarantined=1, quarantine_reasons={"empty_text": 1})
    assert not failed(run_checks(one_bad, [], "daily"), ERROR)


def test_volume_check_only_for_daily_with_history():
    assert "volume_vs_trailing_7" in names(run_checks(stats(rows_in=10, manifest_rows=10), [100, 100, 100],
                                                       "daily"), WARN)
    assert all(r.name != "volume_vs_trailing_7" for r in run_checks(stats(), [100, 100, 100], "history"))
    assert all(r.name != "volume_vs_trailing_7" for r in run_checks(stats(), [100], "daily"))


def test_row_count_must_match_manifest():
    assert "rows_match_manifest" in names(run_checks(stats(manifest_rows=101), [], "daily"), ERROR)
