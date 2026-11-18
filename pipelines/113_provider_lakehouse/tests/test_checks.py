from datetime import date

import pytest
from conftest import HEADER, with_check_digit

from provider_lake.checks import ERROR, failed, file_gates, npi_is_valid


@pytest.mark.parametrize("npi", ["1234567893", "1114877974", "1003000126"])
def test_valid_npis(npi):
    # 1234567893 is the CMS worked example, the other two are real NPIs from the source files
    assert npi_is_valid(npi)


@pytest.mark.parametrize("npi", ["1234567890", "123456789", "12345678931", "12345A7893", "", None])
def test_invalid_npis(npi):
    assert not npi_is_valid(npi)


def test_every_single_digit_typo_is_caught():
    good = "1234567893"
    for pos in range(10):
        for d in "0123456789":
            if d == good[pos]:
                continue
            assert not npi_is_valid(good[:pos] + d + good[pos + 1:])


def test_check_digit_helper_matches_validator():
    for base in ("100000001", "999999999", "123456789", "182736450"):
        assert npi_is_valid(with_check_digit(base))


def gates(**over):
    args = dict(header=HEADER, contract=HEADER, rows=1000, quarantined=0, duplicate_npis=0,
                period_start=date(2026, 11, 9), period_end=date(2026, 11, 15), last_period_end=date(2026, 11, 8),
                kind="weekly", unknown_taxonomy=0, invalid_state=0, no_primary_flag=0, recent_rows=[1000])
    args.update(over)
    return file_gates(**args)


def test_clean_weekly_passes():
    assert all(r.passed for r in gates())


def test_skipped_week_blocks():
    errs = failed(gates(period_start=date(2026, 11, 16), period_end=date(2026, 11, 22)), ERROR)
    assert [r.name for r in errs] == ["weekly_continuity"]


def test_replaying_an_old_week_blocks():
    errs = failed(gates(period_start=date(2026, 11, 2), period_end=date(2026, 11, 8)), ERROR)
    assert [r.name for r in errs] == ["weekly_continuity"]


def test_overlapping_day_is_allowed():
    # the real monthly ends 11/02 and the first weekly starts 11/02
    assert all(r.passed for r in gates(period_start=date(2026, 11, 2), period_end=date(2026, 11, 8),
                                       last_period_end=date(2026, 11, 2)))


def test_header_drift_blocks():
    renamed = HEADER[:-1] + ["Certification Dt"]
    errs = failed(gates(header=renamed), ERROR)
    assert errs[0].name == "header_matches_contract"
    assert "Certification Dt" in errs[0].detail


def test_quarantine_rate_threshold():
    assert not failed(gates(quarantined=5), ERROR)
    assert failed(gates(quarantined=6), ERROR)[0].name == "quarantine_rate"


def test_warnings_do_not_block():
    out = gates(unknown_taxonomy=3, recent_rows=[100])
    assert not failed(out, ERROR)
    assert {r.name for r in out if not r.passed} == {"unknown_taxonomy", "weekly_volume_vs_recent"}
