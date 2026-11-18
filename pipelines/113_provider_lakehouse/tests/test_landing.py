import gzip
from datetime import date

from conftest import WEEK1, A, B, C, D, E, deactivation, make_zip, provider

from provider_lake.landing import land_zip
from provider_lake.sources import classify, inner_period


def test_file_names():
    assert classify("NPPES_Data_Dissemination_November_2026_V2.zip").kind == "monthly"
    assert classify("NPPES_Data_Dissemination_110926_111526_Weekly_V2.zip").kind == "weekly"
    assert classify("NPPES_Deactivated_NPI_Report_110926_V2.zip") is None
    assert classify("NPPES_Data_Dissemination_November_2026.zip") is None  # V1, dropped by CMS in March 2026


def test_replay_order_puts_monthly_before_weeklies():
    names = ["NPPES_Data_Dissemination_110926_111526_Weekly_V2.zip",
             "NPPES_Data_Dissemination_November_2026_V2.zip",
             "NPPES_Data_Dissemination_110226_110826_Weekly_V2.zip"]
    ordered = sorted((classify(n) for n in names), key=lambda f: f.order_key)
    assert [f.kind for f in ordered] == ["monthly", "weekly", "weekly"]


def test_inner_period():
    assert inner_period("npidata_pfile_20050523-20261102.csv") == (date(2005, 5, 23), date(2026, 11, 2))


def test_landing_keeps_scope_and_known_npis(zips, tmp_path):
    rows = [
        provider(A, state="TX"),
        provider(E, state="OK"),                  # out of scope, unknown: dropped
        provider(C, state="CA", mail_state="TX"),  # mailing address in scope: kept
        provider(D, state="NM"),                  # moved out of TX but already known: kept
        deactivation("1306722293", "11/02/2026"),  # bare deactivation for an NPI we never had: dropped
        deactivation(B, "11/02/2026"),            # bare deactivation for a known NPI: kept
    ]
    z = make_zip(zips, WEEK1, "20261102-20261108", rows)
    landed = land_zip(z, "weekly", tmp_path / "landing", ["TX"], known_npis={D, B})
    assert (landed.rows_read, landed.rows_kept) == (6, 4)
    assert landed.period_start == "2026-11-02"
    with gzip.open(landed.path, "rt") as f:
        kept = [line.split(",")[0].strip('"') for line in f.read().splitlines()[1:]]
    assert kept == [A, C, D, B]
