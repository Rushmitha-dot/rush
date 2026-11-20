"""Fixtures. Claim lines here are small hand-written rows on the real 28-column CMS header, so landing,
the row rules and every experiment run against the true layout in a few seconds."""
import csv
import os
from pathlib import Path

import pytest

from claims_perf import checks, sources
from claims_perf.settings import Settings


def with_check_digit(base9: str) -> str:
    total = 24
    for i, ch in enumerate(reversed(base9)):
        d = int(ch)
        if i % 2 == 0:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return base9 + str((10 - total % 10) % 10)


# npi, provider type, state, entity
PROVIDERS = [
    (with_check_digit(f"10000000{i}"), ptype, state, "I")
    for i, (ptype, state) in enumerate([
        ("Internal Medicine", "TX"), ("Family Practice", "TX"), ("Nurse Practitioner", "CA"),
        ("Internal Medicine", "CA"), ("Cardiology", "NY"), ("Diagnostic Radiology", "NY"),
        ("Family Practice", "FL"), ("Nurse Practitioner", "FL"), ("Internal Medicine", "NY"),
    ])
]

DESC = {
    "99213": "Established patient office or other outpatient visit, 20-29 minutes",
    "99214": "Established patient office or other outpatient visit, 30-39 minutes",
    "93000": "Routine EKG using at least 12 leads including interpretation and report",
    "71046": "X-ray of chest, 2 views",
    "J1100": "Injection, dexamethasone sodium phosphate, 1 mg",
    "G0439": "Annual wellness visit, includes a personalized prevention plan of service (pps), subsequent visit",
    "0241U": "Detection test for SARS-CoV-2 and influenza viruses",
}


def line(npi, ptype, state, code, pos="O", benes="20", srvcs="40", allowed="90.5", payment="70.25", entity="I",
         desc=None):
    row = dict.fromkeys(sources.COLUMNS, "")
    row.update({
        "Rndrng_NPI": npi, "Rndrng_Prvdr_Last_Org_Name": "Doe", "Rndrng_Prvdr_First_Name": "Jane",
        "Rndrng_Prvdr_Crdntls": "M.D.", "Rndrng_Prvdr_Ent_Cd": entity, "Rndrng_Prvdr_St1": "100 Main St",
        "Rndrng_Prvdr_City": "Dallas", "Rndrng_Prvdr_State_Abrvtn": state, "Rndrng_Prvdr_State_FIPS": "48",
        "Rndrng_Prvdr_Zip5": "75201", "Rndrng_Prvdr_RUCA": "1", "Rndrng_Prvdr_RUCA_Desc": "Metropolitan area core",
        "Rndrng_Prvdr_Cntry": "US", "Rndrng_Prvdr_Type": ptype, "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "Y",
        "HCPCS_Cd": code, "HCPCS_Desc": desc or DESC.get(code, "fixture code"),
        "HCPCS_Drug_Ind": "Y" if code.startswith("J") else "N", "Place_Of_Srvc": pos, "Tot_Benes": benes,
        "Tot_Srvcs": srvcs, "Tot_Bene_Day_Srvcs": srvcs, "Avg_Sbmtd_Chrg": "180.0", "Avg_Mdcr_Alowd_Amt": allowed,
        "Avg_Mdcr_Pymt_Amt": payment, "Avg_Mdcr_Stdzd_Amt": payment,
    })
    return row


def good_lines(year: int):
    """Every provider bills the two office visit codes (the hot keys), some bill the rest."""
    bump = 0 if year == 2023 else 5
    rows = []
    for k, (npi, ptype, state, _entity) in enumerate(PROVIDERS):
        rows.append(line(npi, ptype, state, "99213", srvcs=str(30 + k + bump)))
        rows.append(line(npi, ptype, state, "99214", srvcs=str(40 + k + bump), allowed="130.1", payment="101.9"))
        rows.append(line(npi, ptype, state, "99214", pos="F", srvcs=str(22 + k), allowed="95.0", payment="74.0"))
        if k % 2 == 0:
            rows.append(line(npi, ptype, state, "G0439", benes="15", srvcs="15", allowed="140", payment="110"))
        if k % 3 == 0:
            rows.append(line(npi, ptype, state, "J1100", benes="12", srvcs="300", allowed="0.1", payment="0.08"))
    rows.append(line(PROVIDERS[4][0], "Cardiology", "NY", "93000", srvcs="80", allowed="16.2", payment="12.7"))
    rows.append(line(PROVIDERS[5][0], "Diagnostic Radiology", "NY", "71046", srvcs="900", allowed="30", payment="23"))
    rows.append(line(PROVIDERS[3][0], "Internal Medicine", "CA", "0241U", srvcs="25", allowed="142", payment="142"))
    # one provider billing far more 99213 services per beneficiary than peers, the outlier mart should catch it
    rows.append(line(PROVIDERS[8][0], "Internal Medicine", "NY", "99213", pos="F", benes="11", srvcs="400"))
    for k in range(4):
        rows.append(line(PROVIDERS[k][0], PROVIDERS[k][1], PROVIDERS[k][2], "99213", pos="F", srvcs=str(12 + k)))
    return rows


def bad_lines():
    a, b = PROVIDERS[0], PROVIDERS[1]
    return [
        line("1000000001", "Internal Medicine", "TX", "99213"),                # check digit wrong
        line(a[0], a[1], a[2], "", pos="O"),                                    # no code
        line(b[0], b[1], b[2], "93000", benes="5"),                             # under the CMS floor
        line(b[0], b[1], b[2], "71046", allowed="20", payment="25"),            # paid more than allowed
        line(a[0], a[1], a[2], "93000", srvcs="n/a"),                           # not a number
    ]


def write_csv(path: Path, rows) -> Path:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=sources.COLUMNS)
        w.writeheader()
        w.writerows(rows)
    return path


TEST_GATES = checks.Gates(max_quarantine_share=0.2, min_states=4)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from claims_perf.spark import get_spark

    home = tmp_path_factory.mktemp("spark_home")
    os.environ["SPARK_UI"] = "false"
    # Python workers (UDFs) import claims_perf too, and they only see the environment, not sys.path
    src = str(Path(__file__).resolve().parents[1] / "src")
    os.environ["PYTHONPATH"] = os.pathsep.join(p for p in (src, os.environ.get("PYTHONPATH")) if p)
    s = get_spark(Settings(home=home), app="claims_perf_tests", extra={"spark.sql.shuffle.partitions": "4"})
    yield s
    s.stop()


@pytest.fixture
def cfg(tmp_path, monkeypatch, spark):
    """A fresh data home per test, sharing the one Spark session (JVM start is the slow part).
    The event log stays where the session put it."""
    home = tmp_path / "data"
    monkeypatch.setenv("CLAIMS_HOME", str(home))
    c = Settings(home=home)
    monkeypatch.setattr(Settings, "eventlogs", property(lambda self: Path(spark.conf.get("spark.eventLog.dir")
                                                                          .replace("file:", ""))))
    return c


@pytest.fixture
def sources_2y(tmp_path, monkeypatch):
    files = {}
    for year in (2023, 2024):
        rows = good_lines(year) + bad_lines()
        files[year] = write_csv(tmp_path / f"claims_{year}.csv", rows)
        monkeypatch.setenv(f"SOURCE_URL_{year}", f"file://{files[year]}")
    return files
