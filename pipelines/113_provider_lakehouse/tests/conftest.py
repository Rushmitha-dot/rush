"""Test fixtures. Provider rows here are small hand-written fixtures, built on the real 330-column
NPPES V2 header so the parsing and gates run against the true layout."""
import csv
import io
import os
import zipfile
from pathlib import Path

import pytest

from provider_lake.bronze import contract_columns
from provider_lake.settings import Settings

HEADER = contract_columns()
IDX = {h: i for i, h in enumerate(HEADER)}


def with_check_digit(base9: str) -> str:
    total = 24
    for i, ch in enumerate(reversed(base9)):
        d = int(ch)
        if i % 2 == 0:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return base9 + str((10 - total % 10) % 10)


def provider(npi, *, entity="1", last="DOE", first="JANE", org="", state="TX", city="DALLAS",
             address="100 MAIN ST", postal="752010000", taxonomy="207Q00000X", updated="10/15/2026",
             deactivated="", reactivated="", mail_state=None):
    row = [""] * len(HEADER)

    def put(col, val):
        row[IDX[col]] = val

    put("NPI", npi)
    put("Entity Type Code", entity)
    if entity == "1":
        put("Provider Last Name (Legal Name)", last)
        put("Provider First Name", first)
    elif entity == "2":
        put("Provider Organization Name (Legal Business Name)", org)
    put("Provider First Line Business Mailing Address", address)
    put("Provider Business Mailing Address City Name", city)
    put("Provider Business Mailing Address State Name", mail_state or state)
    put("Provider Business Mailing Address Country Code (If outside U.S.)", "US")
    put("Provider First Line Business Practice Location Address", address)
    put("Provider Business Practice Location Address City Name", city)
    put("Provider Business Practice Location Address State Name", state)
    put("Provider Business Practice Location Address Postal Code", postal)
    put("Provider Business Practice Location Address Country Code (If outside U.S.)", "US")
    put("Provider Business Practice Location Address Telephone Number", "2145550100")
    put("Provider Enumeration Date", "01/01/2010")
    put("Last Update Date", updated)
    put("NPI Deactivation Date", deactivated)
    put("NPI Reactivation Date", reactivated)
    put("Healthcare Provider Taxonomy Code_1", taxonomy)
    put("Healthcare Provider Primary Taxonomy Switch_1", "Y")
    return row


def deactivation(npi, date):
    """What NPPES actually sends for a deactivated NPI: the number and the date, nothing else."""
    row = [""] * len(HEADER)
    row[IDX["NPI"]] = npi
    row[IDX["NPI Deactivation Date"]] = date
    return row


def make_zip(folder: Path, name: str, inner_period: str, rows, header=None) -> Path:
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_ALL)
    w.writerow(header or HEADER)
    w.writerows(rows)
    path = folder / name
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"npidata_pfile_{inner_period}.csv", buf.getvalue())
        z.writestr(f"npidata_pfile_{inner_period}_fileheader.csv", ",".join(HEADER))
    return path


MONTHLY = "NPPES_Data_Dissemination_November_2026_V2.zip"
WEEK1 = "NPPES_Data_Dissemination_110226_110826_Weekly_V2.zip"
WEEK2 = "NPPES_Data_Dissemination_110926_111526_Weekly_V2.zip"
WEEK_GAP = "NPPES_Data_Dissemination_112326_112926_Weekly_V2.zip"

A, B, C, D, E = (with_check_digit(b) for b in ("100000001", "100000002", "100000003", "100000004", "100000005"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from provider_lake.spark import get_spark

    home = tmp_path_factory.mktemp("lake")
    os.environ.update(SHUFFLE_PARTITIONS="1", DELTA_SNAPSHOT_PARTITIONS="1", SPARK_UI="false")
    s = get_spark(Settings(home=home, scope_states=("TX",)), app="provider_lake_tests")
    yield s
    s.stop()


@pytest.fixture
def cfg(spark, tmp_path, monkeypatch):
    """A clean lake for each test. Tables are dropped, the session is shared because JVM startup is slow."""
    from provider_lake.ops import ensure_ops

    warehouse = Path(spark.conf.get("spark.sql.warehouse.dir").replace("file:", ""))
    for schema in ("analyst", "gold", "silver", "bronze", "ops"):
        spark.sql(f"drop schema if exists {schema} cascade")
    monkeypatch.setenv("LANDING_DIR", str(tmp_path / "landing"))
    c = Settings(home=warehouse.parent, scope_states=("TX",))
    ensure_ops(spark, c)
    spark.createDataFrame(
        [("207Q00000X", "Allopathic & Osteopathic Physicians", "Family Medicine", None, "Family Medicine Physician",
          "Individual"),
         ("207R00000X", "Allopathic & Osteopathic Physicians", "Internal Medicine", None,
          "Internal Medicine Physician", "Individual"),
         ("261QP2300X", "Ambulatory Health Care Facilities", "Clinic/Center", "Primary Care",
          "Primary Care Clinic", "Non-Individual")],
        "taxonomy_code string, grouping string, classification string, specialization string, display_name string, "
        "section string").write.format("delta").saveAsTable("gold.dim_taxonomy")
    return c


@pytest.fixture
def zips(tmp_path):
    folder = tmp_path / "downloads"
    folder.mkdir()
    return folder
