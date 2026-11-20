import json
import zipfile
from datetime import date

from labelfeed import dailymed

from .conftest import METF, ROSU, make_update, spl_xml

LISTING = """
<li><a href="https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_weekly_update_110926_111326.zip">
dm_spl_weekly_update_110926_111326.zip</a> <ul><li>Number of labels: 1,201</li><li>File size: 491.41MB</li>
<li>MD5 checksum: 56c4bbdc1a3171470d3a640557974f1b</li></ul></li>
<li><a href="https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_daily_update_11192026.zip">
dm_spl_daily_update_11192026.zip</a> <ul><li>Number of labels: 263</li>
<li>MD5 checksum: 452c6af8526fdfbe04dfb654e6f70c04</li></ul></li>
<li><a href="https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_daily_update_11182026.zip">
dm_spl_daily_update_11182026.zip</a></li>
<li><a href="x">dm_spl_monthly_update_oct2026.zip</a></li>
"""


def test_file_dates_for_each_kind_of_update():
    assert dailymed.file_date("dm_spl_daily_update_11192026.zip") == date(2026, 11, 19)
    assert dailymed.file_date("dm_spl_weekly_update_110926_111326.zip") == date(2026, 11, 13)
    assert dailymed.file_date("bootstrap_20261101.zip") == date(2026, 11, 1)
    assert dailymed.file_date("dm_spl_monthly_update_oct2026.zip") is None


def test_order_puts_bootstrap_first_and_weekly_before_daily():
    names = ["dm_spl_daily_update_11162026.zip", "dm_spl_weekly_update_110926_111326.zip",
             "bootstrap_20261101.zip", "dm_spl_daily_update_11132026.zip"]
    assert sorted(names, key=dailymed.order_key) == [
        "bootstrap_20261101.zip", "dm_spl_weekly_update_110926_111326.zip",
        "dm_spl_daily_update_11132026.zip", "dm_spl_daily_update_11162026.zip"]


def test_listing_gives_names_and_listed_md5():
    assert dailymed.available_updates(LISTING) == {
        "dm_spl_weekly_update_110926_111326.zip": "56c4bbdc1a3171470d3a640557974f1b",
        "dm_spl_daily_update_11182026.zip": None,
        "dm_spl_daily_update_11192026.zip": "452c6af8526fdfbe04dfb654e6f70c04",
    }


def test_slim_keeps_rx_xml_and_lists_and_writes_manifest(tmp_path):
    manifest_path = make_update(tmp_path / "updates", "dm_spl_daily_update_11162026.zip",
                                {ROSU: spl_xml(ROSU, 2), METF: spl_xml(METF, 1, drug="Metformin")},
                                deleted=[ROSU])
    m = json.loads(manifest_path.read_text())
    assert (m["rx_added"], m["rx_deleted"], m["rx_xml"]) == (2, 1, 2)
    with zipfile.ZipFile(tmp_path / "updates" / "dm_spl_daily_update_11162026.zip") as z:
        names = sorted(z.namelist())
        added = z.read("files_added.txt").decode().split()
    assert names == ["files_added.txt", "files_deleted.txt",
                     f"prescription/20261116_{ROSU}.xml", f"prescription/20261116_{METF}.xml"]
    assert all(a.startswith("prescription/") for a in added)
    assert m["slim_md5"] == dailymed.md5_file(tmp_path / "updates" / "dm_spl_daily_update_11162026.zip")


def test_fetch_refuses_a_download_that_does_not_match_the_listed_md5(tmp_path, monkeypatch):
    make_update(tmp_path / "scratch", "dm_spl_daily_update_11162026.zip", {ROSU: spl_xml(ROSU, 2)})
    raw = (tmp_path / "scratch" / ".." / "incoming" / "dm_spl_daily_update_11162026.zip").read_bytes()
    monkeypatch.setattr(dailymed, "_get", lambda url, timeout=0: raw)
    try:
        dailymed.fetch_update("dm_spl_daily_update_11162026.zip", tmp_path / "updates", tmp_path / "in",
                              listed_md5="0" * 32)
        raise AssertionError("expected a checksum error")
    except dailymed.ChecksumMismatch:
        pass
    assert not (tmp_path / "updates" / "dm_spl_daily_update_11162026.zip").exists()
