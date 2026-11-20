"""Replace one year's folder of a table in a single rename.

Spark's dynamic partition overwrite only replaces the partitions present in the new data,
so a state that disappears on a rerun would keep its old files. Writing the whole year to a
staging folder and swapping it in avoids that, and readers never see a half written year.
Folders starting with "_" are skipped by Spark's partition discovery, so staging is invisible.
"""
import shutil
import uuid
from pathlib import Path


def staging_dir(table_root: Path, year: int) -> Path:
    return table_root / f"_staging-year={year}-{uuid.uuid4().hex[:8]}"


def swap_in(staging: Path, table_root: Path, year: int) -> Path:
    target = table_root / f"year={year}"
    old = table_root / f"_old-year={year}-{uuid.uuid4().hex[:8]}"
    if target.exists():
        target.rename(old)
    staging.rename(target)
    if old.exists():
        shutil.rmtree(old)
    return target


def discard(staging: Path) -> None:
    shutil.rmtree(staging, ignore_errors=True)


def clean_leftovers(table_root: Path) -> None:
    """A crash between the two renames leaves an _old folder. Put it back if the year is missing."""
    if not table_root.exists():
        return
    for p in table_root.glob("_old-year=*"):
        year = p.name.split("=")[1].split("-")[0]
        target = table_root / f"year={year}"
        if target.exists():
            shutil.rmtree(p)
        else:
            p.rename(target)
    for p in table_root.glob("_staging-year=*"):
        shutil.rmtree(p)
