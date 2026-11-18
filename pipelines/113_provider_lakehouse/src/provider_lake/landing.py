"""Stream the NPI csv out of an NPPES zip and keep only the rows in scope.

The monthly file is about 1.1 GB zipped and 11.8 GB unzipped, so it is never
extracted. Rows go zip -> csv reader -> filter -> gzip one at a time.

A row is in scope if its practice or mailing address is in a scope state, or if
the NPI is already in the provider dimension. The second rule matters: a
provider who moves out of Texas, or gets deactivated (those rows come through
with every field blank except the NPI and the date), would otherwise be
filtered out and the dimension would never hear about it.
"""
import csv
import gzip
import io
import json
import logging
import time
import zipfile
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

from .sources import inner_period, sha256_of

log = logging.getLogger(__name__)

NPI, MAIL_STATE, PRACTICE_STATE = 0, 23, 31
EXPECTED_COLUMNS = 330


@dataclass
class LandedFile:
    source_file: str
    source_sha256: str
    kind: str
    inner_file: str
    period_start: str
    period_end: str
    rows_read: int
    rows_kept: int
    header: list[str]
    path: str
    seconds: float

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=1))

    @classmethod
    def read(cls, path: Path) -> "LandedFile":
        return cls(**json.loads(path.read_text()))


def find_npi_member(zf: zipfile.ZipFile) -> str:
    names = [n for n in zf.namelist() if n.startswith("npidata_pfile_") and not n.endswith("_fileheader.csv")]
    if len(names) != 1:
        raise ValueError(f"expected one npidata csv in zip, found {names}")
    return names[0]


def filter_rows(rows: Iterable[list[str]], states: set[str], known_npis: set[str]):
    for row in rows:
        if len(row) <= PRACTICE_STATE:
            yield row  # let the row gate deal with short rows instead of hiding them here
        elif row[PRACTICE_STATE] in states or row[MAIL_STATE] in states or row[NPI] in known_npis:
            yield row


def land_zip(zip_path: Path, kind: str, out_dir: Path, states: Iterable[str],
             known_npis: set[str] | None = None, sha256: str | None = None) -> LandedFile:
    t0 = time.time()
    states = set(states)
    known_npis = known_npis or set()
    sha256 = sha256 or sha256_of(zip_path)
    dest_dir = out_dir / zip_path.stem
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / "npidata.csv.gz"
    tmp = out_path.with_suffix(".gz.part")

    with zipfile.ZipFile(zip_path) as zf:
        member = find_npi_member(zf)
        start, end = inner_period(member)
        with zf.open(member) as raw, gzip.open(tmp, "wt", encoding="utf-8", newline="", compresslevel=5) as out:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
            writer = csv.writer(out, quoting=csv.QUOTE_ALL)
            header = next(reader)
            writer.writerow(header)
            counted = _Counted(reader)
            kept = 0
            for row in filter_rows(counted, states, known_npis):
                writer.writerow(row)
                kept += 1
            read = counted.n
    tmp.rename(out_path)

    landed = LandedFile(zip_path.name, sha256, kind, member, start.isoformat(), end.isoformat(),
                        read, kept, header, str(out_path), round(time.time() - t0, 1))
    landed.save(dest_dir / "manifest.json")
    log.info("landed %s, kept %s of %s rows in %.0fs", zip_path.name, kept, read, landed.seconds)
    return landed


class _Counted:
    def __init__(self, rows):
        self.rows, self.n = rows, 0

    def __iter__(self):
        for r in self.rows:
            self.n += 1
            yield r
