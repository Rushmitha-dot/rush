"""Stream one data year from CMS straight into Parquet, so the 3 GB CSV never sits on disk.

Every column stays a string here. Typing and the row rules happen in the stage step, where a
bad value can go to quarantine instead of failing the whole read.
"""
import hashlib
import io
import json
import time

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from . import publish, sources

ROWS_PER_FILE = 1_000_000


class HashingReader(io.RawIOBase):
    def __init__(self, raw):
        self.raw = raw
        self.sha = hashlib.sha256()
        self.bytes = 0

    def readable(self):
        return True

    def readinto(self, b):
        chunk = self.raw.read(len(b))
        n = len(chunk)
        b[:n] = chunk
        self.sha.update(chunk)
        self.bytes += n
        return n


class HeaderMismatch(Exception):
    pass


def land_year(cfg, year: int, url: str, last_modified: str) -> dict:
    root = cfg.landing
    root.mkdir(parents=True, exist_ok=True)
    publish.clean_leftovers(root)
    staging = publish.staging_dir(root, year)
    staging.mkdir(parents=True)
    started = time.time()
    try:
        with sources.open_url(url, timeout=300) if not url.startswith("file://") else open(url[7:], "rb") as resp:
            reader = HashingReader(resp)
            stream = io.BufferedReader(reader, buffer_size=1 << 22)
            csv = pacsv.open_csv(
                stream,
                read_options=pacsv.ReadOptions(block_size=1 << 24),
                convert_options=pacsv.ConvertOptions(column_types=dict.fromkeys(sources.COLUMNS, pa.string())),
            )
            header = csv.schema.names
            if header != sources.COLUMNS:
                missing = sorted(set(sources.COLUMNS) - set(header))
                extra = sorted(set(header) - set(sources.COLUMNS))
                raise HeaderMismatch(f"header changed, missing {missing} extra {extra}")
            keep = [c for c in sources.COLUMNS if c not in sources.DROPPED_AT_LANDING]
            rows, part, in_part, writer = 0, 0, 0, None
            for batch in csv:
                table = pa.Table.from_batches([batch]).select(keep)
                if writer is None or in_part >= ROWS_PER_FILE:
                    if writer:
                        writer.close()
                    writer = pq.ParquetWriter(staging / f"part-{part:04d}.parquet", table.schema, compression="zstd")
                    part, in_part = part + 1, 0
                writer.write_table(table)
                rows += table.num_rows
                in_part += table.num_rows
            if writer:
                writer.close()
    except BaseException:
        publish.discard(staging)
        raise
    manifest = {
        "year": year, "url": url, "last_modified": last_modified, "rows": rows, "files": part,
        "source_bytes": reader.bytes, "sha256": reader.sha.hexdigest(), "columns": keep,
        "seconds": round(time.time() - started, 1),
    }
    # written last, the stage step refuses a landing folder without it
    (staging / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    publish.swap_in(staging, root, year)
    return manifest


def read_manifest(cfg, year: int) -> dict | None:
    p = cfg.landing / f"year={year}" / "_manifest.json"
    return json.loads(p.read_text()) if p.exists() else None
