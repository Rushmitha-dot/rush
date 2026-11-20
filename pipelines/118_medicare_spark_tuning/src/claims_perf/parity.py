"""Backfill vs incremental. Both lakes are fingerprinted table by table and year by year.

A fingerprint is the row count plus the sum of a 64 bit hash of every row. Doubles are rounded
to the cent first, because sums over partitions can land in a different order between runs and
differ in the last bits.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType

from . import stage, summarize


def fingerprint(df: DataFrame) -> tuple[int, int]:
    cols = [F.round(F.col(f.name), 2) if isinstance(f.dataType, (DoubleType, FloatType)) else F.col(f.name)
            for f in sorted(df.schema.fields, key=lambda f: f.name)]
    row = df.agg(F.count("*").alias("n"), F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h")).first()
    return int(row["n"]), int(row["h"] or 0)


def compare(spark: SparkSession, home_a, home_b, years: list[int]) -> list[dict]:
    out = []
    tables = [("lake", t) for t in stage.TABLES] + [("marts", m) for m in summarize.MARTS]
    for year in years:
        for layer, table in tables:
            a = home_a / layer / table / f"year={year}"
            b = home_b / layer / table / f"year={year}"
            if not a.exists() or not b.exists():
                out.append({"year": year, "table": table, "same": False, "note": "missing on one side"})
                continue
            fa, fb = fingerprint(spark.read.parquet(str(a))), fingerprint(spark.read.parquet(str(b)))
            out.append({"year": year, "table": table, "rows": fa[0], "same": fa == fb})
    return out
