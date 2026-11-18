"""Landed csv -> bronze Delta, every column kept as a string.

Bronze is partitioned by source file and each load replaces only its own
partition, so loading the same file twice leaves one copy.
"""
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .landing import LandedFile
from .settings import Settings

CONTRACT = Path(__file__).parent / "contracts" / "npidata_v2_columns.txt"


def contract_columns() -> list[str]:
    return CONTRACT.read_text().splitlines()


def snake(name: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", name.lower()).strip("_")


def read_landed_csv(spark: SparkSession, path: str, header: list[str]):
    schema = StructType([StructField(snake(h), StringType()) for h in header])
    return (spark.read.schema(schema)
            .option("header", "true")
            # CSV doubles quotes inside fields, Spark's default escape is a backslash. The Texas
            # subset has no quotes today, but one free-text name would break the parse otherwise.
            .option("escape", '"')
            .option("mode", "FAILFAST")
            .csv(path))


def load_bronze(spark: SparkSession, cfg: Settings, landed: LandedFile) -> int:
    table = cfg.table("bronze", "nppes_npidata")
    df = (read_landed_csv(spark, landed.path, landed.header)
          .withColumn("_source_file", F.lit(landed.source_file))
          .withColumn("_source_sha256", F.lit(landed.source_sha256))
          .withColumn("_period_end", F.lit(landed.period_end).cast("date"))
          .withColumn("_ingested_at", F.current_timestamp()))
    exists = spark.catalog.tableExists(table)
    writer = df.write.format("delta").mode("overwrite").partitionBy("_source_file")
    if exists:
        writer = writer.option("replaceWhere", f"_source_file = '{landed.source_file}'")
    writer.saveAsTable(table)
    return spark.table(table).filter(F.col("_source_file") == landed.source_file).count()
