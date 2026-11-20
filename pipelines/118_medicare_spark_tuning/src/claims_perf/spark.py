import os

from pyspark.sql import SparkSession

from .settings import Settings

# what the nightly job runs with. Experiments start from these and change one thing at a time.
TUNED_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
    "spark.sql.shuffle.partitions": "200",
}


def get_spark(cfg: Settings, app: str = "claims_perf", extra: dict | None = None) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    cfg.eventlogs.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[3]"))
        .appName(app)
        # the Mac is shared, so the driver (which is also the only executor in local mode) stays small
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", os.getenv("SPARK_UI", "true"))
        .config("spark.ui.port", "8118")
        # the event log is where stage times, shuffle bytes and task skew come from
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", str(cfg.eventlogs.resolve()))
        .config("spark.sql.parquet.compression.codec", "zstd")
        # Arrow (pandas UDFs) needs this from Java 9 on. It is not enough on Java 21, see arrow_works.
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .config("spark.sql.sources.partitionOverwriteMode", "static")
    )
    for k, v in {**TUNED_CONF, **(extra or {})}.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def set_conf(spark: SparkSession, conf: dict) -> dict:
    """Apply runtime SQL conf and return the old values so a caller can put them back."""
    old = {}
    for k, v in conf.items():
        old[k] = spark.conf.get(k, None)
        spark.conf.set(k, v)
    return old


def restore_conf(spark: SparkSession, old: dict) -> None:
    for k, v in old.items():
        if v is None:
            spark.conf.unset(k)
        else:
            spark.conf.set(k, v)


def java_major(spark: SparkSession) -> int:
    v = spark.sparkContext._jvm.System.getProperty("java.version")
    return int(v.split(".")[0])


def arrow_works(spark: SparkSession) -> tuple[bool, str]:
    """Spark 3.5 ships Arrow 12 and Netty 4.1.96, which look for the DirectByteBuffer(long, int)
    constructor that JDK 21 changed. On Java 21 every pandas UDF batch fails, so it can't be measured
    there. Spark 3.5 supports Java 8, 11 and 17. CI runs on 17."""
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("long")
    def plus_one(x):
        return x + 1

    try:
        spark.range(2).select(plus_one("id")).collect()
        return True, ""
    except Exception as e:
        text = str(e)
        cause = "DirectByteBuffer.<init>(long, int) not available" if "DirectByteBuffer" in text else text[:200]
        return False, f"pandas UDF failed on Java {java_major(spark)}: {cause}"
