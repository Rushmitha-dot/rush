import os

from pyspark.sql import SparkSession

from .settings import Settings


def get_spark(cfg: Settings, app: str = "provider_lake") -> SparkSession:
    # On Databricks a session already exists and Unity Catalog owns the tables.
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    from delta import configure_spark_with_delta_pip

    home = cfg.home.resolve()
    home.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[4]"))
        .appName(app)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(cfg.warehouse.resolve()))
        # one week is a few thousand rows, the default 200 partitions is mostly empty tasks
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.adaptive.enabled", "true")
        # Delta reads its log with 50 partitions by default, which dominates small commits locally
        .config("spark.databricks.delta.snapshotPartitions", os.getenv("DELTA_SNAPSHOT_PARTITIONS", "4"))
        .config("spark.ui.enabled", os.getenv("SPARK_UI", "true"))
        .config("spark.ui.port", "8113")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        # Hive metastore in Derby so table names survive between runs, kept under the lake home
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={home}/metastore_db;create=true")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={home}")
        # Delta keeps the real schema in its log and Hive only holds a placeholder. Without this,
        # Hive rejects Delta's schema sync on every write and logs a long stack trace.
        .config("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
