"""Bronze rows for one file -> SCD Type 2 provider dimension.

A new version opens only when the row hash changes. Last Update Date is left
out of the hash on purpose, because NPPES bumps it on edits to fields this
dimension doesn't keep (other identifiers, endpoints) and on re-attestations.
Hashing it would open a version for most weekly rows with nothing to show for it.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .settings import Settings

OPEN_END = "9999-12-31"

# silver name -> NPPES column (snake-cased header)
FIELDS = {
    "entity_type": "entity_type_code",
    "replacement_npi": "replacement_npi",
    "org_name": "provider_organization_name_legal_business_name",
    "last_name": "provider_last_name_legal_name",
    "first_name": "provider_first_name",
    "middle_name": "provider_middle_name",
    "name_suffix": "provider_name_suffix_text",
    "credential": "provider_credential_text",
    "sex_code": "provider_sex_code",
    "mail_address_1": "provider_first_line_business_mailing_address",
    "mail_address_2": "provider_second_line_business_mailing_address",
    "mail_city": "provider_business_mailing_address_city_name",
    "mail_state": "provider_business_mailing_address_state_name",
    "mail_postal_code": "provider_business_mailing_address_postal_code",
    "mail_country": "provider_business_mailing_address_country_code_if_outside_u_s",
    "practice_address_1": "provider_first_line_business_practice_location_address",
    "practice_address_2": "provider_second_line_business_practice_location_address",
    "practice_city": "provider_business_practice_location_address_city_name",
    "practice_state": "provider_business_practice_location_address_state_name",
    "practice_postal_code": "provider_business_practice_location_address_postal_code",
    "practice_country": "provider_business_practice_location_address_country_code_if_outside_u_s",
    "practice_phone": "provider_business_practice_location_address_telephone_number",
    "is_sole_proprietor": "is_sole_proprietor",
    "is_org_subpart": "is_organization_subpart",
    "parent_org_name": "parent_organization_lbn",
    "auth_official_last_name": "authorized_official_last_name",
    "auth_official_first_name": "authorized_official_first_name",
    "auth_official_title": "authorized_official_title_or_position",
}
DATE_FIELDS = {
    "enumeration_date": "provider_enumeration_date",
    "deactivation_date": "npi_deactivation_date",
    "reactivation_date": "npi_reactivation_date",
}
TAXONOMY_SLOTS = 15

# what a version is made of; changing this list changes every hash, so it's a migration, not a tweak
TRACKED = list(FIELDS) + list(DATE_FIELDS) + ["primary_taxonomy", "taxonomy_codes", "primary_license",
                                               "primary_license_state"]

DIM_COLUMNS = (["provider_sk", "npi", "version_no"] + TRACKED +
               ["is_active", "last_update_date", "row_hash", "valid_from", "valid_to", "is_current",
                "source_file", "loaded_at"])


def _clean(col_name: str):
    return F.nullif(F.trim(F.col(col_name)), F.lit(""))


def stage(bronze_rows: DataFrame, period_end: str) -> DataFrame:
    """Typed, hashed rows for one file. Still one row per source row, no dedup here."""
    cols = [F.trim(F.col("npi")).alias("npi")]
    cols += [_clean(src).alias(dst) for dst, src in FIELDS.items()]
    cols += [F.to_date(_clean(src), "MM/dd/yyyy").alias(dst) for dst, src in DATE_FIELDS.items()]
    cols.append(_clean("last_update_date").alias("last_update_raw"))
    cols.append(F.to_date(_clean("last_update_date"), "MM/dd/yyyy").alias("last_update_date"))

    codes = [_clean(f"healthcare_provider_taxonomy_code_{i}") for i in range(1, TAXONOMY_SLOTS + 1)]
    switches = [_clean(f"healthcare_provider_primary_taxonomy_switch_{i}") for i in range(1, TAXONOMY_SLOTS + 1)]
    licenses = [_clean(f"provider_license_number_{i}") for i in range(1, TAXONOMY_SLOTS + 1)]
    lic_states = [_clean(f"provider_license_number_state_code_{i}") for i in range(1, TAXONOMY_SLOTS + 1)]
    flagged = [F.when(s == "Y", i) for i, s in enumerate(switches)]
    cols.append(F.array_compact(F.array(*codes)).alias("taxonomy_codes"))
    cols.append(F.coalesce(*flagged).alias("_primary_slot"))
    cols.append(F.array(*codes).alias("_codes"))
    cols.append(F.array(*licenses).alias("_licenses"))
    cols.append(F.array(*lic_states).alias("_lic_states"))

    df = bronze_rows.select(*cols)
    # No Y flag on any slot happens (see docs). Fall back to slot 1 and count it as a WARN.
    slot = F.coalesce(F.col("_primary_slot"), F.lit(0)) + 1
    df = (df.withColumn("no_primary_flag", F.col("_primary_slot").isNull() & (F.size("taxonomy_codes") > 0))
            .withColumn("primary_taxonomy", F.element_at("_codes", slot))
            .withColumn("primary_license", F.element_at("_licenses", slot))
            .withColumn("primary_license_state", F.element_at("_lic_states", slot))
            .drop("_primary_slot", "_codes", "_licenses", "_lic_states"))
    return df.withColumn("period_end", F.lit(period_end).cast("date"))


def row_hash(cols: list[str]):
    parts = [F.coalesce(F.array_join(F.col(c), ",") if c == "taxonomy_codes" else F.col(c).cast("string"),
                        F.lit("")) for c in cols]
    return F.sha2(F.concat_ws("\u0001", *parts), 256)


def is_active_expr():
    # the coalesce matters, a deactivated row with no reactivation is "false or null", which is null
    reactivated = F.coalesce(F.col("reactivation_date") >= F.col("deactivation_date"), F.lit(False))
    return F.col("deactivation_date").isNull() | reactivated


def classify(spark: SparkSession, cfg: Settings, staged: DataFrame, dim_version: int) -> DataFrame:
    """Compare clean staged rows with the current dimension and decide what each one does.

    change_type is one of new, changed, deactivated, reactivated, same_day_fix, unchanged, stale.

    The dimension is read at a pinned Delta version. The result is cached, but if Spark evicts
    a block and recomputes it after the merge, an unpinned read would see the merged rows and
    call everything unchanged. That happened on the first real monthly load.
    """
    current = (spark.read.option("versionAsOf", dim_version).table(cfg.table("silver", "dim_provider"))
               .filter("is_current")
               .select("npi", *[F.col(c).alias(f"cur_{c}") for c in TRACKED + ["version_no", "valid_from",
                                                                               "row_hash", "is_active"]]))
    s = staged.join(current, "npi", "left")

    # A deactivation arrives as a bare row, NPI and date only. Keep who the provider was
    # and only change the deactivation fields.
    # Freeze the flag first. The loop overwrites entity_type, and a flag computed from it on
    # the fly would turn false for every column after the first.
    s = s.withColumn("_bare", F.col("entity_type").isNull() & F.col("deactivation_date").isNotNull()
                     & F.col("cur_version_no").isNotNull())
    keep_from_current = [c for c in TRACKED if c not in ("deactivation_date", "reactivation_date")]
    for c in keep_from_current:
        s = s.withColumn(c, F.when(F.col("_bare"), F.col(f"cur_{c}")).otherwise(F.col(c)))
    s = s.drop("_bare")

    s = (s.withColumn("row_hash", row_hash(TRACKED))
          .withColumn("is_active", is_active_expr())
          .withColumn("valid_from", F.coalesce("last_update_date", "deactivation_date", "period_end")))

    change = (
        F.when(F.col("cur_version_no").isNull(), "new")
        .when(F.col("row_hash") == F.col("cur_row_hash"), "unchanged")
        .when(F.col("valid_from") < F.col("cur_valid_from"), "stale")
        .when(F.col("valid_from") == F.col("cur_valid_from"), "same_day_fix")
        .when(F.col("cur_is_active") & ~F.col("is_active"), "deactivated")
        .when(~F.col("cur_is_active") & F.col("is_active"), "reactivated")
        .otherwise("changed"))
    s = s.withColumn("change_type", change)

    diffs = [F.when(~F.col(c).eqNullSafe(F.col(f"cur_{c}")), F.lit(c)) for c in TRACKED]
    s = s.withColumn("changed_fields", F.when(F.col("change_type").isin("new", "unchanged"), F.array())
                     .otherwise(F.array_compact(F.array(*diffs))))
    s = s.withColumn("version_no", F.when(F.col("change_type") == "new", F.lit(1))
                     .when(F.col("change_type") == "same_day_fix", F.col("cur_version_no"))
                     .otherwise(F.col("cur_version_no") + 1))
    return s


def merge_dimension(spark: SparkSession, cfg: Settings, classified: DataFrame, source_file: str) -> None:
    dim = cfg.table("silver", "dim_provider")
    apply = (classified.filter(F.col("change_type").isin("new", "changed", "deactivated", "reactivated",
                                                         "same_day_fix"))
             .withColumn("provider_sk", F.xxhash64("npi", "version_no"))
             .withColumn("valid_to", F.lit(OPEN_END).cast("date"))
             .withColumn("is_current", F.lit(True))
             .withColumn("source_file", F.lit(source_file))
             .withColumn("loaded_at", F.current_timestamp())
             .select(*DIM_COLUMNS, "change_type"))

    # Classic SCD2 trick: every row once keyed by NPI (closes or fixes the current version, or
    # inserts a brand new NPI), plus changed rows again with a null key so they fall through
    # to the insert branch as the new current version.
    keyed = apply.withColumn("merge_key", F.col("npi"))
    reinsert = (apply.filter(F.col("change_type").isin("changed", "deactivated", "reactivated"))
                .withColumn("merge_key", F.lit(None).cast("string")))
    keyed.unionByName(reinsert).createOrReplaceTempView("dim_merge_src")

    attrs = [c for c in DIM_COLUMNS if c not in ("provider_sk", "npi", "version_no", "valid_from", "valid_to",
                                                  "is_current")]
    fix_set = ", ".join(f"t.{c} = s.{c}" for c in attrs)
    insert_cols = ", ".join(DIM_COLUMNS)
    insert_vals = ", ".join(f"s.{c}" for c in DIM_COLUMNS)
    spark.sql(f"""
        merge into {dim} t
        using dim_merge_src s
        on t.npi = s.merge_key and t.is_current
        when matched and s.change_type = 'same_day_fix' then update set {fix_set}
        when matched then update set t.is_current = false, t.valid_to = s.valid_from
        when not matched and s.change_type <> 'same_day_fix' then insert ({insert_cols}) values ({insert_vals})
    """)


def create_dimension(spark: SparkSession, cfg: Settings) -> None:
    spark.sql(f"""
        create table if not exists {cfg.table('silver', 'dim_provider')} (
            provider_sk bigint, npi string, version_no int,
            {', '.join(f'{c} string' for c in FIELDS)},
            {', '.join(f'{c} date' for c in DATE_FIELDS)},
            primary_taxonomy string, taxonomy_codes array<string>, primary_license string,
            primary_license_state string, is_active boolean, last_update_date date, row_hash string,
            valid_from date, valid_to date, is_current boolean, source_file string, loaded_at timestamp)
        using delta
        tblproperties (delta.enableChangeDataFeed = true)
    """)
    spark.sql(f"""
        create table if not exists {cfg.table('silver', 'provider_quarantine')} (
            source_file string, npi string, reason string, entity_type string, last_update_raw string,
            row_json string, quarantined_at timestamp)
        using delta
    """)
    spark.sql(f"""
        create table if not exists {cfg.table('silver', 'provider_change_log')} (
            source_file string, npi string, change_type string, changed_fields array<string>,
            version_no int, valid_from date, logged_at timestamp)
        using delta
    """)


def dedupe_report(staged: DataFrame) -> int:
    """NPIs appearing more than once in one file. Should never happen, so it is an ERROR gate."""
    w = Window.partitionBy("npi")
    return staged.withColumn("n", F.count("*").over(w)).filter("n > 1").select("npi").distinct().count()
