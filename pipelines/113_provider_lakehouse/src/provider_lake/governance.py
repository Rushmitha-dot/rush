"""Analyst access to the provider dimension.

On Databricks this is a Unity Catalog column mask and row filter
(databricks/sql/governance.sql). Open source Spark has neither, so locally the
same rules live in a view, with two small ops tables standing in for account
groups. The rules are identical so the tests cover both.

    provider_admins     see everything
    everyone else       only rows for states granted in ops.analyst_state_access,
                        street address and phone masked for individual providers (entity type 1),
                        ZIP cut to 3 digits for individuals
"""
from pyspark.sql import SparkSession

from .settings import Settings

ADMIN_GROUP = "provider_admins"
MASK = "'***'"


def create_analyst_views(spark: SparkSession, cfg: Settings) -> None:
    members = cfg.table("ops", "group_members")
    access = cfg.table("ops", "analyst_state_access")

    def masked(col):
        return f"case when g.is_admin or p.entity_type = '2' then p.{col} else {MASK} end as {col}"

    spark.sql(f"""
        create or replace view {cfg.table('analyst', 'dim_provider')} as
        select p.provider_sk, p.npi, p.version_no, p.entity_type, p.org_name, p.last_name, p.first_name,
               p.credential, p.primary_taxonomy, p.taxonomy_classification, p.taxonomy_specialization,
               {masked('practice_address_1')},
               {masked('practice_address_2')},
               {masked('practice_phone')},
               {masked('mail_address_1')},
               {masked('mail_address_2')},
               p.practice_city, p.practice_state,
               case when g.is_admin or p.entity_type = '2' then p.practice_postal_code
                    else substr(p.practice_postal_code, 1, 3) end as practice_postal_code,
               p.is_active, p.valid_from, p.valid_to, p.is_current
        from {cfg.table('gold', 'dim_provider')} p
        cross join (select count(*) > 0 as is_admin from {members}
                    where user_name = current_user() and group_name = '{ADMIN_GROUP}') g
        where g.is_admin
           or p.practice_state in (select state from {access} where user_name = current_user())
    """)


def grant_states(spark: SparkSession, cfg: Settings, user: str, states: list[str]) -> None:
    rows = [(user, s) for s in states]
    spark.createDataFrame(rows, "user_name string, state string").write.format("delta").mode("append") \
        .saveAsTable(cfg.table("ops", "analyst_state_access"))


def add_to_group(spark: SparkSession, cfg: Settings, user: str, group: str) -> None:
    spark.createDataFrame([(user, group)], "user_name string, group_name string").write.format("delta") \
        .mode("append").saveAsTable(cfg.table("ops", "group_members"))
