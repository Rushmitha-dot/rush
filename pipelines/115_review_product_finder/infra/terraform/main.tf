# Snowflake objects for the product finder. Validated with `terraform validate`,
# never applied: I don't have a Snowflake account for this project.

terraform {
  required_version = ">= 1.6"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }
}

provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.private_key
  role              = "SYSADMIN"
}

resource "snowflake_database" "finder" {
  name    = "FINDER"
  comment = "review-scored product finder"
}

resource "snowflake_schema" "layers" {
  for_each = toset(["RAW", "NLP", "OPS", "STAGING", "INTERMEDIATE", "MARTS", "REF"])
  database = snowflake_database.finder.name
  name     = each.key
}

# XS is enough: a daily batch is tens to a few hundred reviews, and the full
# rebuild of the marts is a few million rows. 60s auto-suspend so the warehouse
# isn't billed while Airflow waits between tasks.
resource "snowflake_warehouse" "transform" {
  name                = "FINDER_XS"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.finder.name
}

resource "snowflake_resource_monitor" "finder" {
  name            = "FINDER_MONTHLY"
  credit_quota    = var.monthly_credit_quota
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"
  notify_triggers = [80]
  suspend_trigger = 100
}

resource "snowflake_account_role" "transform" {
  name = "FINDER_TRANSFORM"
}

resource "snowflake_account_role" "reader" {
  name    = "FINDER_READER"
  comment = "the chatbot service user, read only on marts"
}

resource "snowflake_grant_privileges_to_account_role" "transform_wh" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transform_db" {
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.finder.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transform_schemas" {
  for_each          = snowflake_schema.layers
  account_role_name = snowflake_account_role.transform.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = each.value.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_marts" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = snowflake_schema.layers["MARTS"].fully_qualified_name
    }
  }
}
