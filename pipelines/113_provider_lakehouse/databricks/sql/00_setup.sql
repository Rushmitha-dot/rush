-- One-time setup, run by a workspace admin before the first deploy.
create catalog if not exists provider_lake;
create schema if not exists provider_lake.bronze;
create schema if not exists provider_lake.silver;
create schema if not exists provider_lake.gold;
create schema if not exists provider_lake.ops;
create schema if not exists provider_lake.gov;
create volume if not exists provider_lake.ops.files;
