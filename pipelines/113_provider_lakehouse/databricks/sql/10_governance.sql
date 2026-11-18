-- Analyst access to the provider dimension, Unity Catalog version.
-- Same rules as src/provider_lake/governance.py, which is how they run locally.
--   provider_admins  see everything
--   analysts         only the states granted to them, and for individual providers
--                    (entity type 1) no street address or phone, ZIP cut to 3 digits
-- NPPES itself is public. The point is the pattern, which is the same for member data.

create or replace function provider_lake.gov.mask_individual(value string, entity_type string)
returns string
return case when is_account_group_member('provider_admins') or entity_type = '2' then value else '***' end;

create or replace function provider_lake.gov.zip3_for_individuals(value string, entity_type string)
returns string
return case when is_account_group_member('provider_admins') or entity_type = '2' then value
            else substr(value, 1, 3) end;

create or replace function provider_lake.gov.state_filter(practice_state string)
returns boolean
return is_account_group_member('provider_admins')
    or exists (select 1 from provider_lake.ops.analyst_state_access a
               where a.user_name = current_user() and a.state = practice_state);

alter table provider_lake.gold.dim_provider alter column practice_address_1
  set mask provider_lake.gov.mask_individual using columns (entity_type);
alter table provider_lake.gold.dim_provider alter column practice_address_2
  set mask provider_lake.gov.mask_individual using columns (entity_type);
alter table provider_lake.gold.dim_provider alter column practice_phone
  set mask provider_lake.gov.mask_individual using columns (entity_type);
alter table provider_lake.gold.dim_provider alter column mail_address_1
  set mask provider_lake.gov.mask_individual using columns (entity_type);
alter table provider_lake.gold.dim_provider alter column mail_address_2
  set mask provider_lake.gov.mask_individual using columns (entity_type);
alter table provider_lake.gold.dim_provider alter column practice_postal_code
  set mask provider_lake.gov.zip3_for_individuals using columns (entity_type);
alter table provider_lake.gold.dim_provider
  set row filter provider_lake.gov.state_filter on (practice_state);

-- analysts read gold only, never bronze, silver or ops
grant use catalog on catalog provider_lake to `provider_analysts`;
grant use schema on schema provider_lake.gold to `provider_analysts`;
grant select on schema provider_lake.gold to `provider_analysts`;
grant execute on function provider_lake.gov.mask_individual to `provider_analysts`;
grant execute on function provider_lake.gov.zip3_for_individuals to `provider_analysts`;
grant execute on function provider_lake.gov.state_filter to `provider_analysts`;
