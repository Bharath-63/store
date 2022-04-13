{{ config (
materialized="table"
)}}
with __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES"
select
    _AIRBYTE_STORES_HASHID,

        get_path(parse_json(table_alias.OPERATIONALHOURS), '"todayHrs"')
     as TODAYHRS,

        get_path(parse_json(table_alias.OPERATIONALHOURS), '"open24Hours"')
     as OPEN24HOURS,

        get_path(parse_json(table_alias.OPERATIONALHOURS), '"tomorrowHrs"')
     as TOMORROWHRS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES" as table_alias
-- OPERATIONALHOURS at store_list/data/storesBySearchTerm/stores/operationalHours
where 1 = 1
and OPERATIONALHOURS is not null

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB1
select
    _AIRBYTE_STORES_HASHID,
    cast(TODAYHRS as
    variant
) as TODAYHRS,
    OPEN24HOURS,
    cast(TOMORROWHRS as
    variant
) as TOMORROWHRS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB1
-- OPERATIONALHOURS at store_list/data/storesBySearchTerm/stores/operationalHours
where 1 = 1

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_STORES_HASHID as
    varchar
), '') || '-' || coalesce(cast(TODAYHRS as
    varchar
), '') || '-' || coalesce(cast(OPEN24HOURS as
    varchar
), '') || '-' || coalesce(cast(TOMORROWHRS as
    varchar
), '') as
    varchar
)) as _AIRBYTE_OPERATIONALHOURS_HASHID,
    tmp.*
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB2 tmp
-- OPERATIONALHOURS at store_list/data/storesBySearchTerm/stores/operationalHours
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB3
select
    _AIRBYTE_STORES_HASHID,
    TODAYHRS,
    OPEN24HOURS,
    TOMORROWHRS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OPERATIONALHOURS_HASHID
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_AB3
-- OPERATIONALHOURS at store_list/data/storesBySearchTerm/stores/operationalHours from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES"
where 1 = 1