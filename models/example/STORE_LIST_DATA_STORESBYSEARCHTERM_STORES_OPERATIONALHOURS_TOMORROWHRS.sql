{{ config (
materialized="table"
)}}
with __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS"
select
    _AIRBYTE_OPERATIONALHOURS_HASHID,
    to_varchar(get_path(parse_json(TOMORROWHRS), '"endHr"')) as ENDHR,
    to_varchar(get_path(parse_json(TOMORROWHRS), '"startHr"')) as STARTHR,

        get_path(parse_json(table_alias.TOMORROWHRS), '"openFullDay"')
     as OPENFULLDAY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS" as table_alias
-- TOMORROWHRS at store_list/data/storesBySearchTerm/stores/operationalHours/tomorrowHrs
where 1 = 1
and TOMORROWHRS is not null

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB1
select
    _AIRBYTE_OPERATIONALHOURS_HASHID,
    cast(ENDHR as
    varchar
) as ENDHR,
    cast(STARTHR as
    varchar
) as STARTHR,
    OPENFULLDAY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB1
-- TOMORROWHRS at store_list/data/storesBySearchTerm/stores/operationalHours/tomorrowHrs
where 1 = 1

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_OPERATIONALHOURS_HASHID as
    varchar
), '') || '-' || coalesce(cast(ENDHR as
    varchar
), '') || '-' || coalesce(cast(STARTHR as
    varchar
), '') || '-' || coalesce(cast(OPENFULLDAY as
    varchar
), '') as
    varchar
)) as _AIRBYTE_TOMORROWHRS_HASHID,
    tmp.*
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB2 tmp
-- TOMORROWHRS at store_list/data/storesBySearchTerm/stores/operationalHours/tomorrowHrs
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB3
select
    _AIRBYTE_OPERATIONALHOURS_HASHID,
    ENDHR,
    STARTHR,
    OPENFULLDAY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_TOMORROWHRS_HASHID
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS_TOMORROWHRS_AB3
-- TOMORROWHRS at store_list/data/storesBySearchTerm/stores/operationalHours/tomorrowHrs from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_OPERATIONALHOURS"
where 1 = 1