{{ config (
materialized="table"
)}}
with __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".WALMART."STORE_LIST_DATA"
select
    _AIRBYTE_DATA_HASHID,
    get_path(parse_json(STORESBYSEARCHTERM), '"stores"') as STORES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".WALMART."STORE_LIST_DATA" as table_alias
-- STORESBYSEARCHTERM at store_list/data/storesBySearchTerm
where 1 = 1
and STORESBYSEARCHTERM is not null

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB1
select
    _AIRBYTE_DATA_HASHID,
    STORES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB1
-- STORESBYSEARCHTERM at store_list/data/storesBySearchTerm
where 1 = 1

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_DATA_HASHID as
    varchar
), '') || '-' || coalesce(cast(STORES as
    varchar
), '') as
    varchar
)) as _AIRBYTE_STORESBYSEARCHTERM_HASHID,
    tmp.*
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB2 tmp
-- STORESBYSEARCHTERM at store_list/data/storesBySearchTerm
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB3
select
    _AIRBYTE_DATA_HASHID,
    STORES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_STORESBYSEARCHTERM_HASHID
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_AB3
-- STORESBYSEARCHTERM at store_list/data/storesBySearchTerm from "DB".WALMART."STORE_LIST_DATA"
where 1 = 1
