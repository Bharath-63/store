{{ config (
materialized="table"
)}}
with __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES"
select
    _AIRBYTE_STORES_HASHID,
    to_varchar(get_path(parse_json(STORETYPE), '"id"')) as ID,
    to_varchar(get_path(parse_json(STORETYPE), '"displayName"')) as DISPLAYNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES" as table_alias
-- STORETYPE at store_list/data/storesBySearchTerm/stores/storeType
where 1 = 1
and STORETYPE is not null

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB1
select
    _AIRBYTE_STORES_HASHID,
    cast(ID as
    varchar
) as ID,
    cast(DISPLAYNAME as
    varchar
) as DISPLAYNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB1
-- STORETYPE at store_list/data/storesBySearchTerm/stores/storeType
where 1 = 1

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_STORES_HASHID as
    varchar
), '') || '-' || coalesce(cast(ID as
    varchar
), '') || '-' || coalesce(cast(DISPLAYNAME as
    varchar
), '') as
    varchar
)) as _AIRBYTE_STORETYPE_HASHID,
    tmp.*
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB2 tmp
-- STORETYPE at store_list/data/storesBySearchTerm/stores/storeType
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB3
select
    _AIRBYTE_STORES_HASHID,
    ID,
    DISPLAYNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_STORETYPE_HASHID
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_STORETYPE_AB3
-- STORETYPE at store_list/data/storesBySearchTerm/stores/storeType from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES"
where 1 = 1
