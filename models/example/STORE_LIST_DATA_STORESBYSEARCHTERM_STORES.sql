{{ config (
materialized="table"
)}}
with __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM"

select
    _AIRBYTE_STORESBYSEARCHTERM_HASHID,
    to_varchar(get_path(parse_json(STORES.value), '"id"')) as ID,

        get_path(parse_json(STORES.value), '"address"')
     as ADDRESS,
    to_varchar(get_path(parse_json(STORES.value), '"distance"')) as DISTANCE,

        get_path(parse_json(STORES.value), '"geoPoint"')
     as GEOPOINT,
    to_varchar(get_path(parse_json(STORES.value), '"timeZone"')) as TIMEZONE,

        get_path(parse_json(STORES.value), '"storeType"')
     as STORETYPE,
    to_varchar(get_path(parse_json(STORES.value), '"displayName"')) as DISPLAYNAME,

        get_path(parse_json(STORES.value), '"operationalHours"')
     as OPERATIONALHOURS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM" as table_alias
-- STORES at store_list/data/storesBySearchTerm/stores
cross join table(flatten(STORES)) as STORES
where 1 = 1
and STORES is not null

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB1
select
    _AIRBYTE_STORESBYSEARCHTERM_HASHID,
    cast(ID as
    bigint
) as ID,
    cast(ADDRESS as
    variant
) as ADDRESS,
    cast(DISTANCE as
    float
) as DISTANCE,
    cast(GEOPOINT as
    variant
) as GEOPOINT,
    cast(TIMEZONE as
    varchar
) as TIMEZONE,
    cast(STORETYPE as
    variant
) as STORETYPE,
    cast(DISPLAYNAME as
    varchar
) as DISPLAYNAME,
    cast(OPERATIONALHOURS as
    variant
) as OPERATIONALHOURS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB1
-- STORES at store_list/data/storesBySearchTerm/stores
where 1 = 1

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_STORESBYSEARCHTERM_HASHID as
    varchar
), '') || '-' || coalesce(cast(ID as
    varchar
), '') || '-' || coalesce(cast(ADDRESS as
    varchar
), '') || '-' || coalesce(cast(DISTANCE as
    varchar
), '') || '-' || coalesce(cast(GEOPOINT as
    varchar
), '') || '-' || coalesce(cast(TIMEZONE as
    varchar
), '') || '-' || coalesce(cast(STORETYPE as
    varchar
), '') || '-' || coalesce(cast(DISPLAYNAME as
    varchar
), '') || '-' || coalesce(cast(OPERATIONALHOURS as
    varchar
), '') as
    varchar
)) as _AIRBYTE_STORES_HASHID,
    tmp.*
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB2 tmp
-- STORES at store_list/data/storesBySearchTerm/stores
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB3
select
    _AIRBYTE_STORESBYSEARCHTERM_HASHID,
    ID,
    ADDRESS,
    DISTANCE,
    GEOPOINT,
    TIMEZONE,
    STORETYPE,
    DISPLAYNAME,
    OPERATIONALHOURS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_STORES_HASHID
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_AB3
-- STORES at store_list/data/storesBySearchTerm/stores from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM"
where 1 = 1
