{{ config (
materialized="table"
)}}
with __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES"
select
    _AIRBYTE_STORES_HASHID,
    to_varchar(get_path(parse_json(ADDRESS), '"city"')) as CITY,
    to_varchar(get_path(parse_json(ADDRESS), '"state"')) as STATE,
    to_varchar(get_path(parse_json(ADDRESS), '"address"')) as ADDRESS,
    to_varchar(get_path(parse_json(ADDRESS), '"country"')) as COUNTRY,
    to_varchar(get_path(parse_json(ADDRESS), '"postalCode"')) as POSTALCODE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES" as table_alias
-- ADDRESS at store_list/data/storesBySearchTerm/stores/address
where 1 = 1
and ADDRESS is not null

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB1
select
    _AIRBYTE_STORES_HASHID,
    cast(CITY as
    varchar
) as CITY,
    cast(STATE as
    varchar
) as STATE,
    cast(ADDRESS as
    varchar
) as ADDRESS,
    cast(COUNTRY as
    varchar
) as COUNTRY,
    cast(POSTALCODE as
    varchar
) as POSTALCODE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB1
-- ADDRESS at store_list/data/storesBySearchTerm/stores/address
where 1 = 1

),  __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_STORES_HASHID as
    varchar
), '') || '-' || coalesce(cast(CITY as
    varchar
), '') || '-' || coalesce(cast(STATE as
    varchar
), '') || '-' || coalesce(cast(ADDRESS as
    varchar
), '') || '-' || coalesce(cast(COUNTRY as
    varchar
), '') || '-' || coalesce(cast(POSTALCODE as
    varchar
), '') as
    varchar
)) as _AIRBYTE_ADDRESS_HASHID,
    tmp.*
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB2 tmp
-- ADDRESS at store_list/data/storesBySearchTerm/stores/address
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB3
select
    _AIRBYTE_STORES_HASHID,
    CITY,
    STATE,
    ADDRESS,
    COUNTRY,
    POSTALCODE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_ADDRESS_HASHID
from __dbt__cte__STORE_LIST_DATA_STORESBYSEARCHTERM_STORES_ADDRESS_AB3
-- ADDRESS at store_list/data/storesBySearchTerm/stores/address from "DB".WALMART."STORE_LIST_DATA_STORESBYSEARCHTERM_STORES"
where 1 = 1
