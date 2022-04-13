{{ config (
materialized="table"
)}}
with __dbt__cte__STORE_LIST_DATA_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".WALMART."STORE_LIST"
select
    _AIRBYTE_STORE_LIST_HASHID,

        get_path(parse_json(table_alias.DATA), '"storesBySearchTerm"')
     as STORESBYSEARCHTERM,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".WALMART."STORE_LIST" as table_alias
-- DATA at store_list/data
where 1 = 1
and DATA is not null

),  __dbt__cte__STORE_LIST_DATA_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__STORE_LIST_DATA_AB1
select
    _AIRBYTE_STORE_LIST_HASHID,
    cast(STORESBYSEARCHTERM as
    variant
) as STORESBYSEARCHTERM,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__STORE_LIST_DATA_AB1
-- DATA at store_list/data
where 1 = 1

),  __dbt__cte__STORE_LIST_DATA_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__STORE_LIST_DATA_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_STORE_LIST_HASHID as
    varchar
), '') || '-' || coalesce(cast(STORESBYSEARCHTERM as
    varchar
), '') as
    varchar
)) as _AIRBYTE_DATA_HASHID,
    tmp.*
from __dbt__cte__STORE_LIST_DATA_AB2 tmp
-- DATA at store_list/data
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__STORE_LIST_DATA_AB3
select
    _AIRBYTE_STORE_LIST_HASHID,
    STORESBYSEARCHTERM,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_DATA_HASHID
from __dbt__cte__STORE_LIST_DATA_AB3
-- DATA at store_list/data from "DB".WALMART."STORE_LIST"
where 1 = 1
