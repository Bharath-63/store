{{ config (
materialized="table"
)}}
with __dbt__cte__STORE_LIST_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".WALMART._AIRBYTE_RAW_STORE_LIST
select

        get_path(parse_json(table_alias._airbyte_data), '"data"')
     as DATA,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".WALMART._AIRBYTE_RAW_STORE_LIST as table_alias
-- STORE_LIST
where 1 = 1

),  __dbt__cte__STORE_LIST_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__STORE_LIST_AB1
select
    cast(DATA as
    variant
) as DATA,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__STORE_LIST_AB1
-- STORE_LIST
where 1 = 1

),  __dbt__cte__STORE_LIST_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__STORE_LIST_AB2
select
    md5(cast(coalesce(cast(DATA as
    varchar
), '') as
    varchar
)) as _AIRBYTE_STORE_LIST_HASHID,
    tmp.*
from __dbt__cte__STORE_LIST_AB2 tmp
-- STORE_LIST
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__STORE_LIST_AB3
select
    DATA,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_STORE_LIST_HASHID
from __dbt__cte__STORE_LIST_AB3
-- STORE_LIST from "DB".WALMART._AIRBYTE_RAW_STORE_LIST
where 1 = 1
