{{ config(materialized='view') }}

select
    dispatching_base_num,		
    pickup_datetime,		
    dropOff_datetime,		
    PUlocationID,
    DOlocationID,
    SR_Flag,
    Affiliated_base_number
from  {{ source('staging','fhv_tripdata_2019') }}



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}