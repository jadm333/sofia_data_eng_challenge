with plans as (
    select distinct
        plan_code,
        plan_name
    from {{ ref('stg_patients') }}
),
deduplicated as (

    select
        *,
        row_number() over (partition by plan_code) as rn
    from plans

)

select
    plan_code,
    plan_name
from deduplicated
