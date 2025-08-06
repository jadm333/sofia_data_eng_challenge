with source as (

    select * from {{ ref('claim_providers') }}

),

trim_and_cast as (

    select
        trim(claim_id) as claim_id,
        trim(provider_id) as provider_id,
        trim(provider_role) as provider_role

    from source

),

deduplicated as (

    select
        *,
        row_number() over (partition by claim_id, provider_id order by claim_id) as rn
    from trim_and_cast

)

select
    claim_id,
    provider_id,
    provider_role
from deduplicated
where rn = 1
