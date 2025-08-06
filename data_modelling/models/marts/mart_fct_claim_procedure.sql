with source as (

    select
        claim_id,
        procedure_codes
    from {{ ref('stg_claims') }}
    where procedure_codes is not null 
        and len(procedure_codes) > 0

),

unnested as (

    select
        claim_id,
        unnest(procedure_codes) as procedure_code
    from source

)

select
    claim_id,
    trim(procedure_code) as procedure_code
from unnested
where procedure_code is not null 
    and procedure_code != ''