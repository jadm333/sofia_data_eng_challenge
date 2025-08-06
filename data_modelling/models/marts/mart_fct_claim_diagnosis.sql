with source as (

    select
        claim_id,
        diagnosis_codes
    from {{ ref('stg_claims') }}
    where diagnosis_codes is not null 
        and len(diagnosis_codes) > 0

),

unnested as (

    select
        claim_id,
        unnest(diagnosis_codes) as diagnosis_code
    from source

)
select
    claim_id,
    trim(diagnosis_code) as diagnosis_code
from unnested
where diagnosis_code is not null 
    and diagnosis_code != ''