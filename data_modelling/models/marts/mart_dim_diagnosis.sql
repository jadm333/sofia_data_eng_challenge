with source as (

    select
        diagnosis_codes,
        diagnosis_descriptions
    from {{ ref('stg_claims') }}
    where diagnosis_codes is not null 
        and diagnosis_descriptions is not null
        and len(diagnosis_codes) > 0
        and len(diagnosis_descriptions) > 0

),

unnested as (

    select
        unnest(diagnosis_codes) as diagnosis_code,
        unnest(diagnosis_descriptions) as diagnosis_description
    from source

),
trimmed as (

    select
        trim(diagnosis_code) as diagnosis_code,
        trim(diagnosis_description) as diagnosis_description
    from unnested
),
deduplicated as (

    select distinct
        *,
        row_number() over (partition by diagnosis_code) as rn
    from trimmed
    where diagnosis_code is not null 
          and diagnosis_code != ''
)
select
    diagnosis_code,
    diagnosis_description
from deduplicated
where rn = 1
