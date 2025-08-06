with source as (

    select
        procedure_codes,
        procedure_descriptions
    from {{ ref('stg_claims') }}
    where procedure_codes is not null 
        and procedure_descriptions is not null
        and len(procedure_codes) > 0
        and len(procedure_descriptions) > 0

),

unnested as (

    select
        unnest(procedure_codes) as procedure_code,
        unnest(procedure_descriptions) as procedure_description
    from source

),
trimmed as (

    select
        trim(procedure_code) as procedure_code,
        trim(procedure_description) as procedure_description
    from unnested
),
deduplicated as (

    select distinct
        *,
        row_number() over (partition by procedure_code) as rn
    from trimmed
    where procedure_code is not null 
          and procedure_code != ''
)

select *
from deduplicated
where rn = 1


