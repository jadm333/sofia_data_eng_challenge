with source as (

    select * from {{ ref('providers') }}

),

renamed as (

    select
        trim(provider_id) as provider_id,
        trim(provider_name) as provider_name,
        trim(provider_type) as provider_type,
        trim(specialty) as specialty,
        cedula_profesional,
        trim(tax_id) as tax_id,
        trim(street_address) as street_address,
        trim(city) as city,
        trim(state) as state,
        trim(zip_code) as zip_code,
        trim(phone) as phone,
        trim(email) as email,
        trim(network_tier) as network_tier,
        cast(contract_start_date as date) as contract_start_date,
        cast(contract_end_date as date) as contract_end_date,
        cast(is_accepting_patients as boolean) as is_accepting_patients,
        cast(quality_rating as DOUBLE) as quality_rating

    from source

),

deduplicated as (

    select
        *,
        row_number() over (partition by provider_id order by contract_start_date) as rn
    from renamed

)

select
    provider_id,
    provider_name,
    provider_type,
    specialty,
    cedula_profesional,
    tax_id,
    street_address,
    city,
    state,
    zip_code,
    phone,
    email,
    network_tier,
    contract_start_date,
    contract_end_date,
    is_accepting_patients,
    quality_rating
from deduplicated
where rn = 1
