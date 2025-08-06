with source as (

    select * from {{ ref('patients') }}

),

renamed as (

    select
        trim(patient_id) as patient_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(maternal_surname) as maternal_surname,
        cast(date_of_birth as date) as date_of_birth,
        trim(gender) as gender,
        trim(curp) as curp,
        trim(street_address) as street_address,
        trim(city) as city,
        trim(state) as state,
        trim(zip_code) as zip_code,
        trim(email) as email,
        trim(plan_name) as plan_name,
        trim(plan_code) as plan_code,
        trim(policy_number) as policy_number,
        trim(group_id) as group_id,
        cast(enrollment_date as date) as enrollment_date,
        cast(is_active as boolean) as is_active,
        cast(annual_deductible as DOUBLE) as annual_deductible,
        cast(annual_out_of_pocket_max as DOUBLE) as annual_out_of_pocket_max

    from source

),

deduplicated as (

    select
        *,
        row_number() over (partition by patient_id order by enrollment_date) as rn
    from renamed

)

select
    patient_id,
    first_name,
    last_name,
    maternal_surname,
    date_of_birth,
    gender,
    curp,
    street_address,
    city,
    state,
    zip_code,
    email,
    plan_name,
    plan_code,
    policy_number,
    group_id,
    enrollment_date,
    is_active,
    annual_deductible,
    annual_out_of_pocket_max
from deduplicated
where rn = 1
