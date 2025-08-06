with source as (

    select * from {{ ref('claims') }}

),

trim_and_cast as (

    select
        trim(claim_id) as claim_id,
        trim(patient_id) as patient_id,
        cast(service_date as date) as service_date,
        cast(claim_submitted_date as date) as claim_submitted_date,
        trim(claim_type) as claim_type,
        trim(place_of_service) as place_of_service,
        from_json(json(diagnosis_codes),'["VARCHAR"]') as diagnosis_codes,
        from_json(json(diagnosis_descriptions),'["VARCHAR"]') as diagnosis_descriptions,
        from_json(json(procedure_codes),'["VARCHAR"]') as procedure_codes,
        from_json(json(procedure_descriptions),'["VARCHAR"]') as procedure_descriptions,
        cast(billed_amount as DOUBLE) as billed_amount,
        cast(allowed_amount as DOUBLE) as allowed_amount,
        cast(deductible_amount as DOUBLE) as deductible_amount,
        cast(coinsurance_amount as DOUBLE) as coinsurance_amount,
        cast(copay_amount as DOUBLE) as copay_amount,
        cast(insurance_paid_amount as DOUBLE) as insurance_paid_amount,
        cast(patient_responsibility as DOUBLE) as patient_responsibility,
        trim(claim_status) as claim_status,
        trim(denial_reason) as denial_reason,
        cast(is_in_network as boolean) as is_in_network,
        trim(network_tier) as network_tier,
        trim(prior_auth_number) as prior_auth_number,
        cast(processed_date as date) as processed_date

    from source

),

deduplicated as (

    select
        *,
        row_number() over (partition by claim_id order by service_date) as rn
    from trim_and_cast

)

select
    claim_id,
    patient_id,
    service_date,
    claim_submitted_date,
    claim_type,
    place_of_service,
    diagnosis_codes,
    diagnosis_descriptions,
    procedure_codes,
    procedure_descriptions,
    billed_amount,
    allowed_amount,
    deductible_amount,
    coinsurance_amount,
    copay_amount,
    insurance_paid_amount,
    patient_responsibility,
    claim_status,
    denial_reason,
    is_in_network,
    network_tier,
    prior_auth_number,
    processed_date
from deduplicated
where rn = 1