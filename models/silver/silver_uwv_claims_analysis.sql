/*
  Silver Layer - UWV Claims Analysis
  
  Purpose: Joins UWV WW claims data with employer information to create a unified dataset
  for analyzing unemployment patterns, reintegration success, and employer characteristics.
  Performs data cleansing, standardization, and derives meaningful metrics.
  
  Data Sources: bronze_uwv_ww_claims, bronze_uwv_employer_data
  Next Layer: Gold layer (gold_uwv_analytics)
  
  Context: This model supports analysis of Dutch unemployment insurance effectiveness,
  regional unemployment patterns, and reintegration program success rates.
*/

{{ config(
    materialized='table',
    tags=['silver', 'uwv', 'claims_analysis', 'transformed'],
    column_types={
        '_last_updated': 'datetime2(6)',
        '_processed_at': 'datetime2(6)'
    }
) }}

with claims as (
    select
        claim_id,
        citizen_service_number,
        age,
        gender,
        education_level,
        municipality,
        province,
        industry_sector,
        job_title,
        salary_before_unemployment,
        employer_before,
        employer_after,
        claim_start_date,
        claim_end_date,
        benefit_duration_weeks,
        benefit_amount_weekly,
        claim_reason,
        reintegration_program,
        job_found_date,
        _loaded_at as claims_loaded_at
    from {{ ref('bronze_uwv_ww_claims') }}
),

employers_before as (
    select
        employer_name,
        kvk_number,
        industry_sector as employer_industry,
        province as employer_province,
        municipality as employer_municipality,
        employee_count,
        annual_layoffs_2024,
        reintegration_partnerships,
        _loaded_at as employer_loaded_at
    from {{ ref('bronze_uwv_employer_data') }}
),

employers_after as (
    select
        employer_name,
        industry_sector as new_employer_industry,
        province as new_employer_province,
        employee_count as new_employer_size
    from {{ ref('bronze_uwv_employer_data') }}
)

select
    -- Primary identifiers
    claims.claim_id,
    claims.citizen_service_number,
    
    -- Personal demographics (standardized)
    claims.age,
    upper(trim(claims.gender)) as gender,
    case 
        when upper(claims.education_level) = 'WO' then 'University (WO)'
        when upper(claims.education_level) = 'HBO' then 'Applied Sciences (HBO)'
        when upper(claims.education_level) = 'MBO' then 'Vocational (MBO)'
        when upper(claims.education_level) = 'VMBO' then 'Pre-vocational (VMBO)'
        when upper(claims.education_level) = 'HAVO' then 'General Secondary (HAVO)'
        when upper(claims.education_level) = 'VWO' then 'Pre-university (VWO)'
        else coalesce(claims.education_level, 'Unknown')
    end as education_level_standardized,
    upper(trim(claims.municipality)) as municipality,
    upper(trim(claims.province)) as province,
    
    -- Employment details
    upper(trim(claims.industry_sector)) as industry_sector,
    claims.job_title,
    claims.salary_before_unemployment,
    claims.employer_before,
    claims.employer_after,
    
    -- Claim information
    claims.claim_start_date,
    claims.claim_end_date,
    claims.benefit_duration_weeks,
    claims.benefit_amount_weekly,
    claims.benefit_duration_weeks * claims.benefit_amount_weekly as total_benefit_amount,
    upper(trim(claims.claim_reason)) as claim_reason,
    
    -- Reintegration and success metrics
    claims.reintegration_program,
    claims.job_found_date,
    case when claims.job_found_date is not null then 1 else 0 end as job_found,
    case 
        when claims.job_found_date is not null and claims.claim_end_date is not null then
            case when claims.job_found_date <= claims.claim_end_date then 1 else 0 end
        else null
    end as reintegration_success,
    
    -- Duration calculations (in weeks)
    case 
        when claims.claim_end_date is not null then
            {{ xdb_month_diff('claims.claim_start_date', 'claims.claim_end_date') }} * 4.33  -- Convert months to weeks
        else claims.benefit_duration_weeks
    end as actual_claim_duration_weeks,
    
    -- Employer characteristics (before unemployment)
    employers_before.kvk_number as employer_kvk,
    employers_before.employer_industry,
    employers_before.employer_province,
    employers_before.employee_count as employer_size,
    employers_before.annual_layoffs_2024,
    employers_before.reintegration_partnerships as employer_has_reintegration,
    
    -- New employer characteristics (after reintegration)
    employers_after.new_employer_industry,
    employers_after.new_employer_province,
    employers_after.new_employer_size,
    
    -- Derived analytical categories
    case 
        when claims.age between 18 and 25 then '18-25'
        when claims.age between 26 and 35 then '26-35'
        when claims.age between 36 and 45 then '36-45'
        when claims.age between 46 and 55 then '46-55'
        when claims.age between 56 and 65 then '56-65'
        else '65+'
    end as age_group,
    
    case 
        when claims.salary_before_unemployment < 2500 then 'Low (< €2,500)'
        when claims.salary_before_unemployment between 2500 and 4000 then 'Medium (€2,500-€4,000)'
        when claims.salary_before_unemployment between 4001 and 6000 then 'High (€4,001-€6,000)'
        when claims.salary_before_unemployment > 6000 then 'Very High (> €6,000)'
        else 'Unknown'
    end as salary_bracket,
    
    case 
        when employers_before.employee_count < 50 then 'Small (< 50)'
        when employers_before.employee_count between 50 and 250 then 'Medium (50-250)'
        when employers_before.employee_count between 251 and 1000 then 'Large (251-1000)'
        when employers_before.employee_count > 1000 then 'Very Large (> 1000)'
        else 'Unknown'
    end as employer_size_category,
    
    -- Data quality flags
    case when claims.municipality != employers_before.employer_municipality then 1 else 0 end as municipality_mismatch,
    case when claims.industry_sector != employers_before.employer_industry then 1 else 0 end as industry_mismatch,
    case when claims.benefit_amount_weekly is null or claims.benefit_amount_weekly <= 0 then 1 else 0 end as invalid_benefit_amount,
    case when claims.age < 18 or claims.age > 67 then 1 else 0 end as questionable_age,
    
    -- Metadata
    case 
        when claims.claims_loaded_at >= employers_before.employer_loaded_at then claims.claims_loaded_at
        else employers_before.employer_loaded_at
    end as _last_updated,
    {{ xdb_now() }} as _processed_at

from claims
left join employers_before on claims.employer_before = employers_before.employer_name
left join employers_after on claims.employer_after = employers_after.employer_name