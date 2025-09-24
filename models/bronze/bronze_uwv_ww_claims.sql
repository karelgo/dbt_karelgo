/*
  Bronze Layer - UWV WW (Werkloosheidswet) Claims Data
  
  Purpose: Ingests raw UWV unemployment insurance claims data from the Dutch Employee Insurance Agency.
  This layer stores WW claims information as received from UWV systems, with minimal processing 
  and added metadata for data lineage.
  
  Data Source: uwv_ww_claims seed file
  Next Layer: Silver layer (silver_uwv_claims_analysis)
  
  Context: WW (Werkloosheidswet) is the Dutch unemployment insurance law that provides temporary 
  income support for unemployed workers who meet certain conditions.
*/

{{ config(
    materialized='table',
    tags=['bronze', 'uwv', 'ww_claims', 'raw'],
    column_types={
      '_loaded_at': 'datetime2(6)'
    }
) }}

select
    -- Primary identifiers
    claim_id,
    bsn as citizen_service_number,  -- Burgerservicenummer (Dutch social security number)
    
    -- Personal demographics
    age,
    gender,
    education_level,
    municipality,
    province,
    
    -- Employment details
    industry_sector,
    job_title,
    cast(salary_before_unemployment as decimal(10,2)) as salary_before_unemployment,
    employer_before,
    employer_after,
    
    -- Claim information
    cast(claim_start_date as date) as claim_start_date,
    cast(claim_end_date as date) as claim_end_date,
    cast(benefit_duration_weeks as integer) as benefit_duration_weeks,
    cast(benefit_amount_weekly as decimal(8,2)) as benefit_amount_weekly,
    claim_reason,
    
    -- Reintegration details
    case 
        when lower(trim(reintegration_program)) in ('yes','y','true','1','ja') then 1
        when lower(trim(reintegration_program)) in ('no','n','false','0','nee') then 0
        else null
    end as reintegration_program,
    cast(job_found_date as date) as job_found_date,
    
    -- Data lineage metadata
    {{ xdb_now() }} as _loaded_at,
    'uwv_ww_claims_seed' as _source_system
    
from {{ ref('uwv_ww_claims') }}