/*
  Bronze Layer -  Employer Data
  
  Purpose: Ingests raw employer data from  systems, including information about companies
  that have employees receiving unemployment benefits, their characteristics, and 
  reintegration partnerships.
  
  Data Source: employer_data seed file
  Next Layer: Silver layer (silver_claims_analysis)
  
  Context: This data helps analyze patterns in unemployment by employer characteristics
  and supports reintegration program effectiveness analysis.
*/

{{ config(
    materialized='table',
    tags=['bronze', '', 'employers', 'raw'],
    column_types={
      '_loaded_at': 'datetime2(6)'
    }
) }}

select
    -- Primary identifiers
    employer_id,
    employer_name,
    kvk_number,  -- Kamer van Koophandel (Dutch Chamber of Commerce number)
    
    -- Business characteristics
    industry_sector,
    province,
    municipality,
    cast(employee_count as integer) as employee_count,
    cast(annual_layoffs_2024 as integer) as annual_layoffs_2024,
    
    -- Reintegration partnerships
    case 
        when lower(trim(reintegration_partnerships)) in ('yes','y','true','1','ja') then 1
        when lower(trim(reintegration_partnerships)) in ('no','n','false','0','nee') then 0
        else null
    end as reintegration_partnerships,
    
    -- Data lineage metadata
    {{ xdb_now() }} as _loaded_at,
    'employer_data_seed' as _source_system
    
from {{ ref('employer_data') }}