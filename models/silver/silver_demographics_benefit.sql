/*
  Silver Layer - Demographics and Benefit Data Unified
  
  Purpose: Joins personal demographics data with benefit details to create
  a unified dataset. Performs data cleansing, standardization, and handles
  missing values for improved data quality.
  
  Data Sources: bronze_personal_client, bronze_benefit_data
  Next Layer: Gold layer (gold_benefit_analysis)
*/

{{ config(
        materialized='table',
        tags=['silver', 'demographics', 'benefits', 'unified'],
        column_types={
            '_last_updated': 'datetime2(6)',
            '_processed_at': 'datetime2(6)'
        }
) }}

with demographics as (
    select
        client_id,
        age,
        gender,
        education_level,
        region,
        industry,
        years_experience,
        last_employer,
        _loaded_at as demographics_loaded_at
    from {{ ref('bronze_personal_client') }}
),

benefits as (
    select
        client_id,
        benefit_type,
        start_date,
        end_date,
        duration_months,
        case 
        when lower(ltrim(rtrim(transition_to_work))) in ('yes','y','true','1') then 1
        when lower(ltrim(rtrim(transition_to_work))) in ('no','n','false','0') then 0
        else null
        end as transition_to_work,
        benefit_amount,
        region as benefit_region,
        _loaded_at as benefits_loaded_at
    from {{ ref('bronze_benefit_data') }}
)

select
    -- Primary keys and identifiers
    demographics.client_id,
    
    -- Demographics (cleaned and standardized)
    demographics.age,
    upper(trim(demographics.gender)) as gender,
    case 
        when lower(demographics.education_level) like '%bachelor%' then 'Bachelor''s Degree'
        when lower(demographics.education_level) like '%master%' then 'Master''s Degree'
        when lower(demographics.education_level) like '%phd%' or lower(demographics.education_level) like '%doctorate%' then 'Doctorate'
        when lower(demographics.education_level) like '%high school%' or lower(demographics.education_level) like '%secondary%' then 'High School'
        else coalesce(demographics.education_level, 'Unknown')
    end as education_level_standardized,
    upper(trim(demographics.region)) as region,
    upper(trim(demographics.industry)) as industry,
    demographics.years_experience,
    demographics.last_employer,
    
    -- Benefits information (cleaned)
    benefits.benefit_type,
    benefits.start_date,
    benefits.end_date,
    -- Calculate actual duration if end_date exists, otherwise use reported duration
    case 
        when benefits.end_date is not null then 
            DATEDIFF(MONTH, CAST(benefits.start_date AS date), CAST(benefits.end_date AS date))
        else benefits.duration_months
    end as actual_duration_months,
    benefits.duration_months as reported_duration_months,
    benefits.transition_to_work,
    benefits.benefit_amount,
    
    -- Data quality flags
    case when demographics.region != benefits.benefit_region then 1 else 0 end as region_mismatch,
    case when benefits.benefit_amount is null or benefits.benefit_amount <= 0 then 1 else 0 end as invalid_benefit_amount,
    case when demographics.age < 18 or demographics.age > 100 then 1 else 0 end as questionable_age,
    
    -- Derived metrics
    case 
        when demographics.age between 18 and 25 then '18-25'
        when demographics.age between 26 and 35 then '26-35'
        when demographics.age between 36 and 45 then '36-45'
        when demographics.age between 46 and 55 then '46-55'
        when demographics.age between 56 and 65 then '56-65'
        else '65+'
    end as age_group,
    
    case 
        when demographics.years_experience >= 15 then 'Senior (15+ years)'
        when demographics.years_experience >= 5 then 'Mid-level (5-14 years)'
        when demographics.years_experience >= 1 then 'Junior (1-4 years)'
        else 'Entry level (0 years)'
    end as experience_level,
    
    -- Metadata
    CASE 
        WHEN demographics.demographics_loaded_at >= benefits.benefits_loaded_at THEN CAST(demographics.demographics_loaded_at AS datetime2(6))
        ELSE CAST(benefits.benefits_loaded_at AS datetime2(6))
    END as _last_updated,
    CAST(SYSDATETIME() AS datetime2(6)) as _processed_at

from demographics
inner join benefits on demographics.client_id = benefits.client_id