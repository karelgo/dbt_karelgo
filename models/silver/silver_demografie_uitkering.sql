/*
    Zilverlaag - Geünificeerde demografie- en uitkeringsdata
  
    Doel: Voegt demografische cliëntgegevens samen met uitkeringsdetails
    tot één dataset. Voert opschoning en standaardisatie uit, en behandelt
    ontbrekende waarden voor betere datakwaliteit.
  
        Databronnen: brons_persoonlijke_client_data, brons_uitkerings_data
    Volgende laag: Goud (goud_uitkerings_analysis)
*/

{{ config(
        materialized='table',
    tags=['zilver', 'demografie', 'uitkeringen', 'geuniformeerd'],
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
    from {{ ref('brons_persoonlijke_client_data') }}
),

benefits as (
    select
        client_id,
        benefit_type,
        start_date,
        end_date,
        duration_months,
    case 
    when lower(ltrim(rtrim(transition_to_work))) in ('ja','j','yes','y','true','waar','1') then 1
    when lower(ltrim(rtrim(transition_to_work))) in ('nee','n','no','false','onwaar','0') then 0
    else null
    end as transition_to_work,
        benefit_amount,
        region as benefit_region,
        _loaded_at as benefits_loaded_at
    from {{ ref('brons_uitkerings_data') }}
)

select
    -- Primary keys and identifiers
    demographics.client_id,
    
    -- Demographics (cleaned and standardized)
    demographics.age,
    upper(trim(demographics.gender)) as gender,
    case 
        when lower(demographics.education_level) like '%bachelor%' then 'Bachelor'
        when lower(demographics.education_level) like '%master%' then 'Master'
        when lower(demographics.education_level) like '%phd%' or lower(demographics.education_level) like '%dr%' then 'Doctoraat'
        when lower(demographics.education_level) like '%middelbare%' or lower(demographics.education_level) like '%voortgezet%' then 'Middelbare school'
        else coalesce(demographics.education_level, 'Onbekend')
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
            {{ xdb_month_diff('CAST(benefits.start_date AS date)', 'CAST(benefits.end_date AS date)') }}
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
        when demographics.years_experience >= 15 then 'Senior (15+ jaar)'
        when demographics.years_experience >= 5 then 'Medior (5-14 jaar)'
        when demographics.years_experience >= 1 then 'Junior (1-4 jaar)'
        else 'Instapniveau (0 jaar)'
    end as experience_level,
    
    -- Metadata
    CASE 
        WHEN demographics.demographics_loaded_at >= benefits.benefits_loaded_at THEN demographics.demographics_loaded_at
        ELSE benefits.benefits_loaded_at
    END as _last_updated,
    {{ xdb_now() }} as _processed_at

from demographics
inner join benefits on demographics.client_id = benefits.client_id