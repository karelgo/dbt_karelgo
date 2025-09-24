/*
  Gold Layer - UWV Analytics Dashboard
  
  Purpose: Aggregates UWV unemployment insurance metrics for executive reporting and 
  policy analysis. Provides key insights on reintegration success, regional patterns,
  employer characteristics, and program effectiveness.
  
  Data Source: silver_uwv_claims_analysis
  Use Case: UWV executive dashboard, policy analysis, and performance monitoring
  
  Context: Supports Dutch employment policy decisions and UWV program evaluation
  with focus on Werkloosheidswet (unemployment insurance) effectiveness.
*/

{{ config(
    materialized='table',
    tags=['gold', 'uwv', 'analytics', 'dashboard'],
    column_types={
        '_calculated_at': 'datetime2(6)'
    }
) }}

with base_data as (
    select * from {{ ref('silver_uwv_claims_analysis') }}
),

provincial_analysis as (
    select
        province,
        count(*) as total_ww_claims,
        count(distinct citizen_service_number) as unique_claimants,
        sum(case when job_found = 1 then 1 else 0 end) as successful_reintegrations,
        sum(case when reintegration_success = 1 then 1 else 0 end) as timely_reintegrations,
        sum(case when reintegration_program = 1 then 1 else 0 end) as participated_in_programs,
        avg(actual_claim_duration_weeks) as avg_claim_duration_weeks,
        avg(total_benefit_amount) as avg_total_benefit_amount,
        sum(total_benefit_amount) as total_benefits_paid,
        avg(salary_before_unemployment) as avg_salary_before_unemployment,
        min(total_benefit_amount) as min_benefit_amount,
        max(total_benefit_amount) as max_benefit_amount
    from base_data
    group by province
),

industry_analysis as (
    select
        industry_sector,
        count(*) as total_ww_claims,
        sum(case when job_found = 1 then 1 else 0 end) as successful_reintegrations,
        case 
            when count(*) = 0 then 0 
            else round(100.0 * sum(case when job_found = 1 then 1 else 0 end) / count(*), 2) 
        end as reintegration_success_rate_pct,
        avg(actual_claim_duration_weeks) as avg_claim_duration_weeks,
        avg(total_benefit_amount) as avg_benefit_amount,
        avg(salary_before_unemployment) as avg_salary_before_unemployment
    from base_data
    group by industry_sector
),

demographic_analysis as (
    select
        age_group,
        gender,
        education_level_standardized,
        count(*) as total_ww_claims,
        sum(case when job_found = 1 then 1 else 0 end) as successful_reintegrations,
        case 
            when count(*) = 0 then 0 
            else round(100.0 * sum(case when job_found = 1 then 1 else 0 end) / count(*), 2) 
        end as reintegration_success_rate_pct,
        avg(actual_claim_duration_weeks) as avg_claim_duration_weeks,
        avg(salary_before_unemployment) as avg_salary_before_unemployment
    from base_data
    group by age_group, gender, education_level_standardized
),

employer_analysis as (
    select
        employer_size_category,
        industry_sector,
        count(*) as total_layoffs,
        sum(case when job_found = 1 then 1 else 0 end) as successful_reintegrations,
        case 
            when count(*) = 0 then 0 
            else round(100.0 * sum(case when job_found = 1 then 1 else 0 end) / count(*), 2) 
        end as reintegration_success_rate_pct,
        avg(actual_claim_duration_weeks) as avg_claim_duration_weeks,
        sum(case when employer_has_reintegration = 1 then 1 else 0 end) as employers_with_partnerships,
        avg(annual_layoffs_2024) as avg_annual_layoffs
    from base_data
    where employer_size_category != 'Unknown'
    group by employer_size_category, industry_sector
),

claim_reason_analysis as (
    select
        claim_reason,
        count(*) as total_claims,
        sum(case when job_found = 1 then 1 else 0 end) as successful_reintegrations,
        case 
            when count(*) = 0 then 0 
            else round(100.0 * sum(case when job_found = 1 then 1 else 0 end) / count(*), 2) 
        end as reintegration_success_rate_pct,
        avg(actual_claim_duration_weeks) as avg_claim_duration_weeks,
        avg(total_benefit_amount) as avg_benefit_amount
    from base_data
    group by claim_reason
),

reintegration_program_analysis as (
    select
        case when reintegration_program = 1 then 'Participated' else 'Did not participate' end as program_participation,
        count(*) as total_claimants,
        sum(case when job_found = 1 then 1 else 0 end) as found_employment,
        case 
            when count(*) = 0 then 0 
            else round(100.0 * sum(case when job_found = 1 then 1 else 0 end) / count(*), 2) 
        end as employment_success_rate_pct,
        avg(actual_claim_duration_weeks) as avg_claim_duration_weeks
    from base_data
    where reintegration_program is not null
    group by reintegration_program
)

-- Combine all analysis into a unified gold table with consistent structure
select
    'provincial' as analysis_type,
    province as dimension_1,
    null as dimension_2,
    null as dimension_3,
    total_ww_claims as total_count,
    successful_reintegrations,
    round(100.0 * successful_reintegrations / total_ww_claims, 2) as success_rate_pct,
    avg_claim_duration_weeks,
    avg_total_benefit_amount as avg_metric_1,
    total_benefits_paid as total_metric_1,
    avg_salary_before_unemployment as avg_metric_2,
    participated_in_programs as additional_metric_1,
    {{ xdb_now() }} as _calculated_at
from provincial_analysis

union all

select
    'industry' as analysis_type,
    industry_sector as dimension_1,
    null as dimension_2,
    null as dimension_3,
    total_ww_claims as total_count,
    successful_reintegrations,
    reintegration_success_rate_pct as success_rate_pct,
    avg_claim_duration_weeks,
    avg_benefit_amount as avg_metric_1,
    null as total_metric_1,
    avg_salary_before_unemployment as avg_metric_2,
    null as additional_metric_1,
    {{ xdb_now() }} as _calculated_at
from industry_analysis

union all

select
    'demographics' as analysis_type,
    age_group as dimension_1,
    gender as dimension_2,
    education_level_standardized as dimension_3,
    total_ww_claims as total_count,
    successful_reintegrations,
    reintegration_success_rate_pct as success_rate_pct,
    avg_claim_duration_weeks,
    null as avg_metric_1,
    null as total_metric_1,
    avg_salary_before_unemployment as avg_metric_2,
    null as additional_metric_1,
    {{ xdb_now() }} as _calculated_at
from demographic_analysis

union all

select
    'employer_characteristics' as analysis_type,
    employer_size_category as dimension_1,
    industry_sector as dimension_2,
    null as dimension_3,
    total_layoffs as total_count,
    successful_reintegrations,
    reintegration_success_rate_pct as success_rate_pct,
    avg_claim_duration_weeks,
    avg_annual_layoffs as avg_metric_1,
    null as total_metric_1,
    null as avg_metric_2,
    employers_with_partnerships as additional_metric_1,
    {{ xdb_now() }} as _calculated_at
from employer_analysis

union all

select
    'claim_reasons' as analysis_type,
    claim_reason as dimension_1,
    null as dimension_2,
    null as dimension_3,
    total_claims as total_count,
    successful_reintegrations,
    reintegration_success_rate_pct as success_rate_pct,
    avg_claim_duration_weeks,
    avg_benefit_amount as avg_metric_1,
    null as total_metric_1,
    null as avg_metric_2,
    null as additional_metric_1,
    {{ xdb_now() }} as _calculated_at
from claim_reason_analysis

union all

select
    'reintegration_programs' as analysis_type,
    program_participation as dimension_1,
    null as dimension_2,
    null as dimension_3,
    total_claimants as total_count,
    found_employment as successful_reintegrations,
    employment_success_rate_pct as success_rate_pct,
    avg_claim_duration_weeks,
    null as avg_metric_1,
    null as total_metric_1,
    null as avg_metric_2,
    null as additional_metric_1,
    {{ xdb_now() }} as _calculated_at
from reintegration_program_analysis