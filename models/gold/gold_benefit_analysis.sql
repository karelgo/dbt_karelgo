/*
  Gold Layer - Benefit Analysis Aggregations
  
  Purpose: Aggregates metrics for analytics including benefit recipients per region,
  average benefit duration, and transitions back to work analysis. This layer provides
  business-ready insights for stakeholders and reporting.
  
  Data Source: silver_demographics_benefit
  Use Case: Analytics dashboard for benefit program effectiveness
*/

{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'aggregations']
) }}

with base_data as (
    select * from {{ ref('silver_demographics_benefit') }}
),

regional_analysis as (
    select
        region,
        count(*) as total_recipients,
        count(distinct client_id) as unique_clients,
        count(case when transition_to_work = true then 1 end) as successful_transitions,
        count(case when transition_to_work = false then 1 end) as no_transitions,
        count(case when transition_to_work is null then 1 end) as unknown_transitions,
        avg(actual_duration_months) as avg_benefit_duration_months,
        avg(benefit_amount) as avg_benefit_amount,
        sum(benefit_amount) as total_benefit_amount,
        min(benefit_amount) as min_benefit_amount,
        max(benefit_amount) as max_benefit_amount
    from base_data
    group by region
),

benefit_type_analysis as (
    select
        benefit_type,
        count(*) as total_recipients,
        count(case when transition_to_work = true then 1 end) as successful_transitions,
        round(100.0 * count(case when transition_to_work = true then 1 end) / count(*), 2) as transition_success_rate_pct,
        avg(actual_duration_months) as avg_duration_months,
        avg(benefit_amount) as avg_benefit_amount
    from base_data
    group by benefit_type
),

demographic_analysis as (
    select
        age_group,
        gender,
        education_level_standardized,
        count(*) as total_recipients,
        count(case when transition_to_work = true then 1 end) as successful_transitions,
        round(100.0 * count(case when transition_to_work = true then 1 end) / count(*), 2) as transition_success_rate_pct,
        avg(actual_duration_months) as avg_duration_months
    from base_data
    group by age_group, gender, education_level_standardized
),

experience_analysis as (
    select
        experience_level,
        industry,
        count(*) as total_recipients,
        count(case when transition_to_work = true then 1 end) as successful_transitions,
        round(100.0 * count(case when transition_to_work = true then 1 end) / count(*), 2) as transition_success_rate_pct,
        avg(actual_duration_months) as avg_duration_months
    from base_data
    group by experience_level, industry
)

-- Combine all analysis into a unified gold table
select
    'regional' as analysis_type,
    region as dimension_1,
    null as dimension_2,
    null as dimension_3,
    total_recipients,
    successful_transitions,
    round(100.0 * successful_transitions / total_recipients, 2) as transition_success_rate_pct,
    avg_benefit_duration_months as avg_duration_months,
    avg_benefit_amount,
    total_benefit_amount,
    min_benefit_amount,
    max_benefit_amount,
    current_localtimestamp() as _calculated_at
from regional_analysis

union all

select
    'benefit_type' as analysis_type,
    benefit_type as dimension_1,
    null as dimension_2,
    null as dimension_3,
    total_recipients,
    successful_transitions,
    transition_success_rate_pct,
    avg_duration_months,
    avg_benefit_amount,
    null as total_benefit_amount,
    null as min_benefit_amount,
    null as max_benefit_amount,
    current_localtimestamp() as _calculated_at
from benefit_type_analysis

union all

select
    'demographics' as analysis_type,
    age_group as dimension_1,
    gender as dimension_2,
    education_level_standardized as dimension_3,
    total_recipients,
    successful_transitions,
    transition_success_rate_pct,
    avg_duration_months,
    null as avg_benefit_amount,
    null as total_benefit_amount,
    null as min_benefit_amount,
    null as max_benefit_amount,
    current_localtimestamp() as _calculated_at
from demographic_analysis

union all

select
    'experience' as analysis_type,
    experience_level as dimension_1,
    industry as dimension_2,
    null as dimension_3,
    total_recipients,
    successful_transitions,
    transition_success_rate_pct,
    avg_duration_months,
    null as avg_benefit_amount,
    null as total_benefit_amount,
    null as min_benefit_amount,
    null as max_benefit_amount,
    current_localtimestamp() as _calculated_at
from experience_analysis