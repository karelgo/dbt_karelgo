--  Analytics Example Queries
-- These queries demonstrate key insights from the  unemployment insurance analytics

-- 1. Provincial Reintegration Success Analysis
-- Shows which Dutch provinces have the highest reintegration success rates
SELECT 
    dimension_1 as province,
    total_count as total_ww_claims,
    successful_reintegrations,
    success_rate_pct as reintegration_success_rate,
    ROUND(avg_claim_duration_weeks, 1) as avg_weeks_on_benefits,
    ROUND(avg_metric_1, 0) as avg_total_benefit_euros
FROM gold_analytics 
WHERE analysis_type = 'provincial'
ORDER BY success_rate_pct DESC;

-- 2. Industry Recovery Pattern Analysis  
-- Identifies which industries show best employment recovery after layoffs
SELECT 
    dimension_1 as industry_sector,
    total_count as total_layoffs,
    successful_reintegrations,
    success_rate_pct as reintegration_rate,
    ROUND(avg_claim_duration_weeks, 1) as avg_benefit_weeks,
    ROUND(avg_metric_2, 0) as avg_previous_salary_euros
FROM gold_analytics 
WHERE analysis_type = 'industry'
ORDER BY total_count DESC, success_rate_pct DESC;

-- 3. Education Level Impact on Employment Success
-- Analyzes how Dutch education levels correlate with reintegration success
SELECT 
    dimension_3 as education_level,
    dimension_1 as age_group,
    dimension_2 as gender,
    total_count as total_claimants,
    success_rate_pct as employment_success_rate,
    ROUND(avg_claim_duration_weeks, 1) as avg_benefit_weeks
FROM gold_analytics 
WHERE analysis_type = 'demographics'
  AND total_count >= 2  -- Filter for meaningful sample sizes
ORDER BY education_level, success_rate_pct DESC;

-- 4. Employer Size and Partnership Impact
-- Shows how company size and reintegration partnerships affect outcomes
SELECT 
    dimension_1 as company_size_category,
    dimension_2 as industry,
    total_count as total_layoffs,
    success_rate_pct as reintegration_success_rate,
    ROUND(avg_claim_duration_weeks, 1) as avg_benefit_weeks,
    additional_metric_1 as employers_with_partnerships
FROM gold_analytics 
WHERE analysis_type = 'employer_characteristics'
ORDER BY success_rate_pct DESC;

-- 5. Claim Reason Impact Analysis
-- Analyzes success rates by reason for unemployment
SELECT 
    dimension_1 as unemployment_reason,
    total_count as total_claims,
    successful_reintegrations,
    success_rate_pct as reintegration_success_rate,
    ROUND(avg_claim_duration_weeks, 1) as avg_benefit_weeks,
    ROUND(avg_metric_1, 0) as avg_total_benefit_euros
FROM gold_analytics 
WHERE analysis_type = 'claim_reasons'
ORDER BY success_rate_pct DESC;

-- 6. Reintegration Program Effectiveness
-- Compares outcomes for those who participated in reintegration programs vs. those who didn't
SELECT 
    dimension_1 as program_participation,
    total_count as total_claimants,
    successful_reintegrations as found_employment,
    success_rate_pct as employment_success_rate,
    ROUND(avg_claim_duration_weeks, 1) as avg_claim_duration_weeks
FROM gold_analytics 
WHERE analysis_type = 'reintegration_programs'
ORDER BY success_rate_pct DESC;

-- 7. Detailed Claims Analysis (from Silver layer)
-- Shows individual claim patterns with employer information
SELECT 
    claim_id,
    age,
    gender,
    education_level_standardized,
    province,
    industry_sector,
    salary_before_unemployment,
    ROUND(total_benefit_amount, 0) as total_benefit_euros,
    ROUND(actual_claim_duration_weeks, 1) as actual_weeks_on_benefit,
    CASE WHEN job_found = 1 THEN 'Yes' ELSE 'No' END as found_new_job,
    CASE WHEN reintegration_success = 1 THEN 'Yes' ELSE 'No' END as successful_reintegration,
    employer_size_category,
    CASE WHEN employer_has_reintegration = 1 THEN 'Yes' ELSE 'No' END as employer_partnership
FROM silver_claims_analysis
ORDER BY total_benefit_amount DESC
LIMIT 20;

-- 8. High-Performance Provinces for Policy Learning
-- Identifies provinces with above-average success rates for best practice analysis
WITH provincial_avg AS (
    SELECT AVG(success_rate_pct) as overall_avg_success_rate
    FROM gold_analytics 
    WHERE analysis_type = 'provincial'
)
SELECT 
    p.dimension_1 as province,
    p.total_count as total_claims,
    p.success_rate_pct as success_rate,
    ROUND(p.success_rate_pct - a.overall_avg_success_rate, 1) as performance_vs_average,
    ROUND(p.avg_claim_duration_weeks, 1) as avg_weeks_on_benefits
FROM gold_analytics p
CROSS JOIN provincial_avg a
WHERE p.analysis_type = 'provincial'
  AND p.success_rate_pct > a.overall_avg_success_rate
ORDER BY performance_vs_average DESC;

-- 9. Industry-Province Cross Analysis
-- Custom analysis combining industry and geographic patterns
SELECT 
    u.province,
    u.industry_sector,
    COUNT(*) as total_claims,
    SUM(CASE WHEN u.job_found = 1 THEN 1 ELSE 0 END) as successful_reintegrations,
    ROUND(100.0 * SUM(CASE WHEN u.job_found = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate_pct,
    ROUND(AVG(u.actual_claim_duration_weeks), 1) as avg_benefit_weeks,
    ROUND(AVG(u.salary_before_unemployment), 0) as avg_previous_salary
FROM silver_claims_analysis u
GROUP BY u.province, u.industry_sector
HAVING COUNT(*) >= 2  -- Ensure meaningful sample size
ORDER BY u.province, success_rate_pct DESC;