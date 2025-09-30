#  Use Case: Unemployment Insurance Analytics

## Overview

This use case demonstrates the analysis of a dummy unemployment insurance (WW - Werkloosheidswet) data. The implementation follows the existing medallion architecture (Bronze → Silver → Gold) to provide comprehensive insights into unemployment patterns, reintegration program effectiveness, and policy outcomes.

## Business Context

### About 
 This Dummy insurance agency is responsible for:
- Managing unemployment insurance (WW - Werkloosheidswet)
- Providing disability benefits (WIA/WAO)
- Supporting job seekers and employers
- Operating reintegration programs
- Collecting labor market data

### Business Value
This analytics solution enables  to:
- **Monitor Program Effectiveness**: Track reintegration success rates across demographics, regions, and industries
- **Optimize Resource Allocation**: Identify which programs and regions need more support
- **Support Policy Decisions**: Provide data-driven insights for unemployment insurance policy
- **Improve Employer Relations**: Analyze employer patterns and partnership effectiveness
- **Regional Planning**: Understand geographic unemployment patterns for targeted interventions

## Data Architecture

### Medallion Architecture Implementation

#### Bronze Layer (Raw Data Ingestion)
- **`bronze_ww_claims`**: Raw WW claims data
- **`bronze_employer_data`**: Raw employer characteristics and partnership information

#### Silver Layer (Data Transformation & Unification)
- **`silver_claims_analysis`**: Unified claims and employer data with standardized Dutch education levels, geographic regions, and derived analytics

#### Gold Layer (Business Analytics)
- **`gold_analytics`**: Executive dashboard metrics for policy analysis and program evaluation

## Key Features

### Dutch-Specific Data Elements
- **BSN (Burgerservicenummer)**: Social security numbers for unique citizen identification
- **Education Levels**: WO (University), HBO (Applied Sciences), MBO (Vocational), VMBO (Pre-vocational)
- **Geographic Hierarchy**: Provinces and municipalities
- **KvK Numbers**: Chamber of Commerce registration numbers for employers
- **Industry Classifications**: Aligned with labor market sectors

### Analytics Capabilities
- **Reintegration Success Analysis**: Track job placement rates by demographics and programs
- **Regional Unemployment Patterns**: Province and municipality-level insights
- **Employer Impact Analysis**: Company size, industry, and layoff pattern analysis
- **Benefit Duration Analytics**: Actual vs. planned benefit periods
- **Program Effectiveness**: ROI analysis of reintegration programs

## Data Model

### Core Entities

#### WW Claims (`ww_claims.csv`)
```csv
claim_id,bsn,age,gender,education_level,municipality,province,industry_sector,
job_title,salary_before_unemployment,claim_start_date,claim_end_date,
benefit_duration_weeks,benefit_amount_weekly,reintegration_program,
job_found_date,employer_before,employer_after,claim_reason
```

#### Employer Data (`employer_data.csv`)
```csv
employer_id,employer_name,kvk_number,industry_sector,province,municipality,
employee_count,annual_layoffs_2024,reintegration_partnerships
```

### Key Relationships
- Claims → Employers (via employer_before/employer_after)
- Geographic hierarchy (Municipality → Province)
- Temporal relationships (claim_start_date → job_found_date)

## Usage Examples

### 1. Provincial Reintegration Success Analysis
```sql
SELECT 
    dimension_1 as province,
    total_count as total_claims,
    successful_reintegrations,
    success_rate_pct,
    avg_claim_duration_weeks
FROM gold_analytics 
WHERE analysis_type = 'provincial'
ORDER BY success_rate_pct DESC;
```

### 2. Industry-Specific Unemployment Patterns
```sql
SELECT 
    dimension_1 as industry,
    total_count as layoffs,
    success_rate_pct as reintegration_rate,
    avg_claim_duration_weeks
FROM gold_analytics 
WHERE analysis_type = 'industry'
ORDER BY total_count DESC;
```

### 3. Education Level Impact on Employment Success
```sql
SELECT 
    dimension_3 as education_level,
    dimension_1 as age_group,
    dimension_2 as gender,
    success_rate_pct,
    total_count
FROM gold_analytics 
WHERE analysis_type = 'demographics'
ORDER BY success_rate_pct DESC;
```

### 4. Employer Size and Reintegration Partnership Analysis
```sql
SELECT 
    dimension_1 as company_size,
    dimension_2 as industry,
    total_count as total_layoffs,
    success_rate_pct,
    additional_metric_1 as employers_with_partnerships
FROM gold_analytics 
WHERE analysis_type = 'employer_characteristics'
ORDER BY success_rate_pct DESC;
```

## Running the  Use Case

### Prerequisites
1. dbt-core with supported adapters (DuckDB, SQL Server, Databricks, SQLite)
2. Python environment with requirements installed

### Execution Steps
```bash
# 1. Seed the  data
dbt seed --select ww_claims employer_data

# 2. Run Bronze layer (data ingestion)
dbt run --select tag: tag:bronze

# 3. Run Silver layer (data transformation)
dbt run --select silver_claims_analysis

# 4. Run Gold layer (analytics)
dbt run --select gold_analytics

# 5. Run all  models together
dbt run --select +gold_analytics

# 6. Test data quality
dbt test --select +gold_analytics
```

### Layered Execution
```bash
# Execute by medallion layer
dbt run --select tag: tag:bronze
dbt run --select tag: tag:silver  
dbt run --select tag: tag:gold
```

## Key Metrics & KPIs

### Primary Success Metrics
- **Reintegration Success Rate**: Percentage of claimants who find employment
- **Average Claim Duration**: Time from claim start to employment or claim end
- **Program Participation Rate**: Percentage enrolled in reintegration programs
- **Regional Success Variance**: Geographic differences in employment outcomes

### Secondary Metrics
- **Benefit Amount Analysis**: Total costs and duration patterns
- **Employer Partnership Impact**: Success rates for companies with reintegration partnerships
- **Education Level Correlation**: Employment success by education level
- **Industry Recovery Patterns**: Sector-specific reintegration rates

## Policy Insights

### Potential Analysis Questions
1. **Which provinces have the highest reintegration success rates?**
2. **Do reintegration programs significantly improve employment outcomes?**
3. **What is the correlation between education level and benefit duration?**
4. **Which industries show the best recovery patterns post-layoff?**
5. **How effective are employer reintegration partnerships?**
6. **What demographic groups need additional support?**

### Data-Driven Policy Recommendations
- Target additional resources to underperforming provinces
- Expand successful reintegration programs
- Develop industry-specific support programs
- Strengthen employer partnership initiatives
- Create education-level-specific interventions

## Technical Implementation Notes

### Cross-Database Compatibility
The  models use the existing cross-database macros:
- `xdb_now()`: Current timestamp across all supported databases
- `xdb_month_diff()`: Month calculations for claim duration analysis

### Data Quality Features
- Comprehensive test coverage for all models
- Data quality flags for inconsistencies
- Standardization of Dutch-specific values
- Referential integrity checks

### Performance Considerations
- Materialized as tables for fast analytics queries
- Proper indexing on key dimensions (province, industry, education_level)
- Aggregated analytics in Gold layer for dashboard performance

## Future Enhancements

### Potential Extensions
1. **Longitudinal Analysis**: Track individual career progression over time
2. **Seasonal Patterns**: Analyze unemployment trends by season/month
3. **Economic Indicators Integration**: Correlate with GDP, inflation, and job market data
4. **Machine Learning Models**: Predict reintegration success probability
5. **Real-time Dashboard**: Connect to BI tools for live monitoring
6. **Employer Recommendation Engine**: Suggest best-fit employers for claimants

### Additional Data Sources
- **CBS (Statistics Netherlands)**: Population and economic data
- **LinkedIn/Job Boards**: Market demand analysis  
- **Training Providers**: Skills development program outcomes
- **Municipal Data**: Local economic conditions and programs

## Compliance & Privacy

### Data Protection
- BSN handling follows privacy regulations (AVG/GDPR)
- Anonymization strategies for public reporting
- Secure data processing pipelines
- Audit trails for data lineage

### Reporting Standards
- Alignment with  annual reporting requirements
- Integration with government data standards

## Conclusion

The  use case demonstrates how modern data analytics can support evidence-based policy making in social security systems. By leveraging the medallion architecture, this solution provides scalable, maintainable, and insightful analysis of unemployment insurance effectiveness, supporting 's mission to help Dutch citizens transition back to meaningful employment.

The implementation showcases data handling while maintaining compatibility with multiple database platforms, making it a robust foundation for national employment policy analysis and program optimization.
