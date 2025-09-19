# dbt MVP Project

This is a minimal dbt MVP setup that can run on **Microsoft Fabric**, **Databricks**, and **DuckDB**. 

The project implements a **medallion architecture** for analyzing the relationship between demographic characteristics, benefit types, and transitions back to work.

## Medallion Architecture

### Bronze Layer (Raw Data Ingestion)
The bronze layer contains raw data with minimal processing and data lineage tracking:

- **`bronze_personal_client`**: Raw demographic data from personal_client_data seed
- **`bronze_benefit_data`**: Raw benefit program data from benefit_data seed  

### Silver Layer (Data Cleansing and Transformation)
The silver layer performs data cleansing, standardization, and unification:

- **`silver_demographics_benefit`**: Unified dataset joining demographic and benefit data with:
  - Data standardization (education levels, gender, regions)
  - Data quality flags (region mismatches, invalid amounts, questionable ages)
  - Derived categorizations (age groups, experience levels)
  - Missing value handling

### Gold Layer (Aggregated Analytics)  
The gold layer provides business-ready analytics and insights:

- **`gold_benefit_analysis`**: Comprehensive analysis including:
  - Regional analysis: Recipients, transitions, and benefit amounts by region
  - Benefit type analysis: Success rates and duration by benefit type
  - Demographic analysis: Outcomes by age group, gender, and education
  - Experience analysis: Success rates by experience level and industry

## Use Case: Demographics-Benefit Analysis

This implementation addresses the specific use case of linking demographic characteristics to benefit types and analyzing transitions back to work. Key analytics include:

- Number of benefit recipients per region
- Average benefit duration across different demographics
- Transition success rates by benefit type and demographics

## Structure
- `models/` → Example models with schema tests
- `seeds/` → Example seed data
- `requirements.txt` → Python dependencies
- `.github/workflows/dbt.yml` → CI/CD pipeline for dbt (cloud databases)
- `.github/workflows/dbt-duckdb-ci.yml` → CI/CD pipeline for dbt using DuckDB (fast testing)
- `profiles.yml` → dbt connection config (generated dynamically in CI)

## Usage
1. Install dependencies: `pip install -r requirements.txt`
2. Run locally with: `dbt run`
3. CI/CD runs on push/PR and deploys docs to GitHub Pages.

### Local Development with DuckDB
For fast local development and testing:
```bash
# Set up local profile for DuckDB
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml <<EOF
mvp_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'database.duckdb'
      threads: 4
EOF

# Run in shell
source .venv/bin/activate

# Run dbt commands
dbt seed
dbt run
dbt test
dbt docs generate
```

### Cloud Deployment
Switch between Fabric and Databricks by setting the GitHub secret `DBT_TARGET`.

## CI/CD Pipelines
- **DuckDB CI** (`.github/workflows/dbt-duckdb-ci.yml`): Fast testing with DuckDB on every push/PR
- **Cloud CI** (`.github/workflows/dbt.yml`): Full deployment to cloud databases and docs to GitHub Pages
