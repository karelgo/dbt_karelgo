# dbt MVP Project

This is a minimal dbt MVP setup that can run on **Microsoft Fabric**, **Databricks**, and **DuckDB**. The project implements a comprehensive **medallion architecture** for modern data processing.

## 🏗️ Medallion Architecture

This project implements a three-layer medallion architecture:

- **🥉 Bronze Layer**: Raw data ingestion with minimal transformation
- **🥈 Silver Layer**: Cleaned and standardized data ready for business use  
- **🥇 Gold Layer**: Business-ready aggregated data for reporting and analytics

For detailed information about the architecture, see [MEDALLION_ARCHITECTURE.md](MEDALLION_ARCHITECTURE.md).

## Structure
- `models/bronze/` → Raw data layer with minimal transformation
- `models/silver/` → Cleaned and standardized data layer
- `models/gold/` → Business-ready aggregated data layer
- `models/` → Example models with schema tests
- `seeds/` → Example seed data (enhanced with business scenarios)
- `requirements.txt` → Python dependencies
- `.github/workflows/dbt.yml` → CI/CD pipeline for dbt (cloud databases)
- `.github/workflows/dbt-duckdb-ci.yml` → CI/CD pipeline for dbt using DuckDB (fast testing)
- `profiles.yml` → dbt connection config (generated dynamically in CI)
- `MEDALLION_ARCHITECTURE.md` → Detailed architecture documentation

## Usage
1. Install dependencies: `pip install -r requirements.txt`
2. Run locally with: `dbt run`
3. CI/CD runs on push/PR and deploys docs to GitHub Pages.

### Running the Medallion Architecture
```bash
# Run the complete pipeline
dbt seed && dbt run && dbt test

# Run by layer
dbt run --select tag:bronze    # Raw data layer
dbt run --select tag:silver    # Cleaned data layer  
dbt run --select tag:gold      # Business data layer

# Run specific domains
dbt run --select tag:sales     # Sales-related models
dbt run --select tag:customers # Customer-related models
dbt run --select tag:products  # Product-related models
```

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
