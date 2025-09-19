# dbt MVP Project

This is a minimal dbt MVP setup that can run on **Microsoft Fabric**, **Databricks**, and **DuckDB**.

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
