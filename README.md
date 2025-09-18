# dbt MVP Project

This is a minimal dbt MVP setup that can run on **Microsoft Fabric** and **Databricks**.

## Structure
- `models/` → Example models
- `seeds/` → Example seed data
- `requirements.txt` → Python dependencies
- `.github/workflows/dbt.yml` → CI/CD pipeline for dbt
- `profiles.yml` → dbt connection config (generated dynamically in CI)

## Usage
1. Install dependencies: `pip install -r requirements.txt`
2. Run locally with: `dbt run`
3. CI/CD runs on push/PR and deploys docs to GitHub Pages.

Switch between Fabric and Databricks by setting the GitHub secret `DBT_TARGET`.
