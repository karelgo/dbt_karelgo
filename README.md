# dbt MVP Project

Portable dbt medallion architecture running on **Microsoft Fabric / SQL Server**, **Databricks (Spark)**, **DuckDB**, and **SQLite**. It analyzes how demographic attributes relate to benefit program participation and transitions back to work.

## Use Cases

### 1. General Benefit Analysis
Analyzes demographic patterns in benefit program participation and work transition success rates across different regions and benefit types.

### 2.  (Dutch Unemployment Insurance) Analytics
**NEW**: Comprehensive analysis of Dutch unemployment insurance (WW - Werkloosheidswet) data from  (Uitvoeringsinstituut Werknemersverzekeringen). This use case provides specialized analytics for:
- Reintegration program effectiveness
- Provincial unemployment patterns  
- Employer characteristics and partnership impact
- Dutch-specific education level and geographic analysis

📋 **[View detailed  use case documentation](docs/_use_case.md)**

## Medallion Layers
### Bronze (Raw Ingestion)
- `bronze_personal_client` – demographics as received
- `bronze_benefit_data` – benefit program facts  
- `bronze_ww_claims` –  unemployment insurance claims (Dutch WW data)
- `bronze_employer_data` –  employer characteristics and partnerships

### Silver (Standardization & Unification)
- `silver_demographics_benefit` – joins bronze tables, normalizes categorical values, derives age & experience bands, computes actual duration, flags data quality.
- `silver_claims_analysis` – unified  claims with employer data, Dutch education standardization, reintegration analytics

### Gold (Analytics)
- `gold_benefit_analysis` – consolidated analytics across region, benefit type, demographics, and experience.
- `gold_analytics` –  executive dashboard metrics for policy analysis and reintegration program evaluation

## Key Metrics
### General Benefit Analysis
- Recipients & unique clients per region / benefit type
- Transition success rates (percentage moving back to work)
- Actual vs reported duration (months)
- Outcomes segmented by age group, gender, education, experience level, industry

### -Specific Metrics  
- WW claim reintegration success rates by province and industry
- Employer partnership effectiveness analysis
- Dutch education level impact on employment outcomes
- Benefit duration vs. salary bracket correlations
- Regional unemployment pattern analysis

## Structure
- `models/bronze|silver|gold` – transformation SQL
- `seeds/` – `personal_client_data.csv`, `benefit_data.csv`, `ww_claims.csv`, `employer_data.csv`
- `macros/datetime_utils.sql` – cross‑database temporal helpers
- `docs/` – use case documentation including detailed  analytics guide
- `requirements.txt` – dbt-core + adapters
- `dbt_project.yml` – tagging & materialization

## Cross-Database Macros
Defined in `macros/datetime_utils.sql` to avoid vendor-specific SQL:

| Macro | Purpose | SQL Server / Fabric | Databricks | DuckDB | SQLite |
|-------|---------|---------------------|------------|--------|--------|
| `xdb_now()` | Current timestamp | `CAST(SYSDATETIME() AS datetime2(6))` | `current_timestamp()` | `now()` | `CURRENT_TIMESTAMP` |
| `xdb_month_diff(a,b)` | Whole months between dates | `DATEDIFF(MONTH,a,b)` | `datediff(month,a,b)` | `date_diff('month',a,b)` | `CAST((julianday(b)-julianday(a))/30 AS INTEGER)` |

All models now use these macros instead of hard-coded functions (`SYSDATETIME`, `DATEDIFF`, etc.).

### Caveats
- SQLite month diff is approximate (30-day divisor). For exact boundary logic, use a calendar dimension.
- `datetime2(6)` down-casts on engines lacking high-precision timestamp types.
- Keep adapter versions aligned with `dbt-core` to reduce incompatibility risk.

## Installation
```bash
pip install -r requirements.txt
```

## Example Multi-Adapter Profile (`~/.dbt/profiles.yml`)
```yaml
mvp_profile:
	target: duckdb
	outputs:
		duckdb:
			type: duckdb
			path: database.duckdb
			threads: 4
		sqlite:
			type: sqlite
			threads: 1
			database: ./mvp_local.db
			schema: main
		fabric:
			type: sqlserver
			driver: 'ODBC Driver 18 for SQL Server'
			server: <server>
			port: 1433
			user: <user>
			password: <password>
			database: <database>
			schema: dbo
			encrypt: true
		databricks:
			type: databricks
			catalog: main
			schema: default
			host: https://<workspace-host>
			http_path: /sql/1.0/warehouses/<warehouse-id>
			token: <personal-access-token>
			threads: 4
```

## Running
```bash
# Run all models
dbt seed --target duckdb
dbt run  --target duckdb
dbt test --target duckdb

# Run specific use cases
dbt run --select +gold_benefit_analysis      # General benefit analysis
dbt run --select +gold_analytics         #  unemployment insurance analysis

# Cross-platform execution
dbt run --target sqlite
dbt run --target fabric
dbt run --target databricks
```

Or automatically switch by editing `target:` in the profile.

## Layered Execution (Optional)
```bash
dbt seed --target <target>
dbt run --select tag:bronze  --target <target>
dbt run --select tag:silver  --target <target>
dbt run --select tag:gold    --target <target>

# -specific execution
dbt run --select tag: tag:bronze   --target <target>
dbt run --select tag: tag:silver   --target <target>  
dbt run --select tag: tag:gold     --target <target>
```

## CI/CD (Suggested)
- Lightweight: Run DuckDB seed/run/test on PR.
- Full: Scheduled Fabric / Databricks run + docs generation.

## Future Enhancements
- Add SCD Type 2 dimension (snapshots or incremental) for client history.
- Calendar dimension for precise month computations.
- Generic & singular tests (unique, accepted values, not null) + exposures.
- Macro unit tests via ephemeral models.

## Troubleshooting
- Missing relation errors: run `dbt seed` first.
- Permission denied (Fabric/SQL Server): ensure CREATE/ALTER on target schema.
- Function not found (e.g. SYSDATETIME) indicates a model not yet migrated to macros.

## License
Internal / evaluation use (add explicit license if distributing externally).
