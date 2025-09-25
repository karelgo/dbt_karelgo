dbt test
dbt docs generate
# dbt MVP Project (NL)

Portabele dbt-medallionarchitectuur op **Microsoft Fabric / SQL Server**, **Databricks (Spark)**, **DuckDB** en **SQLite**. Analyses richten zich op de relatie tussen demografische kenmerken, uitkeringsdeelname en terugkeer naar werk.

## Medallion-lagen
### Brons (Ruwe broninname)
- `brons_persoonlijke_client_data` – demografie zoals ontvangen
- `brons_uitkerings_data` – uitkeringsfeiten

### Zilver (Standaardisatie & Unificatie)
- `silver_demografie_uitkering` – voegt brons samen, normaliseert categorieën, leidt leeftijds- en ervaringsklassen af, berekent werkelijke duur en zet kwaliteitsvlaggen.

### Goud (Analytics)
- `goud_uitkerings_analysis` – geconsolideerde analyses per regio, regelingstype, demografie en ervaring.

## Belangrijkste indicatoren
- Ontvangers en unieke cliënten per regio/regelingstype
- Succesratio terugkeer naar werk
- Werkelijke vs gerapporteerde duur (maanden)
- Segmentatie naar leeftijdsgroep, gender, opleiding, ervaringsniveau, sector

## Structuur
- `models/bronze|silver|gold` – transformaties
- `seeds/` – `persoonlijke_client_data.csv`, `uitkerings_data.csv`
- `macros/datetime_utils.sql` – cross-database tijdshelpers
- `requirements.txt` – dbt-core + adapters
- `dbt_project.yml` – tagging & materialisatie

## Cross-database macros
Gedefinieerd in `macros/datetime_utils.sql` om vendorspecifieke SQL te vermijden:

| Macro | Doel | SQL Server / Fabric | Databricks | DuckDB | SQLite |
|-------|------|---------------------|------------|--------|--------|
| `xdb_now()` | Huidige timestamp | `CAST(SYSDATETIME() AS datetime2(6))` | `current_timestamp()` | `now()` | `CURRENT_TIMESTAMP` |
| `xdb_month_diff(a,b)` | Volledige maanden tussen data | `DATEDIFF(MONTH,a,b)` | `datediff(month,a,b)` | `date_diff('month',a,b)` | `CAST((julianday(b)-julianday(a))/30 AS INTEGER)` |

Alle modellen gebruiken deze macros i.p.v. hardgecodeerde functies.

### Kanttekeningen
- Maandverschil in SQLite is benadering (deling door 30). Voor exacte grenzen gebruik een kalenderdimensie.
- `datetime2(6)` wordt gedegradeerd op engines zonder hoge precisie.
- Houd adapterversies in lijn met `dbt-core` om incompatibiliteit te voorkomen.

## Installatie
```bash
pip install -r requirements.txt
```

## Voorbeeld multi-adapterprofiel (`~/.dbt/profiles.yml`)
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

## Uitvoeren
```bash
dbt seed --target duckdb
dbt run  --target duckdb
dbt test --target duckdb

dbt run --target sqlite
dbt run --target fabric
dbt run --target databricks
```

Of wijzig `target:` in het profiel.

## Documentatie (lokaal)
```bash
dbt docs generate
dbt docs serve --port 8080
```

## Gelaagde uitvoering (optioneel)
```bash
dbt seed --target <target>
dbt run --select tag:brons   --target <target>
dbt run --select tag:zilver  --target <target>
dbt run --select tag:goud    --target <target>
```

## CI/CD (aanbevolen)
- Lichtgewicht: DuckDB seed/run/test op PR
- Volledig: Geplande Fabric/Databricks-run + documentatie

## Toekomst
- SCD Type 2-dimensie (snapshots of incrementeel) voor cliëntgeschiedenis
- Kalenderdimensie voor exacte maandberekeningen
- Generieke en specifieke tests + exposures/lineage
- Macrounittests via ephemeral modellen

## Probleemoplossing
- Ontbrekende tabellen: voer eerst `dbt seed` uit
- Rechtenfout (Fabric/SQL Server): zorg voor CREATE/ALTER op schema
- Functie niet gevonden (bijv. SYSDATETIME): model nog niet gemigreerd naar macro

## Licentie
Interne/evaluatie-doeleinden (voeg expliciete licentie toe bij externe distributie)
Internal / evaluation use (add explicit license if distributing externally).
