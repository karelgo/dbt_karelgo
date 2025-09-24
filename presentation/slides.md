---
layout: title
class: center, middle
title: Portable Medallion Analytics with dbt
subtitle: Multi-Engine Design (Fabric / Databricks / DuckDB / SQLite)
author: Architecture Briefing
date: 2025-09-23
---

# 1. Objective

Enable a portable, governed analytics foundation that can execute on multiple compute engines with minimal SQL divergence and clear lineage.

Speaker Notes:
- Emphasize portability + governance readiness.
- Focus: small footprint proving architectural pattern.

---
# 2. Business Drivers
- Reduce vendor lock-in risk
- Accelerate iteration (local dev vs cloud)
- Consistent semantic layer across engines
- Prepare for future governance (lineage, tests, catalog)

Notes: Architects want alignment with enterprise standards and optionality.

---
# 3. High-Level Requirements
- Layered separation (raw → unified → analytics)
- Deterministic builds (seeded inputs)
- Cross-database compatibility
- Extensible macro layer (avoid dialect sprawl)
- CI-friendly (fast + full paths)

---
# 4. Medallion Architecture Applied
Raw (Bronze) → Standardize/Unify (Silver) → Curated Analytics (Gold).

```
+--------+   +--------+   +--------+   +-------+   +-----------+
| Seeds  |-->| Bronze |-->| Silver |-->| Gold  |-->| Dashboards|
+--------+   +--------+   +--------+   +-------+   +-----------+
```

Notes: Keep conceptual; later slides show implementation specifics.

---
# 5. Model Inventory (MVP)
| Layer | Model | Purpose |
|-------|-------|---------|
| Bronze | bronze_personal_client | Raw demographics + lineage |
| Bronze | bronze_benefit_data | Raw benefits + lineage |
| Silver | silver_demographics_benefit | Unified cleansed dataset |
| Gold | gold_benefit_analysis | Multi-dimensional aggregates |

Notes: Intentional minimal surface to emphasize pattern.

---
# 6. Portability Macro Layer
Macros abstract temporal differences:
```
{{ xdb_now() }}           -- current timestamp
{{ xdb_month_diff(a,b) }} -- integer months diff
```
Engines: SQL Server/Fabric, Databricks, DuckDB, SQLite.

Notes: This is the contract enabling multi-engine execution.

---
# 7. Macro Expansion Example
Model excerpt (Silver):
```
case when end_date is not null then
  {{ xdb_month_diff('CAST(start_date AS date)', 'CAST(end_date AS date)') }}
else duration_months end as actual_duration_months
```
Generated (Fabric): `DATEDIFF(MONTH, CAST(start_date AS date), CAST(end_date AS date))`

Notes: Show before/after if questions arise.

---
# 8. Cross-Engine Capability Matrix
| Capability | Fabric/SQLServer | Databricks | DuckDB | SQLite |
|------------|------------------|------------|--------|--------|
| Timestamp fn | SYSDATETIME() | current_timestamp() | now() | CURRENT_TIMESTAMP |
| Month diff | DATEDIFF(MONTH) | datediff(month) | date_diff('month') | julianday arithmetic |
| Materialization | table | table | table | table |
| Local Dev Speed | Medium | Medium | Fast | Fast |

Notes: SQLite used only for lightweight semantic validation.

---
# 9. Data Quality & Governance Hooks
- Lineage columns in Bronze (`_loaded_at`, `_source_system`)
- Standardization & flags in Silver (region mismatch, invalid amounts)
- Clear derivations (age_group, experience_level)
- Future: tests, exposures, snapshots, SCD2 dimension

Notes: Architecture is governance-ready.

---
# 10. Operational Workflow
1. Local iterate (DuckDB / SQLite)
2. Commit → CI fast path
3. Merge → full run (Fabric/Databricks)
4. Optional: docs & lineage publish

Notes: Minimizes cloud spend for experimentation.

---
# 11. Deployment Flow Diagram
```
[Dev Laptop]
   | dbt seed/run (DuckDB)
   v
[CI Fast Tests]
   | (duckdb)
   v
[Main Branch]
   | Full Run (Fabric/Databricks)
   v
[Docs / Catalog / BI]
```

---
# 12. Extensibility Pattern
Add domain:
1. New seed/source → bronze model
2. Join/standardize in silver
3. Extend gold aggregates / new marts
Macros remain unchanged.

Notes: Scales horizontally without macro rewrites.

---
# 13. Performance & Scale Considerations
- Predicate & projection pushdown (simple SQL)
- Future: incremental models for large tables
- Avoid heavy Python/UDF logic inside macros
- Warehouse-native optimizations remain accessible

Notes: Pattern scales before refactor needed.

---
# 14. Security & Access
- Schema-level grants (Fabric/SQL Server)
- Role-based separation per layer (future)
- Potential column masking at warehouse layer

Notes: dbt enforces structure; warehouse enforces policy.

---
# 15. Live Demo Plan (5–7 min)
Script:
```
show tree
open silver model (highlight macros)
dbt seed --target duckdb
dbt run --select tag:gold --target duckdb
dbt test --target duckdb
dbt run --target sqlite --select gold_benefit_analysis
show sample rows
```
Fallback: screenshots if network issues.

---
# 16. Roadmap
- SCD Type 2 dimension (snapshots or incremental)
- Calendar dimension (precise month diff)
- Test suite (generic + custom)
- Exposures & lineage publishing
- Add cost monitoring & performance benchmarks

---
# 17. Risks & Mitigations
| Risk | Mitigation |
|------|------------|
| Adapter version skew | Align versions in dependency management |
| Macro drift | Central review & tests for macros |
| Missing tests | Incrementally add generic tests |
| Approx month diff (SQLite) | Use calendar dimension in prod engines |

---
# 18. Key Takeaways
- Layered, portable architecture
- Macro contract abstracts engines
- Governance hooks embedded early
- Ready to scale features & domains

---
# 19. Q & A
Prompt: Which aspects need deeper alignment with enterprise standards?

Notes: Have SCD2 + test strategy details ready.

---
# Appendix A: Macro Code
```
{% raw %}{% macro xdb_now() %}
  {% if target.type in ['sqlserver','fabric'] %}CAST(SYSDATETIME() AS datetime2(6))
  {% elif target.type in ['databricks'] %}current_timestamp()
  {% elif target.type in ['duckdb'] %}now()
  {% elif target.type in ['sqlite'] %}CURRENT_TIMESTAMP
  {% else %}current_timestamp{% endif %}
{% endmacro %}{% endraw %}
```
Use appendix if deeper dive requested.

---
# Appendix B: Silver Model Excerpt
```
case when benefits.end_date is not null then
  {{ xdb_month_diff('CAST(benefits.start_date AS date)', 'CAST(benefits.end_date AS date)') }}
else benefits.duration_months end as actual_duration_months
```

---
# Appendix C: Future SCD2 (Concept)
Columns: surrogate_key, natural_key, valid_from, valid_to, is_current.
Approach: snapshot or incremental merge logic.

---
# End
Thank you.
