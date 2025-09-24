# Presentation Supplemental Assets

## 1. Diagram Drafts (ASCII)

### 1.1 Data Flow (Seeds → Bronze → Silver → Gold → Consumption)
```
+--------+   +--------+   +--------+   +-------+   +-----------+
| Seeds  |-->| Bronze |-->| Silver |-->| Gold  |-->| Dashboards|
+--------+   +--------+   +--------+   +-------+   +-----------+
                 |            |            |
             Lineage     Standardize    Curated
```

### 1.2 Macro Dispatch Pattern
```
          +------------------+
          |   Model SQL      |
          |  (uses macros)   |
          +---------+--------+
                    |
                    v
          +------------------+
          |  Macro Layer     |
          | xdb_now(),       |
          | xdb_month_diff() |
          +---------+--------+
                    |
                    v
          +------------------+
          | Adapter Resolver |
          +---------+--------+
                    |
    +---------------+---------------+---------------+---------------+
    |               |               |               |               |
    v               v               v               v               v
+-------+       +---------+     +--------+      +---------+     +---------+
|Fabric |       |Databricks|    |DuckDB  |      |SQLite   |     |(Future) |
+-------+       +---------+     +--------+      +---------+     +---------+
```

### 1.3 Operational Pipeline
```
Developer -> Local Run (DuckDB) -> PR -> CI (DuckDB tests) -> Main -> Fabric/Databricks Full Run -> Docs & BI
```

### 1.4 Future SCD2 Dimension (Concept)
```
+------------+----------------+-----------+----------+-----------+
| surrogate  | natural_key    | valid_from| valid_to | is_current|
+------------+----------------+-----------+----------+-----------+
| 101        | CLIENT_123     | 2025-01-01| 2025-06-30| 0        |
| 202        | CLIENT_123     | 2025-07-01| null     | 1        |
+------------+----------------+-----------+----------+-----------+
```

## 2. Demo Checklist
- [ ] Virtual environment active & dependencies installed
- [ ] `profiles.yml` contains duckdb & sqlite targets
- [ ] Seeds load successfully (`dbt seed --target duckdb`)
- [ ] Bronze tables materialized (verify one row count)
- [ ] Silver & Gold models run cleanly on DuckDB
- [ ] SQLite run tested once pre-presentation
- [ ] Optional: Pre-generated docs (`dbt docs generate`) for screenshots
- [ ] Fallback query results saved (CSV or screenshot)
- [ ] Terminal font size increased for visibility
- [ ] Network / VPN stable

## 3. Demo Command Script
```bash
# 1. Show structure
ls -1 models/bronze models/silver models/gold

# 2. Highlight macro
sed -n '1,80p' macros/datetime_utils.sql | head -n 25

# 3. Seed + run (fast path)
dbt seed --target duckdb
dbt run --select tag:gold --target duckdb
dbt test --target duckdb

# 4. Switch engine
dbt run --target sqlite --select gold_benefit_analysis

# 5. Sample output (DuckDB CLI or Python)
duckdb -c "select * from gold_benefit_analysis limit 5;"
```

## 4. Anticipated Q&A (Expanded)
| Topic | Likely Question | Suggested Answer |
|-------|-----------------|------------------|
| Portability | How hard to add another engine? | Add adapter + map functions in macro; minimal model changes. |
| Governance | Where do tests live? | dbt generic tests on keys/constraints + custom tests for domain logic; exposures for lineage. |
| SCD2 | Implementation path? | Snapshot or incremental with merge; add valid_from/valid_to/is_current; minimal downstream change. |
| Performance | Large data scaling? | Introduce incremental & clustering/partitioning; macros remain lightweight; pushdown preserved. |
| Cost | How controlled? | Local dev lowers warehouse usage; CI selective; future: schedule & cost tags. |
| Data Quality | Missing tests risk? | Start with critical dimensions, grow coverage via test matrix in CI. |
| Security | Row/column security? | Enforced at warehouse; dbt supplies schema boundaries & tagging for policy mapping. |
| Version Drift | Mixed adapter versions? | Maintain pinned matrix; add compatibility check in CI (simple script). |
| Approximation | Month diff accuracy on SQLite? | Dev aid only; production engines have exact month diff functions. |
| Observability | How to monitor? | Add run artifacts to log aggregator; integrate OpenLineage or DataDog after scaling. |

## 5. Risk Register (Condensed)
| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Adapter incompatibility | Broken pipelines | Medium | Pin & test matrix CI |
| Unbounded model growth | Maintenance drag | Medium | Layering & naming standards |
| Lack of tests | Silent data issues | High initially | Incremental test adoption plan |
| Macro misuse | Portability erosion | Low | Code review + macro coverage tests |

## 6. Roadmap Detail
Phase 1 (Now): Macro portability, core medallion, seeds.
Phase 2: Add tests, docs exposures, SCD2 dimension.
Phase 3: Calendar dimension, cost monitoring, lineage integration.
Phase 4: Incremental performance tuning, domain expansion.

## 7. Slide Export Tips
- Convert with pandoc: `pandoc slides.md -t pptx -o medallion_portability.pptx`
- Or use reveal.js: `npx @marp-team/marp-cli presentation/slides.md -o slides.html`

## 8. Branding / Theming Suggestions
- Use consistent color per layer (Bronze=#cd7f32, Silver=#c0c0c0, Gold=#d4af37)
- Macro layer callouts in neutral accent (blue/gray) to separate from data layers.

## 9. Metrics for Success (Optional Slide)
- Time-to-first-model reduction (local dev speed)
- Number of engines supported without model edits
- Test coverage growth over time
- Build duration (fast vs full pipeline)

---
End of supplemental assets.
