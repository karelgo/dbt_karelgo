{% macro xdb_now() %}
  {# Cross-database current timestamp with reasonable precision #}
  {% if target.type in ['sqlserver','fabric'] %}
    CAST(SYSDATETIME() AS datetime2(6))
  {% elif target.type in ['databricks'] %}
    current_timestamp()
  {% elif target.type in ['duckdb'] %}
    now()
  {% elif target.type in ['sqlite'] %}
    CURRENT_TIMESTAMP -- returns UTC ISO8601 e.g. 2024-09-19 12:34:56
  {% else %}
    current_timestamp
  {% endif %}
{% endmacro %}

{% macro xdb_month_diff(start_date, end_date) %}
  {# Returns integer number of months between two dates #}
  {% if target.type in ['sqlserver','fabric'] %}
    DATEDIFF(MONTH, {{ start_date }}, {{ end_date }})
  {% elif target.type in ['databricks'] %}
    datediff(month, {{ start_date }}, {{ end_date }})
  {% elif target.type in ['duckdb'] %}
    date_diff('month', {{ start_date }}, {{ end_date }})
  {% elif target.type in ['sqlite'] %}
    CAST( ( (julianday({{ end_date }}) - julianday({{ start_date }}) ) / 30 ) AS INTEGER)
  {% else %}
    datediff(month, {{ start_date }}, {{ end_date }})
  {% endif %}
{% endmacro %}
