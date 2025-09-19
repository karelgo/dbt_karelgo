/*
  Gold Layer - Daily Sales Summary
  
  Purpose: Aggregates daily sales performance metrics for business reporting.
  This model provides key performance indicators (KPIs) for daily sales
  analysis and executive dashboards.
  
  Data Sources: silver_sales_cleaned, silver_customers_cleaned, silver_products_cleaned
  Business Use: Daily sales reporting, performance tracking
  
  Metrics Included:
  - Total sales revenue and transaction count
  - Average order value and quantities
  - Customer acquisition metrics
  - Product performance indicators
*/

{{ config(
    materialized='table',
    tags=['gold', 'sales', 'daily', 'kpi']
) }}

with daily_sales_base as (
    select
        s.transaction_date_clean as sale_date,
        s.store_location_clean,
        s.category_clean,
        count(distinct s.transaction_id) as transaction_count,
        count(distinct s.customer_id) as unique_customers,
        sum(s.total_amount) as total_revenue,
        sum(s.quantity_valid) as total_quantity_sold,
        avg(s.total_amount) as avg_order_value,
        max(s.total_amount) as max_order_value,
        min(s.total_amount) as min_order_value
    from {{ ref('silver_sales_cleaned') }} s
    where s.is_valid_transaction = true
    group by 1, 2, 3
),

customer_metrics as (
    select
        s.transaction_date_clean as sale_date,
        count(distinct case when c.customer_tenure_segment = 'New' then s.customer_id end) as new_customers,
        count(distinct case when c.value_segment = 'High Value' then s.customer_id end) as high_value_customers
    from {{ ref('silver_sales_cleaned') }} s
    left join {{ ref('silver_customers_cleaned') }} c
        on s.customer_id = c.customer_id
    where s.is_valid_transaction = true
    group by 1
)

select
    ds.sale_date,
    ds.store_location_clean as store_location,
    ds.category_clean as product_category,
    
    -- Sales metrics
    ds.transaction_count,
    ds.unique_customers,
    round(ds.total_revenue, 2) as total_revenue,
    ds.total_quantity_sold,
    round(ds.avg_order_value, 2) as avg_order_value,
    ds.max_order_value,
    ds.min_order_value,
    
    -- Customer metrics
    coalesce(cm.new_customers, 0) as new_customers_count,
    coalesce(cm.high_value_customers, 0) as high_value_customers_count,
    
    -- Performance ratios
    round(ds.total_revenue / nullif(ds.transaction_count, 0), 2) as revenue_per_transaction,
    round(ds.total_quantity_sold / nullif(ds.transaction_count, 0), 2) as avg_items_per_transaction,
    round(ds.unique_customers / nullif(ds.transaction_count, 0) * 100, 2) as customer_concentration_pct,
    
    -- Data quality indicators
    case 
        when ds.transaction_count > 0 and ds.total_revenue > 0 
        then 'Valid'
        else 'Check Required'
    end as data_quality_status,
    
    current_timestamp() as _gold_created_at
    
from daily_sales_base ds
left join customer_metrics cm
    on ds.sale_date = cm.sale_date
order by ds.sale_date desc, ds.store_location_clean, ds.category_clean