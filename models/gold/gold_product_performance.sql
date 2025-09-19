/*
  Gold Layer - Product Performance Analytics
  
  Purpose: Provides comprehensive product performance metrics for inventory
  management, pricing strategy, and product portfolio optimization.
  
  Data Sources: silver_sales_cleaned, silver_products_cleaned
  Business Use: Product management, inventory planning, pricing optimization
  
  Metrics Included:
  - Sales performance by product
  - Profitability analysis
  - Inventory turnover metrics
  - Category performance comparison
*/

{{ config(
    materialized='table',
    tags=['gold', 'products', 'performance', 'analytics']
) }}

with product_sales_summary as (
    select
        s.product_id,
        count(distinct s.transaction_id) as total_sales_transactions,
        count(distinct s.customer_id) as unique_customers,
        sum(s.quantity_valid) as total_quantity_sold,
        sum(s.total_amount) as total_revenue,
        avg(s.total_amount) as avg_sale_amount,
        max(s.total_amount) as max_sale_amount,
        min(s.transaction_date_clean) as first_sale_date,
        max(s.transaction_date_clean) as last_sale_date
    from {{ ref('silver_sales_cleaned') }} s
    where s.is_valid_transaction = true
    group by 1
),

category_performance as (
    select
        p.category_clean,
        sum(pss.total_revenue) as category_total_revenue,
        sum(pss.total_quantity_sold) as category_total_quantity,
        count(distinct p.product_id) as products_in_category
    from {{ ref('silver_products_cleaned') }} p
    left join product_sales_summary pss
        on p.product_id = pss.product_id
    group by 1
)

select
    p.product_id,
    p.product_name_clean as product_name,
    p.category_clean as category,
    p.subcategory_clean as subcategory,
    p.brand_clean as brand,
    p.price_tier,
    p.inventory_status,
    
    -- Sales performance metrics
    coalesce(pss.total_sales_transactions, 0) as total_sales_transactions,
    coalesce(pss.unique_customers, 0) as unique_customers,
    coalesce(pss.total_quantity_sold, 0) as total_quantity_sold,
    round(coalesce(pss.total_revenue, 0), 2) as total_revenue,
    round(coalesce(pss.avg_sale_amount, 0), 2) as avg_sale_amount,
    pss.first_sale_date,
    pss.last_sale_date,
    
    -- Financial metrics
    p.cost_price_valid as cost_price,
    p.retail_price_valid as retail_price,
    round(p.profit_margin_amount, 2) as profit_margin_amount,
    p.profit_margin_percent,
    round(coalesce(pss.total_quantity_sold, 0) * p.profit_margin_amount, 2) as total_profit,
    
    -- Inventory metrics
    p.stock_quantity_valid as current_stock,
    p.reorder_level_valid as reorder_level,
    
    -- Performance ratios and indicators
    round(coalesce(pss.total_revenue, 0) / nullif(pss.total_sales_transactions, 0), 2) as revenue_per_transaction,
    round(coalesce(pss.total_quantity_sold, 0) / nullif(pss.total_sales_transactions, 0), 2) as avg_quantity_per_sale,
    round(coalesce(pss.unique_customers, 0) / nullif(pss.total_sales_transactions, 0) * 100, 2) as customer_repeat_rate,
    
    -- Category context
    cp.category_total_revenue,
    round(coalesce(pss.total_revenue, 0) / nullif(cp.category_total_revenue, 0) * 100, 2) as category_revenue_share_pct,
    
    -- Performance classification
    case 
        when pss.total_revenue >= 2000 and pss.total_quantity_sold >= 5 then 'Star Product'
        when pss.total_revenue >= 1000 then 'Strong Performer'
        when pss.total_revenue >= 500 then 'Average Performer'
        when pss.total_revenue > 0 then 'Underperformer'
        else 'No Sales'
    end as performance_category,
    
    case 
        when p.profit_margin_percent >= 30 then 'High Margin'
        when p.profit_margin_percent >= 15 then 'Medium Margin'
        when p.profit_margin_percent > 0 then 'Low Margin'
        else 'Loss Making'
    end as profitability_tier,
    
    -- Inventory turnover indicator
    -- Estimate starting inventory as ending inventory + total sold (assuming all sold units were in stock at start)
    -- Average inventory = (starting_inventory + ending_inventory) / 2
    case 
        when p.stock_quantity_valid >= 0 and pss.total_quantity_sold > 0
        then round(
            pss.total_quantity_sold / nullif(
                ((p.stock_quantity_valid + (p.stock_quantity_valid + pss.total_quantity_sold)) / 2), 0
            ), 2
        )
        else 0
    end as inventory_turnover_ratio,
    
    -- Business recommendations
    case 
        when pss.total_revenue >= 1000 and p.profit_margin_percent >= 20 and p.inventory_status = 'Low Stock'
        then 'Restock Immediately'
        when pss.total_revenue = 0 and p.stock_quantity_valid > 10
        then 'Consider Discontinuing'
        when p.profit_margin_percent < 10 and pss.total_revenue > 0
        then 'Review Pricing'
        else 'Monitor Performance'
    end as business_recommendation,
    
    current_timestamp() as _gold_created_at
    
from {{ ref('silver_products_cleaned') }} p
left join product_sales_summary pss
    on p.product_id = pss.product_id
left join category_performance cp
    on p.category_clean = cp.category_clean
where p.is_valid_product = true
order by coalesce(pss.total_revenue, 0) desc