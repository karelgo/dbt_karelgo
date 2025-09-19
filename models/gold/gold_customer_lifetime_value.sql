/*
  Gold Layer - Customer Lifetime Value Analysis
  
  Purpose: Calculates customer lifetime value (CLV) and segmentation metrics
  for customer relationship management and marketing strategy.
  
  Data Sources: silver_sales_cleaned, silver_customers_cleaned
  Business Use: Customer segmentation, CLV analysis, retention strategies
  
  Metrics Included:
  - Customer lifetime value calculations
  - Purchase behavior patterns
  - Customer segmentation and scoring
  - Retention and frequency metrics
*/

{{ config(
    materialized='table',
    tags=['gold', 'customers', 'clv', 'analytics']
) }}

with customer_purchase_summary as (
    select
        s.customer_id,
        count(distinct s.transaction_id) as total_transactions,
        sum(s.total_amount) as total_spent,
        avg(s.total_amount) as avg_order_value,
        max(s.total_amount) as highest_order_value,
        min(s.transaction_date_clean) as first_purchase_date,
        max(s.transaction_date_clean) as last_purchase_date,
        count(distinct s.transaction_date_clean) as purchase_days,
        sum(s.quantity_valid) as total_items_purchased
    from {{ ref('silver_sales_cleaned') }} s
    where s.is_valid_transaction = true
    group by 1
),

customer_behavior_metrics as (
    select
        customer_id,
        total_transactions,
        total_spent,
        avg_order_value,
        highest_order_value,
        first_purchase_date,
        last_purchase_date,
        purchase_days,
        total_items_purchased,
        
        -- Calculate days between first and last purchase
        case 
            when last_purchase_date > first_purchase_date
            then last_purchase_date - first_purchase_date
            else 0
        end as customer_lifespan_days,
        
        -- Calculate purchase frequency
        case 
            when last_purchase_date > first_purchase_date
            then total_transactions / nullif((last_purchase_date - first_purchase_date), 0)
            else total_transactions
        end as purchase_frequency,
        
        -- Days since last purchase
        current_date() - last_purchase_date as days_since_last_purchase
        
    from customer_purchase_summary
)

select
    cbm.customer_id,
    c.full_name,
    c.customer_type_clean as customer_type,
    c.value_segment,
    c.customer_tenure_segment,
    
    -- Purchase behavior metrics
    cbm.total_transactions,
    round(cbm.total_spent, 2) as total_spent,
    round(cbm.avg_order_value, 2) as avg_order_value,
    cbm.highest_order_value,
    cbm.total_items_purchased,
    cbm.first_purchase_date,
    cbm.last_purchase_date,
    cbm.purchase_days,
    cbm.customer_lifespan_days,
    cbm.days_since_last_purchase,
    
    -- Customer lifetime value calculation
    round(cbm.total_spent, 2) as current_clv,
    round(cbm.avg_order_value * cbm.total_transactions * 1.2, 2) as projected_clv,
    
    -- Customer scoring and segmentation
    case 
        when cbm.total_spent >= 2000 and cbm.total_transactions >= 3 then 'Champion'
        when cbm.total_spent >= 1000 and cbm.total_transactions >= 2 then 'Loyal Customer'
        when cbm.total_spent >= 500 then 'Potential Loyalist'
        when cbm.total_transactions = 1 then 'New Customer'
        else 'At Risk'
    end as customer_segment,
    
    case 
        when cbm.days_since_last_purchase <= 30 then 'Active'
        when cbm.days_since_last_purchase <= 90 then 'Dormant'
        else 'Inactive'
    end as activity_status,
    
    -- Purchase patterns
    round(cbm.purchase_frequency, 4) as purchase_frequency_score,
    round(cbm.total_items_purchased / nullif(cbm.total_transactions, 0), 2) as avg_items_per_order,
    
    -- Business value indicators
    case 
        when cbm.total_spent >= 1500 and cbm.total_transactions >= 3 and cbm.days_since_last_purchase <= 60
        then 'High Value'
        when cbm.total_spent >= 500 and cbm.total_transactions >= 2
        then 'Medium Value'
        else 'Low Value'
    end as business_value_tier,
    
    current_timestamp() as _gold_created_at
    
from customer_behavior_metrics cbm
left join {{ ref('silver_customers_cleaned') }} c
    on cbm.customer_id = c.customer_id
where c.is_valid_customer = true
order by cbm.total_spent desc