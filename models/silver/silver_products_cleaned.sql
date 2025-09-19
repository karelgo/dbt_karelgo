/*
  Silver Layer - Cleaned Product Data
  
  Purpose: Cleans and standardizes product catalog data from the Bronze layer.
  This layer applies data quality rules, calculates profit margins, and
  standardizes product categorization.
  
  Data Source: bronze_products
  Next Layer: Gold layer aggregations
  
  Transformations Applied:
  - Text standardization for categories and names
  - Price validation and margin calculations
  - Inventory status classification
  - Product performance metrics
*/

{{ config(
    materialized='table',
    tags=['silver', 'products', 'cleaned']
) }}

select
    product_id,
    
    -- Standardized text fields
    trim(initcap(product_name)) as product_name_clean,
    trim(upper(category)) as category_clean,
    trim(upper(subcategory)) as subcategory_clean,
    trim(initcap(brand)) as brand_clean,
    trim(initcap(supplier)) as supplier_clean,
    
    -- Validated price fields
    case 
        when cost_price >= 0 then cost_price 
        else null 
    end as cost_price_valid,
    
    case 
        when retail_price >= 0 then retail_price 
        else null 
    end as retail_price_valid,
    
    -- Calculated margin metrics
    case 
        when cost_price > 0 and retail_price > cost_price
        then retail_price - cost_price
        else null 
    end as profit_margin_amount,
    
    case 
        when cost_price > 0 and retail_price > cost_price
        then round((retail_price - cost_price) / cost_price * 100, 2)
        else null 
    end as profit_margin_percent,
    
    -- Inventory management fields
    case 
        when stock_quantity >= 0 then stock_quantity 
        else 0 
    end as stock_quantity_valid,
    
    case 
        when reorder_level >= 0 then reorder_level 
        else 0 
    end as reorder_level_valid,
    
    -- Inventory status classification
    case 
        when stock_quantity_valid <= reorder_level_valid then 'Low Stock'
        when stock_quantity_valid <= reorder_level_valid * 2 then 'Medium Stock'
        else 'High Stock'
    end as inventory_status,
    
    -- Product classification
    case 
        when retail_price >= 1000 then 'Premium'
        when retail_price >= 100 then 'Standard'
        else 'Budget'
    end as price_tier,
    
    -- Quality flags
    case 
        when product_name is not null 
        and category is not null
        and cost_price >= 0 
        and retail_price >= cost_price
        and stock_quantity >= 0
        then true 
        else false 
    end as is_valid_product,
    
    -- Metadata from bronze layer
    _loaded_at,
    _source_system,
    current_timestamp() as _silver_processed_at
    
from {{ ref('bronze_products') }}
where product_id is not null