/*
  Silver Layer - Cleaned Sales Data
  
  Purpose: Cleans and standardizes sales transaction data from the Bronze layer.
  This layer applies data quality rules, standardizes formats, and adds 
  calculated fields for business use.
  
  Data Source: bronze_sales
  Next Layer: Gold layer aggregations
  
  Transformations Applied:
  - Date parsing and standardization
  - Price calculations (total_amount)
  - Data quality validations
  - Text standardization
*/

{{ config(
    materialized='table',
    tags=['silver', 'sales', 'cleaned']
) }}

select
    transaction_id,
    customer_id,
    product_id,
    
    -- Standardized text fields
    trim(upper(product_name)) as product_name_clean,
    trim(upper(category)) as category_clean,
    trim(upper(store_location)) as store_location_clean,
    
    -- Validated numeric fields
    case 
        when quantity > 0 then quantity 
        else null 
    end as quantity_valid,
    
    case 
        when unit_price >= 0 then unit_price 
        else null 
    end as unit_price_valid,
    
    -- Calculated fields
    case 
        when quantity > 0 and unit_price >= 0 
        then quantity * unit_price 
        else null 
    end as total_amount,
    
    -- Date parsing
    cast(transaction_date as date) as transaction_date_clean,
    
    -- Staff and location data
    sales_rep_id,
    
    -- Quality flags
    case 
        when quantity > 0 and unit_price >= 0 and transaction_date is not null
        then true 
        else false 
    end as is_valid_transaction,
    
    -- Metadata from bronze layer
    _loaded_at,
    _source_system,
    current_timestamp() as _silver_processed_at
    
from {{ ref('bronze_sales') }}
where 
    transaction_id is not null
    and customer_id is not null
    and product_id is not null