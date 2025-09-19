/*
  Bronze Layer - Raw Sales Data
  
  Purpose: Ingests raw sales transaction data from the source system.
  This layer preserves the original data structure and adds basic metadata
  for data lineage tracking.
  
  Data Source: raw_sales seed file
  Next Layer: Silver layer (silver_sales_cleaned)
*/

{{ config(
    materialized='table',
    tags=['bronze', 'sales', 'raw']
) }}

select
    -- Original transaction data
    transaction_id,
    customer_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    transaction_date,
    sales_rep_id,
    store_location,
    
    -- Data lineage metadata
    current_timestamp() as _loaded_at,
    'raw_sales_seed' as _source_system
    
from {{ ref('raw_sales') }}