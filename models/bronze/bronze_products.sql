/*
  Bronze Layer - Raw Product Data
  
  Purpose: Ingests raw product catalog data from the source system.
  This layer maintains the original product information structure
  with data lineage tracking.
  
  Data Source: raw_products seed file
  Next Layer: Silver layer (silver_products_cleaned)
*/

{{ config(
    materialized='table',
    tags=['bronze', 'products', 'raw']
) }}

select
    -- Original product data
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    supplier,
    cost_price,
    retail_price,
    stock_quantity,
    reorder_level,
    
    -- Data lineage metadata
    current_localtimestamp() as _loaded_at,
    'raw_products_seed' as _source_system
    
from {{ ref('raw_products') }}