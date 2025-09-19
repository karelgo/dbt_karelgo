/*
  Bronze Layer - Raw Customer Data
  
  Purpose: Ingests raw customer data from the source system.
  This layer stores customer information as received from the source,
  with minimal processing and added metadata.
  
  Data Source: raw_customers seed file
  Next Layer: Silver layer (silver_customers_cleaned)
*/

{{ config(
    materialized='table',
    tags=['bronze', 'customers', 'raw']
) }}

select
    -- Original customer data
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    registration_date,
    customer_type,
    credit_limit,
    
    -- Data lineage metadata
    current_localtimestamp() as _loaded_at,
    'raw_customers_seed' as _source_system
    
from {{ ref('raw_customers') }}