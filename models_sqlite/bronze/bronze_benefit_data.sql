/*
  Bronze Layer - Raw Benefit Data
  
  Purpose: Ingests raw benefit data from the source system.
  This layer stores benefit information as received from the source,
  with minimal processing and added metadata for data lineage.
  
  Data Source: uitkerings_data seed file
  Next Layer: Silver layer (silver_demografie_uitkering)
*/

{{ config(
    materialized='table',
    tags=['bronze', 'benefits', 'raw'],
    column_types={
      '_loaded_at': 'datetime2(6)'
    }
) }}

select
    -- Original benefit data
    client_id,
    benefit_type,
    start_date,
    end_date,
    duration_months,
    transition_to_work,
    benefit_amount,
    region,
    
    -- Data lineage metadata
  -- CAST(SYSDATETIME() AS datetime2(6)) as _loaded_at,
    current_timestamp as _loaded_at,
  'uitkerings_data_seed' as _source_system
    
from {{ ref('uitkerings_data') }}