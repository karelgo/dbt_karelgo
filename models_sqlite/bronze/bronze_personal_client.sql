/*
  Bronze Layer - Raw Personal Client Data
  
  Purpose: Ingests raw personal demographics data from the source system.
  This layer stores client demographic information as received from the source,
  with minimal processing and added metadata for data lineage.
  
  Data Source: persoonlijke_client_data seed file
  Next Layer: Silver layer (silver_demografie_uitkering)
*/

{{ config(
    materialized='table',
    tags=['bronze', 'demographics', 'raw'],
    column_types={
      '_loaded_at': 'datetime2(6)'
    }
) }}

select
    -- Original demographic data
    id as client_id,
    age,
    gender,
    education_level,
    region,
    industry,
    years_experience,
    last_employer,
    
    -- Data lineage metadata
  -- CAST(SYSDATETIME() AS datetime2(6)) as _loaded_at,
    current_timestamp as _loaded_at,
  'persoonlijke_client_data_seed' as _source_system
    
from {{ ref('persoonlijke_client_data') }}