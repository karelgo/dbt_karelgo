/*
  Bronze Layer - Raw Personal Client Data
  
  Purpose: Ingests raw personal demographics data from the source system.
  This layer stores client demographic information as received from the source,
  with minimal processing and added metadata for data lineage.
  
  Data Source: personal_client_data seed file
  Next Layer: Silver layer (silver_demographics_benefit)
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
  {{ xdb_now() }} as _loaded_at,
    'personal_client_data_seed' as _source_system
    
from {{ ref('personal_client_data') }}