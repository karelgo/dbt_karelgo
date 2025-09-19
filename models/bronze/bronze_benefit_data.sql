/*
  Bronze Layer - Raw Benefit Data
  
  Purpose: Ingests raw benefit data from the source system.
  This layer stores benefit information as received from the source,
  with minimal processing and added metadata for data lineage.
  
  Data Source: benefit_data seed file
  Next Layer: Silver layer (silver_demographics_benefit)
*/

{{ config(
    materialized='table',
    tags=['bronze', 'benefits', 'raw']
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
    current_localtimestamp() as _loaded_at,
    'benefit_data_seed' as _source_system
    
from {{ ref('benefit_data') }}