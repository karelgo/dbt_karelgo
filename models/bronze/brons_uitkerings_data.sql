/*
  Bronslaag - Ruwe uitkeringsgegevens
  
  Doel: Laadt ruwe uitkeringsdata uit het bronsysteem.
  Deze laag bewaart de informatie zoals ontvangen, met minimale bewerking
  en extra metadata voor herkomst/lineage.
  
  Databron: seedbestand uitkerings_data
  Volgende laag: Zilver (silver_demografie_uitkering)
*/

{{ config(
    materialized='table',
  tags=['brons', 'uitkeringen', 'raw'],
    column_types={
      '_loaded_at': 'datetime2(6)'
    }
) }}

select
  -- Originele uitkeringsdata
    client_id,
    benefit_type,
    start_date,
    end_date,
    duration_months,
    transition_to_work,
    benefit_amount,
    region,
    
    -- Metadata voor dataherkomst
  {{ xdb_now() }} as _loaded_at,
    'uitkerings_data_seed' as _source_system
    
from {{ ref('uitkerings_data') }}