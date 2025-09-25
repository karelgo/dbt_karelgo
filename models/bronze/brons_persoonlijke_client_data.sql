/*
  Bronslaag - Ruwe demografische cliëntgegevens
  
  Doel: Laadt ruwe demografische gegevens uit het bronsysteem.
  Deze laag bewaart de cliëntinformatie zoals ontvangen met minimale bewerking
  en extra metadata voor herkomst/lineage.
  
  Databron: seedbestand persoonlijke_client_data
  Volgende laag: Zilver (silver_demografie_uitkering)
*/

{{ config(
    materialized='table',
  tags=['brons', 'demografie', 'raw'],
    column_types={
      '_loaded_at': 'datetime2(6)'
    }
) }}

select
  -- Originele demografische gegevens
    id as client_id,
    age,
    gender,
    education_level,
    region,
    industry,
    years_experience,
    last_employer,
    
  -- Metadata voor dataherkomst
  {{ xdb_now() }} as _loaded_at,
    'persoonlijke_client_data_seed' as _source_system
    
from {{ ref('persoonlijke_client_data') }}