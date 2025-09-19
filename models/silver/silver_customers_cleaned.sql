/*
  Silver Layer - Cleaned Customer Data
  
  Purpose: Cleans and standardizes customer data from the Bronze layer.
  This layer applies data quality rules, standardizes formats, and adds
  derived fields for better analytics.
  
  Data Source: bronze_customers
  Next Layer: Gold layer aggregations
  
  Transformations Applied:
  - Name formatting and standardization
  - Email validation
  - Phone number formatting
  - Customer segmentation
  - Data quality checks
*/

{{ config(
    materialized='table',
    tags=['silver', 'customers', 'cleaned']
) }}

select
    customer_id,
    
    -- Standardized name fields
    trim(initcap(first_name)) as first_name_clean,
    trim(initcap(last_name)) as last_name_clean,
    trim(initcap(first_name)) || ' ' || trim(initcap(last_name)) as full_name,
    
    -- Email validation and standardization
    case 
        when email like '%@%.%' and length(email) > 5
        then lower(trim(email))
        else null 
    end as email_clean,
    
    -- Phone number cleaning (basic formatting)
    trim(phone) as phone_clean,
    
    -- Date standardization
    cast(registration_date as date) as registration_date_clean,
    
    -- Customer segmentation
    trim(upper(customer_type)) as customer_type_clean,
    
    -- Financial data validation
    case 
        when credit_limit >= 0 then credit_limit 
        else null 
    end as credit_limit_valid,
    
    -- Derived fields
    case 
        when current_date() - cast(registration_date as date) <= 30 
        then 'New'
        when current_date() - cast(registration_date as date) <= 365 
        then 'Recent'
        else 'Established'
    end as customer_tenure_segment,
    
    case 
        when credit_limit >= 5000 then 'High Value'
        when credit_limit >= 2000 then 'Medium Value'
        else 'Standard Value'
    end as value_segment,
    
    -- Quality flags
    case 
        when email like '%@%.%' 
        and first_name is not null 
        and last_name is not null
        and registration_date is not null
        then true 
        else false 
    end as is_valid_customer,
    
    -- Metadata from bronze layer
    _loaded_at,
    _source_system,
    current_timestamp() as _silver_processed_at
    
from {{ ref('bronze_customers') }}
where customer_id is not null