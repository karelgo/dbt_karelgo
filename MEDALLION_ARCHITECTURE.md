# Medallion Architecture Implementation

This document describes the medallion architecture implemented in the dbt_karelgo project. The medallion architecture is a data design pattern that organizes data into three layers: Bronze, Silver, and Gold.

## Architecture Overview

### Data Flow
```
Raw Data Sources → Bronze Layer → Silver Layer → Gold Layer → Business Applications
```

### Layer Descriptions

#### 🥉 Bronze Layer (Raw Data)
The Bronze layer contains raw data ingested from source systems with minimal transformation.

**Purpose:**
- Store data as close to the source format as possible
- Provide audit trail and data lineage
- Enable data recovery and reprocessing

**Models:**
- `bronze_sales` - Raw sales transaction data
- `bronze_customers` - Raw customer information
- `bronze_products` - Raw product catalog data

**Key Features:**
- Preserves original data structure
- Adds metadata for data lineage (`_loaded_at`, `_source_system`)
- Minimal data validation
- Table materialization for performance

#### 🥈 Silver Layer (Cleaned Data)
The Silver layer contains cleaned, validated, and standardized data ready for business use.

**Purpose:**
- Apply data quality rules and validations
- Standardize formats and naming conventions
- Add derived fields for business logic
- Create a single source of truth

**Models:**
- `silver_sales_cleaned` - Cleaned sales data with calculated totals and validations
- `silver_customers_cleaned` - Standardized customer data with segmentation
- `silver_products_cleaned` - Enhanced product data with profit margins and inventory status

**Key Features:**
- Data quality validations and cleansing
- Standardized naming conventions (UPPER, initcap)
- Calculated fields (profit margins, customer segments)
- Business rules implementation
- Quality flags for monitoring

#### 🥇 Gold Layer (Business-Ready Data)
The Gold layer contains aggregated, business-specific data optimized for reporting and analytics.

**Purpose:**
- Provide business-ready datasets
- Enable fast querying for dashboards
- Support specific business use cases
- Deliver actionable insights

**Models:**
- `gold_daily_sales_summary` - Daily KPIs and sales performance metrics
- `gold_customer_lifetime_value` - Customer segmentation and CLV analysis
- `gold_product_performance` - Product analytics and inventory insights

**Key Features:**
- Pre-aggregated metrics for performance
- Business-specific calculations (CLV, performance scores)
- Automated recommendations
- Executive-ready reporting formats

## Data Quality Framework

### Testing Strategy
Each layer includes comprehensive data quality tests:

#### Bronze Layer Tests
- Primary key uniqueness
- Not null validations for critical fields
- Data lineage metadata validation

#### Silver Layer Tests
- Data cleansing validation
- Business rule enforcement
- Standardization verification
- Quality flag testing

#### Gold Layer Tests
- Aggregation accuracy
- Business metric validation
- Classification logic verification
- Performance indicator testing

### Quality Monitoring
- `is_valid_transaction` flags in silver_sales_cleaned
- `is_valid_customer` flags in silver_customers_cleaned
- `is_valid_product` flags in silver_products_cleaned
- `data_quality_status` indicators in gold layer models

## Business Use Cases

### Sales Analytics
- **Daily Performance Tracking**: `gold_daily_sales_summary` provides KPIs for daily sales monitoring
- **Store Comparison**: Location-based performance metrics
- **Category Analysis**: Product category performance tracking

### Customer Management
- **Customer Segmentation**: Automated customer classification (Champion, Loyal, At Risk)
- **Lifetime Value Analysis**: CLV calculation and projection
- **Retention Strategies**: Activity status monitoring (Active, Dormant, Inactive)

### Product Management
- **Inventory Optimization**: Stock level monitoring and reorder recommendations
- **Pricing Strategy**: Profit margin analysis and pricing tier classification
- **Product Portfolio**: Performance-based product recommendations

## Implementation Details

### Materialization Strategy
- **Bronze**: Table materialization for data durability
- **Silver**: Table materialization for performance
- **Gold**: Table materialization for fast querying

### Tagging Strategy
- Layer tags: `bronze`, `silver`, `gold`
- Domain tags: `sales`, `customers`, `products`
- Purpose tags: `raw`, `cleaned`, `business`, `kpi`, `analytics`

### Configuration
The medallion architecture is configured in `dbt_project.yml`:
- Layer-specific materializations
- Default tags and descriptions
- Data quality settings

## Usage Instructions

### Running the Pipeline
```bash
# Load seed data
dbt seed

# Run all models in dependency order
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
```

### Running by Layer
```bash
# Bronze layer only
dbt run --select tag:bronze

# Silver layer only
dbt run --select tag:silver

# Gold layer only
dbt run --select tag:gold
```

### Data Lineage
The pipeline follows strict dependencies:
1. Seeds → Bronze models
2. Bronze → Silver models
3. Silver → Gold models

## Monitoring and Maintenance

### Performance Monitoring
- Monitor model run times and optimize as needed
- Review query performance in gold layer models
- Track data volume growth across layers

### Data Quality Monitoring
- Review test results regularly
- Monitor quality flags in silver and gold layers
- Set up alerts for data quality failures

### Schema Evolution
- Document schema changes in each layer
- Maintain backward compatibility when possible
- Use incremental strategies for large datasets

## Extension Opportunities

### Additional Data Sources
- Add new bronze models for additional source systems
- Extend silver layer with new cleaning logic
- Create specialized gold models for new business needs

### Advanced Analytics
- Add machine learning features in silver layer
- Create predictive models in gold layer
- Implement real-time streaming for bronze layer

### Data Governance
- Implement data classification and privacy controls
- Add data retention policies
- Enhance audit trail capabilities