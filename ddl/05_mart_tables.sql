-- ############################################################################
-- PROPERTY MART TABLES
-- ############################################################################

CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_active_property_distribution_summary
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_type LowCardinality(String),
    ownership_category LowCardinality(String),
    usage_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_type, ownership_category, usage_category)
SETTINGS index_granularity = 8192;



CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_new_properties_by_fy
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    new_property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- DEMAND MART TABLES
-- ############################################################################

CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_demand_and_collection_summary
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    total_demand Decimal(18, 2),
    total_collection Decimal(18, 2),
    total_outstanding Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_collections_by_month
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    year_month LowCardinality(String),
    total_collected_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (tenant_id, year_month)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_properties_with_demand_by_fy
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    properties_with_demand String
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year)
SETTINGS index_granularity = 8192;




CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_defaulters
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_id String,
    demand_id String,
    financial_year LowCardinality(String),
    total_tax_amount Decimal(12, 2),
    total_collection_amount Decimal(12, 2),
    outstanding_amount Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_property_change_metrics
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_id String,
    property_type LowCardinality(String),
    ownership_category_changed UInt8,
    usage_category_changed UInt8,
    area_changed UInt8,
    workflow_state_changed UInt8,
    owners_changed UInt8,
    audit_created_time DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_type, property_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_property_risk_summary
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_id String,
    property_type LowCardinality(String),
    total_updates UInt32,
    ownership_changes UInt32,
    area_changes UInt32,
    workflow_reopens UInt32,
    risk_score UInt8
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_type, property_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_property_risk_summary_by_fy
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_id String,
    property_type LowCardinality(String),
    financial_year LowCardinality(String),
    total_updates UInt32,
    ownership_changes UInt32,
    area_changes UInt32,
    workflow_reopens UInt32,
    risk_score UInt8
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_type, financial_year, property_id)
SETTINGS index_granularity = 8192;

CREATE TABLE punjab_property_tax.mart_property_demand_vs_assessed_by_fy
(
    data_refresh_date Date DEFAULT today(),

    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),

    total_properties_assessed UInt64,
    total_properties_with_demand UInt64,
    total_properties_with_demand_no_assessment UInt64, -- properties with demand but no assessment
    total_properties_with_assessment_no_demand UInt64 -- properties with assessment but no demand
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year);

CREATE TABLE IF NOT EXISTS punjab_property_tax.mart_assessment_summary_by_fy
(
    data_refresh_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    property_type LowCardinality(String),
    channel LowCardinality(String),
    total_assessments UInt64,
    assessments_by_owner UInt64,
    assessments_by_others UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year, property_type, channel)
SETTINGS index_granularity = 8192;