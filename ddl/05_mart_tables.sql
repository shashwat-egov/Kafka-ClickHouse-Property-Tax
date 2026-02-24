-- ############################################################################
-- PROPERTY MART TABLES
-- ############################################################################

CREATE TABLE IF NOT EXISTS replacing_test.mart_active_property_distribution_summary
(
    data_refresh_date Date,
    tenant_id LowCardinality(String),
    ownership_category LowCardinality(String),
    usage_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, ownership_category, usage_category)
SETTINGS index_granularity = 8192;



CREATE TABLE IF NOT EXISTS replacing_test.mart_new_properties_by_fy
(
    data_refresh_date Date,
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

CREATE TABLE IF NOT EXISTS replacing_test.mart_demand_and_collection_summary
(
    data_refresh_date Date,
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    total_demand Decimal(18, 2),
    total_collection Decimal(18, 2),
    total_outstanding Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS replacing_test.mart_collections_by_month
(
    data_refresh_date Date,
    tenant_id LowCardinality(String),
    year_month LowCardinality(String),
    total_collected_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (tenant_id, year_month)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS replacing_test.mart_properties_with_demand_by_fy
(
    data_refresh_date Date,
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    properties_with_demand String
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year)
SETTINGS index_granularity = 8192;


CREATE TABLE replacing_test.mart_property_demand_coverage_by_fy
(
    snapshot_date Date,

    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),

    total_active_properties UInt64,
    properties_with_demand UInt64,
    properties_without_demand UInt64,
    coverage_ratio Float64,
)
ENGINE = MergeTree
ORDER BY (tenant_id, financial_year);


CREATE TABLE IF NOT EXISTS replacing_test.mart_defaulters
(
    data_refresh_date Date,
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
