-- ############################################################################
-- PROPERTY MART TABLES
-- ############################################################################

CREATE TABLE IF NOT EXISTS replacing_test.mart_property_agg
(
    snapshot_date Date,
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
    snapshot_date Date,
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

CREATE TABLE IF NOT EXISTS replacing_test.mart_demand_value_by_fy
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    total_demand Decimal(18, 2),
    total_collection Decimal(18, 2),
    total_outstanding Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS replacing_test.mart_collections_by_month
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    year_month LowCardinality(String),
    total_collected_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (snapshot_date, year_month)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS replacing_test.mart_properties_with_demand_by_fy
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    properties_with_demand String
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS replacing_test.mart_defaulters
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    property_id String,
    demand_id String,
    financial_year LowCardinality(String),
    total_tax_amount Decimal(12, 2),
    total_collection_amount Decimal(12, 2),
    outstanding_amount Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (tenant_id, demand_id)
SETTINGS index_granularity = 8192;
