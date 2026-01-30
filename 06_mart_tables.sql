-- ============================================================================
-- ANALYTICAL MART TABLES
-- ============================================================================
-- Purpose: Pre-aggregated analytical outputs for dashboards and reporting
-- Source: Snapshot tables only (deduplicated, latest state)
-- Refresh: Daily after snapshot refresh completes via RMVs
-- ============================================================================

-- ############################################################################
-- MART 1: Active Property Count + Ownership + Usage (Combined)
-- ############################################################################
CREATE TABLE IF NOT EXISTS punjab_kafka_test.mart_property_snapshot_agg
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    ownership_category LowCardinality(String),
    usage_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (snapshot_date, tenant_id, ownership_category, usage_category)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 2: New Property Count by Financial Year
-- ############################################################################
CREATE TABLE IF NOT EXISTS punjab_kafka_test.mart_new_properties_by_fy
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    new_property_count UInt64
)
ENGINE = MergeTree
ORDER BY (snapshot_date, tenant_id, financial_year)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 3: Properties with Demand Generated per Financial Year
-- ############################################################################
CREATE TABLE IF NOT EXISTS punjab_kafka_test.mart_properties_with_demand
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    properties_with_demand UInt64
)
ENGINE = MergeTree
ORDER BY (snapshot_date, tenant_id, financial_year)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 4: Demand Value by Financial Year (with tax head breakdown)
-- ############################################################################
CREATE TABLE IF NOT EXISTS punjab_kafka_test.mart_demand_value
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    pt_tax_amount Decimal(18, 4),
    pt_cancer_cess Decimal(18, 4),
    pt_fire_cess Decimal(18, 4),
    pt_roundoff Decimal(18, 4),
    pt_owner_exemption Decimal(18, 4),
    pt_unit_usage_exemption Decimal(18, 4),
    total_demand Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (snapshot_date, tenant_id, financial_year)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 5: Collections by Financial Year (with payment details)
-- ############################################################################
CREATE TABLE IF NOT EXISTS punjab_kafka_test.mart_property_collections
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    financial_year LowCardinality(String),
    payment_month Date,
    collection_mode LowCardinality(String),
    collection_source LowCardinality(String),
    pt_tax_amount Decimal(18, 4),
    pt_cancer_cess Decimal(18, 4),
    pt_fire_cess Decimal(18, 4),
    pt_roundoff Decimal(18, 4),
    pt_owner_exemption Decimal(18, 4),
    pt_unit_usage_exemption Decimal(18, 4),
    net_amount_collected Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (snapshot_date, tenant_id, financial_year, payment_month)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 6: Month-wise Collections (Simple)
-- ############################################################################
CREATE TABLE IF NOT EXISTS punjab_kafka_test.mart_collections_by_month
(
    snapshot_date Date,
    year_month LowCardinality(String),
    total_collected_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (snapshot_date, year_month)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 7: Defaulter List with Details (per demand)
-- ############################################################################
CREATE TABLE IF NOT EXISTS punjab_kafka_test.mart_defaulters_details
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    property_id String,
    demand_id String,
    financial_year LowCardinality(String),
    total_tax_amount Decimal(18, 4),
    total_collected_amount Decimal(18, 4),
    outstanding_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (snapshot_date, tenant_id, property_id, demand_id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 8: Defaulter List (Aggregated by property)
-- ############################################################################
CREATE TABLE IF NOT EXISTS punjab_kafka_test.mart_defaulters
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    property_id String,
    due_fy_count UInt32,
    total_outstanding Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (snapshot_date, tenant_id, property_id)
SETTINGS index_granularity = 8192;
