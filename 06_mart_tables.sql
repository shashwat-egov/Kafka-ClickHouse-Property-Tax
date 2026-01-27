-- ============================================================================
-- ANALYTICAL MART TABLES
-- ============================================================================
-- Purpose: Pre-aggregated analytical outputs for dashboards and reporting
-- Source: Snapshot tables only (deduplicated, latest state)
-- Refresh: Daily after snapshot refresh completes
-- ============================================================================

-- ############################################################################
-- MART 1: Property Count by Tenant
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_property_count_by_tenant
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 2: Property Count by Tenant and Ownership Category
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_property_count_by_ownership
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    ownership_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, ownership_category)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 3: Property Count by Tenant and Usage Category
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_property_count_by_usage
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    usage_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, usage_category)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 4: New Property Count by Financial Year
-- Financial Year: April to March (e.g., 2024-25 = Apr 2024 - Mar 2025)
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_new_properties_by_fy
(
    snapshot_date Date DEFAULT today(),
    financial_year LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 5: Properties with Demand Generated per Financial Year
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_properties_with_demand_by_fy
(
    snapshot_date Date DEFAULT today(),
    financial_year LowCardinality(String),
    properties_with_demand UInt64
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 6: Demand Value by Financial Year
-- Total tax_amount from demand details
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_demand_value_by_fy
(
    snapshot_date Date DEFAULT today(),
    financial_year LowCardinality(String),
    total_demand_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 7: Property Tax Collected per Financial Year
-- Total collection_amount from demand details
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_collections_by_fy
(
    snapshot_date Date DEFAULT today(),
    financial_year LowCardinality(String),
    total_collected_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 8: Month-wise Collections
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_collections_by_month
(
    snapshot_date Date DEFAULT today(),
    year_month LowCardinality(String),  -- Format: YYYY-MM
    total_collected_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (year_month)
SETTINGS index_granularity = 8192;

-- ############################################################################
-- MART 9: Defaulter List
-- Outstanding = tax_amount - collection_amount > 0
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_defaulters
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_id String,
    demand_id String,
    financial_year LowCardinality(String),
    total_tax_amount Decimal(18, 4),
    total_collected_amount Decimal(18, 4),
    outstanding_amount Decimal(18, 4)
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id, demand_id)
SETTINGS index_granularity = 8192;
