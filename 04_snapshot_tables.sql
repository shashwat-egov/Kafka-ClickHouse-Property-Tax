-- ============================================================================
-- SNAPSHOT TABLES (DAILY REFRESHED)
-- ============================================================================
-- Purpose: Store the "latest state" of each entity for analytical queries
-- Deduplication: Uses argMax(column, (last_modified_time, version))
-- Refresh: Daily at 01:00 AM via orchestrated INSERT ... SELECT
-- Rules:
--   - Physical MergeTree tables (NOT views)
--   - TRUNCATE + INSERT for full refresh (idempotent, re-runnable)
--   - ORDER BY optimized for analytics (tenant_id first)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Property Snapshot
-- Dedup Key: (tenant_id, property_id)
-- Maps to eg_pt_property table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS property_snapshot
(
    -- Snapshot Metadata
    snapshot_date Date DEFAULT today(),

    -- Business Keys
    id String,
    tenant_id LowCardinality(String),
    property_id String,

    -- Property Attributes
    survey_id String,
    account_id String,
    old_property_id String,
    property_type LowCardinality(String),
    usage_category LowCardinality(String),
    ownership_category LowCardinality(String),
    status LowCardinality(String),
    acknowledgement_number String,
    creation_reason LowCardinality(String),
    no_of_floors Int8,
    source LowCardinality(String),
    channel LowCardinality(String),

    -- Land Info
    land_area Decimal(10, 2),
    super_built_up_area Decimal(10, 2),

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Unit Snapshot
-- Dedup Key: (tenant_id, property_id, unit_id)
-- Maps to eg_pt_unit table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS unit_snapshot
(
    -- Snapshot Metadata
    snapshot_date Date DEFAULT today(),

    -- Business Keys
    tenant_id LowCardinality(String),
    property_id String,
    unit_id String,

    -- Unit Attributes
    floor_no Int8,
    unit_type LowCardinality(String),
    usage_category LowCardinality(String),
    occupancy_type LowCardinality(String),
    occupancy_date Date,

    -- Area Info
    carpet_area Decimal(10, 2),
    built_up_area Decimal(18, 2),
    plinth_area Decimal(10, 2),
    super_built_up_area Decimal(10, 2),

    -- ARV
    arv Decimal(12, 2),

    -- Construction Info
    construction_type LowCardinality(String),
    construction_date Int64,

    -- Status
    active UInt8,

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id, unit_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Owner Snapshot
-- Dedup Key: (tenant_id, property_id, owner_info_uuid)
-- Maps to eg_pt_owner table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS owner_snapshot
(
    -- Snapshot Metadata
    snapshot_date Date DEFAULT today(),

    -- Business Keys (from eg_pt_owner)
    tenant_id LowCardinality(String),
    property_id String,
    owner_info_uuid String,
    user_id String,

    -- Owner Status
    status LowCardinality(String),
    is_primary_owner UInt8,

    -- Owner Type
    owner_type LowCardinality(String),
    ownership_percentage String,
    institution_id String,
    relationship LowCardinality(String),

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id, owner_info_uuid)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Address Snapshot
-- Dedup Key: (tenant_id, property_id)
-- Maps to eg_pt_address table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS address_snapshot
(
    -- Snapshot Metadata
    snapshot_date Date DEFAULT today(),

    -- Business Keys
    tenant_id LowCardinality(String),
    property_id String,
    address_id String,

    -- Address Components
    door_no String,
    plot_no String,
    building_name String,
    street String,
    landmark String,
    locality LowCardinality(String),
    city LowCardinality(String),
    district LowCardinality(String),
    region LowCardinality(String),
    state LowCardinality(String),
    country LowCardinality(String),
    pin_code LowCardinality(String),

    -- Geo Coordinates
    latitude Decimal(9, 6),
    longitude Decimal(10, 7),

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Demand Snapshot
-- Dedup Key: (tenant_id, demand_id)
-- Maps to egbs_demand_v1 table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demand_snapshot
(
    -- Snapshot Metadata
    snapshot_date Date DEFAULT today(),

    -- Business Keys
    tenant_id LowCardinality(String),
    demand_id String,

    -- References
    consumer_code String,
    consumer_type LowCardinality(String),
    business_service LowCardinality(String),

    -- Payer (single column as in PG)
    payer String,

    -- Demand Period
    tax_period_from DateTime64(3),
    tax_period_to DateTime64(3),

    -- Status
    status LowCardinality(String),
    is_payment_completed UInt8,

    -- Financial Year
    financial_year LowCardinality(String),

    -- Amounts
    minimum_amount_payable Decimal(18, 4),

    -- Bill Expiry
    bill_expiry_time Int64,
    fixed_bill_expiry_date Int64,

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, demand_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Demand Detail Snapshot
-- Dedup Key: (tenant_id, demand_id, tax_head_code)
-- Maps to egbs_demanddetail_v1 table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demand_detail_snapshot
(
    -- Snapshot Metadata
    snapshot_date Date DEFAULT today(),

    -- Business Keys
    tenant_id LowCardinality(String),
    demand_id String,
    demand_detail_id String,

    -- Tax Head Reference
    tax_head_code LowCardinality(String),

    -- Amounts
    tax_amount Decimal(12, 2),
    collection_amount Decimal(12, 2),

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, demand_id, tax_head_code)
SETTINGS index_granularity = 8192;
