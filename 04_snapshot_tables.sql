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
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS property_snapshot
(
    -- Snapshot Metadata
    snapshot_date Date DEFAULT today(),

    -- Business Keys
    tenant_id LowCardinality(String),
    property_id String,

    -- Property Attributes
    property_type LowCardinality(String),
    usage_category LowCardinality(String),
    ownership_category LowCardinality(String),
    status LowCardinality(String),
    acknowledgement_number String,
    assessment_number String,
    financial_year LowCardinality(String),
    source LowCardinality(String),
    channel LowCardinality(String),

    -- Land Info
    land_area Decimal(18, 4),
    land_area_unit LowCardinality(String),

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
    floor_no LowCardinality(String),
    unit_type LowCardinality(String),
    usage_category LowCardinality(String),
    occupancy_type LowCardinality(String),
    occupancy_date Date,

    -- Area Info
    constructed_area Decimal(18, 4),
    carpet_area Decimal(18, 4),
    built_up_area Decimal(18, 4),

    -- ARV
    arv_amount Decimal(18, 4),

    -- Audit Fields
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id, unit_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Owner Snapshot
-- Dedup Key: (tenant_id, property_id, owner_id)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS owner_snapshot
(
    -- Snapshot Metadata
    snapshot_date Date DEFAULT today(),

    -- Business Keys
    tenant_id LowCardinality(String),
    property_id String,
    owner_id String,

    -- Owner Attributes
    name String,
    mobile_number String,
    email String,
    gender LowCardinality(String),
    father_or_husband_name String,
    relationship LowCardinality(String),

    -- Owner Type
    owner_type LowCardinality(String),
    owner_info_uuid String,
    institution_id String,

    -- Document Info
    document_type LowCardinality(String),
    document_uid String,

    -- Ownership Details
    ownership_percentage Decimal(5, 2),
    is_primary_owner UInt8,
    is_active UInt8,

    -- Audit Fields
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id, owner_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Address Snapshot
-- Dedup Key: (tenant_id, property_id)
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
    building_name String,
    street String,
    locality_code LowCardinality(String),
    locality_name String,
    city LowCardinality(String),
    district LowCardinality(String),
    region LowCardinality(String),
    state LowCardinality(String),
    country LowCardinality(String),
    pin_code LowCardinality(String),

    -- Geo Coordinates
    latitude Decimal(10, 7),
    longitude Decimal(10, 7),

    -- Audit Fields
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Demand Snapshot
-- Dedup Key: (tenant_id, demand_id)
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

    -- Demand Period
    tax_period_from DateTime64(3),
    tax_period_to DateTime64(3),
    billing_period LowCardinality(String),

    -- Status
    status LowCardinality(String),
    is_payment_completed UInt8,

    -- Financial Year
    financial_year LowCardinality(String),

    -- Amounts
    minimum_amount_payable Decimal(18, 4),

    -- Payer Info
    payer_name String,
    payer_mobile String,
    payer_email String,

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
    tax_head_master_id String,

    -- Amounts
    tax_amount Decimal(18, 4),
    collection_amount Decimal(18, 4),

    -- Period
    tax_period_from DateTime64(3),
    tax_period_to DateTime64(3),

    -- Audit Fields
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, demand_id, tax_head_code)
SETTINGS index_granularity = 8192;
