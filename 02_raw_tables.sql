-- ============================================================================
-- RAW TABLES (APPEND-ONLY)
-- ============================================================================
-- Purpose: Store all incoming events as immutable facts
-- Rules:
--   - MergeTree only (NO ReplacingMergeTree)
--   - NO UPDATE, NO DELETE
--   - ORDER BY (tenant_id, business_id, lastModifiedTime)
--   - All arrays flattened via ARRAY JOIN in MVs
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Property Raw Table
-- Business Key: (tenant_id, property_id)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS property_raw
(
    -- Metadata
    event_time DateTime64(3) DEFAULT now64(3),

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

    -- Version for tie-breaking (if available)
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, last_modified_time)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Unit Raw Table
-- Business Key: (tenant_id, property_id, unit_id)
-- Flattened from property.units[] array
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS unit_raw
(
    -- Metadata
    event_time DateTime64(3) DEFAULT now64(3),

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

    -- ARV (Annual Rental Value)
    arv Decimal(12, 2),

    -- Construction Info
    construction_type LowCardinality(String),
    construction_date Int64,

    -- Status
    active UInt8 DEFAULT 1,

    -- Audit Fields (inherited from parent property event)
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),

    -- Version
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, unit_id, last_modified_time)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Owner Raw Table
-- Business Key: (tenant_id, property_id, owner_info_uuid)
-- Flattened from property.owners[] array
-- Maps to eg_pt_owner table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS owner_raw
(
    -- Metadata
    event_time DateTime64(3) DEFAULT now64(3),

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

    -- Version
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, owner_info_uuid, last_modified_time)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Address Raw Table
-- Business Key: (tenant_id, property_id)
-- One address per property (1:1 relationship)
-- Maps to eg_pt_address table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS address_raw
(
    -- Metadata
    event_time DateTime64(3) DEFAULT now64(3),

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
    country LowCardinality(String) DEFAULT 'IN',
    pin_code LowCardinality(String),

    -- Geo Coordinates
    latitude Decimal(9, 6),
    longitude Decimal(10, 7),

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),

    -- Version
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, last_modified_time)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Demand Raw Table
-- Business Key: (tenant_id, demand_id)
-- Maps to egbs_demand_v1 table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demand_raw
(
    -- Metadata
    event_time DateTime64(3) DEFAULT now64(3),

    -- Business Keys
    tenant_id LowCardinality(String),
    demand_id String,

    -- References
    consumer_code String,  -- Links to property_id
    consumer_type LowCardinality(String),
    business_service LowCardinality(String),

    -- Payer (single column in PG)
    payer String,

    -- Demand Period
    tax_period_from DateTime64(3),
    tax_period_to DateTime64(3),

    -- Status
    status LowCardinality(String),
    is_payment_completed UInt8 DEFAULT 0,

    -- Financial Year (derived from tax_period_from)
    financial_year LowCardinality(String),

    -- Amounts
    minimum_amount_payable Decimal(18, 4),

    -- Bill Expiry
    bill_expiry_time Int64 DEFAULT 0,
    fixed_bill_expiry_date Int64 DEFAULT 0,

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),

    -- Version
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, demand_id, last_modified_time)
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- Demand Detail Raw Table
-- Business Key: (tenant_id, demand_id, tax_head_code)
-- Flattened from demand.demandDetails[] array
-- Maps to egbs_demanddetail_v1 table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demand_detail_raw
(
    -- Metadata
    event_time DateTime64(3) DEFAULT now64(3),

    -- Business Keys
    tenant_id LowCardinality(String),
    demand_id String,
    demand_detail_id String,

    -- Tax Head Reference
    tax_head_code LowCardinality(String),

    -- Amounts
    tax_amount Decimal(12, 2),
    collection_amount Decimal(12, 2) DEFAULT 0,

    -- Audit Fields
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),

    -- Version
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, demand_id, tax_head_code, last_modified_time)
SETTINGS index_granularity = 8192;
