-- ============================================================================
-- RAW TABLES (APPEND-ONLY)
-- ============================================================================
-- Purpose: Store all incoming events as immutable facts
-- Rules:
--   - MergeTree only (NO ReplacingMergeTree)
--   - NO UPDATE, NO DELETE
--   - ORDER BY (tenant_id, business_id, last_modified_time)
-- ============================================================================

-- ############################################################################
-- PROPERTY TABLES
-- ############################################################################

CREATE TABLE IF NOT EXISTS property_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id String,
    tenant_id LowCardinality(String),
    property_id String,
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
    land_area Decimal(10, 2),
    super_built_up_area Decimal(10, 2),
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, last_modified_time)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS unit_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    tenant_id LowCardinality(String),
    property_id String,
    unit_id String,
    floor_no Int8,
    unit_type LowCardinality(String),
    usage_category LowCardinality(String),
    occupancy_type LowCardinality(String),
    occupancy_date Date,
    carpet_area Decimal(10, 2),
    built_up_area Decimal(18, 2),
    plinth_area Decimal(10, 2),
    super_built_up_area Decimal(10, 2),
    arv Decimal(12, 2),
    construction_type LowCardinality(String),
    construction_date Int64,
    active UInt8 DEFAULT 1,
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, unit_id, last_modified_time)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS owner_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    tenant_id LowCardinality(String),
    property_id String,
    owner_info_uuid String,
    user_id String,
    status LowCardinality(String),
    is_primary_owner UInt8,
    owner_type LowCardinality(String),
    ownership_percentage String,
    institution_id String,
    relationship LowCardinality(String),
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, owner_info_uuid, last_modified_time)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS address_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    tenant_id LowCardinality(String),
    property_id String,
    address_id String,
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
    latitude Decimal(9, 6),
    longitude Decimal(10, 7),
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, last_modified_time)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- DEMAND TABLES (Denormalized for streaming aggregation)
-- ############################################################################
-- Demand + DemandDetails combined to avoid JOINs
-- Each row = one demand_detail with parent demand attributes embedded

CREATE TABLE IF NOT EXISTS demand_with_details_raw
(
    event_time DateTime64(3) DEFAULT now64(3),

    -- Demand keys
    tenant_id LowCardinality(String),
    demand_id String,

    -- Demand attributes (denormalized)
    consumer_code String,
    consumer_type LowCardinality(String),
    business_service LowCardinality(String),
    payer String,
    tax_period_from DateTime64(3),
    tax_period_to DateTime64(3),
    demand_status LowCardinality(String),
    is_payment_completed UInt8,
    financial_year LowCardinality(String),
    minimum_amount_payable Decimal(18, 4),
    bill_expiry_time Int64,
    fixed_bill_expiry_date Int64,

    -- Demand detail keys
    demand_detail_id String,
    tax_head_code LowCardinality(String),

    -- Amounts
    tax_amount Decimal(12, 2),
    collection_amount Decimal(12, 2),

    -- Audit
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, demand_id, tax_head_code, last_modified_time)
SETTINGS index_granularity = 8192;
