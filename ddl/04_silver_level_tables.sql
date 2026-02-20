CREATE TABLE IF NOT EXISTS replacing_test.property_address_entity
(
    -- Ingestion metadata
    _ingested_at DateTime64(3) DEFAULT now64(3),

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

    -- Audit details
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    financial_year LowCardinality(String),

    additionaldetails String,

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
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, property_id)
SETTINGS index_granularity = 8192;




CREATE TABLE IF NOT EXISTS replacing_test.property_unit_entity
(
    -- Ingestion metadata
    _ingested_at DateTime64(3) DEFAULT now64(3),

    -- Unit columns
    tenant_id LowCardinality(String),
    property_uuid String,
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

    -- Audit details
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),


    -- Property columns
    property_id String,
    property_type LowCardinality(String),
    ownership_category LowCardinality(String),
    property_status LowCardinality(String),
    no_of_floors Int8,

)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, unit_id)
SETTINGS index_granularity = 8192;




CREATE TABLE IF NOT EXISTS replacing_test.property_owner_entity
(
    -- Ingestion metadata
    _ingested_at DateTime64(3) DEFAULT now64(3),


    tenant_id LowCardinality(String),
    property_uuid String,
    owner_info_uuid String,
    user_id String,
    status LowCardinality(String),
    is_primary_owner UInt8,
    owner_type LowCardinality(String),
    ownership_percentage String,
    institution_id String,
    relationship LowCardinality(String),

    -- Audit details
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    
    -- Property columns
    property_id String,
    property_type LowCardinality(String),
    ownership_category LowCardinality(String),
    property_status LowCardinality(String),
    no_of_floors Int8,

)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, owner_info_uuid)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS replacing_test.demand_with_details_entity
(
    -- Ingestion metadata
    _ingested_at DateTime64(3) DEFAULT now64(3),

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

    -- Amounts
    total_tax_amount Decimal(12, 2),
    total_collection_amount Decimal(12, 2),


    pt_tax Decimal(18,4),
    pt_cancer_cess Decimal(18,4),
    pt_fire_cess Decimal(18,4),
    pt_roundoff Decimal(18,4),
    pt_owner_exemption Decimal(18,4),
    pt_unit_usage_exemption Decimal(18,4),

    -- Derived columns
    outstanding_amount Decimal(18, 4),
    is_paid UInt8,

    -- Audit details
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    
)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, demand_id)
SETTINGS index_granularity = 8192;
