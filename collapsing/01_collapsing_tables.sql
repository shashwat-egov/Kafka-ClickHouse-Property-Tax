-- ============================================================================
-- COLLAPSING MERGE TREE TABLES
-- ============================================================================
-- Replaces append-only MergeTree with CollapsingMergeTree(sign)
-- Sign column: +1 = active row, -1 = cancelled row
-- Background merges collapse pairs with matching ORDER BY keys and opposite signs
-- ============================================================================

-- ############################################################################
-- Property Table (CollapsingMergeTree)
-- ############################################################################
CREATE TABLE IF NOT EXISTS property_collapsing
(
    -- Business keys (part of ORDER BY for collapsing)
    tenant_id           LowCardinality(String),
    property_id         String,

    -- Collapsing sign column
    sign                Int8,

    -- Version for tie-breaking during queries
    version             UInt32,

    -- Timestamps
    created_time        DateTime64(3),
    last_modified_time  DateTime64(3),

    -- Business columns
    id                  String,
    survey_id           String,
    account_id          String,
    old_property_id     String,
    property_type       LowCardinality(String),
    usage_category      LowCardinality(String),
    ownership_category  LowCardinality(String),
    status              LowCardinality(String),
    acknowledgement_number String,
    creation_reason     LowCardinality(String),
    no_of_floors        UInt8,
    source              LowCardinality(String),
    channel             LowCardinality(String),
    land_area           Decimal(18, 4),
    super_built_up_area Decimal(18, 4),
    created_by          String,
    last_modified_by    String,

    -- Ingestion metadata
    _ingested_at        DateTime64(3) DEFAULT now64(3)
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, property_id, version)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- Demand Table (CollapsingMergeTree)
-- ############################################################################
CREATE TABLE IF NOT EXISTS demand_collapsing
(
    tenant_id               LowCardinality(String),
    demand_id               String,

    sign                    Int8,
    version                 UInt32,

    created_time            DateTime64(3),
    last_modified_time      DateTime64(3),

    consumer_code           String,
    consumer_type           LowCardinality(String),
    business_service        LowCardinality(String),
    payer                   String,
    tax_period_from         DateTime64(3),
    tax_period_to           DateTime64(3),
    status                  LowCardinality(String),
    is_payment_completed    Bool,
    financial_year          LowCardinality(String),
    minimum_amount_payable  Decimal(18, 2),
    bill_expiry_time        DateTime64(3),
    fixed_bill_expiry_date  Date,
    created_by              String,
    last_modified_by        String,

    _ingested_at            DateTime64(3) DEFAULT now64(3)
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, demand_id, version)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- Demand Detail Table (CollapsingMergeTree)
-- ############################################################################
CREATE TABLE IF NOT EXISTS demand_detail_collapsing
(
    tenant_id           LowCardinality(String),
    demand_id           String,
    tax_head_code       LowCardinality(String),

    sign                Int8,
    version             UInt32,

    created_time        DateTime64(3),
    last_modified_time  DateTime64(3),

    demand_detail_id    String,
    tax_amount          Decimal(18, 2),
    collection_amount   Decimal(18, 2),
    created_by          String,
    last_modified_by    String,

    _ingested_at        DateTime64(3) DEFAULT now64(3)
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(last_modified_time)
ORDER BY (tenant_id, demand_id, tax_head_code, version)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- Raw JSON Staging Table (for Airflow to read from)
-- This replaces the MV-based JSON parsing
-- ############################################################################
CREATE TABLE IF NOT EXISTS kafka_raw_json
(
    topic               LowCardinality(String),
    partition           UInt16,
    offset              UInt64,
    timestamp           DateTime64(3),
    key                 String,
    payload             String,  -- Raw JSON
    _consumed_at        DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (topic, partition, offset)
TTL timestamp + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;


-- ############################################################################
-- Kafka Engine Table (minimal - just stores raw JSON)
-- ############################################################################
CREATE TABLE IF NOT EXISTS kafka_property_events
(
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'property-events',
    kafka_group_name = 'clickhouse-collapsing-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 2;


-- ############################################################################
-- MV: Kafka → Raw JSON staging (no transformation)
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_raw_json
TO kafka_raw_json
AS
SELECT
    'property-events' AS topic,
    0 AS partition,  -- Would need _partition from Kafka if tracking
    0 AS offset,     -- Would need _offset from Kafka if tracking
    now64(3) AS timestamp,
    '' AS key,
    payload
FROM kafka_property_events;


-- ============================================================================
-- PROCESSING STATE TABLE (for Airflow idempotency)
-- ============================================================================
CREATE TABLE IF NOT EXISTS airflow_processing_state
(
    dag_id              String,
    task_id             String,
    execution_date      DateTime64(3),
    window_start        DateTime64(3),
    window_end          DateTime64(3),
    records_processed   UInt64,
    status              Enum8('running' = 1, 'completed' = 2, 'failed' = 3),
    started_at          DateTime64(3),
    completed_at        Nullable(DateTime64(3)),
    idempotency_key     String,  -- Hash of window + dag_run_id

    PRIMARY KEY (dag_id, task_id, execution_date)
)
ENGINE = ReplacingMergeTree(started_at)
ORDER BY (dag_id, task_id, execution_date);
