-- ============================================================================
-- RAW TABLES (APPEND-ONLY, JSON-AS-STRING)
-- ============================================================================
-- Purpose: Store all incoming events as immutable JSON payloads
-- Rules:
--   - MergeTree only (NO ReplacingMergeTree)
--   - NO UPDATE, NO DELETE
--   - Raw JSON stored as String — no parsing at this layer
--   - Parsing/extraction happens downstream in MVs
-- ============================================================================

-- ############################################################################
-- PROPERTY EVENTS RAW
-- ############################################################################

CREATE TABLE IF NOT EXISTS replacing_test.property_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- DEMAND EVENTS RAW
-- ############################################################################

CREATE TABLE IF NOT EXISTS replacing_test.demand_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- BILL EVENTS RAW
-- ############################################################################

CREATE TABLE IF NOT EXISTS replacing_test.bill_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- PAYMENT EVENTS RAW
-- ############################################################################

CREATE TABLE IF NOT EXISTS replacing_test.payment_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- ASSESSMENT EVENTS RAW
-- ############################################################################

CREATE TABLE IF NOT EXISTS replacing_test.assessment_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    id         UUID          DEFAULT generateUUIDv4(),
    raw        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, id)
SETTINGS index_granularity = 8192;