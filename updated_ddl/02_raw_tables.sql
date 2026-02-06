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

CREATE TABLE IF NOT EXISTS collapsing_test.property_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    raw String
)
ENGINE = MergeTree
PARTITION BY toYYYY(event_time)
ORDER BY event_time
SETTINGS index_granularity = 8192;


-- ############################################################################
-- DEMAND EVENTS RAW
-- ############################################################################

CREATE TABLE IF NOT EXISTS collapsing_test.demand_events_raw
(
    event_time DateTime64(3) DEFAULT now64(3),
    raw String
)
ENGINE = MergeTree
PARTITION BY toYYYY(event_time)
ORDER BY event_time
SETTINGS index_granularity = 8192;
