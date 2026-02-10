-- ============================================================================
-- MATERIALIZED VIEWS: Kafka → Raw Tables
-- ============================================================================
-- Purpose: Move raw JSON payloads from Kafka engine tables to raw MergeTree tables
-- Rules:
--   - No JSON parsing at this layer
--   - Just pass through the raw String as-is
--   - Parsing/extraction happens in downstream MVs
-- ============================================================================

-- Property Events: Kafka → property_events_raw
CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.mv_property_events_raw
TO collapsing_test.property_events_raw
AS
SELECT raw
FROM collapsing_test.kafka_property_events;


-- Demand Events: Kafka → demand_events_raw
CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.mv_demand_events_raw
TO collapsing_test.demand_events_raw
AS
SELECT raw
FROM collapsing_test.kafka_demand_events;