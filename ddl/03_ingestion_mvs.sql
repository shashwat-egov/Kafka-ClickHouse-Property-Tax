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
CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.mv_property_events_raw
TO replacing_test.property_events_raw
AS
SELECT raw
FROM replacing_test.kafka_property_events;


-- Demand Events: Kafka → demand_events_raw
CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.mv_demand_events_raw
TO replacing_test.demand_events_raw
AS
SELECT raw
FROM replacing_test.kafka_demand_events;


-- Bill Events: Kafka → bill_events_raw
CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.mv_bill_events_raw
TO replacing_test.bill_events_raw
AS
SELECT raw
FROM replacing_test.kafka_bill_events;


-- Payment Events: Kafka → payment_events_raw
CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.mv_payment_events_raw
TO replacing_test.payment_events_raw
AS
SELECT raw
FROM replacing_test.kafka_payment_events;


-- Assessment Events: Kafka → assessment_events_raw
CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.mv_assessment_events_raw
TO replacing_test.assessment_events_raw
AS
SELECT raw
FROM replacing_test.kafka_assessment_events;
