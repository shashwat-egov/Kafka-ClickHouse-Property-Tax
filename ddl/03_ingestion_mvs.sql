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


-- Property Snapshot History: property_events_raw → property_snapshot_history

CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.mv_property_snapshot_history
TO replacing_test.property_snapshot_history
AS
SELECT
    JSONExtractString(raw, 'property', 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,
    toDateTime64(event_time, 3) AS event_time,
    JSONExtractString(raw, 'property', 'ownershipCategory') AS ownership_category,
    JSONExtractString(raw, 'property', 'usageCategory') AS usage_category,
    JSONExtractString(raw, 'property', 'status') AS property_status,
    JSONExtractString(raw, 'property', 'workflow', 'state', 'state') AS workflow_state,
    JSONExtractFloat(raw, 'property', 'superBuiltUpArea') AS super_built_up_area,
    JSONExtractFloat(raw, 'property', 'landArea') AS land_area,
    length(JSONExtractArrayRaw(raw, 'property', 'owners')) AS owner_count,
    toDateTime64(JSONExtractUInt(raw, 'auditcreatedtime') / 1000, 3) AS audit_created_time,
    toDateTime64(JSONExtractUInt(raw, 'property', 'auditDetails', 'createdTime') / 1000, 3) AS created_time,
    toDateTime64(JSONExtractUInt(raw, 'property', 'auditDetails', 'lastModifiedTime') / 1000, 3) AS last_modified_time
FROM replacing_test.property_events_raw;