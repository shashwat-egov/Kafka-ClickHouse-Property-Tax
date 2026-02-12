-- ============================================================================
-- KAFKA INGESTION TABLES
-- ============================================================================
-- Purpose: Ingest raw JSON payloads from Kafka topics
-- Rule: No JSON parsing here - store as raw String using JSONAsString
-- ============================================================================

-- Property Events
CREATE TABLE IF NOT EXISTS replacing_test.kafka_property_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'property-events',
    kafka_group_name = 'clickhouse-property-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 2,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

-- Demand Events
CREATE TABLE IF NOT EXISTS replacing_test.kafka_demand_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'demand-events',
    kafka_group_name = 'clickhouse-demand-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 2,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;
