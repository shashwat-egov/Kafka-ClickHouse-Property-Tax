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
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'save-property-registry,update-property-registry',
    kafka_group_name = 'clickhouse-property-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;

-- Demand Events
CREATE TABLE IF NOT EXISTS replacing_test.kafka_demand_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'save-demand,update-demand',
    kafka_group_name = 'clickhouse-demand-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;


-- Bill Events
CREATE TABLE IF NOT EXISTS replacing_test.kafka_bill_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'save-bill-db,update-bill-db',
    kafka_group_name = 'clickhouse-bill-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;


-- Payment Events
CREATE TABLE IF NOT EXISTS replacing_test.kafka_payment_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'egov.collection.payment-create',
    kafka_group_name = 'clickhouse-payment-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;


-- Payment Events
CREATE TABLE IF NOT EXISTS replacing_test.kafka_assessment_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'release-name-kafka.kafka-kraft.svc.cluster.local:9092',
    kafka_topic_list = 'save-pt-assessment,update-pt-assessment',
    kafka_group_name = 'clickhouse-assessment-consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100;