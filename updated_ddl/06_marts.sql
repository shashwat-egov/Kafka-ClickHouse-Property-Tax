-- ============================================================================
-- ANALYTICAL MARTS (Refreshable Materialized Views)
-- ============================================================================
-- Mart tables + RMVs that refresh daily at 02:00 AM (after the 01:30 AM DAG).
-- RMVs re-execute the full query against CollapsingMergeTree silver tables.
--
-- Pattern:
--   Inner subquery: GROUP BY sort_key HAVING sum(sign) > 0  →  one row per live entity
--   Outer query:    WHERE filters + aggregation across entities
--
-- Target tables use ReplacingMergeTree(snapshot_date) so each daily refresh
-- replaces the previous snapshot.
-- ============================================================================


-- ############################################################################
-- PROPERTY MART TABLES
-- ############################################################################

CREATE TABLE IF NOT EXISTS collapsing_test.mart_property_count_by_tenant
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS collapsing_test.mart_property_count_by_usage
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    usage_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, usage_category)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS collapsing_test.mart_property_count_by_ownership
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    ownership_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, ownership_category)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- DEMAND MART TABLES
-- ############################################################################

CREATE TABLE IF NOT EXISTS collapsing_test.mart_demand_value_by_fy
(
    snapshot_date Date,
    financial_year LowCardinality(String),
    total_demand_amount Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS collapsing_test.mart_collections_by_fy
(
    snapshot_date Date,
    financial_year LowCardinality(String),
    total_collected_amount Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS collapsing_test.mart_collections_by_month
(
    snapshot_date Date,
    year_month String,
    total_collected_amount Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY (year_month)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS collapsing_test.mart_properties_with_demand_by_fy
(
    snapshot_date Date,
    financial_year LowCardinality(String),
    properties_with_demand UInt64
)
ENGINE = MergeTree
ORDER BY (financial_year)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS collapsing_test.mart_defaulters
(
    snapshot_date Date,
    tenant_id LowCardinality(String),
    property_id String,
    demand_id String,
    financial_year LowCardinality(String),
    total_tax_amount Decimal(12, 2),
    total_collection_amount Decimal(12, 2),
    outstanding_amount Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (tenant_id, demand_id)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- PROPERTY RMVs (refresh daily at 02:00 AM, after the 01:30 AM Airflow DAG)
-- ############################################################################

CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_property_count_by_tenant
REFRESH EVERY 1 DAY OFFSET 2 HOUR
TO collapsing_test.mart_property_count_by_tenant
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    count() AS property_count
FROM
(
    SELECT tenant_id, property_id, any(status) AS status
    FROM collapsing_test.property_address_collapsing
    GROUP BY tenant_id, property_id
    HAVING sum(sign) > 0
)
WHERE status = 'ACTIVE'
GROUP BY tenant_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_property_count_by_usage
REFRESH EVERY 1 DAY OFFSET 2 HOUR
TO collapsing_test.mart_property_count_by_usage
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    usage_category,
    count() AS property_count
FROM
(
    SELECT tenant_id, property_id,
           any(status) AS status,
           any(usage_category) AS usage_category
    FROM collapsing_test.property_address_collapsing
    GROUP BY tenant_id, property_id
    HAVING sum(sign) > 0
)
WHERE status = 'ACTIVE'
GROUP BY tenant_id, usage_category;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_property_count_by_ownership
REFRESH EVERY 1 DAY OFFSET 2 HOUR
TO collapsing_test.mart_property_count_by_ownership
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    ownership_category,
    count() AS property_count
FROM
(
    SELECT tenant_id, property_id,
           any(status) AS status,
           any(ownership_category) AS ownership_category
    FROM collapsing_test.property_address_collapsing
    GROUP BY tenant_id, property_id
    HAVING sum(sign) > 0
)
WHERE status = 'ACTIVE'
GROUP BY tenant_id, ownership_category;


-- ############################################################################
-- DEMAND RMVs (refresh daily at 02:00 AM)
-- ############################################################################

CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_demand_value_by_fy
REFRESH EVERY 1 DAY OFFSET 2 HOUR
TO collapsing_test.mart_demand_value_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    financial_year,
    sum(total_tax_amount) AS total_demand_amount
FROM
(
    SELECT tenant_id, demand_id,
           any(business_service) AS business_service,
           any(financial_year) AS financial_year,
           sum(total_tax_amount * sign) AS total_tax_amount
    FROM collapsing_test.demand_with_details_collapsing
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_collections_by_fy
REFRESH EVERY 1 DAY OFFSET 2 HOUR
TO collapsing_test.mart_collections_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    financial_year,
    sum(total_collection_amount) AS total_collected_amount
FROM
(
    SELECT tenant_id, demand_id,
           any(business_service) AS business_service,
           any(financial_year) AS financial_year,
           sum(total_collection_amount * sign) AS total_collection_amount
    FROM collapsing_test.demand_with_details_collapsing
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_collections_by_month
REFRESH EVERY 1 DAY OFFSET 2 HOUR
TO collapsing_test.mart_collections_by_month
EMPTY
AS
SELECT
    today() AS snapshot_date,
    year_month,
    sum(total_collection_amount) AS total_collected_amount
FROM
(
    SELECT tenant_id, demand_id,
           any(business_service) AS business_service,
           formatDateTime(any(last_modified_time), '%Y-%m') AS year_month,
           sum(total_collection_amount * sign) AS total_collection_amount
    FROM collapsing_test.demand_with_details_collapsing
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND total_collection_amount > 0
  AND year_month != '1970-01'
GROUP BY year_month;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_properties_with_demand_by_fy
REFRESH EVERY 1 DAY OFFSET 2 HOUR
TO collapsing_test.mart_properties_with_demand_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    financial_year,
    uniqExact(consumer_code) AS properties_with_demand
FROM
(
    SELECT tenant_id, demand_id,
           any(business_service) AS business_service,
           any(financial_year) AS financial_year,
           any(consumer_code) AS consumer_code
    FROM collapsing_test.demand_with_details_collapsing
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_defaulters
REFRESH EVERY 1 DAY OFFSET 2 HOUR
TO collapsing_test.mart_defaulters
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    consumer_code AS property_id,
    demand_id,
    financial_year,
    total_tax_amount,
    total_collection_amount,
    total_tax_amount - total_collection_amount AS outstanding_amount
FROM
(
    SELECT tenant_id, demand_id,
           any(consumer_code) AS consumer_code,
           any(business_service) AS business_service,
           any(financial_year) AS financial_year,
           sum(total_tax_amount * sign) AS total_tax_amount,
           sum(total_collection_amount * sign) AS total_collection_amount
    FROM collapsing_test.demand_with_details_collapsing
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND total_tax_amount - total_collection_amount > 0;
