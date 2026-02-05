-- ============================================================================
-- ANALYTICAL MARTS
-- ============================================================================
-- Property marts: Use batch RMVs (small tables)
-- Demand marts: Use streaming views (large tables, no batch needed)
-- ============================================================================

-- ############################################################################
-- PROPERTY MARTS (Batch RMVs)
-- ############################################################################

CREATE TABLE IF NOT EXISTS mart_property_count_by_tenant
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree ORDER BY (tenant_id);

CREATE TABLE IF NOT EXISTS mart_property_count_by_usage
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    usage_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree ORDER BY (tenant_id, usage_category);

CREATE TABLE IF NOT EXISTS mart_property_count_by_ownership
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    ownership_category LowCardinality(String),
    property_count UInt64
)
ENGINE = MergeTree ORDER BY (tenant_id, ownership_category);


CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_property_count_by_tenant
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_property_snapshot
TO mart_property_count_by_tenant
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    count() AS property_count
FROM property_snapshot
WHERE status = 'ACTIVE'
GROUP BY tenant_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_property_count_by_usage
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_property_snapshot
TO mart_property_count_by_usage
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    usage_category,
    count() AS property_count
FROM property_snapshot
WHERE status = 'ACTIVE'
GROUP BY tenant_id, usage_category;


CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_property_count_by_ownership
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_property_snapshot
TO mart_property_count_by_ownership
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    ownership_category,
    count() AS property_count
FROM property_snapshot
WHERE status = 'ACTIVE'
GROUP BY tenant_id, ownership_category;


-- ############################################################################
-- DEMAND MARTS (Streaming Views - no batch, always current)
-- ############################################################################

CREATE OR REPLACE VIEW v_mart_demand_value_by_fy AS
SELECT
    today() AS snapshot_date,
    financial_year,
    sum(tax_amount) AS total_demand_amount
FROM v_demand_detail_current
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year
ORDER BY financial_year;


CREATE OR REPLACE VIEW v_mart_collections_by_fy AS
SELECT
    today() AS snapshot_date,
    financial_year,
    sum(collection_amount) AS total_collected_amount
FROM v_demand_detail_current
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year
ORDER BY financial_year;


CREATE OR REPLACE VIEW v_mart_collections_by_month AS
SELECT
    today() AS snapshot_date,
    formatDateTime(last_modified_time, '%Y-%m') AS year_month,
    sum(collection_amount) AS total_collected_amount
FROM v_demand_detail_current
WHERE business_service = 'PT'
  AND collection_amount > 0
  AND last_modified_time > toDateTime64('1970-01-01 00:00:00', 3)
GROUP BY year_month
ORDER BY year_month;


CREATE OR REPLACE VIEW v_mart_properties_with_demand_by_fy AS
SELECT
    today() AS snapshot_date,
    financial_year,
    uniqExact(consumer_code) AS properties_with_demand
FROM v_demand_detail_current
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year
ORDER BY financial_year;


CREATE OR REPLACE VIEW v_mart_defaulters AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    consumer_code AS property_id,
    demand_id,
    financial_year,
    sum(tax_amount) AS total_tax_amount,
    sum(collection_amount) AS total_collected_amount,
    sum(tax_amount) - sum(collection_amount) AS outstanding_amount
FROM v_demand_detail_current
WHERE business_service = 'PT'
GROUP BY tenant_id, consumer_code, demand_id, financial_year
HAVING outstanding_amount > 0
ORDER BY tenant_id, consumer_code, demand_id;
