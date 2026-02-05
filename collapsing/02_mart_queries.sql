-- ============================================================================
-- MART QUERY PATTERNS FOR COLLAPSING MERGE TREE
-- ============================================================================
-- Two approaches: FINAL (convenient) vs SUM(sign) (performant)
-- ============================================================================


-- ############################################################################
-- APPROACH 1: Using FINAL (Simple but slower)
-- ############################################################################
-- FINAL forces merge at query time - good for small tables or point queries
-- Avoid for large analytical scans

-- Get current state of a single property
SELECT *
FROM property_collapsing FINAL
WHERE tenant_id = 'pb.amritsar'
  AND property_id = 'PT-AMRITSAR-000001';

-- Count properties by tenant (FINAL approach - NOT recommended for large tables)
SELECT
    tenant_id,
    count() AS property_count
FROM property_collapsing FINAL
WHERE status = 'ACTIVE'
GROUP BY tenant_id;


-- ############################################################################
-- APPROACH 2: Using SUM(sign) (Performant - RECOMMENDED)
-- ############################################################################
-- Works correctly even before background merges complete
-- Required pattern for aggregations

-- Property count by tenant (CORRECT)
SELECT
    tenant_id,
    sum(sign) AS property_count
FROM property_collapsing
WHERE status = 'ACTIVE'
GROUP BY tenant_id
HAVING property_count > 0;

-- Property count by usage category
SELECT
    tenant_id,
    usage_category,
    sum(sign) AS property_count
FROM property_collapsing
WHERE status = 'ACTIVE'
GROUP BY tenant_id, usage_category
HAVING property_count > 0
ORDER BY tenant_id, property_count DESC;


-- ############################################################################
-- MART: Defaulters (Outstanding Amount)
-- ############################################################################
-- Join demands with demand_details, using sum(sign) on both sides

SELECT
    d.tenant_id,
    d.consumer_code AS property_id,
    d.financial_year,
    -- Tax amount: sum of (amount * sign) for each detail
    sum(dd.tax_amount * dd.sign) AS total_tax,
    sum(dd.collection_amount * dd.sign) AS total_collected,
    sum(dd.tax_amount * dd.sign) - sum(dd.collection_amount * dd.sign) AS outstanding
FROM demand_collapsing d
INNER JOIN demand_detail_collapsing dd
    ON d.tenant_id = dd.tenant_id
   AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
  AND d.status = 'ACTIVE'
  AND d.sign = 1  -- Only consider active demand rows
GROUP BY
    d.tenant_id,
    d.consumer_code,
    d.financial_year
HAVING outstanding > 0
ORDER BY outstanding DESC;


-- ############################################################################
-- MART: Collections by Financial Year
-- ############################################################################
SELECT
    d.tenant_id,
    d.financial_year,
    sum(dd.collection_amount * dd.sign) AS total_collected
FROM demand_collapsing d
INNER JOIN demand_detail_collapsing dd
    ON d.tenant_id = dd.tenant_id
   AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
  AND d.status = 'ACTIVE'
  AND d.sign = 1
  AND dd.collection_amount > 0
GROUP BY d.tenant_id, d.financial_year
ORDER BY d.tenant_id, d.financial_year;


-- ############################################################################
-- MART: Property Snapshot Aggregation (Pre-materialized)
-- ############################################################################
CREATE TABLE IF NOT EXISTS mart_property_snapshot_agg_collapsing
(
    snapshot_date       Date,
    tenant_id           LowCardinality(String),
    ownership_category  LowCardinality(String),
    usage_category      LowCardinality(String),
    property_count      Int64
)
ENGINE = ReplacingMergeTree(snapshot_date)
ORDER BY (snapshot_date, tenant_id, ownership_category, usage_category);

-- Refresh query (run by Airflow)
INSERT INTO mart_property_snapshot_agg_collapsing
SELECT
    today() AS snapshot_date,
    tenant_id,
    ownership_category,
    usage_category,
    sum(sign) AS property_count
FROM property_collapsing
WHERE status = 'ACTIVE'
GROUP BY tenant_id, ownership_category, usage_category
HAVING property_count > 0;


-- ############################################################################
-- PATTERN: Getting latest row when you need all columns
-- ############################################################################
-- When you need the actual row data (not aggregates), use argMax with version

SELECT
    tenant_id,
    property_id,
    argMax(status, version) AS status,
    argMax(usage_category, version) AS usage_category,
    argMax(ownership_category, version) AS ownership_category,
    argMax(last_modified_time, version) AS last_modified_time,
    sum(sign) AS existence  -- 1 if exists, 0 if collapsed
FROM property_collapsing
GROUP BY tenant_id, property_id
HAVING existence > 0;


-- ############################################################################
-- PATTERN: Point lookup with guaranteed correctness
-- ############################################################################
-- For single-row lookups, FINAL is acceptable

SELECT *
FROM property_collapsing FINAL
WHERE tenant_id = 'pb.amritsar'
  AND property_id = 'PT-AMRITSAR-000001'
  AND sign = 1;  -- Extra safety filter

-- Or without FINAL, using ORDER BY + LIMIT
SELECT *
FROM property_collapsing
WHERE tenant_id = 'pb.amritsar'
  AND property_id = 'PT-AMRITSAR-000001'
ORDER BY version DESC
LIMIT 1;


-- ############################################################################
-- ANTI-PATTERN: DON'T DO THIS
-- ############################################################################

-- WRONG: count() without considering sign
-- SELECT tenant_id, count() FROM property_collapsing GROUP BY tenant_id;

-- WRONG: sum() without multiplying by sign
-- SELECT tenant_id, sum(tax_amount) FROM demand_detail_collapsing GROUP BY tenant_id;

-- WRONG: Using FINAL on large table scans
-- SELECT * FROM property_collapsing FINAL WHERE created_time > '2024-01-01';
