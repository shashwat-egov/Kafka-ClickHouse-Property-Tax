-- ============================================================================
-- VERIFY PAYMENT FLOW AND COLLECTION UPDATES
-- ============================================================================
-- Purpose: Verify that payment updates flow correctly through the pipeline
-- Run after: generate_demands.py, generate_updates.py, RMV refresh
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Demand Detail Version Analysis
-- Shows demand details with payment updates (collection_amount > 0)
-- ----------------------------------------------------------------------------
SELECT
    '=== DEMAND DETAIL VERSION ANALYSIS ===' AS section;

-- Raw data showing payment progression
SELECT
    tenant_id,
    demand_id,
    tax_head_code,
    count() AS version_count,
    groupArray(tuple(version, tax_amount, collection_amount)) AS version_history
FROM demand_detail_raw
WHERE collection_amount > 0
GROUP BY tenant_id, demand_id, tax_head_code
ORDER BY version_count DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- 2. Collection Amount Comparison: Raw vs Snapshot
-- Verifies that snapshot shows latest collection_amount
-- ----------------------------------------------------------------------------
SELECT
    '=== COLLECTION AMOUNT COMPARISON ===' AS section;

-- Demands with payments in raw
SELECT
    'RAW' AS source,
    tenant_id,
    demand_id,
    tax_head_code,
    tax_amount,
    collection_amount,
    version
FROM demand_detail_raw
WHERE collection_amount > 0
ORDER BY demand_id, tax_head_code, version
LIMIT 20;

-- Same demands in snapshot (should show latest collection)
SELECT
    'SNAPSHOT' AS source,
    tenant_id,
    demand_id,
    tax_head_code,
    tax_amount,
    collection_amount,
    version
FROM demand_detail_snapshot
WHERE collection_amount > 0
ORDER BY demand_id, tax_head_code
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 3. Payment Status Summary by Financial Year
-- ----------------------------------------------------------------------------
SELECT
    '=== PAYMENT STATUS BY FY ===' AS section;

SELECT
    d.financial_year,
    count(DISTINCT d.demand_id) AS total_demands,
    countIf(d.is_payment_completed = 1) AS fully_paid,
    countIf(d.is_payment_completed = 0) AS pending
FROM demand_snapshot d
WHERE d.business_service = 'PT'
GROUP BY d.financial_year
ORDER BY d.financial_year;

-- ----------------------------------------------------------------------------
-- 4. Collection vs Demand Analysis
-- ----------------------------------------------------------------------------
SELECT
    '=== COLLECTION VS DEMAND ANALYSIS ===' AS section;

SELECT
    d.financial_year,
    round(sum(dd.tax_amount), 2) AS total_demand,
    round(sum(dd.collection_amount), 2) AS total_collected,
    round(sum(dd.collection_amount) / nullIf(sum(dd.tax_amount), 0) * 100, 2) AS collection_percentage
FROM demand_snapshot d
INNER JOIN demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
GROUP BY d.financial_year
ORDER BY d.financial_year;

-- ----------------------------------------------------------------------------
-- 5. Defaulters List (Outstanding > 0)
-- ----------------------------------------------------------------------------
SELECT
    '=== CURRENT DEFAULTERS ===' AS section;

SELECT
    tenant_id,
    property_id,
    demand_id,
    financial_year,
    total_tax_amount,
    total_collected_amount,
    outstanding_amount
FROM mart_defaulters
ORDER BY outstanding_amount DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 6. Defaulter Count by Tenant
-- ----------------------------------------------------------------------------
SELECT
    '=== DEFAULTER COUNT BY TENANT ===' AS section;

SELECT
    tenant_id,
    count() AS defaulter_count,
    round(sum(outstanding_amount), 2) AS total_outstanding
FROM mart_defaulters
GROUP BY tenant_id
ORDER BY total_outstanding DESC;

-- ----------------------------------------------------------------------------
-- 7. Verify Fully Paid Properties Not in Defaulters
-- Properties with 100% payment should NOT appear in defaulters
-- ----------------------------------------------------------------------------
SELECT
    '=== FULLY PAID VERIFICATION ===' AS section;

-- Demands that are fully paid (collection = tax)
WITH fully_paid AS (
    SELECT
        d.tenant_id,
        d.consumer_code AS property_id,
        d.demand_id
    FROM demand_snapshot d
    INNER JOIN demand_detail_snapshot dd
        ON d.tenant_id = dd.tenant_id AND d.demand_id = dd.demand_id
    WHERE d.business_service = 'PT'
    GROUP BY d.tenant_id, d.consumer_code, d.demand_id
    HAVING sum(dd.tax_amount) = sum(dd.collection_amount)
)
SELECT
    'Fully paid demands' AS metric,
    count() AS count
FROM fully_paid;

-- Check if any fully paid appear in defaulters (should be 0)
WITH fully_paid AS (
    SELECT
        d.tenant_id,
        d.consumer_code AS property_id,
        d.demand_id
    FROM demand_snapshot d
    INNER JOIN demand_detail_snapshot dd
        ON d.tenant_id = dd.tenant_id AND d.demand_id = dd.demand_id
    WHERE d.business_service = 'PT'
    GROUP BY d.tenant_id, d.consumer_code, d.demand_id
    HAVING sum(dd.tax_amount) = sum(dd.collection_amount)
)
SELECT
    'Fully paid in defaulters (should be 0)' AS metric,
    count() AS count
FROM mart_defaulters md
INNER JOIN fully_paid fp
    ON md.tenant_id = fp.tenant_id
    AND md.property_id = fp.property_id
    AND md.demand_id = fp.demand_id;

-- ----------------------------------------------------------------------------
-- 8. Month-wise Collection Trend
-- ----------------------------------------------------------------------------
SELECT
    '=== MONTH-WISE COLLECTION TREND ===' AS section;

SELECT
    year_month,
    total_collected_amount
FROM mart_collections_by_month
ORDER BY year_month DESC
LIMIT 12;
