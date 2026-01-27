-- ============================================================================
-- VERIFY DEDUPLICATION LOGIC
-- ============================================================================
-- Purpose: Compare raw tables (multiple versions) vs snapshots (latest only)
-- Run after: generate_properties.py, generate_updates.py, RMV refresh
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Property Version Analysis
-- Shows properties with multiple versions in raw table
-- ----------------------------------------------------------------------------
SELECT
    '=== PROPERTY VERSION ANALYSIS ===' AS section;

SELECT
    tenant_id,
    property_id,
    count() AS version_count,
    min(version) AS min_version,
    max(version) AS max_version,
    groupArray(usage_category) AS usage_categories
FROM property_raw
GROUP BY tenant_id, property_id
HAVING version_count > 1
ORDER BY version_count DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 2. Raw vs Snapshot Comparison for Updated Properties
-- Properties 1-10 should show COMMERCIAL in snapshot (not RESIDENTIAL)
-- ----------------------------------------------------------------------------
SELECT
    '=== RAW VS SNAPSHOT COMPARISON ===' AS section;

-- Raw table data (all versions)
SELECT
    'RAW' AS source,
    tenant_id,
    property_id,
    usage_category,
    version,
    last_modified_time
FROM property_raw
WHERE property_id IN (
    'PT-AMRITSAR-000001',
    'PT-JALANDHAR-000002',
    'PT-LUDHIANA-000003',
    'PT-PATIALA-000004',
    'PT-AMRITSAR-000005'
)
ORDER BY property_id, version;

-- Snapshot data (latest version only)
SELECT
    'SNAPSHOT' AS source,
    tenant_id,
    property_id,
    usage_category,
    version,
    last_modified_time
FROM property_snapshot
WHERE property_id IN (
    'PT-AMRITSAR-000001',
    'PT-JALANDHAR-000002',
    'PT-LUDHIANA-000003',
    'PT-PATIALA-000004',
    'PT-AMRITSAR-000005'
)
ORDER BY property_id;

-- ----------------------------------------------------------------------------
-- 3. Usage Category Distribution Comparison
-- Raw will show duplicates, snapshot will show deduplicated counts
-- ----------------------------------------------------------------------------
SELECT
    '=== USAGE CATEGORY DISTRIBUTION ===' AS section;

SELECT
    'RAW (with duplicates)' AS source,
    usage_category,
    count() AS count
FROM property_raw
GROUP BY usage_category
ORDER BY count DESC;

SELECT
    'SNAPSHOT (deduplicated)' AS source,
    usage_category,
    count() AS count
FROM property_snapshot
GROUP BY usage_category
ORDER BY count DESC;

-- ----------------------------------------------------------------------------
-- 4. Deduplication Verification Query
-- Manually apply argMax to verify snapshot matches
-- ----------------------------------------------------------------------------
SELECT
    '=== MANUAL DEDUP VERIFICATION ===' AS section;

WITH deduplicated AS (
    SELECT
        tenant_id,
        property_id,
        argMax(usage_category, (last_modified_time, version)) AS usage_category,
        max(version) AS version
    FROM property_raw
    GROUP BY tenant_id, property_id
)
SELECT
    d.tenant_id,
    d.property_id,
    d.usage_category AS dedup_usage,
    d.version AS dedup_version,
    s.usage_category AS snapshot_usage,
    s.version AS snapshot_version,
    if(d.usage_category = s.usage_category AND d.version = s.version, 'MATCH', 'MISMATCH') AS status
FROM deduplicated d
LEFT JOIN property_snapshot s
    ON d.tenant_id = s.tenant_id AND d.property_id = s.property_id
WHERE d.property_id IN (
    'PT-AMRITSAR-000001',
    'PT-JALANDHAR-000002',
    'PT-LUDHIANA-000003'
)
ORDER BY d.property_id;

-- ----------------------------------------------------------------------------
-- 5. Summary Statistics
-- ----------------------------------------------------------------------------
SELECT
    '=== SUMMARY STATISTICS ===' AS section;

SELECT
    'property_raw' AS table_name,
    count() AS row_count,
    uniqExact(tenant_id, property_id) AS unique_entities
FROM property_raw;

SELECT
    'property_snapshot' AS table_name,
    count() AS row_count,
    uniqExact(tenant_id, property_id) AS unique_entities
FROM property_snapshot;

-- Deduplication ratio
SELECT
    'Deduplication Ratio' AS metric,
    round(
        (SELECT count() FROM property_raw) /
        nullIf((SELECT count() FROM property_snapshot), 0),
        2
    ) AS ratio;
