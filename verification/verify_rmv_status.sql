-- ============================================================================
-- VERIFY RMV (REFRESHABLE MATERIALIZED VIEW) STATUS
-- ============================================================================
-- Purpose: Check health and status of all RMVs in the pipeline
-- Run anytime to monitor the refresh pipeline
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. All RMV Status Overview
-- ----------------------------------------------------------------------------
SELECT
    '=== RMV STATUS OVERVIEW ===' AS section;

SELECT
    database,
    view,
    status,
    last_success_time,
    last_refresh_time,
    next_refresh_time,
    refresh_count,
    exception
FROM system.view_refreshes
WHERE view LIKE 'rmv_%'
ORDER BY view;

-- ----------------------------------------------------------------------------
-- 2. RMV Refresh Schedule
-- ----------------------------------------------------------------------------
SELECT
    '=== RMV REFRESH SCHEDULE ===' AS section;

SELECT
    view,
    status,
    next_refresh_time,
    dateDiff('minute', now(), next_refresh_time) AS minutes_until_next_refresh
FROM system.view_refreshes
WHERE view LIKE 'rmv_%'
ORDER BY next_refresh_time;

-- ----------------------------------------------------------------------------
-- 3. Failed RMVs (if any)
-- ----------------------------------------------------------------------------
SELECT
    '=== FAILED RMVS ===' AS section;

SELECT
    view,
    status,
    last_refresh_time,
    exception
FROM system.view_refreshes
WHERE view LIKE 'rmv_%'
  AND (status = 'Error' OR exception != '')
ORDER BY last_refresh_time DESC;

-- ----------------------------------------------------------------------------
-- 4. RMV Dependencies
-- ----------------------------------------------------------------------------
SELECT
    '=== RMV DEPENDENCY CHECK ===' AS section;

-- Snapshot RMVs (no dependencies - they read from raw tables)
SELECT
    'Snapshot RMVs' AS category,
    view,
    'None (reads from raw)' AS depends_on,
    status
FROM system.view_refreshes
WHERE view IN (
    'rmv_property_snapshot',
    'rmv_unit_snapshot',
    'rmv_owner_snapshot',
    'rmv_address_snapshot',
    'rmv_demand_snapshot',
    'rmv_demand_detail_snapshot'
);

-- Mart RMVs (depend on snapshots)
SELECT
    'Mart RMVs' AS category,
    view,
    CASE
        WHEN view IN ('rmv_mart_property_count_by_tenant', 'rmv_mart_property_count_by_ownership',
                      'rmv_mart_property_count_by_usage', 'rmv_mart_new_properties_by_fy')
            THEN 'rmv_property_snapshot'
        WHEN view = 'rmv_mart_properties_with_demand_by_fy'
            THEN 'rmv_demand_snapshot'
        ELSE 'rmv_demand_snapshot, rmv_demand_detail_snapshot'
    END AS depends_on,
    status
FROM system.view_refreshes
WHERE view LIKE 'rmv_mart_%';

-- ----------------------------------------------------------------------------
-- 5. Last Successful Refresh Times
-- ----------------------------------------------------------------------------
SELECT
    '=== LAST SUCCESSFUL REFRESH ===' AS section;

SELECT
    view,
    last_success_time,
    dateDiff('minute', last_success_time, now()) AS minutes_ago
FROM system.view_refreshes
WHERE view LIKE 'rmv_%'
  AND last_success_time IS NOT NULL
ORDER BY last_success_time DESC;

-- ----------------------------------------------------------------------------
-- 6. Refresh Performance
-- ----------------------------------------------------------------------------
SELECT
    '=== REFRESH PERFORMANCE ===' AS section;

SELECT
    view,
    refresh_count,
    CASE
        WHEN refresh_count > 0 AND last_success_time IS NOT NULL
        THEN 'Healthy'
        ELSE 'Not yet refreshed'
    END AS health_status
FROM system.view_refreshes
WHERE view LIKE 'rmv_%'
ORDER BY view;

-- ----------------------------------------------------------------------------
-- 7. Target Table Row Counts
-- ----------------------------------------------------------------------------
SELECT
    '=== TARGET TABLE ROW COUNTS ===' AS section;

SELECT 'property_snapshot' AS table_name, count() AS row_count FROM property_snapshot
UNION ALL
SELECT 'unit_snapshot', count() FROM unit_snapshot
UNION ALL
SELECT 'owner_snapshot', count() FROM owner_snapshot
UNION ALL
SELECT 'address_snapshot', count() FROM address_snapshot
UNION ALL
SELECT 'demand_snapshot', count() FROM demand_snapshot
UNION ALL
SELECT 'demand_detail_snapshot', count() FROM demand_detail_snapshot
UNION ALL
SELECT 'mart_property_count_by_tenant', count() FROM mart_property_count_by_tenant
UNION ALL
SELECT 'mart_property_count_by_ownership', count() FROM mart_property_count_by_ownership
UNION ALL
SELECT 'mart_property_count_by_usage', count() FROM mart_property_count_by_usage
UNION ALL
SELECT 'mart_new_properties_by_fy', count() FROM mart_new_properties_by_fy
UNION ALL
SELECT 'mart_properties_with_demand_by_fy', count() FROM mart_properties_with_demand_by_fy
UNION ALL
SELECT 'mart_demand_value_by_fy', count() FROM mart_demand_value_by_fy
UNION ALL
SELECT 'mart_collections_by_fy', count() FROM mart_collections_by_fy
UNION ALL
SELECT 'mart_collections_by_month', count() FROM mart_collections_by_month
UNION ALL
SELECT 'mart_defaulters', count() FROM mart_defaulters;

-- ----------------------------------------------------------------------------
-- 8. Manual Refresh Commands
-- ----------------------------------------------------------------------------
SELECT
    '=== MANUAL REFRESH COMMANDS ===' AS section;

SELECT '-- Refresh all snapshot RMVs (run these first):' AS command
UNION ALL SELECT 'SYSTEM REFRESH VIEW rmv_property_snapshot;'
UNION ALL SELECT 'SYSTEM REFRESH VIEW rmv_unit_snapshot;'
UNION ALL SELECT 'SYSTEM REFRESH VIEW rmv_owner_snapshot;'
UNION ALL SELECT 'SYSTEM REFRESH VIEW rmv_address_snapshot;'
UNION ALL SELECT 'SYSTEM REFRESH VIEW rmv_demand_snapshot;'
UNION ALL SELECT 'SYSTEM REFRESH VIEW rmv_demand_detail_snapshot;'
UNION ALL SELECT ''
UNION ALL SELECT '-- Wait for snapshots, then refresh marts:'
UNION ALL SELECT 'SYSTEM REFRESH VIEW rmv_mart_property_count_by_tenant;'
UNION ALL SELECT 'SYSTEM REFRESH VIEW rmv_mart_defaulters;'
UNION ALL SELECT '-- ... (other marts)';
