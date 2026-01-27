-- ============================================================================
-- REFRESHABLE MATERIALIZED VIEWS FOR MART TABLES
-- ============================================================================
-- Purpose: Replace manual bash-orchestrated mart refresh with native
--          ClickHouse Refreshable Materialized Views (RMVs)
-- Schedule: Daily at 01:30 AM (30 min after snapshot refresh)
-- Dependencies: Uses DEPENDS ON to ensure snapshots refresh first
-- Pattern: TRUNCATE + INSERT is implicit in RMV refresh (atomic replace)
-- Requires: ClickHouse 24.10+ for production-ready RMV support
-- ============================================================================

-- ############################################################################
-- MART 1: Property Count by Tenant
-- Depends On: rmv_property_snapshot
-- ############################################################################
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


-- ############################################################################
-- MART 2: Property Count by Tenant and Ownership Category
-- Depends On: rmv_property_snapshot
-- ############################################################################
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
-- MART 3: Property Count by Tenant and Usage Category
-- Depends On: rmv_property_snapshot
-- ############################################################################
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


-- ############################################################################
-- MART 4: New Property Count by Financial Year
-- Depends On: rmv_property_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_new_properties_by_fy
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_property_snapshot
TO mart_new_properties_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    concat(
        toString(
            if(toMonth(created_time) >= 4,
                toYear(created_time),
                toYear(created_time) - 1)
        ),
        '-',
        formatDateTime(
            toDate(
                concat(
                    toString(
                        if(toMonth(created_time) >= 4,
                            toYear(created_time) + 1,
                            toYear(created_time))
                    ),
                    '-01-01'
                )
            ),
            '%y'
        )
    ) AS financial_year,
    count() AS property_count
FROM property_snapshot
WHERE status = 'ACTIVE'
  AND created_time IS NOT NULL
  AND created_time > toDateTime64('1970-01-01 00:00:00', 3)
GROUP BY financial_year;


-- ############################################################################
-- MART 5: Properties with Demand Generated per Financial Year
-- Depends On: rmv_demand_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_properties_with_demand_by_fy
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_demand_snapshot
TO mart_properties_with_demand_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    financial_year,
    uniqExact(consumer_code) AS properties_with_demand
FROM demand_snapshot
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year;


-- ############################################################################
-- MART 6: Demand Value by Financial Year
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_demand_value_by_fy
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_demand_snapshot, rmv_demand_detail_snapshot
TO mart_demand_value_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    d.financial_year,
    sum(dd.tax_amount) AS total_demand_amount
FROM demand_snapshot d
INNER JOIN demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id
    AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
  AND d.financial_year != ''
GROUP BY d.financial_year;


-- ############################################################################
-- MART 7: Property Tax Collected per Financial Year
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_collections_by_fy
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_demand_snapshot, rmv_demand_detail_snapshot
TO mart_collections_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    d.financial_year,
    sum(dd.collection_amount) AS total_collected_amount
FROM demand_snapshot d
INNER JOIN demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id
    AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
  AND d.financial_year != ''
GROUP BY d.financial_year;


-- ############################################################################
-- MART 8: Month-wise Collections
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_collections_by_month
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_demand_snapshot, rmv_demand_detail_snapshot
TO mart_collections_by_month
EMPTY
AS
SELECT
    today() AS snapshot_date,
    formatDateTime(dd.last_modified_time, '%Y-%m') AS year_month,
    sum(dd.collection_amount) AS total_collected_amount
FROM demand_snapshot d
INNER JOIN demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id
    AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
  AND dd.collection_amount > 0
  AND dd.last_modified_time > toDateTime64('1970-01-01 00:00:00', 3)
GROUP BY year_month;


-- ############################################################################
-- MART 9: Defaulter List
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_mart_defaulters
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON rmv_demand_snapshot, rmv_demand_detail_snapshot
TO mart_defaulters
EMPTY
AS
SELECT
    today() AS snapshot_date,
    d.tenant_id,
    d.consumer_code AS property_id,
    d.demand_id,
    d.financial_year,
    sum(dd.tax_amount) AS total_tax_amount,
    sum(dd.collection_amount) AS total_collected_amount,
    sum(dd.tax_amount) - sum(dd.collection_amount) AS outstanding_amount
FROM demand_snapshot d
INNER JOIN demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id
    AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
GROUP BY d.tenant_id, d.consumer_code, d.demand_id, d.financial_year
HAVING outstanding_amount > 0;


-- ============================================================================
-- MANUAL REFRESH COMMANDS (for testing/debugging)
-- ============================================================================
-- To manually trigger mart refresh (snapshots must be refreshed first):
--   SYSTEM REFRESH VIEW rmv_mart_property_count_by_tenant;
--   SYSTEM REFRESH VIEW rmv_mart_property_count_by_ownership;
--   SYSTEM REFRESH VIEW rmv_mart_property_count_by_usage;
--   SYSTEM REFRESH VIEW rmv_mart_new_properties_by_fy;
--   SYSTEM REFRESH VIEW rmv_mart_properties_with_demand_by_fy;
--   SYSTEM REFRESH VIEW rmv_mart_demand_value_by_fy;
--   SYSTEM REFRESH VIEW rmv_mart_collections_by_fy;
--   SYSTEM REFRESH VIEW rmv_mart_collections_by_month;
--   SYSTEM REFRESH VIEW rmv_mart_defaulters;
-- ============================================================================
