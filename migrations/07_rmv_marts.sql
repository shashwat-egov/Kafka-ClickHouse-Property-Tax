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
-- Mart 1: Active Property Count + Ownership + Usage
-- Depends on: rmv_property_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_kafka_test.rmv_mart_property_snapshot_agg
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON punjab_kafka_test.rmv_property_snapshot
TO punjab_kafka_test.mart_property_snapshot_agg
EMPTY
AS
SELECT
    snapshot_date,
    tenant_id,
    ownership_category,
    usage_category,
    count() AS property_count
FROM punjab_kafka_test.property_snapshot
WHERE status = 'ACTIVE'
GROUP BY
    snapshot_date,
    tenant_id,
    ownership_category,
    usage_category;


-- ############################################################################
-- MART 2: New Property Count by Financial Year
-- Depends On: rmv_property_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_kafka_test.rmv_mart_new_properties_by_fy
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON punjab_kafka_test.rmv_property_snapshot
TO punjab_kafka_test.mart_new_properties_by_fy
EMPTY
AS
SELECT
    snapshot_date,
    tenant_id,
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
    countDistinct(property_id) AS new_property_count
FROM punjab_kafka_test.property_snapshot
WHERE created_time IS NOT NULL
AND status = 'ACTIVE'
GROUP BY
    snapshot_date,
    tenant_id,
    financial_year;



-- ############################################################################
-- MART 3: Properties with Demand Generated per Financial Year
-- Depends On: rmv_demand_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_kafka_test.rmv_mart_properties_with_demand_by_fy
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON punjab_kafka_test.rmv_demand_snapshot
TO punjab_kafka_test.mart_properties_with_demand
EMPTY
AS
SELECT
    snapshot_date,
    tenant_id,
    financial_year,
    uniqExact(consumer_code) AS properties_with_demand
FROM punjab_kafka_test.demand_snapshot
WHERE business_service = 'PT'
  AND financial_year != '' 
  AND status = 'ACTIVE'
GROUP BY
    snapshot_date,
    tenant_id,
    financial_year;


-- ############################################################################
-- MART 4: Demand Value by Financial Year
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_kafka_test.rmv_mart_demand_value_by_fy
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON punjab_kafka_test.rmv_demand_snapshot, punjab_kafka_test.rmv_demand_detail_snapshot
TO punjab_kafka_test.mart_demand_value
EMPTY
AS
SELECT
    d.snapshot_date,
    d.tenant_id,
    d.financial_year,

    sumIf(dd.tax_amount, dd.tax_head_code = 'PT_TAX')          AS pt_tax_amount,
    sumIf(dd.tax_amount, dd.tax_head_code = 'PT_CANCER_CESS')    AS pt_cancer_cess,
    sumIf(dd.tax_amount, dd.tax_head_code = 'PT_FIRE_CESS')      AS pt_fire_cess,
    sumIf(dd.tax_amount, dd.tax_head_code = 'PT_ROUNDOFF')  AS pt_roundoff,
    sumIf(dd.tax_amount, dd.tax_head_code = 'PT_OWNER_EXEMPTION')  AS pt_owner_exemption,
    sumIf(dd.tax_amount, dd.tax_head_code = 'PT_UNIT_USAGE_EXEMPTION')  AS pt_unit_usage_exemption,

    sum(dd.tax_amount) AS total_demand
FROM punjab_kafka_test.demand_snapshot d
LEFT JOIN punjab_kafka_test.demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id
   AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
  AND d.status = 'ACTIVE'
  AND d.financial_year != ''
GROUP BY
    d.snapshot_date,
    d.tenant_id,
    d.financial_year;

-- ############################################################################
-- MART 5: Collections by financial year
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_kafka_test.rmv_mart_property_collections
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON punjab_kafka_test.rmv_demand_snapshot, punjab_kafka_test.rmv_demand_detail_snapshot
TO punjab_kafka_test.mart_property_collections
EMPTY
AS
SELECT
    d.snapshot_date,
    d.tenant_id,
    d.financial_year,
    toStartOfMonth(dd.last_modified_time) AS payment_month,

    sumIf(dd.collection_amount, dd.tax_head_code = 'PT_TAX')          AS pt_tax_amount,
    sumIf(dd.collection_amount, dd.tax_head_code = 'PT_CANCER_CESS')    AS pt_cancer_cess,
    sumIf(dd.collection_amount, dd.tax_head_code = 'PT_FIRE_CESS')      AS pt_fire_cess,
    sumIf(dd.collection_amount, dd.tax_head_code = 'PT_ROUNDOFF')  AS pt_roundoff,
    sumIf(dd.collection_amount, dd.tax_head_code = 'PT_OWNER_EXEMPTION')  AS pt_owner_exemption,
    sumIf(dd.collection_amount, dd.tax_head_code = 'PT_UNIT_USAGE_EXEMPTION')  AS pt_unit_usage_exemption,

    sum(dd.collection_amount) AS net_amount_collected
FROM punjab_kafka_test.demand_snapshot d
INNER JOIN punjab_kafka_test.demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id
   AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
  AND d.status = 'ACTIVE'
  AND dd.collection_amount > 0
GROUP BY
    d.snapshot_date,
    d.tenant_id,
    d.financial_year,
    payment_month;


-- ############################################################################
-- MART 6: Month-wise Collections
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_kafka_test.rmv_mart_collections_by_month
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON punjab_kafka_test.rmv_demand_snapshot, punjab_kafka_test.rmv_demand_detail_snapshot
TO punjab_kafka_test.mart_collections_by_month
EMPTY
AS
SELECT
    today() AS snapshot_date,
    formatDateTime(dd.last_modified_time, '%Y-%m') AS year_month,
    sum(dd.collection_amount) AS total_collected_amount
FROM punjab_kafka_test.demand_snapshot d
INNER JOIN punjab_kafka_test.demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id
    AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
  AND dd.collection_amount > 0
  AND dd.last_modified_time > toDateTime64('1970-01-01 00:00:00', 3)
  AND d.status = 'ACTIVE'
GROUP BY year_month;


-- ############################################################################
-- MART 7: Defaulter List with all details
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- Columns : snapshot_date tenant_id property_id demand_id financial_year total_tax collected outstanding
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_kafka_test.rmv_mart_defaulters_details
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON punjab_kafka_test.rmv_demand_snapshot, punjab_kafka_test.rmv_demand_detail_snapshot
TO punjab_kafka_test.mart_defaulters_details
EMPTY
AS
SELECT
    d.snapshot_date,
    d.tenant_id,
    d.consumer_code AS property_id,
    d.demand_id,
    d.financial_year,
    sum(dd.tax_amount) AS total_tax_amount,
    sum(dd.collection_amount) AS total_collected_amount,
    sum(dd.tax_amount) - sum(dd.collection_amount) AS outstanding_amount
FROM punjab_kafka_test.demand_snapshot d
INNER JOIN punjab_kafka_test.demand_detail_snapshot dd
    ON d.tenant_id = dd.tenant_id
    AND d.demand_id = dd.demand_id
WHERE d.business_service = 'PT'
AND d.status = 'ACTIVE'
GROUP BY d.snapshot_date, d.tenant_id, d.consumer_code, d.demand_id, d.financial_year
HAVING outstanding_amount > 0;


-- ############################################################################
-- MART 8: Defaulter List
-- Depends On: rmv_demand_snapshot, rmv_demand_detail_snapshot
-- Columns : snapshot_date tenant_id property_id due_fy_count total_outstanding
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_kafka_test.rmv_mart_defaulters
REFRESH EVERY 1 DAY OFFSET 1 HOUR 30 MINUTE
DEPENDS ON punjab_kafka_test.rmv_demand_snapshot, punjab_kafka_test.rmv_demand_detail_snapshot
TO punjab_kafka_test.mart_defaulters
EMPTY
AS
WITH fy_dues AS (
    SELECT
        d.snapshot_date,
        d.tenant_id,
        d.consumer_code AS property_id,
        d.financial_year,
        sum(dd.tax_amount) - sum(dd.collection_amount) AS outstanding
    FROM punjab_kafka_test.demand_snapshot d
    INNER JOIN punjab_kafka_test.demand_detail_snapshot dd
        ON d.tenant_id = dd.tenant_id
       AND d.demand_id = dd.demand_id
    WHERE d.business_service = 'PT'
    AND d.status = 'ACTIVE'
    GROUP BY
        d.snapshot_date,
        d.tenant_id,
        property_id,
        d.financial_year
)
SELECT
    snapshot_date,
    tenant_id,
    property_id,
    countIf(outstanding > 0) AS due_fy_count,
    sum(outstanding)        AS total_outstanding
FROM fy_dues
GROUP BY
    snapshot_date,
    tenant_id,
    property_id
HAVING due_fy_count > 0;


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
