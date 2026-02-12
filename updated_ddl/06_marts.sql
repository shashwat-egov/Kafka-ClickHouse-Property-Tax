-- ============================================================================
-- ANALYTICAL MARTS (Refreshable Materialized Views)
-- ============================================================================
-- Mart RMVs that are manually refreshed after all silver-layer inserts complete.
-- MVs re-execute the full query against CollapsingMergeTree silver tables.
--
-- Manual refresh:
--   SYSTEM REFRESH VIEW replacing_test.rmv_mart_<name>;
-- Check status:
--   SELECT view, status FROM system.view_refreshes WHERE view LIKE 'rmv_%';
-- ============================================================================


-- ############################################################################
-- PROPERTY RMVs (manual refresh only via SYSTEM REFRESH VIEW)
-- ############################################################################

CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_property_agg
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_property_agg
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    ownership_category,
    usage_category,
    count() AS property_count
FROM replacing_test.property_address_fact FINAL
WHERE status = 'ACTIVE'
GROUP BY
    tenant_id,
    ownership_category,
    usage_category;



-- ############################################################################
-- MART 2: New Property Count by Financial Year
-- Depends On: rmv_property_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_new_properties_by_fy
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_new_properties_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
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
FROM replacing_test.property_address_fact
WHERE created_time IS NOT NULL
AND status = 'ACTIVE'
GROUP BY
    tenant_id,
    financial_year;


-- ############################################################################
-- DEMAND RMVs (manual refresh only via SYSTEM REFRESH VIEW)
-- ############################################################################



CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_collections_by_fy
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_demand_value_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    financial_year,
    sum(total_tax_amount) AS total_demand,
    sum(total_collection_amount) AS total_collection,
    sum(outstanding_amount) AS total_outstanding
FROM replacing_test.demand_with_details_fact
FINAL
WHERE (business_service = 'PT') AND (demand_status = 'ACTIVE') AND (financial_year != '')
GROUP BY
    tenant_id,
    financial_year;


CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_collections_by_month
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_collections_by_month
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    formatDateTime(last_modified_time, '%Y-%m') AS year_month,
    sum(total_collection_amount) AS total_collected_amount
FROM replacing_test.demand_with_details_fact
FINAL
WHERE (total_collection_amount > 0) AND (demand_status = 'ACTIVE')
GROUP BY
    tenant_id,
    year_month;


CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_properties_with_demand_by_fy
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_properties_with_demand_by_fy
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    financial_year,
    consumer_code AS properties_with_demand
FROM replacing_test.demand_with_details_fact FINAL
WHERE business_service = 'PT'
  AND financial_year != ''
  AND demand_status = 'ACTIVE';


CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_defaulters
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_defaulters
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
    outstanding_amount
FROM replacing_test.demand_with_details_fact
FINAL
WHERE (business_service = 'PT') AND (demand_status = 'ACTIVE') AND (outstanding_amount > 0);
