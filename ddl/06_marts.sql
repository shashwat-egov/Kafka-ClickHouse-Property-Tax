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

CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_active_property_distribution_summary
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_active_property_distribution_summary
EMPTY
AS
SELECT
    today() AS data_refresh_date,
    tenant_id,
    ownership_category,
    usage_category,
    count() AS property_count
FROM replacing_test.property_address_entity FINAL
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
    today() AS data_refresh_date,
    tenant_id,
    financial_year,
    count(property_id) AS new_property_count
FROM replacing_test.property_address_entity FINAL
WHERE created_time IS NOT NULL
AND status = 'ACTIVE'
GROUP BY
    tenant_id,
    financial_year;


-- ############################################################################
-- DEMAND RMVs (manual refresh only via SYSTEM REFRESH VIEW)
-- ############################################################################



CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_demand_and_collection_summary
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_demand_and_collection_summary
EMPTY
AS
SELECT
    today() AS data_refresh_date,
    tenant_id,
    financial_year,
    sum(total_tax_amount) AS total_demand,
    sum(total_collection_amount) AS total_collection,
    sum(outstanding_amount) AS total_outstanding
FROM replacing_test.demand_with_details_entity
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
    today() AS data_refresh_date,
    tenant_id,
    formatDateTime(last_modified_time, '%Y-%m') AS year_month,
    sum(total_collection_amount) AS total_collected_amount
FROM replacing_test.demand_with_details_entity
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
    today() AS data_refresh_date,
    tenant_id,
    financial_year,
    consumer_code AS properties_with_demand
FROM replacing_test.demand_with_details_entity FINAL
WHERE business_service = 'PT'
  AND financial_year != ''
  AND demand_status = 'ACTIVE';


CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_defaulters
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_defaulters
EMPTY
AS
SELECT
    today() AS data_refresh_date,
    tenant_id,
    consumer_code AS property_id,
    demand_id,
    financial_year,
    total_tax_amount,
    total_collection_amount,
    outstanding_amount
FROM replacing_test.demand_with_details_entity
FINAL
WHERE (business_service = 'PT') AND (demand_status = 'ACTIVE') AND (outstanding_amount > 0);




CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_mart_property_demand_coverage_by_fy
REFRESH EVERY 1000 YEAR
TO replacing_test.mart_property_demand_coverage_by_fy
EMPTY
AS
WITH
    demand_counts AS
    (
        SELECT
            tenant_id,
            financial_year,
            count() AS properties_with_demand
        FROM replacing_test.mart_properties_with_demand_by_fy
        GROUP BY
            tenant_id,
            financial_year
    ),
    property_base AS
    (
        SELECT
            tenant_id,
            financial_year,
            sum(new_property_count) OVER (PARTITION BY tenant_id ORDER BY financial_year ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_active_properties
        FROM replacing_test.mart_new_properties_by_fy
    )
SELECT
    d.tenant_id,
    d.financial_year,
    p.total_active_properties,
    d.properties_with_demand,
    p.total_active_properties - d.properties_with_demand AS properties_without_demand,
    round(d.properties_with_demand / p.total_active_properties, 4) AS coverage_ratio
FROM demand_counts AS d
INNER JOIN property_base AS p ON (d.tenant_id = p.tenant_id) AND (d.financial_year = p.financial_year)
ORDER BY
    d.tenant_id ASC,
    d.financial_year ASC;


-- Snapshot History → Change Detection
-- Uses lagInFrame() to compare each snapshot with the previous one.

CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_property_change_metrics
REFRESH EVERY 1000 YEAR
TO replacing_test.property_change_metrics
EMPTY
AS
SELECT
    tenant_id,
    property_id,
    event_time,

    if(ownership_category != lagInFrame(ownership_category, 1, ownership_category)
       OVER w, 1, 0) AS ownership_changed,

    if(usage_category != lagInFrame(usage_category, 1, usage_category)
       OVER w, 1, 0) AS usage_changed,

    if(super_built_up_area != lagInFrame(super_built_up_area, 1, super_built_up_area)
       OVER w OR land_area != lagInFrame(land_area, 1, land_area)
       OVER w, 1, 0) AS area_changed,

    if(workflow_state != lagInFrame(workflow_state, 1, workflow_state)
       OVER w, 1, 0) AS workflow_state_changed,

    if(owner_count != lagInFrame(owner_count, 1, owner_count)
       OVER w, 1, 0) AS owner_count_changed

FROM replacing_test.property_snapshot_history
WINDOW w AS (
    PARTITION BY tenant_id, property_id
    ORDER BY event_time
);


-- Change Metrics → Risk Summary
-- Aggregates change flags per property and computes a risk score.
-- Depends on rmv_property_change_metrics completing first.

CREATE MATERIALIZED VIEW IF NOT EXISTS replacing_test.rmv_property_risk_summary
REFRESH EVERY 1000 YEAR
TO replacing_test.property_risk_summary
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    property_id,

    count() AS total_updates,
    sum(ownership_changed) AS ownership_changes,
    sum(area_changed) AS area_changes,
    sum(workflow_state_changed) AS workflow_reopens,

    if(
        sum(ownership_changed) > 1 OR
        sum(area_changed) > 2 OR
        sum(workflow_state_changed) > 1,
        1, 0
    ) AS risk_score
FROM replacing_test.property_change_metrics
GROUP BY tenant_id, property_id;
