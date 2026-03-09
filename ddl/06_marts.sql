-- ============================================================================
-- ANALYTICAL MARTS (Refreshable Materialized Views)
-- ============================================================================
-- Mart RMVs that are manually refreshed after all silver-layer inserts complete.
-- MVs re-execute the full query against CollapsingMergeTree silver tables.
--
-- Manual refresh:
--   SYSTEM REFRESH VIEW punjab_property_tax.rmv_mart_<name>;
-- Check status:
--   SELECT view, status FROM system.view_refreshes WHERE view LIKE 'rmv_%';
-- ============================================================================


-- ############################################################################
-- PROPERTY RMVs (manual refresh only via SYSTEM REFRESH VIEW)
-- ############################################################################

CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_active_property_distribution_summary
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_active_property_distribution_summary
EMPTY
AS
SELECT
    tenant_id,
    property_type,
    ownership_category,
    usage_category,
    count() AS property_count
FROM punjab_property_tax.property_address_entity FINAL
WHERE status = 'ACTIVE'
GROUP BY
    tenant_id,
    property_type,
    ownership_category,
    usage_category;



-- ############################################################################
-- MART 2: New Property Count by Financial Year
-- Depends On: rmv_property_snapshot
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_new_properties_by_fy
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_new_properties_by_fy
EMPTY
AS
SELECT
    tenant_id,
    financial_year,
    count(property_id) AS new_property_count
FROM punjab_property_tax.property_address_entity FINAL
WHERE created_time IS NOT NULL
AND status = 'ACTIVE'
GROUP BY
    tenant_id,
    financial_year;


-- ############################################################################
-- DEMAND RMVs (manual refresh only via SYSTEM REFRESH VIEW)
-- ############################################################################



CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_demand_and_collection_summary
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_demand_and_collection_summary
EMPTY
AS
SELECT
    tenant_id,
    financial_year,
    sum(total_tax_amount) AS total_demand,
    sum(total_collection_amount) AS total_collection,
    sum(outstanding_amount) AS total_outstanding
FROM punjab_property_tax.demand_with_details_entity
FINAL
WHERE (business_service = 'PT') AND (demand_status = 'ACTIVE') AND (financial_year != '')
GROUP BY
    tenant_id,
    financial_year;


CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_collections_by_month
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_collections_by_month
EMPTY
AS
SELECT
    tenant_id,
    formatDateTime(last_modified_time, '%Y-%m') AS year_month,
    sum(total_collection_amount) AS total_collected_amount
FROM punjab_property_tax.demand_with_details_entity
FINAL
WHERE (total_collection_amount > 0) AND (demand_status = 'ACTIVE')
GROUP BY
    tenant_id,
    year_month;


CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_properties_with_demand_by_fy
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_properties_with_demand_by_fy
EMPTY
AS
SELECT
    tenant_id,
    financial_year,
    consumer_code AS properties_with_demand
FROM punjab_property_tax.demand_with_details_entity FINAL
WHERE business_service = 'PT'
  AND financial_year != ''
  AND demand_status = 'ACTIVE';


CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_defaulters
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_defaulters
EMPTY
AS
SELECT
    tenant_id,
    consumer_code AS property_id,
    demand_id,
    financial_year,
    total_tax_amount,
    total_collection_amount,
    outstanding_amount
FROM punjab_property_tax.demand_with_details_entity
FINAL
WHERE (business_service = 'PT') AND (demand_status = 'ACTIVE') AND (outstanding_amount > 0);



-- ############################################################################
-- CHANGE METRICS RMVs (manual refresh only via SYSTEM REFRESH VIEW)
-- ############################################################################

-- Layer 2: Snapshot History → Change Detection
-- Uses lagInFrame() to compare each snapshot with the previous one.

CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_property_change_metrics
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_property_change_metrics
EMPTY
AS
SELECT
    tenant_id,
    property_id,
    property_type,
    audit_created_time,

    if(ownership_category != lagInFrame(ownership_category, 1, ownership_category)
       OVER w, 1, 0) AS ownership_category_changed,

    if(usage_category != lagInFrame(usage_category, 1, usage_category)
       OVER w, 1, 0) AS usage_category_changed,

    if(super_built_up_area != lagInFrame(super_built_up_area, 1, super_built_up_area)
       OVER w OR land_area != lagInFrame(land_area, 1, land_area)
       OVER w, 1, 0) AS area_changed,

    if(workflow_state != lagInFrame(workflow_state, 1, workflow_state)
       OVER w, 1, 0) AS workflow_state_changed,

    if(owner_count != lagInFrame(owner_count, 1, owner_count)
       OVER w, 1, 0) AS owners_changed

FROM punjab_property_tax.property_audit_entity
WINDOW w AS (
    PARTITION BY tenant_id, property_id
    ORDER BY audit_created_time
);


-- Layer 3: Change Metrics → Risk Summary
-- Aggregates change flags per property and computes a risk score.
-- Depends on rmv_property_change_metrics completing first.

CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_property_risk_summary
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_property_risk_summary
EMPTY
AS
SELECT
    tenant_id,
    property_id,
    property_type,

    count() AS total_updates,
    sum(ownership_category_changed) AS ownership_changes,
    sum(area_changed) AS area_changes,
    sum(workflow_state_changed) AS workflow_reopens,

    if(
        sum(ownership_category_changed) > 1 OR
        sum(area_changed) > 2 OR
        sum(workflow_state_changed) > 1,
        1, 0
    ) AS risk_score
FROM punjab_property_tax.mart_property_change_metrics
GROUP BY tenant_id, property_id, property_type;

-- Layer 3b: Change Metrics → Risk Summary by Financial Year
-- Same as rmv_property_risk_summary but aggregated per FY (derived from audit_created_time).
-- Depends on rmv_property_change_metrics completing first.

CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_property_risk_summary_by_fy
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_property_risk_summary_by_fy
EMPTY
AS
SELECT
    tenant_id,
    property_id,
    property_type,
    concat(
    toString(toYear(audit_created_time) - if(toMonth(audit_created_time) < 4, 1, 0)),
    '-',
    substring(toString(toYear(audit_created_time) + if(toMonth(audit_created_time) >= 4, 1, 0)), 3, 2)
    ) AS financial_year,    
        count() AS total_updates,
        sum(ownership_category_changed) AS ownership_changes,
        sum(area_changed) AS area_changes,
        sum(workflow_state_changed) AS workflow_reopens,    
    if(
        sum(ownership_category_changed) > 1 OR
        sum(area_changed) > 2 OR
        sum(workflow_state_changed) > 1,
        1, 0
    ) AS risk_score
FROM punjab_property_tax.mart_property_change_metrics
GROUP BY tenant_id, property_id, property_type, financial_year;

-- This mart shows the number of properties with demand and number of properties that were assessed
-- for each tenant and financial year 

CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_property_demand_vs_assessed_by_fy
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_property_demand_vs_assessed_by_fy
EMPTY
AS
WITH demand_properties AS
 (
     SELECT DISTINCT
         tenant_id,
         financial_year,
         properties_with_demand AS property_id
     FROM punjab_property_tax.mart_properties_with_demand_by_fy
 ),

 assessed_properties AS
 (
     SELECT DISTINCT
         tenant_id,
         financialyear AS financial_year,
         propertyid AS property_id
     FROM punjab_property_tax.property_assessment_entity FINAL
     WHERE status = 'ACTIVE'
 ),

 combined AS
 (
     SELECT
         if(d.tenant_id != '', d.tenant_id, a.tenant_id) AS tenant_id,
         if(d.financial_year != '', d.financial_year, a.financial_year) AS financial_year,
         d.property_id AS demand_property_id,
         a.property_id AS assessed_property_id
     FROM demand_properties d
     FULL OUTER JOIN assessed_properties a
         ON d.tenant_id = a.tenant_id
        AND d.financial_year = a.financial_year
        AND d.property_id = a.property_id
 )

 SELECT
     tenant_id,
     financial_year,
     countIf(assessed_property_id != '') AS total_properties_assessed,
     countIf(demand_property_id != '') AS total_properties_with_demand,
     countIf(demand_property_id != '' AND assessed_property_id = '') AS total_properties_with_demand_no_assessment,
     countIf(assessed_property_id != '' AND demand_property_id = '') AS total_properties_with_assessment_no_demand
 FROM combined
 GROUP BY tenant_id, financial_year
 ORDER BY tenant_id, financial_year;


-- ############################################################################
-- ASSESSMENT SUMMARY BY FY
-- ############################################################################
-- Counts assessments by financial year, property type, and channel,
-- split into assessments done by the property owner vs others.

CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_assessment_summary_by_fy
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_assessment_summary_by_fy
EMPTY
AS
WITH assessments AS (
    SELECT tenant_id, financialyear, propertyid, channel, created_by
    FROM punjab_property_tax.property_assessment_entity FINAL
    WHERE status = 'ACTIVE'
        AND financialyear != ''
),
property_types AS (
    SELECT DISTINCT tenant_id, property_id, property_type
    FROM punjab_property_tax.property_owner_entity FINAL
),
owner_users AS (
    SELECT DISTINCT tenant_id, property_id, user_id
    FROM punjab_property_tax.property_owner_entity FINAL
)
SELECT
    a.tenant_id AS tenant_id,
    a.financialyear AS financial_year,
    pt.property_type AS property_type,
    a.channel AS channel,
    count() AS total_assessments,
    countIf(o.user_id != '') AS assessments_by_owner,
    countIf(o.user_id = '') AS assessments_by_others
FROM assessments AS a
LEFT JOIN property_types AS pt
    ON a.tenant_id = pt.tenant_id AND a.propertyid = pt.property_id
LEFT JOIN owner_users AS o
    ON a.tenant_id = o.tenant_id AND a.propertyid = o.property_id AND a.created_by = o.user_id
GROUP BY a.tenant_id, a.financialyear, pt.property_type, a.channel;


-- ############################################################################
-- PAYMENT SUMMARY BY FY
-- ############################################################################
-- Payment metrics by financial year, property type, and payment mode.
-- Part payment: total_amount_paid < total_due.

CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_payment_summary_by_fy
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_payment_summary_by_fy
EMPTY
AS
WITH payments AS (
    SELECT tenant_id, payment_id,
           concat(
               toString(toYear(transaction_date) - if(toMonth(transaction_date) < 4, 1, 0)),
               '-',
               substring(toString(toYear(transaction_date) + if(toMonth(transaction_date) >= 4, 1, 0)), 3, 2)
           ) AS financial_year,
           payment_mode, total_amount_paid, total_due, billid
    FROM punjab_property_tax.payment_with_details_entity FINAL
    WHERE businessservice = 'PT'
),
bills AS (
    SELECT tenant_id, bill_id, consumercode AS property_id
    FROM punjab_property_tax.bill_entity FINAL
),
property_types AS (
    SELECT tenant_id, property_id, property_type
    FROM punjab_property_tax.property_address_entity FINAL
)
SELECT
    p.tenant_id AS tenant_id,
    p.financial_year AS financial_year,
    pt.property_type AS property_type,
    p.payment_mode AS payment_mode,
    count() AS total_payments,
    sum(p.total_amount_paid) AS total_amount_collected,
    countIf(p.total_amount_paid < p.total_due) AS total_part_payments
FROM payments AS p
LEFT JOIN bills AS b
    ON p.tenant_id = b.tenant_id AND p.billid = b.bill_id
LEFT JOIN property_types AS pt
    ON p.tenant_id = pt.tenant_id AND b.property_id = pt.property_id
GROUP BY p.tenant_id, p.financial_year, pt.property_type, p.payment_mode;


-- ############################################################################
-- REBATE SUMMARY BY FY
-- ############################################################################
-- Average rebate size by financial year and property type.
-- Rebate = abs(pt_time_rebate) + abs(pt_adhoc_rebate).

CREATE MATERIALIZED VIEW IF NOT EXISTS punjab_property_tax.rmv_mart_rebate_summary_by_fy
REFRESH EVERY 1000 YEAR
TO punjab_property_tax.mart_rebate_summary_by_fy
EMPTY
AS
WITH demands AS (
    SELECT tenant_id, demand_id, consumer_code, financial_year,
           abs(pt_time_rebate) + abs(pt_adhoc_rebate) AS rebate_amount
    FROM punjab_property_tax.demand_with_details_entity FINAL
    WHERE business_service = 'PT'
      AND demand_status = 'ACTIVE'
      AND financial_year != ''
      AND (pt_time_rebate != 0 OR pt_adhoc_rebate != 0)
),
property_types AS (
    SELECT tenant_id, property_id, property_type
    FROM punjab_property_tax.property_address_entity FINAL
)
SELECT
    d.tenant_id AS tenant_id,
    d.financial_year AS financial_year,
    pt.property_type AS property_type,
    avg(d.rebate_amount) AS avg_rebate_amount,
    sum(d.rebate_amount) AS total_rebate_amount,
    count() AS demands_with_rebate
FROM demands AS d
LEFT JOIN property_types AS pt
    ON d.tenant_id = pt.tenant_id AND d.consumer_code = pt.property_id
GROUP BY d.tenant_id, d.financial_year, pt.property_type;

