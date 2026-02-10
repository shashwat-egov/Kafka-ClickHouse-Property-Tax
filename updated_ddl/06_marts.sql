-- ============================================================================
-- ANALYTICAL MARTS (Refreshable Materialized Views)
-- ============================================================================
-- Mart tables + MVs that refresh daily after the DAG's successful execution.
-- MVs re-executed the full query against CollapsingMergeTree silver tables.
-- ============================================================================


-- ############################################################################
-- PROPERTY RMVs (refresh daily at 02:00 AM, after the 01:30 AM Airflow DAG)
-- ############################################################################

CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_property_count_by_tenant
REFRESH EVERY 1000 YEAR
TO collapsing_test.mart_property_count_by_tenant
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    count() AS property_count
FROM
(
    SELECT tenant_id, property_id, any(status) AS status
    FROM collapsing_test.property_address_fact
    GROUP BY tenant_id, property_id
    HAVING sum(sign) > 0
)
WHERE status = 'ACTIVE'
GROUP BY tenant_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_property_count_by_usage
REFRESH EVERY 1000 YEAR
TO collapsing_test.mart_property_count_by_usage
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    usage_category,
    count() AS property_count
FROM
(
    SELECT tenant_id, property_id,
           any(status) AS status,
           any(usage_category) AS usage_category
    FROM collapsing_test.property_address_fact
    GROUP BY tenant_id, property_id
    HAVING sum(sign) > 0
)
WHERE status = 'ACTIVE'
GROUP BY tenant_id, usage_category;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_property_count_by_ownership
REFRESH EVERY 1000 YEAR
TO collapsing_test.mart_property_count_by_ownership
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    ownership_category,
    count() AS property_count
FROM
(
    SELECT tenant_id, property_id,
           any(status) AS status,
           any(ownership_category) AS ownership_category
    FROM collapsing_test.property_address_fact
    GROUP BY tenant_id, property_id
    HAVING sum(sign) > 0
)
WHERE status = 'ACTIVE'
GROUP BY tenant_id, ownership_category;


-- ############################################################################
-- DEMAND RMVs (refresh daily at 02:00 AM)
-- ############################################################################

CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_demand_value_by_fy
REFRESH EVERY 1000 YEAR
TO collapsing_test.mart_demand_value_by_fy
AS
SELECT
    today() AS snapshot_date,
    financial_year,
    sum(total_tax_amount) AS total_demand_amount
FROM
(
    SELECT tenant_id, demand_id,
           any(business_service) AS business_service,
           any(financial_year) AS financial_year,
           sum(total_tax_amount * sign) AS total_tax_amount
    FROM collapsing_test.demand_with_details_fact
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_collections_by_fy
REFRESH EVERY 1000 YEAR
TO collapsing_test.mart_collections_by_fy
AS
SELECT
    today() AS snapshot_date,
    financial_year,
    sum(total_collection_amount) AS total_collected_amount
FROM
(
    SELECT tenant_id, demand_id,
           any(business_service) AS business_service,
           any(financial_year) AS financial_year,
           sum(total_collection_amount * sign) AS total_collection_amount
    FROM collapsing_test.demand_with_details_fact
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year;

-- group by fy instead of demand id
CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_collections_by_month
REFRESH EVERY 1000 YEAR
TO collapsing_test.mart_collections_by_month
AS
SELECT
    today() AS snapshot_date,
    year_month,
    sum(total_collection_amount) AS total_collected_amount
FROM
(
    SELECT tenant_id, demand_id,
           any(business_service) AS business_service,
           formatDateTime(any(last_modified_time), '%Y-%m') AS year_month,
           sum(total_collection_amount * sign) AS total_collection_amount
    FROM collapsing_test.demand_with_details_fact
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND total_collection_amount > 0
  AND year_month != '1970-01'
GROUP BY year_month;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_properties_with_demand_by_fy
REFRESH EVERY 1000 YEAR
TO collapsing_test.mart_properties_with_demand_by_fy
AS
SELECT
    today() AS snapshot_date,
    financial_year,
    uniqExact(consumer_code) AS properties_with_demand
FROM
(
    SELECT tenant_id, demand_id,
           any(business_service) AS business_service,
           any(financial_year) AS financial_year,
           any(consumer_code) AS consumer_code
    FROM collapsing_test.demand_with_details_fact
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND financial_year != ''
GROUP BY financial_year;


CREATE MATERIALIZED VIEW IF NOT EXISTS collapsing_test.rmv_mart_defaulters
REFRESH EVERY 1000 YEAR
TO collapsing_test.mart_defaulters
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    consumer_code AS property_id,
    demand_id,
    financial_year,
    total_tax_amount,
    total_collection_amount,
    total_tax_amount - total_collection_amount AS outstanding_amount
FROM
(
    SELECT tenant_id, demand_id,
           any(consumer_code) AS consumer_code,
           any(business_service) AS business_service,
           any(financial_year) AS financial_year,
           sum(total_tax_amount * sign) AS total_tax_amount,
           sum(total_collection_amount * sign) AS total_collection_amount
    FROM collapsing_test.demand_with_details_fact
    GROUP BY tenant_id, demand_id
    HAVING sum(sign) > 0
)
WHERE business_service = 'PT'
  AND total_tax_amount - total_collection_amount > 0;
