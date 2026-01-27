-- ============================================================================
-- REFRESHABLE MATERIALIZED VIEWS FOR SNAPSHOT TABLES
-- ============================================================================
-- Purpose: Replace manual bash-orchestrated snapshot refresh with native
--          ClickHouse Refreshable Materialized Views (RMVs)
-- Schedule: Daily at 01:00 AM (OFFSET 1 HOUR from midnight)
-- Pattern: TRUNCATE + INSERT is implicit in RMV refresh (atomic replace)
-- Requires: ClickHouse 24.10+ for production-ready RMV support
-- ============================================================================

-- ############################################################################
-- RMV: Property Snapshot
-- Source: property_raw
-- Dedup Key: (tenant_id, property_id)
-- Maps to eg_pt_property table
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_property_snapshot
REFRESH EVERY 1 DAY OFFSET 1 HOUR
TO property_snapshot
EMPTY
AS
SELECT
    today() AS snapshot_date,
    argMax(id, (lmt, ver)) AS id,
    tenant_id,
    property_id,
    argMax(survey_id, (lmt, ver)) AS survey_id,
    argMax(account_id, (lmt, ver)) AS account_id,
    argMax(old_property_id, (lmt, ver)) AS old_property_id,
    argMax(property_type, (lmt, ver)) AS property_type,
    argMax(usage_category, (lmt, ver)) AS usage_category,
    argMax(ownership_category, (lmt, ver)) AS ownership_category,
    argMax(status, (lmt, ver)) AS status,
    argMax(acknowledgement_number, (lmt, ver)) AS acknowledgement_number,
    argMax(creation_reason, (lmt, ver)) AS creation_reason,
    argMax(no_of_floors, (lmt, ver)) AS no_of_floors,
    argMax(source, (lmt, ver)) AS source,
    argMax(channel, (lmt, ver)) AS channel,
    argMax(land_area, (lmt, ver)) AS land_area,
    argMax(super_built_up_area, (lmt, ver)) AS super_built_up_area,
    argMax(created_by, (lmt, ver)) AS created_by,
    argMax(created_time, (lmt, ver)) AS created_time,
    argMax(last_modified_by, (lmt, ver)) AS last_modified_by,
    max(lmt) AS last_modified_time,
    max(ver) AS version
FROM (
    SELECT *, last_modified_time AS lmt, version AS ver
    FROM property_raw
)
GROUP BY tenant_id, property_id;


-- ############################################################################
-- RMV: Unit Snapshot
-- Source: unit_raw
-- Dedup Key: (tenant_id, property_id, unit_id)
-- Maps to eg_pt_unit table
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_unit_snapshot
REFRESH EVERY 1 DAY OFFSET 1 HOUR
TO unit_snapshot
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    property_id,
    unit_id,
    argMax(floor_no, (lmt, ver)) AS floor_no,
    argMax(unit_type, (lmt, ver)) AS unit_type,
    argMax(usage_category, (lmt, ver)) AS usage_category,
    argMax(occupancy_type, (lmt, ver)) AS occupancy_type,
    argMax(occupancy_date, (lmt, ver)) AS occupancy_date,
    argMax(carpet_area, (lmt, ver)) AS carpet_area,
    argMax(built_up_area, (lmt, ver)) AS built_up_area,
    argMax(plinth_area, (lmt, ver)) AS plinth_area,
    argMax(super_built_up_area, (lmt, ver)) AS super_built_up_area,
    argMax(arv, (lmt, ver)) AS arv,
    argMax(construction_type, (lmt, ver)) AS construction_type,
    argMax(construction_date, (lmt, ver)) AS construction_date,
    argMax(active, (lmt, ver)) AS active,
    argMax(created_by, (lmt, ver)) AS created_by,
    argMax(created_time, (lmt, ver)) AS created_time,
    argMax(last_modified_by, (lmt, ver)) AS last_modified_by,
    max(lmt) AS last_modified_time,
    max(ver) AS version
FROM (
    SELECT *, last_modified_time AS lmt, version AS ver
    FROM unit_raw
)
GROUP BY tenant_id, property_id, unit_id;


-- ############################################################################
-- RMV: Owner Snapshot
-- Source: owner_raw
-- Dedup Key: (tenant_id, property_id, owner_info_uuid)
-- Maps to eg_pt_owner table
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_owner_snapshot
REFRESH EVERY 1 DAY OFFSET 1 HOUR
TO owner_snapshot
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    property_id,
    owner_info_uuid,
    argMax(user_id, (lmt, ver)) AS user_id,
    argMax(status, (lmt, ver)) AS status,
    argMax(is_primary_owner, (lmt, ver)) AS is_primary_owner,
    argMax(owner_type, (lmt, ver)) AS owner_type,
    argMax(ownership_percentage, (lmt, ver)) AS ownership_percentage,
    argMax(institution_id, (lmt, ver)) AS institution_id,
    argMax(relationship, (lmt, ver)) AS relationship,
    argMax(created_by, (lmt, ver)) AS created_by,
    argMax(created_time, (lmt, ver)) AS created_time,
    argMax(last_modified_by, (lmt, ver)) AS last_modified_by,
    max(lmt) AS last_modified_time,
    max(ver) AS version
FROM (
    SELECT *, last_modified_time AS lmt, version AS ver
    FROM owner_raw
)
GROUP BY tenant_id, property_id, owner_info_uuid;


-- ############################################################################
-- RMV: Address Snapshot
-- Source: address_raw
-- Dedup Key: (tenant_id, property_id)
-- Maps to eg_pt_address table
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_address_snapshot
REFRESH EVERY 1 DAY OFFSET 1 HOUR
TO address_snapshot
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    property_id,
    argMax(address_id, (lmt, ver)) AS address_id,
    argMax(door_no, (lmt, ver)) AS door_no,
    argMax(plot_no, (lmt, ver)) AS plot_no,
    argMax(building_name, (lmt, ver)) AS building_name,
    argMax(street, (lmt, ver)) AS street,
    argMax(landmark, (lmt, ver)) AS landmark,
    argMax(locality, (lmt, ver)) AS locality,
    argMax(city, (lmt, ver)) AS city,
    argMax(district, (lmt, ver)) AS district,
    argMax(region, (lmt, ver)) AS region,
    argMax(state, (lmt, ver)) AS state,
    argMax(country, (lmt, ver)) AS country,
    argMax(pin_code, (lmt, ver)) AS pin_code,
    argMax(latitude, (lmt, ver)) AS latitude,
    argMax(longitude, (lmt, ver)) AS longitude,
    argMax(created_by, (lmt, ver)) AS created_by,
    argMax(created_time, (lmt, ver)) AS created_time,
    argMax(last_modified_by, (lmt, ver)) AS last_modified_by,
    max(lmt) AS last_modified_time,
    max(ver) AS version
FROM (
    SELECT *, last_modified_time AS lmt, version AS ver
    FROM address_raw
)
GROUP BY tenant_id, property_id;


-- ############################################################################
-- RMV: Demand Snapshot
-- Source: demand_raw
-- Dedup Key: (tenant_id, demand_id)
-- Maps to egbs_demand_v1 table
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_demand_snapshot
REFRESH EVERY 1 DAY OFFSET 1 HOUR
TO demand_snapshot
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    demand_id,
    argMax(consumer_code, (lmt, ver)) AS consumer_code,
    argMax(consumer_type, (lmt, ver)) AS consumer_type,
    argMax(business_service, (lmt, ver)) AS business_service,
    argMax(payer, (lmt, ver)) AS payer,
    argMax(tax_period_from, (lmt, ver)) AS tax_period_from,
    argMax(tax_period_to, (lmt, ver)) AS tax_period_to,
    argMax(status, (lmt, ver)) AS status,
    argMax(is_payment_completed, (lmt, ver)) AS is_payment_completed,
    argMax(financial_year, (lmt, ver)) AS financial_year,
    argMax(minimum_amount_payable, (lmt, ver)) AS minimum_amount_payable,
    argMax(bill_expiry_time, (lmt, ver)) AS bill_expiry_time,
    argMax(fixed_bill_expiry_date, (lmt, ver)) AS fixed_bill_expiry_date,
    argMax(created_by, (lmt, ver)) AS created_by,
    argMax(created_time, (lmt, ver)) AS created_time,
    argMax(last_modified_by, (lmt, ver)) AS last_modified_by,
    max(lmt) AS last_modified_time,
    max(ver) AS version
FROM (
    SELECT *, last_modified_time AS lmt, version AS ver
    FROM demand_raw
)
GROUP BY tenant_id, demand_id;


-- ############################################################################
-- RMV: Demand Detail Snapshot
-- Source: demand_detail_raw
-- Dedup Key: (tenant_id, demand_id, tax_head_code)
-- Maps to egbs_demanddetail_v1 table
-- ############################################################################
CREATE MATERIALIZED VIEW IF NOT EXISTS rmv_demand_detail_snapshot
REFRESH EVERY 1 DAY OFFSET 1 HOUR
TO demand_detail_snapshot
EMPTY
AS
SELECT
    today() AS snapshot_date,
    tenant_id,
    demand_id,
    argMax(demand_detail_id, (lmt, ver)) AS demand_detail_id,
    tax_head_code,
    argMax(tax_amount, (lmt, ver)) AS tax_amount,
    argMax(collection_amount, (lmt, ver)) AS collection_amount,
    argMax(created_by, (lmt, ver)) AS created_by,
    argMax(created_time, (lmt, ver)) AS created_time,
    argMax(last_modified_by, (lmt, ver)) AS last_modified_by,
    max(lmt) AS last_modified_time,
    max(ver) AS version
FROM (
    SELECT *, last_modified_time AS lmt, version AS ver
    FROM demand_detail_raw
)
GROUP BY tenant_id, demand_id, tax_head_code;


-- ============================================================================
-- MANUAL REFRESH COMMANDS (for testing/debugging)
-- ============================================================================
-- To manually trigger a refresh:
--   SYSTEM REFRESH VIEW rmv_property_snapshot;
--   SYSTEM REFRESH VIEW rmv_unit_snapshot;
--   SYSTEM REFRESH VIEW rmv_owner_snapshot;
--   SYSTEM REFRESH VIEW rmv_address_snapshot;
--   SYSTEM REFRESH VIEW rmv_demand_snapshot;
--   SYSTEM REFRESH VIEW rmv_demand_detail_snapshot;
--
-- To check refresh status:
--   SELECT * FROM system.view_refreshes WHERE view LIKE 'rmv_%';
-- ============================================================================
