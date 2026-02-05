-- ============================================================================
-- PROPERTY SNAPSHOTS (Batch RMVs - OK for smaller tables)
-- ============================================================================
-- Property tables are small enough for batch refresh without OOM
-- ============================================================================

-- ############################################################################
-- SNAPSHOT TABLES
-- ############################################################################

CREATE TABLE IF NOT EXISTS property_snapshot
(
    snapshot_date Date DEFAULT today(),
    id String,
    tenant_id LowCardinality(String),
    property_id String,
    survey_id String,
    account_id String,
    old_property_id String,
    property_type LowCardinality(String),
    usage_category LowCardinality(String),
    ownership_category LowCardinality(String),
    status LowCardinality(String),
    acknowledgement_number String,
    creation_reason LowCardinality(String),
    no_of_floors Int8,
    source LowCardinality(String),
    channel LowCardinality(String),
    land_area Decimal(10, 2),
    super_built_up_area Decimal(10, 2),
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS unit_snapshot
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_id String,
    unit_id String,
    floor_no Int8,
    unit_type LowCardinality(String),
    usage_category LowCardinality(String),
    occupancy_type LowCardinality(String),
    occupancy_date Date,
    carpet_area Decimal(10, 2),
    built_up_area Decimal(18, 2),
    plinth_area Decimal(10, 2),
    super_built_up_area Decimal(10, 2),
    arv Decimal(12, 2),
    construction_type LowCardinality(String),
    construction_date Int64,
    active UInt8,
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id, unit_id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS owner_snapshot
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_id String,
    owner_info_uuid String,
    user_id String,
    status LowCardinality(String),
    is_primary_owner UInt8,
    owner_type LowCardinality(String),
    ownership_percentage String,
    institution_id String,
    relationship LowCardinality(String),
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id, owner_info_uuid)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS address_snapshot
(
    snapshot_date Date DEFAULT today(),
    tenant_id LowCardinality(String),
    property_id String,
    address_id String,
    door_no String,
    plot_no String,
    building_name String,
    street String,
    landmark String,
    locality LowCardinality(String),
    city LowCardinality(String),
    district LowCardinality(String),
    region LowCardinality(String),
    state LowCardinality(String),
    country LowCardinality(String),
    pin_code LowCardinality(String),
    latitude Decimal(9, 6),
    longitude Decimal(10, 7),
    created_by String,
    created_time DateTime64(3),
    last_modified_by String,
    last_modified_time DateTime64(3),
    version UInt64
)
ENGINE = MergeTree
ORDER BY (tenant_id, property_id)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- REFRESHABLE MATERIALIZED VIEWS (Daily 01:00 AM)
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
FROM (SELECT *, last_modified_time AS lmt, version AS ver FROM property_raw)
GROUP BY tenant_id, property_id;


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
FROM (SELECT *, last_modified_time AS lmt, version AS ver FROM unit_raw)
GROUP BY tenant_id, property_id, unit_id;


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
FROM (SELECT *, last_modified_time AS lmt, version AS ver FROM owner_raw)
GROUP BY tenant_id, property_id, owner_info_uuid;


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
FROM (SELECT *, last_modified_time AS lmt, version AS ver FROM address_raw)
GROUP BY tenant_id, property_id;
