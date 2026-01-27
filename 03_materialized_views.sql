-- ============================================================================
-- MATERIALIZED VIEWS FOR KAFKA → RAW TABLE INGESTION
-- ============================================================================
-- Purpose: Parse JSON and flatten arrays from Kafka into raw tables
-- Rules:
--   - All JSON parsing happens HERE (not in Kafka tables, not in queries)
--   - Arrays are flattened using ARRAY JOIN
--   - One MV per target raw table
-- ============================================================================

-- ----------------------------------------------------------------------------
-- MV: Property Events → property_raw
-- Extracts the property object from the JSON payload
-- Maps to eg_pt_property table
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_property_raw
TO property_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'id') AS id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,

    -- Property Attributes
    JSONExtractString(raw, 'property', 'surveyId') AS survey_id,
    JSONExtractString(raw, 'property', 'accountId') AS account_id,
    JSONExtractString(raw, 'property', 'oldPropertyId') AS old_property_id,
    JSONExtractString(raw, 'property', 'propertyType') AS property_type,
    JSONExtractString(raw, 'property', 'usageCategory') AS usage_category,
    JSONExtractString(raw, 'property', 'ownershipCategory') AS ownership_category,
    JSONExtractString(raw, 'property', 'status') AS status,
    JSONExtractString(raw, 'property', 'acknowldgementNumber') AS acknowledgement_number,
    JSONExtractString(raw, 'property', 'creationReason') AS creation_reason,
    JSONExtractInt(raw, 'property', 'noOfFloors') AS no_of_floors,
    JSONExtractString(raw, 'property', 'source') AS source,
    JSONExtractString(raw, 'property', 'channel') AS channel,

    -- Land Info
    toDecimal64OrZero(JSONExtractString(raw, 'property', 'landArea'), 2) AS land_area,
    toDecimal64OrZero(JSONExtractString(raw, 'property', 'superBuiltUpArea'), 2) AS super_built_up_area,

    -- Audit Fields
    JSONExtractString(raw, 'property', 'auditDetails', 'createdBy') AS created_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'property', 'auditDetails', 'createdTime')) AS created_time,
    JSONExtractString(raw, 'property', 'auditDetails', 'lastModifiedBy') AS last_modified_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'property', 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'property', 'version') AS version

FROM kafka_property_events
WHERE JSONExtractString(raw, 'property', 'propertyId') != '';

-- ----------------------------------------------------------------------------
-- MV: Property Events → unit_raw
-- Flattens the units[] array from each property event
-- Maps to eg_pt_unit table
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_unit_raw
TO unit_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,
    JSONExtractString(unit, 'id') AS unit_id,

    -- Unit Attributes
    JSONExtractInt(unit, 'floorNo') AS floor_no,
    JSONExtractString(unit, 'unitType') AS unit_type,
    JSONExtractString(unit, 'usageCategory') AS usage_category,
    JSONExtractString(unit, 'occupancyType') AS occupancy_type,
    toDate(fromUnixTimestamp64Milli(JSONExtractUInt(unit, 'occupancyDate'))) AS occupancy_date,

    -- Area Info
    toDecimal64OrZero(JSONExtractString(unit, 'carpetArea'), 2) AS carpet_area,
    toDecimal64OrZero(JSONExtractString(unit, 'builtUpArea'), 2) AS built_up_area,
    toDecimal64OrZero(JSONExtractString(unit, 'plinthArea'), 2) AS plinth_area,
    toDecimal64OrZero(JSONExtractString(unit, 'superBuiltUpArea'), 2) AS super_built_up_area,

    -- ARV
    toDecimal64OrZero(JSONExtractString(unit, 'arv'), 2) AS arv,

    -- Construction Info
    JSONExtractString(unit, 'constructionType') AS construction_type,
    JSONExtractInt(unit, 'constructionDate') AS construction_date,

    -- Status
    JSONExtractBool(unit, 'active') AS active,

    -- Audit Fields
    JSONExtractString(unit, 'auditDetails', 'createdBy') AS created_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(unit, 'auditDetails', 'createdTime')) AS created_time,
    JSONExtractString(unit, 'auditDetails', 'lastModifiedBy') AS last_modified_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(unit, 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'property', 'version') AS version

FROM kafka_property_events
ARRAY JOIN JSONExtractArrayRaw(raw, 'property', 'units') AS unit
WHERE JSONExtractString(raw, 'property', 'propertyId') != ''
  AND JSONExtractString(unit, 'id') != '';

-- ----------------------------------------------------------------------------
-- MV: Property Events → owner_raw
-- Flattens the owners[] array from each property event
-- Maps to eg_pt_owner table
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_owner_raw
TO owner_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys (from eg_pt_owner)
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,
    JSONExtractString(owner, 'ownerInfoUuid') AS owner_info_uuid,
    JSONExtractString(owner, 'userId') AS user_id,

    -- Owner Status
    JSONExtractString(owner, 'status') AS status,
    JSONExtractBool(owner, 'isPrimaryOwner') AS is_primary_owner,

    -- Owner Type
    JSONExtractString(owner, 'ownerType') AS owner_type,
    JSONExtractString(owner, 'ownershipPercentage') AS ownership_percentage,
    JSONExtractString(owner, 'institutionId') AS institution_id,
    JSONExtractString(owner, 'relationship') AS relationship,

    -- Audit Fields
    JSONExtractString(owner, 'auditDetails', 'createdBy') AS created_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(owner, 'auditDetails', 'createdTime')) AS created_time,
    JSONExtractString(owner, 'auditDetails', 'lastModifiedBy') AS last_modified_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(owner, 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'property', 'version') AS version

FROM kafka_property_events
ARRAY JOIN JSONExtractArrayRaw(raw, 'property', 'owners') AS owner
WHERE JSONExtractString(raw, 'property', 'propertyId') != ''
  AND JSONExtractString(owner, 'ownerInfoUuid') != '';

-- ----------------------------------------------------------------------------
-- MV: Property Events → address_raw
-- Extracts the address object from each property event
-- Maps to eg_pt_address table
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_address_raw
TO address_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,
    JSONExtractString(raw, 'property', 'address', 'id') AS address_id,

    -- Address Components
    JSONExtractString(raw, 'property', 'address', 'doorNo') AS door_no,
    JSONExtractString(raw, 'property', 'address', 'plotNo') AS plot_no,
    JSONExtractString(raw, 'property', 'address', 'buildingName') AS building_name,
    JSONExtractString(raw, 'property', 'address', 'street') AS street,
    JSONExtractString(raw, 'property', 'address', 'landmark') AS landmark,
    JSONExtractString(raw, 'property', 'address', 'locality') AS locality,
    JSONExtractString(raw, 'property', 'address', 'city') AS city,
    JSONExtractString(raw, 'property', 'address', 'district') AS district,
    JSONExtractString(raw, 'property', 'address', 'region') AS region,
    JSONExtractString(raw, 'property', 'address', 'state') AS state,
    JSONExtractString(raw, 'property', 'address', 'country') AS country,
    JSONExtractString(raw, 'property', 'address', 'pincode') AS pin_code,

    -- Geo Coordinates
    toDecimal64OrZero(JSONExtractString(raw, 'property', 'address', 'latitude'), 6) AS latitude,
    toDecimal64OrZero(JSONExtractString(raw, 'property', 'address', 'longitude'), 7) AS longitude,

    -- Audit Fields
    JSONExtractString(raw, 'property', 'address', 'auditDetails', 'createdBy') AS created_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'property', 'address', 'auditDetails', 'createdTime')) AS created_time,
    JSONExtractString(raw, 'property', 'address', 'auditDetails', 'lastModifiedBy') AS last_modified_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'property', 'address', 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'property', 'version') AS version

FROM kafka_property_events
WHERE JSONExtractString(raw, 'property', 'propertyId') != '';

-- ----------------------------------------------------------------------------
-- MV: Demand Events → demand_raw
-- Extracts the demand object from each demand event
-- Maps to egbs_demand_v1 table
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_demand_raw
TO demand_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'demand', 'id') AS demand_id,

    -- References
    JSONExtractString(raw, 'demand', 'consumerCode') AS consumer_code,
    JSONExtractString(raw, 'demand', 'consumerType') AS consumer_type,
    JSONExtractString(raw, 'demand', 'businessService') AS business_service,

    -- Payer (single column as in PG)
    JSONExtractString(raw, 'demand', 'payer') AS payer,

    -- Demand Period (converted to DateTime64)
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodFrom')) AS tax_period_from,
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodTo')) AS tax_period_to,

    -- Status
    JSONExtractString(raw, 'demand', 'status') AS status,
    JSONExtractBool(raw, 'demand', 'isPaymentCompleted') AS is_payment_completed,

    -- Financial Year (derived from tax period if not provided)
    coalesce(
        nullIf(JSONExtractString(raw, 'demand', 'financialYear'), ''),
        concat(
            toString(if(toMonth(fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodFrom'))) >= 4,
                toYear(fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodFrom'))),
                toYear(fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodFrom'))) - 1)),
            '-',
            toString(if(toMonth(fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodFrom'))) >= 4,
                (toYear(fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodFrom'))) + 1) % 100,
                toYear(fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodFrom'))) % 100))
        )
    ) AS financial_year,

    -- Amounts
    toDecimal64OrZero(JSONExtractString(raw, 'demand', 'minimumAmountPayable'), 4) AS minimum_amount_payable,

    -- Bill Expiry
    JSONExtractInt(raw, 'demand', 'billExpiryTime') AS bill_expiry_time,
    JSONExtractInt(raw, 'demand', 'fixedBillExpiryDate') AS fixed_bill_expiry_date,

    -- Audit Fields (converted to DateTime64)
    JSONExtractString(raw, 'demand', 'auditDetails', 'createdBy') AS created_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'auditDetails', 'createdTime')) AS created_time,
    JSONExtractString(raw, 'demand', 'auditDetails', 'lastModifiedBy') AS last_modified_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'demand', 'version') AS version

FROM kafka_demand_events
WHERE JSONExtractString(raw, 'demand', 'id') != '';

-- ----------------------------------------------------------------------------
-- MV: Demand Events → demand_detail_raw
-- Flattens the demandDetails[] array from each demand event
-- Maps to egbs_demanddetail_v1 table
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_demand_detail_raw
TO demand_detail_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'demand', 'id') AS demand_id,
    JSONExtractString(detail, 'id') AS demand_detail_id,

    -- Tax Head Reference
    JSONExtractString(detail, 'taxHeadCode') AS tax_head_code,

    -- Amounts
    toDecimal64OrZero(JSONExtractString(detail, 'taxAmount'), 2) AS tax_amount,
    toDecimal64OrZero(JSONExtractString(detail, 'collectionAmount'), 2) AS collection_amount,

    -- Audit Fields (converted to DateTime64)
    JSONExtractString(detail, 'auditDetails', 'createdBy') AS created_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(detail, 'auditDetails', 'createdTime')) AS created_time,
    JSONExtractString(detail, 'auditDetails', 'lastModifiedBy') AS last_modified_by,
    fromUnixTimestamp64Milli(JSONExtractUInt(detail, 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'demand', 'version') AS version

FROM kafka_demand_events
ARRAY JOIN JSONExtractArrayRaw(raw, 'demand', 'demandDetails') AS detail
WHERE JSONExtractString(raw, 'demand', 'id') != ''
  AND JSONExtractString(detail, 'taxHeadCode') != '';
