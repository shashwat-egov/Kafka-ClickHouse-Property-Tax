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
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_property_raw
TO property_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,

    -- Property Attributes
    JSONExtractString(raw, 'property', 'propertyType') AS property_type,
    JSONExtractString(raw, 'property', 'usageCategory') AS usage_category,
    JSONExtractString(raw, 'property', 'ownershipCategory') AS ownership_category,
    JSONExtractString(raw, 'property', 'status') AS status,
    JSONExtractString(raw, 'property', 'acknowldgementNumber') AS acknowledgement_number,
    JSONExtractString(raw, 'property', 'assessmentNumber') AS assessment_number,
    JSONExtractString(raw, 'property', 'financialYear') AS financial_year,
    JSONExtractString(raw, 'property', 'source') AS source,
    JSONExtractString(raw, 'property', 'channel') AS channel,

    -- Land Info
    toDecimal64OrZero(JSONExtractString(raw, 'property', 'landArea'), 4) AS land_area,
    JSONExtractString(raw, 'property', 'landAreaUnit') AS land_area_unit,

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
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_unit_raw
TO unit_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,
    JSONExtractString(unit, 'unitId') AS unit_id,

    -- Unit Attributes
    JSONExtractString(unit, 'floorNo') AS floor_no,
    JSONExtractString(unit, 'unitType') AS unit_type,
    JSONExtractString(unit, 'usageCategory') AS usage_category,
    JSONExtractString(unit, 'occupancyType') AS occupancy_type,
    toDateOrZero(JSONExtractString(unit, 'occupancyDate')) AS occupancy_date,

    -- Area Info
    toDecimal64OrZero(JSONExtractString(unit, 'constructedArea'), 4) AS constructed_area,
    toDecimal64OrZero(JSONExtractString(unit, 'carpetArea'), 4) AS carpet_area,
    toDecimal64OrZero(JSONExtractString(unit, 'builtUpArea'), 4) AS built_up_area,

    -- ARV
    toDecimal64OrZero(JSONExtractString(unit, 'arvAmount'), 4) AS arv_amount,

    -- Audit (from parent property)
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'property', 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'property', 'version') AS version

FROM kafka_property_events
ARRAY JOIN JSONExtractArrayRaw(raw, 'property', 'units') AS unit
WHERE JSONExtractString(raw, 'property', 'propertyId') != ''
  AND JSONExtractString(unit, 'unitId') != '';

-- ----------------------------------------------------------------------------
-- MV: Property Events → owner_raw
-- Flattens the owners[] array from each property event
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_owner_raw
TO owner_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,
    JSONExtractString(owner, 'ownerId') AS owner_id,

    -- Owner Attributes
    JSONExtractString(owner, 'name') AS name,
    JSONExtractString(owner, 'mobileNumber') AS mobile_number,
    JSONExtractString(owner, 'email') AS email,
    JSONExtractString(owner, 'gender') AS gender,
    JSONExtractString(owner, 'fatherOrHusbandName') AS father_or_husband_name,
    JSONExtractString(owner, 'relationship') AS relationship,

    -- Owner Type
    JSONExtractString(owner, 'ownerType') AS owner_type,
    JSONExtractString(owner, 'ownerInfoUuid') AS owner_info_uuid,
    JSONExtractString(owner, 'institutionId') AS institution_id,

    -- Document Info
    JSONExtractString(owner, 'documentType') AS document_type,
    JSONExtractString(owner, 'documentUid') AS document_uid,

    -- Ownership Details
    toDecimal64OrZero(JSONExtractString(owner, 'ownershipPercentage'), 2) AS ownership_percentage,
    JSONExtractBool(owner, 'isPrimaryOwner') AS is_primary_owner,
    if(JSONExtractString(owner, 'status') = 'ACTIVE', 1, 0) AS is_active,

    -- Audit
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'property', 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'property', 'version') AS version

FROM kafka_property_events
ARRAY JOIN JSONExtractArrayRaw(raw, 'property', 'owners') AS owner
WHERE JSONExtractString(raw, 'property', 'propertyId') != ''
  AND JSONExtractString(owner, 'ownerId') != '';

-- ----------------------------------------------------------------------------
-- MV: Property Events → address_raw
-- Extracts the address object from each property event
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_address_raw
TO address_raw
AS
SELECT
    now64(3) AS event_time,

    -- Business Keys
    JSONExtractString(raw, 'tenantId') AS tenant_id,
    JSONExtractString(raw, 'property', 'propertyId') AS property_id,
    JSONExtractString(raw, 'property', 'address', 'addressId') AS address_id,

    -- Address Components
    JSONExtractString(raw, 'property', 'address', 'doorNo') AS door_no,
    JSONExtractString(raw, 'property', 'address', 'buildingName') AS building_name,
    JSONExtractString(raw, 'property', 'address', 'street') AS street,
    JSONExtractString(raw, 'property', 'address', 'locality', 'code') AS locality_code,
    JSONExtractString(raw, 'property', 'address', 'locality', 'name') AS locality_name,
    JSONExtractString(raw, 'property', 'address', 'city') AS city,
    JSONExtractString(raw, 'property', 'address', 'district') AS district,
    JSONExtractString(raw, 'property', 'address', 'region') AS region,
    JSONExtractString(raw, 'property', 'address', 'state') AS state,
    JSONExtractString(raw, 'property', 'address', 'country') AS country,
    JSONExtractString(raw, 'property', 'address', 'pinCode') AS pin_code,

    -- Geo Coordinates
    toDecimal64OrZero(JSONExtractString(raw, 'property', 'address', 'geoLocation', 'latitude'), 7) AS latitude,
    toDecimal64OrZero(JSONExtractString(raw, 'property', 'address', 'geoLocation', 'longitude'), 7) AS longitude,

    -- Audit
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'property', 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'property', 'version') AS version

FROM kafka_property_events
WHERE JSONExtractString(raw, 'property', 'propertyId') != '';

-- ----------------------------------------------------------------------------
-- MV: Demand Events → demand_raw
-- Extracts the demand object from each demand event
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

    -- Demand Period
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodFrom')) AS tax_period_from,
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'taxPeriodTo')) AS tax_period_to,
    JSONExtractString(raw, 'demand', 'billingPeriod') AS billing_period,

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

    -- Payer Info
    JSONExtractString(raw, 'demand', 'payer', 'name') AS payer_name,
    JSONExtractString(raw, 'demand', 'payer', 'mobileNumber') AS payer_mobile,
    JSONExtractString(raw, 'demand', 'payer', 'email') AS payer_email,

    -- Audit Fields
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
    JSONExtractString(detail, 'taxHeadMasterCode') AS tax_head_code,
    JSONExtractString(detail, 'taxHeadMasterId') AS tax_head_master_id,

    -- Amounts
    toDecimal64OrZero(JSONExtractString(detail, 'taxAmount'), 4) AS tax_amount,
    toDecimal64OrZero(JSONExtractString(detail, 'collectionAmount'), 4) AS collection_amount,

    -- Period
    fromUnixTimestamp64Milli(JSONExtractUInt(detail, 'taxPeriodFrom')) AS tax_period_from,
    fromUnixTimestamp64Milli(JSONExtractUInt(detail, 'taxPeriodTo')) AS tax_period_to,

    -- Audit
    fromUnixTimestamp64Milli(JSONExtractUInt(raw, 'demand', 'auditDetails', 'lastModifiedTime')) AS last_modified_time,

    -- Version
    JSONExtractUInt(raw, 'demand', 'version') AS version

FROM kafka_demand_events
ARRAY JOIN JSONExtractArrayRaw(raw, 'demand', 'demandDetails') AS detail
WHERE JSONExtractString(raw, 'demand', 'id') != ''
  AND JSONExtractString(detail, 'taxHeadMasterCode') != '';
