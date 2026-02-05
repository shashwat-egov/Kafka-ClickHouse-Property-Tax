-- ============================================================================
-- DEMAND STREAMING AGGREGATION
-- ============================================================================
-- Uses AggregatingMergeTree with argMaxState for memory-efficient dedup
-- Memory: O(unique keys) not O(total rows)
-- No batch refresh needed - always up to date
-- ============================================================================

-- ############################################################################
-- AGGREGATE TABLE: Tracks latest value per demand_detail
-- ############################################################################

CREATE TABLE IF NOT EXISTS agg_demand_detail_latest
(
    -- Dedup keys
    tenant_id LowCardinality(String),
    demand_id String,
    tax_head_code LowCardinality(String),

    -- Denormalized attributes (for filtering without joins)
    consumer_code_state AggregateFunction(argMax, String, Tuple(DateTime64(3), UInt64)),
    financial_year_state AggregateFunction(argMax, LowCardinality(String), Tuple(DateTime64(3), UInt64)),
    business_service_state AggregateFunction(argMax, LowCardinality(String), Tuple(DateTime64(3), UInt64)),

    -- Amount states
    tax_amount_state AggregateFunction(argMax, Decimal(12, 2), Tuple(DateTime64(3), UInt64)),
    collection_amount_state AggregateFunction(argMax, Decimal(12, 2), Tuple(DateTime64(3), UInt64)),

    -- Timestamp state
    last_modified_time_state AggregateFunction(argMax, DateTime64(3), Tuple(DateTime64(3), UInt64))
)
ENGINE = AggregatingMergeTree
ORDER BY (tenant_id, demand_id, tax_head_code)
SETTINGS index_granularity = 8192;


-- ############################################################################
-- STREAMING MV: Raw → Aggregate (updates on every insert)
-- ############################################################################

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_demand_detail_latest
TO agg_demand_detail_latest
AS
SELECT
    tenant_id,
    demand_id,
    tax_head_code,
    argMaxState(consumer_code, (last_modified_time, version)) AS consumer_code_state,
    argMaxState(financial_year, (last_modified_time, version)) AS financial_year_state,
    argMaxState(business_service, (last_modified_time, version)) AS business_service_state,
    argMaxState(tax_amount, (last_modified_time, version)) AS tax_amount_state,
    argMaxState(collection_amount, (last_modified_time, version)) AS collection_amount_state,
    argMaxState(last_modified_time, (last_modified_time, version)) AS last_modified_time_state
FROM demand_with_details_raw
GROUP BY tenant_id, demand_id, tax_head_code;


-- ############################################################################
-- VIEW: Deduplicated current state (instant read)
-- ############################################################################

CREATE OR REPLACE VIEW v_demand_detail_current AS
SELECT
    tenant_id,
    demand_id,
    tax_head_code,
    argMaxMerge(consumer_code_state) AS consumer_code,
    argMaxMerge(financial_year_state) AS financial_year,
    argMaxMerge(business_service_state) AS business_service,
    argMaxMerge(tax_amount_state) AS tax_amount,
    argMaxMerge(collection_amount_state) AS collection_amount,
    argMaxMerge(last_modified_time_state) AS last_modified_time
FROM agg_demand_detail_latest
GROUP BY tenant_id, demand_id, tax_head_code;
