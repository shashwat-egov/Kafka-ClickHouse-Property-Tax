CREATE OR REPLACE VIEW punjab_property_tax.v_cumulative_collections_by_month AS
WITH base AS (
    SELECT
        tenant_id,
        financial_year,
        year_month,
        toInt32(replaceAll(year_month, '-', '')) AS month_sort_key,
        SUM(total_payments) AS total_payments,
        SUM(total_collected_amount) AS total_collected_amount
    FROM punjab_property_tax.mart_collections_by_month
    GROUP BY
        tenant_id,
        financial_year,
        year_month,
        month_sort_key
)
SELECT
    tenant_id,
    financial_year,
    toDate(concat(year_month, '-01'))::Date AS month_date,
    total_payments,
    total_collected_amount,
    sum(total_payments) OVER (
        PARTITION BY tenant_id, financial_year
        ORDER BY month_sort_key
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_payments,
    sum(total_collected_amount) OVER (
        PARTITION BY tenant_id, financial_year
        ORDER BY month_sort_key
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_collected_amount,
    sum(total_payments) OVER (
        PARTITION BY tenant_id, financial_year
    ) AS fy_total_payments,
    sum(total_collected_amount) OVER (
        PARTITION BY tenant_id, financial_year
    ) AS fy_total_collected_amount
FROM base;


CREATE OR REPLACE VIEW punjab_property_tax.v_demand_collection_with_arrears AS
WITH base AS (
    SELECT
        tenant_id,
        financial_year,
        total_demand,
        total_collection,
        total_outstanding,
        toInt32(substring(financial_year, 1, 4)) AS fy_start
    FROM punjab_property_tax.mart_demand_and_collection_summary
    WHERE toInt32(substring(financial_year, 1, 4)) >= 2023
),

arrears AS (
    SELECT
        if(b1.tenant_id != '', b1.tenant_id, b2.tenant_id)              AS tenant_id,
        if(b1.financial_year != '',
            b1.financial_year,
            concat(
                toString(b2.fy_start + 1), '-',
                substring(toString(b2.fy_start + 2), 3, 2)
            )
        )                                                                AS financial_year,
        if(b1.fy_start != 0, b1.fy_start, b2.fy_start + 1)             AS fy_start,

        b1.total_demand,
        b1.total_collection,
        b1.total_outstanding,

        -- Last year (fy_start - 1)
        sumIf(b2.total_demand,       b2.fy_start = if(b1.fy_start != 0, b1.fy_start, b2.fy_start + 1) - 1) AS last_year_demand,
        sumIf(b2.total_collection,   b2.fy_start = if(b1.fy_start != 0, b1.fy_start, b2.fy_start + 1) - 1) AS last_year_collection,
        sumIf(b2.total_outstanding,  b2.fy_start = if(b1.fy_start != 0, b1.fy_start, b2.fy_start + 1) - 1) AS last_year_outstanding,

        -- Arrears (all FYs before current)
        sumIf(b2.total_demand,       b2.fy_start < if(b1.fy_start != 0, b1.fy_start, b2.fy_start + 1)) AS arrears_demand,
        sumIf(b2.total_collection,   b2.fy_start < if(b1.fy_start != 0, b1.fy_start, b2.fy_start + 1)) AS arrears_collection,
        sumIf(b2.total_outstanding,  b2.fy_start < if(b1.fy_start != 0, b1.fy_start, b2.fy_start + 1)) AS arrears_outstanding

    FROM base b1
    FULL OUTER JOIN base b2
        ON  b1.tenant_id = b2.tenant_id
        AND b2.fy_start  < b1.fy_start

    GROUP BY
        tenant_id,
        financial_year,
        fy_start,
        b1.total_demand,
        b1.total_collection,
        b1.total_outstanding
)

SELECT
    tenant_id,
    financial_year,
    total_demand,
    total_collection,
    total_outstanding,
    last_year_demand,
    last_year_collection,
    last_year_outstanding,
    arrears_demand,
    arrears_collection,
    arrears_outstanding
FROM arrears;


CREATE OR REPLACE VIEW punjab_property_tax.v_payment_summary_with_last_year AS
SELECT
    if(t1.tenant_id        != '', t1.tenant_id,        t2.tenant_id)        AS tenant_id,
    if(t1.payment_mode  != '', t1.payment_mode,  t2.payment_mode)  AS payment_mode,
    if(t1.property_type != '', t1.property_type, t2.property_type) AS property_type,
    if(t1.financial_year != '',
        t1.financial_year,
        concat(
            toString(t2.fy_start + 1), '-',
            substring(toString(t2.fy_start + 2), 3, 2)
        )
    )                                                               AS financial_year,

    t1.total_payments                                               AS total_payments,
    t1.total_amount_collected                                       AS total_amount_collected,
    t2.total_amount_collected                                       AS last_year_total_amount,
    t2.total_payments                                               AS last_year_total_payments

FROM (
    SELECT
        tenant_id,
        financial_year,
        payment_mode,
        property_type,
        toInt32(substring(financial_year, 1, 4)) AS fy_start,
        total_payments,
        total_amount_collected
    FROM punjab_property_tax.mart_payment_summary_by_fy
) t1
FULL OUTER JOIN (
    SELECT
        tenant_id,
        financial_year,
        payment_mode,
        property_type,
        toInt32(substring(financial_year, 1, 4)) AS fy_start,
        total_payments,
        total_amount_collected
    FROM punjab_property_tax.mart_payment_summary_by_fy
) t2
    ON  t1.tenant_id        = t2.tenant_id
    AND t1.payment_mode  = t2.payment_mode
    AND t1.property_type = t2.property_type
    AND t1.fy_start      = t2.fy_start + 1;
