#!/bin/bash
# ============================================================================
# Backfill streaming aggregates from existing raw data
# Processes partition-by-partition to avoid OOM
# ============================================================================
set -e

echo "============================================"
echo "Backfill: demand_with_details_raw"
echo "============================================"

# Check if old tables exist
OLD_TABLES=$(clickhouse-client --query "
    SELECT count()
    FROM system.tables
    WHERE name IN ('demand_raw', 'demand_detail_raw')
" 2>/dev/null)

if [ "$OLD_TABLES" -lt 2 ]; then
    echo "Old demand_raw/demand_detail_raw tables not found."
    echo "If this is a fresh install, no backfill needed."
    exit 0
fi

# Get partitions
PARTITIONS=$(clickhouse-client --query "
    SELECT DISTINCT partition
    FROM system.parts
    WHERE table = 'demand_detail_raw' AND active
    ORDER BY partition
" 2>/dev/null)

if [ -z "$PARTITIONS" ]; then
    echo "No data in demand_detail_raw. Nothing to backfill."
    exit 0
fi

TOTAL=$(echo "$PARTITIONS" | wc -l | tr -d ' ')
COUNT=0

for PARTITION in $PARTITIONS; do
    COUNT=$((COUNT + 1))
    echo "[$COUNT/$TOTAL] Processing partition: $PARTITION"

    clickhouse-client --query "
        INSERT INTO demand_with_details_raw
        SELECT
            dd.event_time,
            dd.tenant_id,
            dd.demand_id,
            d.consumer_code,
            d.consumer_type,
            d.business_service,
            d.payer,
            d.tax_period_from,
            d.tax_period_to,
            d.status AS demand_status,
            d.is_payment_completed,
            d.financial_year,
            d.minimum_amount_payable,
            d.bill_expiry_time,
            d.fixed_bill_expiry_date,
            dd.demand_detail_id,
            dd.tax_head_code,
            dd.tax_amount,
            dd.collection_amount,
            dd.created_by,
            dd.created_time,
            dd.last_modified_by,
            dd.last_modified_time,
            greatest(d.version, dd.version) AS version
        FROM demand_detail_raw dd
        INNER JOIN (
            SELECT
                tenant_id, demand_id,
                argMax(consumer_code, (last_modified_time, version)) AS consumer_code,
                argMax(consumer_type, (last_modified_time, version)) AS consumer_type,
                argMax(business_service, (last_modified_time, version)) AS business_service,
                argMax(payer, (last_modified_time, version)) AS payer,
                argMax(tax_period_from, (last_modified_time, version)) AS tax_period_from,
                argMax(tax_period_to, (last_modified_time, version)) AS tax_period_to,
                argMax(status, (last_modified_time, version)) AS status,
                argMax(is_payment_completed, (last_modified_time, version)) AS is_payment_completed,
                argMax(financial_year, (last_modified_time, version)) AS financial_year,
                argMax(minimum_amount_payable, (last_modified_time, version)) AS minimum_amount_payable,
                argMax(bill_expiry_time, (last_modified_time, version)) AS bill_expiry_time,
                argMax(fixed_bill_expiry_date, (last_modified_time, version)) AS fixed_bill_expiry_date,
                max(version) AS version
            FROM demand_raw
            GROUP BY tenant_id, demand_id
        ) d ON dd.tenant_id = d.tenant_id AND dd.demand_id = d.demand_id
        WHERE toYYYYMM(dd.last_modified_time) = $PARTITION
    "
done

echo ""
echo "Optimizing aggregate table..."
clickhouse-client --query "OPTIMIZE TABLE agg_demand_detail_latest FINAL"

echo ""
echo "============================================"
echo "Verification"
echo "============================================"

clickhouse-client --query "
    SELECT
        table,
        formatReadableSize(sum(bytes_on_disk)) AS size,
        formatReadableQuantity(sum(rows)) AS rows
    FROM system.parts
    WHERE table IN ('demand_with_details_raw', 'agg_demand_detail_latest') AND active
    GROUP BY table
"

echo ""
echo "✓ Backfill complete"
