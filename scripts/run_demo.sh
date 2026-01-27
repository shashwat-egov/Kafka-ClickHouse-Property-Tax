#!/bin/bash
# ============================================================================
# END-TO-END DEMO SCRIPT
# ============================================================================
# Purpose: Demonstrate the full RMV pipeline with test data
# Usage: ./scripts/run_demo.sh
# Prerequisites: Run ./scripts/setup.sh first
# ============================================================================

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_step() {
    echo -e "\n${BLUE}[STEP]${NC} $1"
    echo -e "${BLUE}======${NC}"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

wait_for_key() {
    echo -e "\n${YELLOW}Press Enter to continue...${NC}"
    read -r
}

# ----------------------------------------------------------------------------
# Check prerequisites
# ----------------------------------------------------------------------------
log_info "Checking prerequisites..."

if ! docker exec clickhouse clickhouse-client --query "SELECT 1" &> /dev/null; then
    log_error "ClickHouse is not running. Please run ./scripts/setup.sh first."
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    log_error "Python3 is required for test data generation."
    exit 1
fi

log_info "Prerequisites OK"

# ----------------------------------------------------------------------------
# Demo Start
# ----------------------------------------------------------------------------
echo ""
echo "=============================================="
echo "  ClickHouse RMV Pipeline Demo"
echo "=============================================="
echo ""
echo "This demo will:"
echo "  1. Generate 10,000 property events"
echo "  2. Generate 70,000 demand events"
echo "  3. Trigger RMV refresh"
echo "  4. Verify deduplication"
echo "  5. Generate update events"
echo "  6. Re-trigger refresh and verify"
echo ""

wait_for_key

# ----------------------------------------------------------------------------
# Step 1: Generate Properties
# ----------------------------------------------------------------------------
log_step "1. Generating 10,000 property events..."

cd "$PROJECT_DIR/test-data"
python3 generate_properties.py

# Check raw table count
RAW_COUNT=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM property_raw")
log_info "property_raw count: $RAW_COUNT"

wait_for_key

# ----------------------------------------------------------------------------
# Step 2: Generate Demands
# ----------------------------------------------------------------------------
log_step "2. Generating 70,000 demand events..."

python3 generate_demands.py

# Check raw table counts
DEMAND_COUNT=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM demand_raw")
DETAIL_COUNT=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM demand_detail_raw")
log_info "demand_raw count: $DEMAND_COUNT"
log_info "demand_detail_raw count: $DETAIL_COUNT"

wait_for_key

# ----------------------------------------------------------------------------
# Step 3: Trigger Snapshot RMV Refresh
# ----------------------------------------------------------------------------
log_step "3. Triggering snapshot RMV refresh..."

log_info "Refreshing property snapshot..."
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_property_snapshot"

log_info "Refreshing unit snapshot..."
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_unit_snapshot"

log_info "Refreshing owner snapshot..."
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_owner_snapshot"

log_info "Refreshing address snapshot..."
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_address_snapshot"

log_info "Refreshing demand snapshot..."
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_demand_snapshot"

log_info "Refreshing demand detail snapshot..."
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_demand_detail_snapshot"

log_info "Waiting for refreshes to complete..."
sleep 5

# Check snapshot counts
PROP_SNAP=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM property_snapshot")
DEMAND_SNAP=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM demand_snapshot")
log_info "property_snapshot count: $PROP_SNAP"
log_info "demand_snapshot count: $DEMAND_SNAP"

wait_for_key

# ----------------------------------------------------------------------------
# Step 4: Trigger Mart RMV Refresh
# ----------------------------------------------------------------------------
log_step "4. Triggering mart RMV refresh..."

log_info "Refreshing property marts..."
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_property_count_by_tenant"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_property_count_by_ownership"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_property_count_by_usage"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_new_properties_by_fy"

log_info "Refreshing demand marts..."
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_properties_with_demand_by_fy"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_demand_value_by_fy"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_collections_by_fy"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_collections_by_month"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_defaulters"

log_info "Waiting for refreshes to complete..."
sleep 5

# Show mart results
log_info "Property count by tenant:"
docker exec clickhouse clickhouse-client --query "SELECT * FROM mart_property_count_by_tenant FORMAT PrettyCompact"

log_info "Defaulter summary:"
docker exec clickhouse clickhouse-client --query "SELECT tenant_id, count() as count, round(sum(outstanding_amount), 2) as total FROM mart_defaulters GROUP BY tenant_id FORMAT PrettyCompact"

wait_for_key

# ----------------------------------------------------------------------------
# Step 5: Generate Update Events
# ----------------------------------------------------------------------------
log_step "5. Generating update events (demonstrating deduplication)..."

python3 generate_updates.py

# Check for multiple versions in raw
log_info "Checking for multiple versions in raw tables..."
docker exec clickhouse clickhouse-client --query "
SELECT
    property_id,
    count() as versions,
    groupArray(version) as version_list
FROM property_raw
GROUP BY property_id
HAVING versions > 1
LIMIT 5
FORMAT PrettyCompact"

wait_for_key

# ----------------------------------------------------------------------------
# Step 6: Re-trigger Refresh
# ----------------------------------------------------------------------------
log_step "6. Re-triggering refresh after updates..."

docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_property_snapshot"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_demand_snapshot"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_demand_detail_snapshot"

log_info "Waiting for refreshes to complete..."
sleep 5

docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_defaulters"
docker exec clickhouse clickhouse-client --query "SYSTEM REFRESH VIEW rmv_mart_property_count_by_usage"

sleep 3

wait_for_key

# ----------------------------------------------------------------------------
# Step 7: Verify Deduplication
# ----------------------------------------------------------------------------
log_step "7. Verifying deduplication..."

log_info "Raw vs Snapshot comparison for updated properties:"
docker exec clickhouse clickhouse-client --query "
SELECT
    'RAW' AS source,
    property_id,
    usage_category,
    version
FROM property_raw
WHERE property_id IN ('PT-AMRITSAR-000001', 'PT-JALANDHAR-000002', 'PT-LUDHIANA-000003')
ORDER BY property_id, version
FORMAT PrettyCompact"

docker exec clickhouse clickhouse-client --query "
SELECT
    'SNAPSHOT' AS source,
    property_id,
    usage_category,
    version
FROM property_snapshot
WHERE property_id IN ('PT-AMRITSAR-000001', 'PT-JALANDHAR-000002', 'PT-LUDHIANA-000003')
ORDER BY property_id
FORMAT PrettyCompact"

log_info "Usage category distribution after updates:"
docker exec clickhouse clickhouse-client --query "
SELECT usage_category, property_count
FROM mart_property_count_by_usage
ORDER BY property_count DESC
FORMAT PrettyCompact"

wait_for_key

# ----------------------------------------------------------------------------
# Step 8: Check RMV Status
# ----------------------------------------------------------------------------
log_step "8. Checking RMV status..."

docker exec clickhouse clickhouse-client --query "
SELECT
    view,
    status,
    refresh_count,
    last_success_time
FROM system.view_refreshes
WHERE view LIKE 'rmv_%'
ORDER BY view
FORMAT PrettyCompact"

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
echo "=============================================="
log_info "Demo completed successfully!"
echo "=============================================="
echo ""
echo "Summary:"
echo "  - Properties generated: 10,000"
echo "  - Demands generated: 70,000"
echo "  - Property updates: 10 (RESIDENTIAL -> COMMERCIAL)"
echo "  - Payment updates: 30 (20 partial + 10 full)"
echo ""
echo "Key observations:"
echo "  - Raw tables contain multiple versions (append-only)"
echo "  - Snapshots contain only latest version (via argMax)"
echo "  - Marts are automatically refreshed via DEPENDS ON"
echo ""
echo "Run verification queries:"
echo "  docker exec -i clickhouse clickhouse-client < verification/verify_deduplication.sql"
echo "  docker exec -i clickhouse clickhouse-client < verification/verify_payment_flow.sql"
echo "  docker exec -i clickhouse clickhouse-client < verification/verify_rmv_status.sql"
