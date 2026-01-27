#!/bin/bash
# ============================================================================
# SETUP SCRIPT FOR CLICKHOUSE RMV PIPELINE
# ============================================================================
# Purpose: Complete environment setup including Docker, DDLs, and dependencies
# Usage: ./scripts/setup.sh
# ============================================================================

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ----------------------------------------------------------------------------
# Step 1: Check prerequisites
# ----------------------------------------------------------------------------
log_info "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    log_error "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    log_warn "Python3 is not installed. Test data generation will not be available."
fi

log_info "Prerequisites OK"

# ----------------------------------------------------------------------------
# Step 2: Start Docker containers
# ----------------------------------------------------------------------------
log_info "Starting Docker containers..."

cd "$PROJECT_DIR/docker"

# Use docker compose (v2) if available, otherwise docker-compose (v1)
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

$COMPOSE_CMD up -d

log_info "Waiting for services to be healthy..."

# Wait for ClickHouse to be ready
MAX_RETRIES=30
RETRY_COUNT=0
while ! docker exec clickhouse clickhouse-client --query "SELECT 1" &> /dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        log_error "ClickHouse failed to start within timeout"
        exit 1
    fi
    echo -n "."
    sleep 2
done
echo ""
log_info "ClickHouse is ready"

# Wait for Kafka to be ready
RETRY_COUNT=0
while ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        log_error "Kafka failed to start within timeout"
        exit 1
    fi
    echo -n "."
    sleep 2
done
echo ""
log_info "Kafka is ready"

# ----------------------------------------------------------------------------
# Step 3: Create Kafka topics
# ----------------------------------------------------------------------------
log_info "Creating Kafka topics..."

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic property-events \
    --partitions 4 \
    --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic demand-events \
    --partitions 4 \
    --replication-factor 1

log_info "Kafka topics created"

# ----------------------------------------------------------------------------
# Step 4: Run DDL migrations
# ----------------------------------------------------------------------------
log_info "Running DDL migrations..."

cd "$PROJECT_DIR"

# Execute DDLs in order
DDL_FILES=(
    "01_kafka_tables.sql"
    "02_raw_tables.sql"
    "03_materialized_views.sql"
    "04_snapshot_tables.sql"
    "06_mart_tables.sql"
    "migrations/05_rmv_snapshots.sql"
    "migrations/07_rmv_marts.sql"
)

for ddl_file in "${DDL_FILES[@]}"; do
    if [ -f "$ddl_file" ]; then
        log_info "  Executing $ddl_file..."
        docker exec -i clickhouse clickhouse-client < "$ddl_file"
    else
        log_warn "  File not found: $ddl_file (skipping)"
    fi
done

log_info "DDL migrations completed"

# ----------------------------------------------------------------------------
# Step 5: Install Python dependencies (if Python available)
# ----------------------------------------------------------------------------
if command -v python3 &> /dev/null; then
    log_info "Installing Python dependencies..."
    cd "$PROJECT_DIR/test-data"

    if [ -f "requirements.txt" ]; then
        python3 -m pip install -r requirements.txt --quiet
        log_info "Python dependencies installed"
    fi
fi

# ----------------------------------------------------------------------------
# Step 6: Verify setup
# ----------------------------------------------------------------------------
log_info "Verifying setup..."

# Check tables exist
TABLE_COUNT=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM system.tables WHERE database = 'default' AND name LIKE '%_raw' OR name LIKE '%_snapshot' OR name LIKE 'mart_%' OR name LIKE 'rmv_%'")
log_info "  Tables created: $TABLE_COUNT"

# Check RMVs exist
RMV_COUNT=$(docker exec clickhouse clickhouse-client --query "SELECT count() FROM system.view_refreshes WHERE view LIKE 'rmv_%'")
log_info "  RMVs configured: $RMV_COUNT"

# Check Kafka topics
TOPIC_COUNT=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -E "property-events|demand-events" | wc -l)
log_info "  Kafka topics: $TOPIC_COUNT"

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo ""
log_info "=========================================="
log_info "Setup completed successfully!"
log_info "=========================================="
echo ""
echo "Services running:"
echo "  - ClickHouse: http://localhost:8123 (HTTP), localhost:9000 (Native)"
echo "  - Kafka: localhost:29092 (external), kafka:9092 (internal)"
echo "  - Kafka UI: http://localhost:8080"
echo ""
echo "Next steps:"
echo "  1. Generate test data:"
echo "     cd $PROJECT_DIR/test-data"
echo "     python3 generate_properties.py"
echo "     python3 generate_demands.py"
echo ""
echo "  2. Trigger RMV refresh:"
echo "     docker exec clickhouse clickhouse-client --query \"SYSTEM REFRESH VIEW rmv_property_snapshot\""
echo ""
echo "  3. Run verification:"
echo "     docker exec -i clickhouse clickhouse-client < verification/verify_rmv_status.sql"
echo ""
echo "Or run the full demo:"
echo "  ./scripts/run_demo.sh"
