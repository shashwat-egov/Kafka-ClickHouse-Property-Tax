#!/bin/bash
# ============================================================================
# Deploy all DDLs in order
# ============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DDL_DIR="$PROJECT_DIR/ddl"

echo "Deploying ClickHouse DDLs..."

for file in "$DDL_DIR"/*.sql; do
    echo "  → $(basename "$file")"
    clickhouse-client < "$file"
done

echo ""
echo "✓ All DDLs deployed successfully"
echo ""
echo "Next steps:"
echo "  1. If migrating existing data, run: ./scripts/backfill.sh"
echo "  2. To manually refresh property snapshots: SYSTEM REFRESH VIEW rmv_property_snapshot"
