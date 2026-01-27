# Property Tax Analytics Pipeline

An append-only analytical pipeline for Property Tax data using ClickHouse with native Refreshable Materialized Views (RMVs) for automated refresh orchestration.

## Architecture

```
PostgreSQL → Kafka → ClickHouse Raw → [RMV auto] → Snapshots → Marts
                          ↑                              ↑
                     (Append-Only)                 (DEPENDS ON)
```

**Key Innovation**: Replaced manual bash cron orchestration with ClickHouse's native RMV scheduler and `DEPENDS ON` dependency management.

## Quick Start

### 1. Start Infrastructure (Docker)

```bash
cd docker
docker compose up -d
```

Services:
- **ClickHouse**: `localhost:8123` (HTTP), `localhost:9000` (Native)
- **Kafka**: `localhost:29092`
- **Kafka UI**: `localhost:8080`

### 2. Deploy DDLs

```bash
# Core tables
clickhouse-client < 01_kafka_tables.sql
clickhouse-client < 02_raw_tables.sql
clickhouse-client < 03_materialized_views.sql
clickhouse-client < 04_snapshot_tables.sql
clickhouse-client < 06_mart_tables.sql

# RMV definitions (replaces manual refresh)
clickhouse-client < migrations/05_rmv_snapshots.sql
clickhouse-client < migrations/07_rmv_marts.sql
```

### 3. Generate Test Data

```bash
cd test-data
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python generate_properties.py  # 10K properties
python generate_demands.py     # 70K demands
python generate_updates.py     # Update scenarios
```

### 4. Trigger Refresh & Verify

```sql
-- Refresh snapshots
SYSTEM REFRESH VIEW rmv_property_snapshot;
SYSTEM REFRESH VIEW rmv_demand_snapshot;
SYSTEM REFRESH VIEW rmv_demand_detail_snapshot;

-- Marts auto-refresh via DEPENDS ON, or manually:
SYSTEM REFRESH VIEW rmv_mart_defaulters;

-- Check status
SELECT view, status, last_success_time
FROM system.view_refreshes
WHERE view LIKE 'rmv_%';
```

## Connecting to ClickHouse

### Option 1: Using clickhouse-client (Recommended)

**From inside Docker container (no local install needed):**
```bash
docker exec -it docker-clickhouse-1 clickhouse-client
```

**From host machine (requires clickhouse-client installed):**
```bash
clickhouse-client --host localhost --port 9000
```

### Option 2: Using HTTP Interface (curl)

```bash
# Simple query
curl "http://localhost:8123/?query=SELECT+version()"

# Multi-line query
curl "http://localhost:8123/" --data-binary "SELECT count() FROM property_raw"
```

### Option 3: Using GUI Tools (DBeaver, DataGrip)

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `8123` (HTTP) or `9000` (Native) |
| Database | `default` |
| User | `default` |
| Password | *(empty)* |

## Manual Verification Queries

After connecting, run these queries to verify the pipeline:

```sql
-- 1. Check data counts across tables
SELECT 'property_raw' AS table, count() FROM property_raw
UNION ALL SELECT 'property_snapshot', count() FROM property_snapshot
UNION ALL SELECT 'demand_raw', count() FROM demand_raw
UNION ALL SELECT 'demand_snapshot', count() FROM demand_snapshot;

-- 2. Verify deduplication (raw should have more rows than snapshot)
SELECT
    (SELECT count() FROM property_raw) AS raw_count,
    (SELECT count() FROM property_snapshot) AS snapshot_count;

-- 3. Property count by tenant
SELECT tenant_id, count() AS properties
FROM property_snapshot
GROUP BY tenant_id
ORDER BY tenant_id;

-- 4. Check RMV status
SELECT view, status, last_success_time, next_refresh_time
FROM system.view_refreshes
WHERE view LIKE 'rmv_%'
ORDER BY view;

-- 5. View defaulters summary by tenant
SELECT tenant_id,
       count() AS defaulters,
       sum(outstanding_amount) AS total_outstanding
FROM mart_defaulters
GROUP BY tenant_id
ORDER BY total_outstanding DESC;
```

## Test Results

### Data Ingestion
| Table Type | Total Rows |
|------------|------------|
| Raw Tables | 27,313 |
| Snapshot Tables | 27,293 |

### Deduplication Verification
```
Raw table (multiple versions):
PT-AMRITSAR-000004: v1:RESIDENTIAL → v2:COMMERCIAL

Snapshot table (latest only via argMax):
PT-AMRITSAR-000004: v2:COMMERCIAL ✓
```

### Mart Results
| Tenant | Properties | Defaulters | Outstanding |
|--------|------------|------------|-------------|
| pb.amritsar | 250 | 1,755 | ₹5.15Cr |
| pb.jalandhar | 250 | 1,755 | ₹5.17Cr |
| pb.ludhiana | 250 | 1,755 | ₹5.07Cr |
| pb.patiala | 250 | 1,755 | ₹5.21Cr |

### RMV Status
All 15 RMVs (6 snapshots + 9 marts) running with `status: Scheduled`

## File Structure

```
ch-experiment/
├── docker/
│   └── docker-compose.yml           # ClickHouse + Kafka + Zookeeper
├── migrations/
│   ├── 05_rmv_snapshots.sql         # 6 RMVs for snapshot refresh
│   └── 07_rmv_marts.sql             # 9 RMVs with DEPENDS ON
├── test-data/
│   ├── generate_properties.py       # 10K property events
│   ├── generate_demands.py          # 70K demand events
│   ├── generate_updates.py          # Update scenarios
│   └── requirements.txt
├── verification/
│   ├── verify_deduplication.sql     # argMax verification
│   ├── verify_payment_flow.sql      # Collection verification
│   └── verify_rmv_status.sql        # RMV health check
├── scripts/
│   ├── setup.sh                     # Full environment setup
│   └── run_demo.sh                  # End-to-end demo
├── 01_kafka_tables.sql              # Kafka engine tables
├── 02_raw_tables.sql                # Append-only raw tables
├── 03_materialized_views.sql        # JSON parsing MVs
├── 04_snapshot_tables.sql           # Snapshot table schemas
└── 06_mart_tables.sql               # Mart table schemas
```

## RMV Schedule

### Snapshot RMVs (01:00 AM daily)
| RMV | Target Table | Schedule |
|-----|--------------|----------|
| `rmv_property_snapshot` | `property_snapshot` | `EVERY 1 DAY OFFSET 1 HOUR` |
| `rmv_unit_snapshot` | `unit_snapshot` | `EVERY 1 DAY OFFSET 1 HOUR` |
| `rmv_owner_snapshot` | `owner_snapshot` | `EVERY 1 DAY OFFSET 1 HOUR` |
| `rmv_address_snapshot` | `address_snapshot` | `EVERY 1 DAY OFFSET 1 HOUR` |
| `rmv_demand_snapshot` | `demand_snapshot` | `EVERY 1 DAY OFFSET 1 HOUR` |
| `rmv_demand_detail_snapshot` | `demand_detail_snapshot` | `EVERY 1 DAY OFFSET 1 HOUR` |

### Mart RMVs (01:30 AM, after snapshots)
| RMV | Depends On |
|-----|------------|
| `rmv_mart_property_count_by_tenant` | `rmv_property_snapshot` |
| `rmv_mart_property_count_by_ownership` | `rmv_property_snapshot` |
| `rmv_mart_property_count_by_usage` | `rmv_property_snapshot` |
| `rmv_mart_new_properties_by_fy` | `rmv_property_snapshot` |
| `rmv_mart_properties_with_demand_by_fy` | `rmv_demand_snapshot` |
| `rmv_mart_demand_value_by_fy` | `rmv_demand_snapshot`, `rmv_demand_detail_snapshot` |
| `rmv_mart_collections_by_fy` | `rmv_demand_snapshot`, `rmv_demand_detail_snapshot` |
| `rmv_mart_collections_by_month` | `rmv_demand_snapshot`, `rmv_demand_detail_snapshot` |
| `rmv_mart_defaulters` | `rmv_demand_snapshot`, `rmv_demand_detail_snapshot` |

## Core Pattern: argMax Deduplication

Updates are new facts, not mutations. Latest fact wins:

```sql
SELECT
    tenant_id,
    property_id,
    argMax(status, (lmt, ver)) AS status,
    max(lmt) AS last_modified_time,
    max(ver) AS version
FROM (
    SELECT *, last_modified_time AS lmt, version AS ver
    FROM property_raw
)
GROUP BY tenant_id, property_id;
```

## Dedup Keys

| Entity | Dedup Key |
|--------|-----------|
| Property | `(tenant_id, property_id)` |
| Unit | `(tenant_id, property_id, unit_id)` |
| Owner | `(tenant_id, property_id, owner_id)` |
| Address | `(tenant_id, property_id)` |
| Demand | `(tenant_id, demand_id)` |
| DemandDetail | `(tenant_id, demand_id, tax_head_code)` |

## Non-Negotiable Constraints

- ❌ No UPDATE / DELETE
- ❌ No FINAL clause
- ❌ No ReplacingMergeTree
- ❌ No JSON parsing in queries
- ✅ Append-only MergeTree
- ✅ argMax deduplication
- ✅ Idempotent refresh (implicit TRUNCATE + INSERT)
- ✅ RMVs use `EMPTY` clause (no refresh on DDL deploy)

## Verification

```bash
# Verify deduplication
clickhouse-client < verification/verify_deduplication.sql

# Verify payment flow
clickhouse-client < verification/verify_payment_flow.sql

# Check RMV status
clickhouse-client < verification/verify_rmv_status.sql
```

## Requirements

- ClickHouse 24.10+ (RMV with DEPENDS ON support)
- Docker & Docker Compose
- Python 3.8+ (for test data generation)
- kafka-python
