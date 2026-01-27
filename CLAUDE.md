# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an append-only analytical pipeline for Property Tax data using ClickHouse with native Refreshable Materialized Views (RMVs) for automated refresh orchestration. Data flows from PostgreSQL through Kafka into ClickHouse, with daily RMV-scheduled snapshot refreshes that power analytical marts.

## Commands

### Quick Start (Docker)
```bash
cd docker && docker compose up -d   # Start infrastructure
./scripts/setup.sh                  # Or use setup script for full setup
./scripts/run_demo.sh               # Run end-to-end demo
```

### Deploy DDLs (execute in order)
```bash
clickhouse-client < 01_kafka_tables.sql
clickhouse-client < 02_raw_tables.sql
clickhouse-client < 03_materialized_views.sql
clickhouse-client < 04_snapshot_tables.sql
clickhouse-client < 06_mart_tables.sql
clickhouse-client < migrations/05_rmv_snapshots.sql
clickhouse-client < migrations/07_rmv_marts.sql
```

### RMV Manual Refresh
```sql
-- Refresh snapshots first
SYSTEM REFRESH VIEW rmv_property_snapshot;
SYSTEM REFRESH VIEW rmv_demand_snapshot;
SYSTEM REFRESH VIEW rmv_demand_detail_snapshot;

-- Marts auto-refresh via DEPENDS ON, or manually:
SYSTEM REFRESH VIEW rmv_mart_defaulters;

-- Check status
SELECT view, status, last_success_time
FROM system.view_refreshes WHERE view LIKE 'rmv_%';
```

### Test Data Generation
```bash
cd test-data
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python generate_properties.py  # 10K properties
python generate_demands.py     # 70K demands
python generate_updates.py     # Update scenarios
```

## Architecture

```
PostgreSQL → Kafka → ClickHouse Raw → [RMV auto] → Snapshots → Marts
                          ↑                              ↑
                     (Append-Only)                 (DEPENDS ON)
```

**Data flow:**
1. **Continuous**: Kafka topics → Kafka engine tables → Materialized Views → Raw tables
2. **Daily (01:00 AM)**: RMVs refresh Snapshots → DEPENDS ON triggers Marts (01:30 AM)

## Refreshable Materialized Views (RMVs)

### Snapshot RMVs (01:00 AM)
| RMV | Target Table |
|-----|--------------|
| `rmv_property_snapshot` | `property_snapshot` |
| `rmv_unit_snapshot` | `unit_snapshot` |
| `rmv_owner_snapshot` | `owner_snapshot` |
| `rmv_address_snapshot` | `address_snapshot` |
| `rmv_demand_snapshot` | `demand_snapshot` |
| `rmv_demand_detail_snapshot` | `demand_detail_snapshot` |

### Mart RMVs (01:30 AM, via DEPENDS ON)
| RMV | Depends On |
|-----|------------|
| `rmv_mart_property_count_by_tenant` | `rmv_property_snapshot` |
| `rmv_mart_defaulters` | `rmv_demand_snapshot`, `rmv_demand_detail_snapshot` |

## Non-Negotiable Constraints

When modifying this codebase, you MUST NOT:
- Use UPDATE or DELETE statements
- Use FINAL clause
- Use ReplacingMergeTree engine
- Parse JSON in analytical queries (only in MVs)

You MUST ensure:
- All tables are append-only MergeTree
- Deduplication uses `argMax(column, (lmt, ver))` with subquery aliasing
- RMVs use `EMPTY` clause to prevent refresh on DDL deploy
- Marts use `DEPENDS ON` for dependency ordering

## Dedup Keys

| Entity | Dedup Key |
|--------|-----------|
| Property | `(tenant_id, property_id)` |
| Unit | `(tenant_id, property_id, unit_id)` |
| Owner | `(tenant_id, property_id, owner_id)` |
| Address | `(tenant_id, property_id)` |
| Demand | `(tenant_id, demand_id)` |
| DemandDetail | `(tenant_id, demand_id, tax_head_code)` |

## Core Pattern

Updates are new facts, not mutations. Latest fact wins via argMax with subquery aliasing to avoid cyclic references:

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

## File Structure

```
ch-experiment/
├── docker/
│   └── docker-compose.yml           # ClickHouse + Kafka + Zookeeper
├── migrations/
│   ├── 05_rmv_snapshots.sql         # RMV definitions for snapshots
│   └── 07_rmv_marts.sql             # RMV definitions for marts
├── test-data/
│   ├── generate_properties.py       # 10K property events
│   ├── generate_demands.py          # 70K demand events
│   ├── generate_updates.py          # Update scenarios
│   └── requirements.txt
├── verification/
│   ├── verify_deduplication.sql     # Verify argMax works
│   ├── verify_payment_flow.sql      # Verify collection updates
│   └── verify_rmv_status.sql        # Check RMV refresh status
├── scripts/
│   ├── setup.sh                     # Full environment setup
│   └── run_demo.sh                  # End-to-end demo
├── 01_kafka_tables.sql
├── 02_raw_tables.sql
├── 03_materialized_views.sql
├── 04_snapshot_tables.sql
├── 06_mart_tables.sql
├── 11_update_handling_semantics.md
└── CLAUDE.md
```
