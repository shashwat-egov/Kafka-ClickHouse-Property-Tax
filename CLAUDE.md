# Property Tax Analytics Pipeline

Append-only analytical pipeline using ClickHouse with streaming aggregation for memory-efficient processing at scale.

## Quick Start

```bash
cd docker && docker compose up -d
./scripts/deploy.sh
./scripts/backfill.sh  # Only if migrating existing data
```

## Architecture

```
Kafka → Ingestion MVs → Raw Tables → Streaming MVs → AggregatingMergeTree → Views
           ↓                              ↓
    (JSON parsing)              (argMaxState per key)
```

**Key insight**: Deduplication happens at insert-time, not query-time. Memory usage is O(unique keys), not O(total rows).

## File Structure

```
ch-experiment/
├── ddl/
│   ├── 01_kafka.sql              # Kafka engine tables
│   ├── 02_raw_tables.sql         # Append-only raw tables
│   ├── 03_ingestion_mvs.sql      # Kafka → Raw MVs
│   ├── 04_property_snapshots.sql # Property batch snapshots (small tables)
│   ├── 05_demand_streaming.sql   # Demand streaming aggregation
│   └── 06_marts.sql              # Analytical marts/views
├── docker/
│   └── docker-compose.yml
├── scripts/
│   ├── deploy.sh                 # Deploy all DDLs
│   └── backfill.sh               # Migrate existing data
├── test-data/
│   └── generate_*.py
└── CLAUDE.md
```

## Tables

| Table | Engine | Purpose |
|-------|--------|---------|
| `demand_with_details_raw` | MergeTree | Denormalized demand+details (no JOIN) |
| `agg_demand_detail_latest` | AggregatingMergeTree | Streaming dedup via argMaxState |
| `v_demand_detail_current` | View | Deduplicated current state |
| `v_mart_*` | Views | Analytical marts (instant reads) |
| `property_snapshot` | MergeTree | Batch RMV target (small table) |

## Constraints

**MUST NOT:**
- Use UPDATE or DELETE
- Use FINAL clause
- Parse JSON in queries (only in ingestion MVs)

**MUST:**
- Keep raw tables append-only
- Use `argMaxState` for streaming dedup
- Use `argMax(col, (last_modified_time, version))` pattern

## Dedup Keys

| Entity | Key |
|--------|-----|
| Property | `(tenant_id, property_id)` |
| DemandDetail | `(tenant_id, demand_id, tax_head_code)` |

## Core Pattern

```sql
-- Streaming: Store state at insert time
CREATE MATERIALIZED VIEW mv_agg TO agg_table AS
SELECT
    tenant_id, demand_id, tax_head_code,
    argMaxState(amount, (last_modified_time, version)) AS amount_state
FROM raw_table
GROUP BY tenant_id, demand_id, tax_head_code;

-- Query: Finalize state instantly
SELECT argMaxMerge(amount_state) AS amount
FROM agg_table
GROUP BY tenant_id, demand_id, tax_head_code;
```

## Manual Refresh (Property Snapshots)

```sql
SYSTEM REFRESH VIEW rmv_property_snapshot;
SELECT view, status FROM system.view_refreshes WHERE view LIKE 'rmv_%';
```
