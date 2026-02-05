# Property Tax Analytics Pipeline

Append-only analytical pipeline using ClickHouse with **streaming aggregation** for memory-efficient processing at 1.5B+ row scale.

## Architecture

```
Kafka → Ingestion MVs → Raw Tables → Streaming MVs → AggregatingMergeTree → Views
           ↓                              ↓
    (JSON parsing)              (argMaxState per key)
```

**Key insight**: Deduplication happens at insert-time, not query-time. Memory: O(unique keys), not O(total rows).

## Quick Start

```bash
# Start infrastructure
cd docker && docker compose up -d

# Deploy DDLs
./scripts/deploy.sh

# Generate test data
cd test-data
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python generate_properties.py
python generate_demands.py
```

## Connecting to ClickHouse

```bash
# Via Docker (no local install)
docker exec -it docker-clickhouse-1 clickhouse-client

# Via localhost
clickhouse-client --host localhost --port 9000

# Via HTTP
curl "http://localhost:8123/?query=SELECT+version()"
```

## Query Examples

```sql
-- Demand by FY (streaming, instant)
SELECT * FROM v_mart_demand_value_by_fy;

-- Collections by month
SELECT * FROM v_mart_collections_by_month;

-- Defaulters
SELECT * FROM v_mart_defaulters LIMIT 100;

-- Property counts (batch RMV)
SELECT * FROM mart_property_count_by_tenant;

-- Refresh property snapshots
SYSTEM REFRESH VIEW rmv_property_snapshot;
```

## DDL Files

| File | Purpose |
|------|---------|
| `ddl/01_kafka.sql` | Kafka engine tables |
| `ddl/02_raw_tables.sql` | Append-only raw tables |
| `ddl/03_ingestion_mvs.sql` | Kafka → Raw (JSON parsing) |
| `ddl/04_property_snapshots.sql` | Property batch RMVs |
| `ddl/05_demand_streaming.sql` | Demand streaming aggregation |
| `ddl/06_marts.sql` | Analytical marts/views |

## Constraints

- **NO** UPDATE, DELETE, FINAL, ReplacingMergeTree
- **YES** append-only MergeTree, argMaxState/argMax pattern

## Requirements

- ClickHouse 24.10+
- Docker & Docker Compose
- Python 3.8+ (test data)
