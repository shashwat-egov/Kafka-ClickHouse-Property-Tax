# Property Tax Analytics Pipeline

Append-only analytical pipeline using ClickHouse with a **Bronze-Silver-Gold** layered architecture. Silver entity tables are enriched nightly via an Airflow DAG; mart RMVs are refreshed sequentially after silver ingestion completes.

## Architecture

```
Kafka → Ingestion MVs → Raw Tables (Bronze)
                              ↓
                     Airflow DAG (~12:30 AM)
                              ↓
                   ReplacingMergeTree (Silver)
                              ↓
                    Sequential RMV Refresh (Gold)
                              ↓
                        Mart Tables → Dashboards
```

- **Bronze**: Kafka topics are consumed into append-only MergeTree tables storing raw JSON strings. No parsing at this layer.
- **Silver**: An Airflow DAG runs nightly (~12:30 AM), parses the raw JSON, and upserts into ReplacingMergeTree entity tables. Deduplication is handled automatically by ReplacingMergeTree using `last_modified_time` as the version column.
- **Gold**: After silver enrichment completes, Airflow triggers Refreshable Materialized Views (RMVs) **one by one sequentially** — each RMV waits for the previous one to finish before starting. RMVs query silver tables with `FINAL` for guaranteed latest state and write aggregated results into mart tables.

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
-- Property count by ownership/usage category
SELECT * FROM replacing_test.mart_property_agg;

-- Demand/collection/outstanding by financial year
SELECT * FROM replacing_test.mart_demand_value_by_fy;

-- Monthly collections
SELECT * FROM replacing_test.mart_collections_by_month;

-- Defaulters (properties with outstanding balance)
SELECT * FROM replacing_test.mart_defaulters;

-- New properties registered per financial year
SELECT * FROM replacing_test.mart_new_properties_by_fy;

-- Properties with active demand by financial year
SELECT * FROM replacing_test.mart_properties_with_demand_by_fy;
```

## Manual RMV Refresh

RMVs are normally refreshed by the Airflow DAG after silver enrichment. To trigger manually:

```sql
-- Refresh a specific mart
SYSTEM REFRESH VIEW replacing_test.rmv_mart_property_agg;

-- Check refresh status
SELECT view, status FROM system.view_refreshes WHERE view LIKE 'rmv_%';
```

## DDL Files

| File | Layer | Purpose |
|------|-------|---------|
| `updated_ddl/01_kafka.sql` | Bronze | Kafka engine tables (JSONAsString) |
| `updated_ddl/02_raw_tables.sql` | Bronze | Append-only raw JSON storage |
| `updated_ddl/03_ingestion_mvs.sql` | Bronze | Kafka → Raw MVs (pass-through, no parsing) |
| `updated_ddl/04_silver_level_tables.sql` | Silver | ReplacingMergeTree entity tables |
| `updated_ddl/05_mart_tables.sql` | Gold | Mart storage tables |
| `updated_ddl/06_marts.sql` | Gold | Refreshable MV definitions |

## Schema: `replacing_test`

### Silver Entity Tables

| Table | Dedup Key | Purpose |
|-------|-----------|---------|
| `property_address_entity` | `(tenant_id, property_id)` | Denormalized property + address |
| `property_unit_entity` | `(tenant_id, unit_id)` | Property units |
| `property_owner_entity` | `(tenant_id, owner_info_uuid)` | Property owners |
| `demand_with_details_entity` | `(tenant_id, demand_id)` | Denormalized demand + tax breakdowns |

### Gold Mart Tables

| Mart Table | RMV | Purpose |
|------------|-----|---------|
| `mart_property_agg` | `rmv_mart_property_agg` | Property count by tenant/ownership/usage |
| `mart_new_properties_by_fy` | `rmv_mart_new_properties_by_fy` | New properties per financial year |
| `mart_demand_value_by_fy` | `rmv_mart_demand_values_by_fy` | Demand/collection/outstanding by FY |
| `mart_collections_by_month` | `rmv_mart_collections_by_month` | Monthly collection totals |
| `mart_properties_with_demand_by_fy` | `rmv_mart_properties_with_demand_by_fy` | Properties with active demand by FY |
| `mart_defaulters` | `rmv_mart_defaulters` | Properties with outstanding balances |

## Airflow DAG: Nightly Refresh Pipeline

**Schedule**: ~12:30 AM daily

**Steps**:
1. Parse previous day's raw JSON from bronze tables
2. Upsert parsed records into silver ReplacingMergeTree entity tables
3. Trigger mart RMVs sequentially (each waits for the previous to complete):
   1. `rmv_mart_property_agg`
   2. `rmv_mart_new_properties_by_fy`
   3. `rmv_mart_demand_values_by_fy`
   4. `rmv_mart_collections_by_month`
   5. `rmv_mart_properties_with_demand_by_fy`
   6. `rmv_mart_defaulters`

## Constraints

- **NO** UPDATE or DELETE on any table
- **NO** JSON parsing at query time
- **YES** append-only MergeTree for bronze, ReplacingMergeTree for silver
- **YES** FINAL modifier when querying silver tables in mart RMVs

## Requirements

- ClickHouse 24.10+ (RMV support)
- Docker & Docker Compose
- Python 3.8+ (test data generation)
- Apache Airflow (nightly orchestration)
