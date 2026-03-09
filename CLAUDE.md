# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Append-only analytical pipeline for Punjab property tax data using ClickHouse with a Bronze-Silver-Gold layered architecture. Silver tables are enriched nightly via Airflow; mart RMVs are refreshed sequentially after silver ingestion completes.

## Quick Start

```bash
cd docker && docker compose up -d
./scripts/deploy.sh
```

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

**Key insight**: Raw tables store unparsed JSON as append-only event log. Silver entity tables use ReplacingMergeTree for automatic deduplication. Mart RMVs read silver tables with FINAL for guaranteed latest state.

## Airflow Orchestration

- **Schedule**: Airflow DAG runs nightly at ~12:30 AM
- **Silver enrichment**: Parses raw JSON and upserts into ReplacingMergeTree entity tables with the previous day's data
- **Mart refresh**: After all silver tables are populated, RMVs are triggered **one by one sequentially** (each waits for the previous to complete before starting)
- **Manual refresh**:
  ```sql
  SYSTEM REFRESH VIEW punjab_property_tax.rmv_mart_<name>;
  SELECT view, status FROM system.view_refreshes WHERE view LIKE 'rmv_%';
  ```

## File Structure

```
├── ddl/
│   ├── 01_kafka.sql              # Kafka engine tables (JSONAsString)
│   ├── 02_raw_tables.sql         # Bronze: append-only raw JSON storage
│   ├── 03_ingestion_mvs.sql      # Kafka → Raw MVs (no parsing)
│   ├── 04_silver_level_tables.sql # Silver: ReplacingMergeTree entities
│   ├── 05_mart_tables.sql        # Gold: mart storage tables
│   └── 06_marts.sql              # Gold: refreshable MV definitions
├── docker/
│   └── docker-compose.yml
├── scripts/
│   ├── deploy.sh                 # Deploy all DDLs
│   └── backfill.sh               # Migrate existing data
├── test-data/
│   └── generate_*.py
└── CLAUDE.md
```

## Schemas

All tables live under the `punjab_property_tax` schema.

## Tables

### Bronze (Raw)

| Table | Engine | Purpose |
|-------|--------|---------|
| `kafka_property_events` | Kafka | Ingest property JSON from Kafka |
| `kafka_demand_events` | Kafka | Ingest demand JSON from Kafka |
| `property_events_raw` | MergeTree | Append-only property event log (raw JSON) |
| `demand_events_raw` | MergeTree | Append-only demand event log (raw JSON) |

### Silver (Entity)

| Table | Engine | Dedup Key | Purpose |
|-------|--------|-----------|---------|
| `property_address_entity` | ReplacingMergeTree | `(tenant_id, property_id)` | Denormalized property + address |
| `property_unit_entity` | ReplacingMergeTree | `(tenant_id, unit_id)` | Property units |
| `property_owner_entity` | ReplacingMergeTree | `(tenant_id, owner_info_uuid)` | Property owners |
| `demand_with_details_entity` | ReplacingMergeTree | `(tenant_id, demand_id)` | Demand + tax breakdowns |
| `payment_with_details_entity` | ReplacingMergeTree | `(tenant_id, payment_id)` | Payment details |
| `bill_entity` | ReplacingMergeTree | `(tenant_id, bill_id)` | Bills |
| `property_assessment_entity` | ReplacingMergeTree | `(tenant_id, assessmentnumber)` | Assessments by FY |
| `property_audit_entity` | MergeTree | `(tenant_id, property_id, audit_created_time)` | Append-only audit trail |

All ReplacingMergeTree tables use `last_modified_time` as the version column. `property_audit_entity` is the exception — it's append-only MergeTree (not deduped) since it tracks change history.

### Gold Marts (punjab_property_tax)

| Table | RMV | Purpose |
|-------|-----|---------|
| `mart_active_property_distribution_summary` | `rmv_mart_active_property_distribution_summary` | Active property count by tenant/type/ownership/usage |
| `mart_new_properties_by_fy` | `rmv_mart_new_properties_by_fy` | New properties per financial year |
| `mart_demand_and_collection_summary` | `rmv_mart_demand_and_collection_summary` | Demand/collection/outstanding by FY |
| `mart_collections_by_month` | `rmv_mart_collections_by_month` | Monthly collection totals |
| `mart_properties_with_demand_by_fy` | `rmv_mart_properties_with_demand_by_fy` | Properties with active demand by FY |
| `mart_defaulters` | `rmv_mart_defaulters` | Properties with outstanding balances |
| `mart_property_change_metrics` | `rmv_property_change_metrics` | Per-property change detection from audit trail |
| `mart_property_risk_summary` | `rmv_property_risk_summary` | Risk scores from aggregated change flags |
| `mart_property_risk_summary_by_fy` | `rmv_property_risk_summary_by_fy` | Risk scores from aggregated change flags by FY |
| `mart_property_demand_vs_assessed_by_fy` | `rmv_property_demand_vs_assessed_by_fy` | Properties with demand vs properties assessed by FY |
| `mart_assessment_summary_by_fy` | `rmv_mart_assessment_summary_by_fy` | Assessment counts by FY/property type/channel, owner vs others |
| `mart_payment_summary_by_fy` | `rmv_mart_payment_summary_by_fy` | Payment metrics by FY/property type/payment mode |
| `mart_rebate_summary_by_fy` | `rmv_mart_rebate_summary_by_fy` | Average rebate size by FY/property type |

**Mart dependencies**:
- `rmv_property_risk_summary` reads from `mart_property_change_metrics` — must refresh after `rmv_property_change_metrics`
- `rmv_property_risk_summary_by_fy` reads from `mart_property_change_metrics` — must refresh after `rmv_property_change_metrics`
- `rmv_property_demand_vs_assessed_by_fy` reads from `mart_properties_with_demand_by_fy` and `property_assessment_entity` — must refresh after `rmv_mart_properties_with_demand_by_fy`

## RMV Refresh

RMVs use `REFRESH EVERY 1000 YEAR` (effectively manual-only). Airflow triggers them sequentially after silver enrichment.

```sql
-- Manual refresh
SYSTEM REFRESH VIEW punjab_property_tax.rmv_mart_active_property_distribution_summary;

-- Check status
SELECT view, status FROM system.view_refreshes WHERE view LIKE 'rmv_%';
```

## RMV Refresh Order (Sequential via Airflow)

After silver enrichment completes, Airflow triggers each RMV one by one:

1. `rmv_mart_active_property_distribution_summary`
2. `rmv_mart_new_properties_by_fy`
3. `rmv_mart_demand_and_collection_summary`
4. `rmv_mart_collections_by_month`
5. `rmv_mart_properties_with_demand_by_fy`
6. `rmv_mart_defaulters`
7. `rmv_property_change_metrics`
8. `rmv_property_risk_summary` ← depends on #7
9. `rmv_property_risk_summary_by_fy` ← depends on #7
10. `rmv_property_demand_vs_assessed_by_fy` ← depends on #5
11. `rmv_mart_assessment_summary_by_fy`
12. `rmv_mart_payment_summary_by_fy`
13. `rmv_mart_rebate_summary_by_fy`

## Constraints

**MUST NOT:**
- Use UPDATE or DELETE on any table
- Parse JSON at query time (parsing happens during Airflow silver enrichment)

**MUST:**
- Keep raw (bronze) tables append-only with raw JSON strings
- Use ReplacingMergeTree with `last_modified_time` as version column for silver tables
- Use FINAL modifier when querying silver tables in mart RMVs
- Refresh mart RMVs sequentially (not in parallel)

## Dedup Keys

| Entity | Key | Version Column |
|--------|-----|----------------|
| Property+Address | `(tenant_id, property_id)` | `last_modified_time` |
| Unit | `(tenant_id, unit_id)` | `last_modified_time` |
| Owner | `(tenant_id, owner_info_uuid)` | `last_modified_time` |
| Demand+Details | `(tenant_id, demand_id)` | `last_modified_time` |
| Payment+Details | `(tenant_id, payment_id)` | `last_modified_time` |
| Bill | `(tenant_id, bill_id)` | `last_modified_time` |
| Assessment | `(tenant_id, assessmentnumber)` | `last_modified_time` |

## Core Pattern

```sql
-- Silver: ReplacingMergeTree deduplicates on ORDER BY key using version column
CREATE TABLE entity_table (...)
ENGINE = ReplacingMergeTree(last_modified_time)
ORDER BY (tenant_id, entity_id);

-- Gold: RMV reads deduplicated state with FINAL, refreshed on schedule
CREATE MATERIALIZED VIEW rmv_mart
REFRESH EVERY 1000 YEAR  -- manual-only refresh
TO mart_table
AS
SELECT ...
FROM entity_table FINAL
WHERE ...
GROUP BY ...;
```
