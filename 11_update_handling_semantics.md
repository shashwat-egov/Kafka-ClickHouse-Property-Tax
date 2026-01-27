# Update Handling Semantics

## Core Philosophy

> **Updates are not mutations. Updates are new facts. Latest fact wins via argMax.**

This pipeline follows an **append-only, event-sourced architecture** where:
- Every change in the source system produces a **new event**
- Events are **immutable facts** that accumulate over time
- The "current state" is always **derived** from the latest event using `argMax`
- No data is ever deleted or modified in place

---

## How Updates Flow Through the System

### 1. Source System (PostgreSQL)

When a property is updated in PostgreSQL:
```
UPDATE eg_pt_property
SET usage_category = 'COMMERCIAL', lastmodifiedtime = 1705123456789
WHERE propertyid = 'PB-PT-2024-001';
```

The source system emits a **complete JSON event** to Kafka:
```json
{
  "tenantId": "pb.amritsar",
  "property": {
    "propertyId": "PB-PT-2024-001",
    "usageCategory": "COMMERCIAL",
    "status": "ACTIVE",
    "auditDetails": {
      "lastModifiedTime": 1705123456789,
      "lastModifiedBy": "user-123"
    },
    "version": 2
  }
}
```

### 2. Raw Layer (Append-Only)

The raw table **accumulates all versions**:

| tenant_id    | property_id      | usage_category | last_modified_time       | version |
|--------------|------------------|----------------|--------------------------|---------|
| pb.amritsar  | PB-PT-2024-001   | RESIDENTIAL    | 2024-01-01 10:00:00.000  | 1       |
| pb.amritsar  | PB-PT-2024-001   | COMMERCIAL     | 2024-01-13 08:30:00.000  | 2       |

**Both rows exist.** Neither is deleted. This is the audit trail.

### 3. Snapshot Layer (Deduplicated View)

During daily refresh, `argMax` selects the latest version:

```sql
SELECT
    tenant_id,
    property_id,
    argMax(usage_category, (last_modified_time, version)) AS usage_category
FROM property_raw
GROUP BY tenant_id, property_id;
```

Result:
| tenant_id    | property_id      | usage_category |
|--------------|------------------|----------------|
| pb.amritsar  | PB-PT-2024-001   | COMMERCIAL     |

**Only the latest state is visible in snapshots.**

---

## Payment Handling (Critical Path)

Payments are the most important update scenario. Here's how they work:

### Payment Flow

1. **Demand Created** (tax bill generated):
   ```json
   {
     "demand": {
       "id": "DM-2024-001",
       "consumerCode": "PB-PT-2024-001",
       "isPaymentCompleted": false,
       "demandDetails": [
         {"taxHeadMasterCode": "PT_TAX", "taxAmount": 5000, "collectionAmount": 0},
         {"taxHeadMasterCode": "PT_LATE_FEE", "taxAmount": 500, "collectionAmount": 0}
       ],
       "auditDetails": {"lastModifiedTime": 1705000000000}
     }
   }
   ```

2. **Partial Payment Made** (₹3000 paid):
   ```json
   {
     "demand": {
       "id": "DM-2024-001",
       "consumerCode": "PB-PT-2024-001",
       "isPaymentCompleted": false,
       "demandDetails": [
         {"taxHeadMasterCode": "PT_TAX", "taxAmount": 5000, "collectionAmount": 3000},
         {"taxHeadMasterCode": "PT_LATE_FEE", "taxAmount": 500, "collectionAmount": 0}
       ],
       "auditDetails": {"lastModifiedTime": 1705100000000}
     }
   }
   ```

3. **Full Payment Made** (remaining ₹2500 paid):
   ```json
   {
     "demand": {
       "id": "DM-2024-001",
       "consumerCode": "PB-PT-2024-001",
       "isPaymentCompleted": true,
       "demandDetails": [
         {"taxHeadMasterCode": "PT_TAX", "taxAmount": 5000, "collectionAmount": 5000},
         {"taxHeadMasterCode": "PT_LATE_FEE", "taxAmount": 500, "collectionAmount": 500}
       ],
       "auditDetails": {"lastModifiedTime": 1705200000000}
     }
   }
   ```

### What the Raw Table Contains

```
demand_detail_raw:
| demand_id   | tax_head_code | tax_amount | collection_amount | last_modified_time      |
|-------------|---------------|------------|-------------------|-------------------------|
| DM-2024-001 | PT_TAX        | 5000       | 0                 | 2024-01-11 00:00:00     |
| DM-2024-001 | PT_LATE_FEE   | 500        | 0                 | 2024-01-11 00:00:00     |
| DM-2024-001 | PT_TAX        | 5000       | 3000              | 2024-01-12 00:00:00     |
| DM-2024-001 | PT_LATE_FEE   | 500        | 0                 | 2024-01-12 00:00:00     |
| DM-2024-001 | PT_TAX        | 5000       | 5000              | 2024-01-13 00:00:00     |
| DM-2024-001 | PT_LATE_FEE   | 500        | 500               | 2024-01-13 00:00:00     |
```

**All 6 rows exist.** This is the complete payment history.

### What the Snapshot Contains

After `argMax` deduplication:
```
demand_detail_snapshot:
| demand_id   | tax_head_code | tax_amount | collection_amount |
|-------------|---------------|------------|-------------------|
| DM-2024-001 | PT_TAX        | 5000       | 5000              |
| DM-2024-001 | PT_LATE_FEE   | 500        | 500               |
```

**Only the final payment state is visible.**

### Outstanding Calculation

```sql
-- Defaulter query uses snapshot (latest state only)
SELECT
    property_id,
    SUM(tax_amount) - SUM(collection_amount) AS outstanding
FROM demand_detail_snapshot
GROUP BY property_id
HAVING outstanding > 0;
```

---

## Dedup Key Selection Rationale

| Entity        | Dedup Key                              | Rationale                                           |
|---------------|----------------------------------------|-----------------------------------------------------|
| Property      | (tenant_id, property_id)               | One property per tenant                             |
| Unit          | (tenant_id, property_id, unit_id)      | Units are scoped to property                        |
| Owner         | (tenant_id, property_id, owner_id)     | Owners are scoped to property                       |
| Address       | (tenant_id, property_id)               | One address per property                            |
| Demand        | (tenant_id, demand_id)                 | Demand ID is globally unique                        |
| DemandDetail  | (tenant_id, demand_id, tax_head_code)  | One entry per tax head per demand                   |

### Why (tenant_id, demand_id, tax_head_code) for DemandDetail?

In the source system, demand details are identified by `id` (UUID). However, for analytical purposes, we care about "the latest amount for this tax head on this demand." Using `tax_head_code` as part of the dedup key ensures:

1. If the same tax head is updated, we get the latest amount
2. If a new tax head is added, it creates a new row
3. If amounts are adjusted, the latest adjustment wins

---

## Why argMax Instead of ReplacingMergeTree?

| Approach              | Pros                                          | Cons                                                |
|-----------------------|-----------------------------------------------|-----------------------------------------------------|
| ReplacingMergeTree    | Automatic dedup in background                 | Non-deterministic timing; requires FINAL for reads |
| argMax                | Deterministic; explicit; no FINAL needed      | Requires GROUP BY at query time                     |

**We chose argMax because:**
1. **Deterministic**: Same query always produces same result
2. **No FINAL**: FINAL is expensive and unpredictable
3. **Explicit**: Business logic is visible in SQL, not hidden in engine
4. **Audit-friendly**: Raw data preserves all historical states

---

## Timeline Consistency Guarantees

### Within a Single Day (Before Snapshot Refresh)

- Raw tables contain all events received
- Queries against raw tables may see multiple versions
- For real-time queries, use argMax inline:

```sql
SELECT
    tenant_id,
    property_id,
    argMax(status, (last_modified_time, version)) AS status
FROM property_raw
WHERE tenant_id = 'pb.amritsar'
GROUP BY tenant_id, property_id;
```

### After Snapshot Refresh (01:00 AM)

- Snapshot tables contain exactly one row per entity
- All marts reflect the state as of refresh time
- Queries are simple (no argMax needed):

```sql
SELECT * FROM property_snapshot WHERE tenant_id = 'pb.amritsar';
```

---

## Edge Cases and Handling

### 1. Out-of-Order Events

If events arrive out of order (e.g., event with timestamp T2 arrives before T1):

```
Raw table after both arrive:
| property_id | status   | last_modified_time |
|-------------|----------|-------------------|
| PB-001      | ACTIVE   | T2 (later)        |
| PB-001      | INACTIVE | T1 (earlier)      |
```

**argMax correctly selects the row with T2** regardless of insertion order.

### 2. Same Timestamp Tie-Breaking

When two events have the same timestamp, use `version` as tie-breaker:

```sql
argMax(status, (last_modified_time, version))
```

If version is also equal, behavior is deterministic but arbitrary (first encountered wins).

### 3. Retroactive Corrections

If a past event needs correction, emit a new event with:
- **New timestamp** (now)
- **Corrected values**

The new event becomes the "latest fact" and will be picked by argMax.

### 4. Logical Deletes

The source system uses status fields (ACTIVE/INACTIVE) rather than hard deletes:

```json
{"property": {"propertyId": "PB-001", "status": "INACTIVE", ...}}
```

Snapshots will reflect `status = 'INACTIVE'`, and marts filter appropriately:

```sql
WHERE status = 'ACTIVE'
```

---

## Idempotency and Re-runnability

The refresh process is designed to be **idempotent**:

1. **TRUNCATE + INSERT**: Each refresh completely rebuilds the snapshot
2. **Same input → Same output**: Re-running refresh produces identical results
3. **Safe to retry**: Failed refresh can be re-run without side effects

```sql
-- This pattern is idempotent
ALTER TABLE property_snapshot DELETE WHERE 1=1;  -- Truncate
INSERT INTO property_snapshot SELECT ... FROM property_raw ...;  -- Rebuild
```

---

## Summary

| Principle                    | Implementation                                        |
|------------------------------|-------------------------------------------------------|
| Updates are new facts        | Append to raw tables, never modify                    |
| Latest fact wins             | argMax(column, (last_modified_time, version))         |
| No mutations in ClickHouse   | No UPDATE, DELETE, ReplacingMergeTree, or FINAL       |
| Deterministic deduplication  | Explicit GROUP BY with argMax                         |
| Full audit trail             | Raw tables preserve all historical states             |
| Clean analytical views       | Snapshot tables contain only latest state             |
