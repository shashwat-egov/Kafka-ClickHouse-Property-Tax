# CollapsingMergeTree Implementation Guide

## Architecture Overview

```
Kafka → Kafka Engine (JSONAsString) → MV → Raw JSON Staging
                                              ↓
                                        Airflow DAG
                                              ↓
                                   CollapsingMergeTree Tables
                                              ↓
                                         Mart Queries
```

## Key Differences from Append-Only + argMax

| Aspect | Append-Only + argMax | CollapsingMergeTree |
|--------|---------------------|---------------------|
| Storage | All versions stored | Collapsed during merges |
| Query Pattern | `argMax(col, (lmt, ver))` | `sum(sign)` or `FINAL` |
| Update Handling | Implicit (latest wins) | Explicit (+1/-1 rows) |
| Query Complexity | Complex GROUP BY | Simple with FINAL, careful with aggregates |
| Storage Efficiency | Grows unbounded | Collapses over time |
| Audit Trail | Full history in raw | Lost after merge |

## The UPDATE Problem

### The Challenge

To cancel a row in CollapsingMergeTree, you need to insert a row with:
1. **Same ORDER BY key values**
2. **Same non-key column values** (for correct aggregation before merge)
3. **sign = -1**

### Why This Matters

```
Before merge:
| property_id | status   | sign |
|-------------|----------|------|
| PT-001      | ACTIVE   | +1   |  <- Original
| PT-001      | INACTIVE | -1   |  <- Wrong cancellation!
| PT-001      | INACTIVE | +1   |  <- New value

Query: SELECT property_id, sum(sign) as count
Result: PT-001 has count = 1 ✓

Query: SELECT property_id, argMax(status, sign)
Result: INACTIVE (because +1 row has INACTIVE)

But what about:
Query: SELECT status, sum(sign) as count GROUP BY status
Result: ACTIVE = 0, INACTIVE = 1  <- WRONG! Should be 0, 1 but only if ACTIVE was properly cancelled
```

### Solution: Always Fetch Current State

The Airflow DAG fetches the current row before generating the cancellation:

```python
# Fetch current state for this key
current = client.execute(
    "SELECT * FROM property_collapsing WHERE tenant_id=? AND property_id=? AND sign=1 ORDER BY version DESC LIMIT 1",
    [tenant_id, property_id]
)

if current:
    # Insert exact copy with sign = -1
    old_row = dict(current[0])
    old_row['sign'] = -1
    insert(old_row)

# Insert new row with sign = +1
new_row['sign'] = +1
insert(new_row)
```

## Partitioning Strategy

### Recommended: By Month

```sql
PARTITION BY toYYYYMM(last_modified_time)
```

**Why:**
- Most queries filter by time range
- Partition pruning reduces scan scope
- Old partitions can be dropped for retention

### ORDER BY Considerations

```sql
ORDER BY (tenant_id, property_id, version)
```

**Include version** because:
- Allows correct ordering during merges
- Helps with point lookups
- Prevents ambiguity when multiple events have same timestamp

## Query Performance

### FINAL: When to Use

✅ **Use FINAL for:**
- Point lookups (single row by key)
- Small result sets (< 10K rows)
- Debugging/verification queries

❌ **Avoid FINAL for:**
- Large analytical scans
- Aggregations across many rows
- Production dashboards

### sum(sign): Required Pattern

```sql
-- Correct: Aggregation with sign
SELECT tenant_id, sum(sign) AS property_count
FROM property_collapsing
WHERE status = 'ACTIVE'
GROUP BY tenant_id
HAVING property_count > 0;

-- Correct: Sum with sign multiplication
SELECT tenant_id, sum(tax_amount * sign) AS total_tax
FROM demand_detail_collapsing
GROUP BY tenant_id;
```

### Pre-aggregated Marts

For dashboard queries, pre-compute aggregations:

```sql
-- Run daily by Airflow
INSERT INTO mart_property_counts
SELECT
    today() AS snapshot_date,
    tenant_id,
    sum(sign) AS count
FROM property_collapsing
WHERE status = 'ACTIVE'
GROUP BY tenant_id
HAVING count > 0;
```

## Idempotency in Airflow

### The Problem

If Airflow re-runs a task:
1. Same events get processed again
2. Duplicate +1 rows inserted
3. Counts become incorrect

### Solution: Processing State Table

```sql
CREATE TABLE airflow_processing_state (
    dag_id String,
    task_id String,
    execution_date DateTime64(3),
    window_start DateTime64(3),
    window_end DateTime64(3),
    status Enum8('running'=1, 'completed'=2, 'failed'=3),
    records_processed UInt64,
    idempotency_key String
) ENGINE = ReplacingMergeTree(execution_date)
ORDER BY (dag_id, task_id, execution_date);
```

### Idempotency Check Pattern

```python
# At start of task
result = client.execute(
    "SELECT status FROM airflow_processing_state FINAL WHERE dag_id=? AND execution_date=?",
    [dag_id, execution_date]
)

if result and result[0][0] == 'completed':
    logger.info("Already processed, skipping")
    return

# Process...

# At end, mark complete
client.execute(
    "INSERT INTO airflow_processing_state VALUES (..., 'completed', ...)"
)
```

## Pitfalls to Avoid

### 1. Forgetting to Multiply by Sign

```sql
-- WRONG
SELECT sum(tax_amount) FROM demand_detail_collapsing;

-- CORRECT
SELECT sum(tax_amount * sign) FROM demand_detail_collapsing;
```

### 2. Using count() Instead of sum(sign)

```sql
-- WRONG: Counts all rows including cancelled
SELECT count(*) FROM property_collapsing WHERE status = 'ACTIVE';

-- CORRECT
SELECT sum(sign) FROM property_collapsing WHERE status = 'ACTIVE';
```

### 3. Incorrect Cancellation Row

```sql
-- WRONG: Only key columns, wrong values for others
INSERT INTO property_collapsing (tenant_id, property_id, sign, status)
VALUES ('pb.amritsar', 'PT-001', -1, 'UNKNOWN');

-- CORRECT: Exact copy of the row being cancelled
INSERT INTO property_collapsing (tenant_id, property_id, sign, status, usage_category, ...)
SELECT tenant_id, property_id, -1, status, usage_category, ...
FROM property_collapsing
WHERE tenant_id = 'pb.amritsar' AND property_id = 'PT-001' AND sign = 1
ORDER BY version DESC LIMIT 1;
```

### 4. Not Handling Out-of-Order Events

If event with version 3 arrives before version 2:
- You might cancel version 3 and insert version 2
- Result: older data becomes "current"

**Solution**: Always check version before deciding to update:

```python
current_version = fetch_current_version(key)
if new_version > current_version:
    # Proceed with update
else:
    # Skip or log warning
```

### 5. Relying on Immediate Collapse

Background merges are **asynchronous**. Queries immediately after insert may show uncollapsed data.

**Solution**: Always use `sum(sign)` pattern, never assume rows are collapsed.

### 6. Using FINAL in Production Dashboards

```sql
-- SLOW: Forces merge at query time
SELECT * FROM property_collapsing FINAL WHERE ...;

-- FAST: Pre-computed mart
SELECT * FROM mart_property_snapshot WHERE snapshot_date = today();
```

## Alternative: VersionedCollapsingMergeTree

For simpler CDC patterns, consider `VersionedCollapsingMergeTree`:

```sql
CREATE TABLE property_versioned (
    tenant_id String,
    property_id String,
    version UInt32,
    sign Int8,
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (tenant_id, property_id);
```

**Advantages:**
- Automatically handles version ordering
- Less strict about cancellation row values
- Better for true CDC streams (Debezium, etc.)

## Monitoring Queries

```sql
-- Check uncollapsed row ratio
SELECT
    table,
    sum(rows) as total_rows,
    count() as parts,
    sum(rows) / count() as avg_rows_per_part
FROM system.parts
WHERE table LIKE '%collapsing%' AND active
GROUP BY table;

-- Check for orphaned cancellation rows
SELECT
    tenant_id,
    property_id,
    sum(sign) as net_sign
FROM property_collapsing
GROUP BY tenant_id, property_id
HAVING net_sign < 0;  -- Should never happen

-- Force merge for testing (NOT for production)
OPTIMIZE TABLE property_collapsing FINAL;
```

## Migration Checklist

- [ ] Create CollapsingMergeTree tables with sign column
- [ ] Create raw JSON staging table
- [ ] Update Kafka MV to store raw JSON (no parsing)
- [ ] Deploy Airflow DAG with idempotency
- [ ] Create processing state table
- [ ] Update all mart queries to use sum(sign) pattern
- [ ] Remove old MV JSON parsing logic
- [ ] Set up monitoring for orphaned rows
- [ ] Test with out-of-order events
- [ ] Load test with expected throughput
