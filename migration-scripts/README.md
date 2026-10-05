# Punjab Property Tax — PostgreSQL → ClickHouse Migration Scripts

Toolkit to move Punjab Property Tax (PT) data out of the production/replica PostgreSQL
database and into ClickHouse analytics tables, going through local CSV files and a local
PostgreSQL instance as intermediate hops.

## Why the extra hops?

Reading directly from the production replica for a full migration is slow and risky
(long-running queries, replica lag, serialization failures, dropped connections). So the
pipeline is split:

| Hop | Purpose |
|-----|---------|
| Remote PG → local CSV | Pull data once, in small resumable chunks, with minimal load on the replica. |
| CSV row counts | Prove nothing was lost during extraction before moving on. |
| CSV → local PG | Get primary keys and indexes back, so the Go migrator can use fast keyset pagination. |
| Local PG → ClickHouse | Actual migration/transformation into the analytics schema. |

---

## Execution sequence

```
STEP 1  extract_data_dump_from_postgres/extract_from_db_all_tables.py       (per table)
        extract_data_dump_from_postgres/extract_ids_from_bill_for_billdetail.py
        extract_data_dump_from_postgres/extract_from_db_billdetail.py       (needs step 1b output)
                     │
                     ▼  CSV chunks on local disk
STEP 2  count_csv_data_rows.py                       → validate counts vs PostgreSQL
                     │
                     ▼
STEP 3  load_to_local_postgres.py                    → CSV → local PostgreSQL (COPY)
                     │
                     ▼
STEP 4  punjab_migration_clickhouse_script.go        → local PostgreSQL → ClickHouse
```

---

## Step 1 — Extract from PostgreSQL into local CSV chunks

Folder: `extract_data_dump_from_postgres/`

### 1a. `extract_from_db_all_tables.py` — all regular tables

Exports one table at a time into `output/output_<n>.csv` files.

- Set `TABLE_NAME` to the table you want. Tables covered by this script:
  `eg_pt_property`, `eg_pt_address`, `eg_pt_unit`, `eg_pt_owner`,
  `egbs_demand_v1`, `egbs_demanddetail_v1`, `eg_pt_asmt_assessment`,
  `egcl_payment`, `egcl_bill`, `eg_pt_property_audit`
- **Pagination:** keyset on `id` (`WHERE id > <last_id> ORDER BY id LIMIT CHUNK_SIZE`) —
  no `OFFSET`, so page cost stays flat.
- **Window:** rows where `createdtime` **or** `lastmodifiedtime` falls between
  `CREATEDTIME_START` and `CREATEDTIME_LIMIT` (epoch millis). `lastmodifiedtime` is
  included so records created earlier but updated inside the window are also picked up.
- **Tenant filter:** `tenantid != TENANT_ID` — i.e. it *excludes* `pb.testing`
  (the test tenant) and takes every real tenant.
- **Resumable:** writes `checkpoint.json` (`last_id`, `chunk_index`) after every chunk.
  Kill it and re-run — it continues from the last committed chunk.
- **Resilient:** up to 25 retries on `SerializationFailure` / `OperationalError`, with
  full reconnect and TCP keepalives, because replica reads get cancelled often.

Run once per table, moving the `output/` folder and deleting `checkpoint.json` between
tables (the checkpoint is not table-aware).

### 1b. `extract_ids_from_bill_for_billdetail.py` — bill IDs first

`egcl_billdetial` has **no `createdtime` / `lastmodifiedtime` columns**, so it cannot be
sliced by the time window like the other tables. Instead we drive it from its parent:

- Reads `egcl_bill` for `businessservice = 'PT'` within `START_TIME`–`END_TIME`.
- **Pagination:** composite keyset `(createdtime, id) > (last_createdtime, last_id)` —
  needed because `createdtime` alone is not unique.
- Writes **only the `id` column** to `output_bill_ids/output_<n>.csv` (50k IDs per file).
- Resumable via `checkpoint_bill_ids.json` (also tracks `total_count`).

### 1c. `extract_from_db_billdetail.py` — bill details by ID

Consumes the ID files from 1b and fetches the matching detail rows.

- Reads every CSV in `output_bill_ids/`, batches IDs 1000 at a time
  (`BATCH_SIZE`), and runs `WHERE billid = ANY(<batch>)`.
- Writes `bill_detail_output/bill_detail_<file_index>_<batch_count>.csv`.
- Replica-safety measures: `statement_timeout = 300000` (5 min) per query,
  `time.sleep(0.2)` between batches, 5 retries with reconnect, and a batch is
  skipped (logged) rather than aborting the whole run if it keeps failing.

> Note: the table name `egcl_billdetial` is misspelled in the eGov schema itself.
> That spelling is intentional here — do not "fix" it.

**Before running any of these:** fill in `DB_HOST`, `DB_NAME`, `DB_USER`,
`DB_PASSWORD`, `DB_PORT`. They are intentionally left blank in the repo so credentials
are never committed.

---

## Step 2 — Validate CSV row counts

File: `count_csv_data_rows.py`

Counts data rows (header excluded) across every CSV in a folder and prints per-file plus
a grand total. Compare that grand total against the equivalent `SELECT count(*)` on the
source PostgreSQL table to confirm the extraction was complete.

- Set `BASE_FOLDER` to the folder you want to count.
- `PATTERN` defaults to `output_\d+\.csv`. For the bill-detail output you must change it
  to match `bill_detail_*.csv`, otherwise it will report 0 rows.

Do this before Step 3 — it is much cheaper to re-extract than to discover a gap after
loading ClickHouse.

---

## Step 3 — Load CSVs into local PostgreSQL

File: `load_to_local_postgres.py`

Bulk-loads the extracted CSVs into a local PostgreSQL (default `localhost:5435/testdb`)
using `COPY ... FROM STDIN`, which is far faster than row-by-row inserts.

Why a local PostgreSQL at all: the Go migrator relies on keyset pagination over an
indexed primary key. Local PG gives us that plus zero network latency and no replica-lag
risk, so the ClickHouse migration can run at full speed and be retried freely.

Behaviour:
- Walks `CSV_ROOT` recursively, processes files in sorted order.
- One transaction per file: commit on success, rollback on failure.
- **Resumable:** completed file paths are appended to `address_import_progress.log` and
  skipped on re-run. Failures are appended to `address_import_errors.log` and the script
  **stops** so you can fix the cause and resume.
- Re-maps CSV columns explicitly into the target column list, so column order in the CSV
  does not have to match the table.
- Empty strings are loaded as `NULL` (`NULL ''`).

⚠️ As checked in, this script is configured for **`eg_pt_address`** only
(`TABLE_NAME`, the `row.get(...)` list, and the `COPY` column list). To load another
table, change all three to that table's columns — plus `CSV_ROOT`, `PROGRESS_FILE` and
`ERROR_FILE` so progress logs don't collide across tables.

---

## Step 4 — Migrate local PostgreSQL → ClickHouse

File: `punjab_migration_clickhouse_script.go`

A single Go binary that migrates all PT tables from local PostgreSQL into the ClickHouse
analytics schema, transforming the row shape on the way.

### How it works

**Fan-out at two levels.** All requested tables migrate concurrently (one goroutine per
table). Within a table, `parallelByTenant` discovers the distinct `tenantid` values and
distributes them across `-workers` goroutines. Tenants are sorted **largest-first** so
the biggest tenant starts earliest and doesn't become a long tail at the end.

**Keyset pagination per tenant — implemented in `migrateForTenant`.** See
[Keyset pagination](#keyset-pagination-migratefortenant) below for the full detail.

**Prefetch pipeline.** While the current batch is being sent to ClickHouse, the next
PostgreSQL page is already being fetched in a background goroutine — so read and write
overlap instead of alternating.

**Direct scan into the CH batch.** `fetchPage` scans PostgreSQL rows straight into a
prepared ClickHouse batch via a `processRow` callback, so rows are never materialised
into an intermediate slice.

**Resume mode (`-resume`).** For each table, `getCompletedTenants` compares per-tenant
row counts in PostgreSQL against ClickHouse and skips every tenant where
`CH count >= PG count`. Safe to re-run after a partial or failed run.

### Keyset pagination (`migrateForTenant`)

All pagination is keyset (seek) pagination — there is no `OFFSET` anywhere in the file.
It lives in one function, `migrateForTenant` (`punjab_migration_clickhouse_script.go:278`),
and every table inherits it via `parallelByTenant`. Each page runs
`WHERE <key> > '<last key>' ORDER BY <key> LIMIT <batch-size>`, so page cost stays flat
instead of growing the way `OFFSET` does. This is why Step 3 loads into a local
PostgreSQL first — the pager needs an indexed primary key to seek on.

Keyset column per table: `p.id` (property_address, payment_with_details), `u.id`
(property_unit), `o.ownerinfouuid` (property_owner), `id` (assessment, bill,
`_stg_demand`), `bd.id` (bill_detail), `dd.id` (`_stg_demanddetail`), `audituuid`
(property_audit). All are unique — a non-unique key would silently skip or duplicate
rows tied across a page boundary.

Two things to know when editing a `pgQuery` template:

- It must end at its `WHERE … tenantid = $1` clause and carry **no `ORDER BY` of its
  own** — the pager appends its own, and two `ORDER BY` clauses is invalid SQL.
  (`bill_detail` had this bug; fixed.)
- Demand Phase 3 is not paginated — the pivot is one `INSERT … SELECT` inside ClickHouse.

### Transformations applied

- Epoch millis → `DateTime64` via `msToUTC` / `msToUTCVal`. **These do not convert
  anything** — `time.UnixMilli(ms).UTC()` only sets the `time.Time`'s display location,
  so the epoch reaches ClickHouse byte-identical to what Postgres held. (The functions
  were originally named `msToIST`/`msToISTVal`, which wrongly implied a UTC→IST shift;
  renamed for clarity.) Whether ClickHouse *displays* these in UTC or IST is decided by
  the target column's declared timezone in the CH DDL, not by this script.
- `financial_year` derived from a timestamp using an **April–March** Indian FY boundary
  (`computeFinancialYear`, evaluated in `Asia/Kolkata`), formatted `2024-25`. For bill
  details it falls back `fromperiod` → `toperiod` → `"UNKNOWN"`.
- Money fields converted to `decimal.Decimal` for exact `Decimal` columns in ClickHouse.
- `additionaldetails` JSON is validated with `json.Valid` and replaced with `{}` if
  malformed.
- Property audit rows are unpacked from the `property` JSONB blob — including summing
  each unit's `builtUpArea` and counting `owners[].uuid`.

### Table mapping

| `-tables` value | PostgreSQL source | ClickHouse target |
|---|---|---|
| `property_address` | `eg_pt_property` (+ address) | `property_address_entity` |
| `property_unit` | `eg_pt_unit` | `property_unit_entity` |
| `property_owner` | `eg_pt_owner` | `property_owner_entity` |
| `assessment` | `eg_pt_asmt_assessment` | `property_assessment_entity` |
| `payment_with_details` | `egcl_payment` ⋈ `egcl_paymentdetail` | `payment_with_details_entity` |
| `bill` | `egcl_bill` | `bill_entity_v1` |
| `bill_detail` | `egcl_billdetial` ⟕ `egcl_bill` | `bill_detail_entity_v1` |
| `property_audit` | `eg_pt_property_audit` (JSONB) | `property_audit_entity_v2` |
| `demand_details` | `egbs_demand_v1` + `egbs_demanddetail_v1` | `demand_with_details_entity` |

### The demand table is special (3 phases)

Demand details run into the billions of rows, and a `JOIN` + `GROUP BY` in PostgreSQL at
that scale is not viable. So `migrateDemandWithDetails` does the aggregation in
ClickHouse instead:

1. **Phase 1** — stream raw `egbs_demand_v1` (`businessservice = 'PT'`) into the
   ClickHouse staging table `_stg_demand`.
2. **Phase 2** — stream raw `egbs_demanddetail_v1` into `_stg_demanddetail`
   (restricted to demand IDs belonging to PT).
3. **Phase 3** — `pivotDemandInClickHouse` runs one big `INSERT … SELECT` that joins the
   two staging tables and pivots each `taxheadcode` into its own column
   (`pt_tax`, `pt_fire_cess`, `pt_time_rebate`, … plus the matching `*_collection`
   columns), computes `outstanding_amount`, `is_paid` and `financial_year`, and writes
   `demand_with_details_entity`. Uses `join_algorithm = 'full_sorting_merge'` and
   external `GROUP BY` with a 1-hour timeout.

Staging tables are created with `CREATE TABLE IF NOT EXISTS`. `dropDemandStagingTables`
exists but the deferred drop is currently commented out, so staging data survives the
run and can be reused / inspected — drop it manually when you're done.

### Running it

```bash
go mod init punjab-migration
go get github.com/ClickHouse/clickhouse-go/v2 github.com/jackc/pgx/v5/pgxpool github.com/shopspring/decimal

go run punjab_migration_clickhouse_script.go \
  -pg-host localhost -pg-port 5435 -pg-db testdb \
  -pg-user postgres -pg-password postgres \
  -ch-host <clickhouse-host> -ch-port 9440 -ch-db punjab_data_test \
  -ch-user default -ch-password <secret> -ch-secure \
  -tables property_address,property_unit \
  -batch-size 100000 -workers 8
```

Key flags:

| Flag | Default | Meaning |
|---|---|---|
| `-tables` | `all` | Comma-separated table list (see mapping above) or `all`. |
| `-tenant` | *(empty)* | Migrate a single tenant, e.g. `pb.amritsar`. Empty = all tenants. |
| `-resume` | `false` | Skip tenants whose CH count already matches PG. |
| `-batch-size` | `100000` | Rows per keyset page (`LIMIT`) / CH insert batch. |
| `-workers` | `8` | Parallel tenants per table. |
| `-ch-protocol` | `auto` | `auto` picks native for ports 9000/9440, else HTTP. |
| `-ch-secure` | `true` | TLS to ClickHouse. |

Progress is logged every 10 seconds (total rows, rows/sec, elapsed), and a per-table
summary with row counts, durations and status is printed at the end. The process exits
non-zero if any table failed.

### Known issues in the checked-in Go file

- `flag.IntVar(&chPort, "ch-port", , ...)` is missing its default value — the ClickHouse
  port/credentials were stripped before committing. **This will not compile until you
  supply a default** (e.g. `9440`). Verified with `gofmt -e`: this is the only parse
  error in the file, and everything else it reports cascades from this one line.
- The filename has a **trailing space** (`punjab_migration_clickhouse_script.go `).
  Rename it before building.
- `dropDemandStagingTables` is defined but its deferred call is commented out, so
  `_stg_demand` / `_stg_demanddetail` persist after a run — drop them manually.

Fixed: the `bill_detail` `pgQuery` template used to end with `ORDER BY bd.id`, which
collided with the `ORDER BY` that `buildQuery` appends and made every bill-detail page
invalid SQL. The template now ends at its `WHERE` clause, like all the others.

---

## Prerequisites

- Python 3 with `psycopg2` (`pip install psycopg2-binary`)
- Go 1.21+
- A local PostgreSQL with the PT schema (tables must already exist — the loader only
  `COPY`s data, it does not create tables)
- A ClickHouse instance with the `*_entity` target tables already created

## Credentials

Every script has its DB credentials blank or set to local defaults on purpose. Fill them
in locally and **do not commit real values.**
