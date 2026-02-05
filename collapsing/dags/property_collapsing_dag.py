"""
Property Collapsing DAG

Reads raw JSON from ClickHouse staging table, parses events, and writes
to CollapsingMergeTree tables with proper +1/-1 sign logic.

Idempotency: Uses execution_date window + idempotency_key to prevent duplicate processing.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
CLICKHOUSE_HOST = Variable.get("clickhouse_host", default_var="localhost")
CLICKHOUSE_PORT = int(Variable.get("clickhouse_port", default_var="9000"))
CLICKHOUSE_DB = Variable.get("clickhouse_db", default_var="default")

BATCH_SIZE = 10000
WINDOW_MINUTES = 15  # Process 15-minute windows


# ============================================================================
# DAG Definition
# ============================================================================
default_args = {
    "owner": "data-platform",
    "depends_on_past": True,  # Ensure sequential processing
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="property_collapsing_etl",
    default_args=default_args,
    description="Process property events into CollapsingMergeTree",
    schedule_interval=f"*/{WINDOW_MINUTES} * * * *",  # Every 15 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # Prevent parallel runs
    tags=["property-tax", "collapsing", "etl"],
) as dag:

    # ========================================================================
    # Task: Check Idempotency
    # ========================================================================
    @task
    def check_idempotency(execution_date: datetime, **context) -> dict:
        """
        Check if this window was already processed.
        Returns window boundaries and idempotency status.
        """
        window_end = execution_date
        window_start = window_end - timedelta(minutes=WINDOW_MINUTES)

        idempotency_key = hashlib.sha256(
            f"{context['dag'].dag_id}:{execution_date.isoformat()}".encode()
        ).hexdigest()[:32]

        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

        # Check if already processed
        result = client.execute(
            """
            SELECT status, records_processed
            FROM airflow_processing_state FINAL
            WHERE dag_id = %(dag_id)s
              AND task_id = 'process_events'
              AND execution_date = %(execution_date)s
            """,
            {
                "dag_id": context["dag"].dag_id,
                "execution_date": execution_date,
            },
        )

        if result and result[0][0] == "completed":
            logger.info(f"Window already processed: {result[0][1]} records")
            return {
                "already_processed": True,
                "records_processed": result[0][1],
            }

        # Mark as running
        client.execute(
            """
            INSERT INTO airflow_processing_state
            (dag_id, task_id, execution_date, window_start, window_end,
             records_processed, status, started_at, idempotency_key)
            VALUES
            """,
            [
                {
                    "dag_id": context["dag"].dag_id,
                    "task_id": "process_events",
                    "execution_date": execution_date,
                    "window_start": window_start,
                    "window_end": window_end,
                    "records_processed": 0,
                    "status": "running",
                    "started_at": datetime.utcnow(),
                    "idempotency_key": idempotency_key,
                }
            ],
        )

        return {
            "already_processed": False,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "idempotency_key": idempotency_key,
        }

    # ========================================================================
    # Task: Fetch Raw Events
    # ========================================================================
    @task
    def fetch_raw_events(idempotency_result: dict, **context) -> list[dict]:
        """
        Fetch raw JSON events from staging table for the processing window.
        """
        if idempotency_result.get("already_processed"):
            return []

        window_start = datetime.fromisoformat(idempotency_result["window_start"])
        window_end = datetime.fromisoformat(idempotency_result["window_end"])

        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

        # Fetch raw JSON payloads
        rows = client.execute(
            """
            SELECT payload
            FROM kafka_raw_json
            WHERE topic = 'property-events'
              AND _consumed_at >= %(window_start)s
              AND _consumed_at < %(window_end)s
            ORDER BY _consumed_at
            """,
            {"window_start": window_start, "window_end": window_end},
        )

        events = []
        for (payload,) in rows:
            try:
                events.append(json.loads(payload))
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
                continue

        logger.info(f"Fetched {len(events)} events for window {window_start} - {window_end}")
        return events

    # ========================================================================
    # Task: Process Events with Collapsing Logic
    # ========================================================================
    @task
    def process_events(
        events: list[dict], idempotency_result: dict, **context
    ) -> dict:
        """
        Parse events and determine INSERT vs UPDATE operations.
        For UPDATEs, fetch current state and generate cancellation rows.
        """
        if idempotency_result.get("already_processed") or not events:
            return {"properties": [], "stats": {"inserts": 0, "updates": 0}}

        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

        # Collect all property_ids that might be updates
        potential_update_keys = set()
        for event in events:
            prop = event.get("property", {})
            created = prop.get("auditDetails", {}).get("createdTime", 0)
            modified = prop.get("auditDetails", {}).get("lastModifiedTime", 0)
            if created != modified:
                tenant_id = event.get("tenantId", "")
                property_id = prop.get("propertyId", "")
                potential_update_keys.add((tenant_id, property_id))

        # Fetch current state for potential updates (batch)
        current_state = {}
        if potential_update_keys:
            keys_list = list(potential_update_keys)
            # Build query to fetch latest version of each property
            placeholders = ", ".join(
                [f"('{t}', '{p}')" for t, p in keys_list]
            )
            query = f"""
                SELECT
                    tenant_id, property_id, version,
                    id, survey_id, account_id, old_property_id,
                    property_type, usage_category, ownership_category, status,
                    acknowledgement_number, creation_reason, no_of_floors,
                    source, channel, land_area, super_built_up_area,
                    created_by, created_time, last_modified_by, last_modified_time
                FROM property_collapsing
                WHERE (tenant_id, property_id) IN ({placeholders})
                  AND sign = 1
                ORDER BY tenant_id, property_id, version DESC
            """
            rows = client.execute(query)

            # Keep only the latest version per key
            for row in rows:
                key = (row[0], row[1])
                if key not in current_state:
                    current_state[key] = {
                        "tenant_id": row[0],
                        "property_id": row[1],
                        "version": row[2],
                        "id": row[3],
                        "survey_id": row[4],
                        "account_id": row[5],
                        "old_property_id": row[6],
                        "property_type": row[7],
                        "usage_category": row[8],
                        "ownership_category": row[9],
                        "status": row[10],
                        "acknowledgement_number": row[11],
                        "creation_reason": row[12],
                        "no_of_floors": row[13],
                        "source": row[14],
                        "channel": row[15],
                        "land_area": row[16],
                        "super_built_up_area": row[17],
                        "created_by": row[18],
                        "created_time": row[19],
                        "last_modified_by": row[20],
                        "last_modified_time": row[21],
                    }

        # Process events and generate collapsing rows
        rows_to_insert = []
        stats = {"inserts": 0, "updates": 0}

        for event in events:
            prop = event.get("property", {})
            tenant_id = event.get("tenantId", "")
            property_id = prop.get("propertyId", "")
            audit = prop.get("auditDetails", {})

            created_time = audit.get("createdTime", 0)
            modified_time = audit.get("lastModifiedTime", 0)
            version = prop.get("version", 1)

            # Parse the new row data
            new_row = {
                "tenant_id": tenant_id,
                "property_id": property_id,
                "version": version,
                "id": prop.get("id", ""),
                "survey_id": prop.get("surveyId", ""),
                "account_id": prop.get("accountId", ""),
                "old_property_id": prop.get("oldPropertyId", ""),
                "property_type": prop.get("propertyType", ""),
                "usage_category": prop.get("usageCategory", ""),
                "ownership_category": prop.get("ownershipCategory", ""),
                "status": prop.get("status", ""),
                "acknowledgement_number": prop.get("acknowldgementNumber", ""),
                "creation_reason": prop.get("creationReason", ""),
                "no_of_floors": prop.get("noOfFloors", 0),
                "source": prop.get("source", ""),
                "channel": prop.get("channel", ""),
                "land_area": float(prop.get("landArea", 0) or 0),
                "super_built_up_area": float(prop.get("superBuiltUpArea", 0) or 0),
                "created_by": audit.get("createdBy", ""),
                "created_time": datetime.fromtimestamp(created_time / 1000) if created_time else None,
                "last_modified_by": audit.get("lastModifiedBy", ""),
                "last_modified_time": datetime.fromtimestamp(modified_time / 1000) if modified_time else None,
            }

            is_update = created_time != modified_time
            key = (tenant_id, property_id)

            if is_update and key in current_state:
                # UPDATE: Insert cancellation row (-1) then new row (+1)
                old_row = current_state[key].copy()
                old_row["sign"] = -1
                rows_to_insert.append(old_row)

                new_row["sign"] = 1
                rows_to_insert.append(new_row)
                stats["updates"] += 1

                # Update current_state for potential subsequent events
                current_state[key] = new_row
            else:
                # INSERT: Just insert with sign = +1
                new_row["sign"] = 1
                rows_to_insert.append(new_row)
                stats["inserts"] += 1

                # Track for potential subsequent updates in same batch
                current_state[key] = new_row

        return {"properties": rows_to_insert, "stats": stats}

    # ========================================================================
    # Task: Batch Insert to ClickHouse
    # ========================================================================
    @task
    def batch_insert(processed: dict, idempotency_result: dict, **context) -> int:
        """
        Batch insert processed rows into CollapsingMergeTree table.
        """
        if idempotency_result.get("already_processed"):
            return idempotency_result.get("records_processed", 0)

        rows = processed.get("properties", [])
        if not rows:
            return 0

        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

        # Insert in batches
        total_inserted = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]

            client.execute(
                """
                INSERT INTO property_collapsing
                (tenant_id, property_id, sign, version, created_time, last_modified_time,
                 id, survey_id, account_id, old_property_id, property_type, usage_category,
                 ownership_category, status, acknowledgement_number, creation_reason,
                 no_of_floors, source, channel, land_area, super_built_up_area,
                 created_by, last_modified_by)
                VALUES
                """,
                [
                    (
                        r["tenant_id"], r["property_id"], r["sign"], r["version"],
                        r["created_time"], r["last_modified_time"],
                        r["id"], r["survey_id"], r["account_id"], r["old_property_id"],
                        r["property_type"], r["usage_category"], r["ownership_category"],
                        r["status"], r["acknowledgement_number"], r["creation_reason"],
                        r["no_of_floors"], r["source"], r["channel"],
                        r["land_area"], r["super_built_up_area"],
                        r["created_by"], r["last_modified_by"],
                    )
                    for r in batch
                ],
            )
            total_inserted += len(batch)

        stats = processed.get("stats", {})
        logger.info(
            f"Inserted {total_inserted} rows "
            f"(inserts: {stats.get('inserts', 0)}, updates: {stats.get('updates', 0)})"
        )

        return total_inserted

    # ========================================================================
    # Task: Update Processing State
    # ========================================================================
    @task
    def update_state(
        records_inserted: int, idempotency_result: dict, **context
    ) -> None:
        """
        Mark processing window as completed.
        """
        if idempotency_result.get("already_processed"):
            return

        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

        client.execute(
            """
            INSERT INTO airflow_processing_state
            (dag_id, task_id, execution_date, window_start, window_end,
             records_processed, status, started_at, completed_at, idempotency_key)
            VALUES
            """,
            [
                {
                    "dag_id": context["dag"].dag_id,
                    "task_id": "process_events",
                    "execution_date": context["execution_date"],
                    "window_start": datetime.fromisoformat(idempotency_result["window_start"]),
                    "window_end": datetime.fromisoformat(idempotency_result["window_end"]),
                    "records_processed": records_inserted,
                    "status": "completed",
                    "started_at": datetime.utcnow(),
                    "completed_at": datetime.utcnow(),
                    "idempotency_key": idempotency_result["idempotency_key"],
                }
            ],
        )

    # ========================================================================
    # Task: Trigger Mart Refresh
    # ========================================================================
    @task
    def trigger_mart_refresh(records_inserted: int, **context) -> None:
        """
        Trigger downstream mart refresh queries.
        Only runs if records were actually inserted.
        """
        if records_inserted == 0:
            logger.info("No records inserted, skipping mart refresh")
            return

        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

        # Refresh marts using collapsing-aware queries
        mart_queries = [
            # Property count by tenant (using sum(sign) for correct counts)
            """
            INSERT INTO mart_property_count_by_tenant
            SELECT
                today() AS snapshot_date,
                tenant_id,
                sum(sign) AS property_count
            FROM property_collapsing
            WHERE status = 'ACTIVE'
            GROUP BY tenant_id
            HAVING property_count > 0
            """,
            # Add more mart refresh queries as needed
        ]

        for query in mart_queries:
            try:
                client.execute(query)
                logger.info(f"Executed mart refresh query")
            except Exception as e:
                logger.error(f"Mart refresh failed: {e}")
                raise

    # ========================================================================
    # DAG Flow
    # ========================================================================
    idempotency_check = check_idempotency()
    raw_events = fetch_raw_events(idempotency_check)
    processed = process_events(raw_events, idempotency_check)
    inserted_count = batch_insert(processed, idempotency_check)
    update_state(inserted_count, idempotency_check)
    trigger_mart_refresh(inserted_count)
