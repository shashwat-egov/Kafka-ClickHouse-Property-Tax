"""
Collapsing Logic Pseudocode

This module demonstrates the core INSERT vs UPDATE detection
and CollapsingMergeTree row generation logic.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PropertyEvent:
    """Incoming event from Kafka."""
    tenant_id: str
    property_id: str
    version: int
    status: str
    usage_category: str
    ownership_category: str
    created_time: datetime
    last_modified_time: datetime
    # ... other fields


@dataclass
class CollapsingRow:
    """Row to insert into CollapsingMergeTree."""
    tenant_id: str
    property_id: str
    version: int
    sign: int  # +1 for insert/new, -1 for cancel
    status: str
    usage_category: str
    ownership_category: str
    created_time: datetime
    last_modified_time: datetime
    # ... other fields


def is_insert(event: PropertyEvent) -> bool:
    """
    Determine if event represents an INSERT (new record) or UPDATE.

    Rule: If createdTime == lastModifiedTime, it's a new record.
    """
    return event.created_time == event.last_modified_time


def fetch_current_state(
    client,
    tenant_id: str,
    property_id: str
) -> Optional[CollapsingRow]:
    """
    Fetch the current active row for a given key.

    Returns None if no active row exists (new insert).
    """
    result = client.execute(
        """
        SELECT
            tenant_id, property_id, version, status,
            usage_category, ownership_category,
            created_time, last_modified_time
        FROM property_collapsing
        WHERE tenant_id = %(tenant_id)s
          AND property_id = %(property_id)s
          AND sign = 1
        ORDER BY version DESC
        LIMIT 1
        """,
        {"tenant_id": tenant_id, "property_id": property_id}
    )

    if not result:
        return None

    row = result[0]
    return CollapsingRow(
        tenant_id=row[0],
        property_id=row[1],
        version=row[2],
        sign=1,
        status=row[3],
        usage_category=row[4],
        ownership_category=row[5],
        created_time=row[6],
        last_modified_time=row[7],
    )


def generate_collapsing_rows(
    event: PropertyEvent,
    current_state: Optional[CollapsingRow]
) -> list[CollapsingRow]:
    """
    Generate the rows to insert based on event type.

    INSERT (new record):
        - Insert 1 row with sign = +1

    UPDATE (modified record):
        - Insert 1 row with sign = -1 (cancel old)
        - Insert 1 row with sign = +1 (new state)

    Returns list of CollapsingRow to insert.
    """
    rows = []

    if is_insert(event):
        # New record: single +1 row
        rows.append(CollapsingRow(
            tenant_id=event.tenant_id,
            property_id=event.property_id,
            version=event.version,
            sign=+1,
            status=event.status,
            usage_category=event.usage_category,
            ownership_category=event.ownership_category,
            created_time=event.created_time,
            last_modified_time=event.last_modified_time,
        ))

    else:
        # Update: cancel old + insert new
        if current_state is not None:
            # Cancel the old row with EXACT same values but sign = -1
            cancel_row = CollapsingRow(
                tenant_id=current_state.tenant_id,
                property_id=current_state.property_id,
                version=current_state.version,
                sign=-1,  # Cancellation
                status=current_state.status,
                usage_category=current_state.usage_category,
                ownership_category=current_state.ownership_category,
                created_time=current_state.created_time,
                last_modified_time=current_state.last_modified_time,
            )
            rows.append(cancel_row)

        # Insert new state with sign = +1
        rows.append(CollapsingRow(
            tenant_id=event.tenant_id,
            property_id=event.property_id,
            version=event.version,
            sign=+1,
            status=event.status,
            usage_category=event.usage_category,
            ownership_category=event.ownership_category,
            created_time=event.created_time,
            last_modified_time=event.last_modified_time,
        ))

    return rows


def process_batch(client, events: list[PropertyEvent]) -> int:
    """
    Process a batch of events with optimized state fetching.

    1. Identify potential updates (createdTime != lastModifiedTime)
    2. Batch-fetch current state for all update keys
    3. Generate collapsing rows
    4. Batch-insert to ClickHouse
    """
    # Step 1: Separate inserts from potential updates
    insert_events = []
    update_events = []

    for event in events:
        if is_insert(event):
            insert_events.append(event)
        else:
            update_events.append(event)

    # Step 2: Batch-fetch current state for updates
    current_states = {}
    if update_events:
        keys = [(e.tenant_id, e.property_id) for e in update_events]
        # Deduplicate keys (same property might have multiple events)
        unique_keys = list(set(keys))

        # Batch query
        placeholders = ", ".join([f"('{t}', '{p}')" for t, p in unique_keys])
        result = client.execute(f"""
            SELECT tenant_id, property_id, version, status,
                   usage_category, ownership_category,
                   created_time, last_modified_time
            FROM property_collapsing
            WHERE (tenant_id, property_id) IN ({placeholders})
              AND sign = 1
            ORDER BY tenant_id, property_id, version DESC
        """)

        # Keep only latest version per key
        for row in result:
            key = (row[0], row[1])
            if key not in current_states:
                current_states[key] = CollapsingRow(
                    tenant_id=row[0],
                    property_id=row[1],
                    version=row[2],
                    sign=1,
                    status=row[3],
                    usage_category=row[4],
                    ownership_category=row[5],
                    created_time=row[6],
                    last_modified_time=row[7],
                )

    # Step 3: Generate all collapsing rows
    all_rows = []

    for event in insert_events:
        all_rows.extend(generate_collapsing_rows(event, None))

    for event in update_events:
        key = (event.tenant_id, event.property_id)
        current = current_states.get(key)
        rows = generate_collapsing_rows(event, current)
        all_rows.extend(rows)

        # Update current_states for subsequent events in same batch
        # (in case same property has multiple updates in one batch)
        if rows:
            new_state = [r for r in rows if r.sign == +1][0]
            current_states[key] = new_state

    # Step 4: Batch insert
    if all_rows:
        client.execute(
            """
            INSERT INTO property_collapsing
            (tenant_id, property_id, version, sign, status,
             usage_category, ownership_category,
             created_time, last_modified_time)
            VALUES
            """,
            [
                (r.tenant_id, r.property_id, r.version, r.sign, r.status,
                 r.usage_category, r.ownership_category,
                 r.created_time, r.last_modified_time)
                for r in all_rows
            ]
        )

    return len(all_rows)


# ============================================================================
# Example Flow
# ============================================================================
"""
Event 1: New property created
{
    "tenantId": "pb.amritsar",
    "property": {
        "propertyId": "PT-001",
        "status": "ACTIVE",
        "usageCategory": "RESIDENTIAL",
        "version": 1,
        "auditDetails": {
            "createdTime": 1705000000000,
            "lastModifiedTime": 1705000000000  # Same = INSERT
        }
    }
}

Result: Insert 1 row
| tenant_id    | property_id | version | sign | status | usage_category |
|--------------|-------------|---------|------|--------|----------------|
| pb.amritsar  | PT-001      | 1       | +1   | ACTIVE | RESIDENTIAL    |

---

Event 2: Property updated (usage changed)
{
    "tenantId": "pb.amritsar",
    "property": {
        "propertyId": "PT-001",
        "status": "ACTIVE",
        "usageCategory": "COMMERCIAL",  # Changed
        "version": 2,
        "auditDetails": {
            "createdTime": 1705000000000,
            "lastModifiedTime": 1705100000000  # Different = UPDATE
        }
    }
}

Result: Insert 2 rows
| tenant_id    | property_id | version | sign | status | usage_category |
|--------------|-------------|---------|------|--------|----------------|
| pb.amritsar  | PT-001      | 1       | -1   | ACTIVE | RESIDENTIAL    |  <- Cancel old
| pb.amritsar  | PT-001      | 2       | +1   | ACTIVE | COMMERCIAL     |  <- New state

---

After background merge:
| tenant_id    | property_id | version | sign | status | usage_category |
|--------------|-------------|---------|------|--------|----------------|
| pb.amritsar  | PT-001      | 2       | +1   | ACTIVE | COMMERCIAL     |

The v1 rows collapsed (cancelled each other out).
"""
