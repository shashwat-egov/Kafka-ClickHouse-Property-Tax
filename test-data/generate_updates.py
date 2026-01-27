#!/usr/bin/env python3
"""
Generate update events to demonstrate deduplication.

This script creates:
- 10 property updates: RESIDENTIAL -> COMMERCIAL (version 2)
- 20 partial payments: 50% collection_amount (version 2)
- 10 full payments: 100% collection_amount (version 3)

These updates demonstrate how argMax deduplication works:
- Raw tables will have multiple versions
- Snapshots will only show the latest version

Usage:
    python generate_updates.py [--bootstrap-servers localhost:29092]

Prerequisites:
    - Run generate_properties.py first
    - Run generate_demands.py first
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configuration
KAFKA_PROPERTY_TOPIC = "property-events"
KAFKA_DEMAND_TOPIC = "demand-events"

# Update counts
NUM_PROPERTY_UPDATES = 10
NUM_PARTIAL_PAYMENTS = 20
NUM_FULL_PAYMENTS = 10

# Reference data (must match generate_properties.py)
TENANTS = ["pb.amritsar", "pb.jalandhar", "pb.ludhiana", "pb.patiala"]

LOCALITIES = [
    ("LOC001", "Model Town"),
    ("LOC002", "Civil Lines"),
    ("LOC003", "Industrial Area"),
]

CITIES = {
    "pb.amritsar": ("Amritsar", "Amritsar", "Punjab"),
    "pb.jalandhar": ("Jalandhar", "Jalandhar", "Punjab"),
    "pb.ludhiana": ("Ludhiana", "Ludhiana", "Punjab"),
    "pb.patiala": ("Patiala", "Patiala", "Punjab"),
}

FIRST_NAMES = [
    "Rajesh", "Sunil", "Anil", "Vijay", "Sandeep",
    "Amit", "Deepak", "Rakesh", "Mukesh", "Manoj",
]

LAST_NAMES = [
    "Singh", "Sharma", "Kumar", "Gupta", "Verma",
]

# Financial year for payment updates
CURRENT_FY = "2024-25"
FY_START = datetime(2024, 4, 1)
FY_END = datetime(2025, 3, 31)


def datetime_to_millis(dt: datetime) -> int:
    """Convert datetime to milliseconds timestamp."""
    return int(dt.timestamp() * 1000)


def get_property_info(property_index: int) -> tuple:
    """Get tenant_id and property_id for a property index."""
    tenant_index = property_index % len(TENANTS)
    tenant_id = TENANTS[tenant_index]
    property_id = f"PT-{tenant_id.split('.')[1].upper()}-{property_index:06d}"
    return tenant_id, property_id


def generate_property_update(property_index: int) -> dict:
    """
    Generate a property update event.
    Changes usage_category from RESIDENTIAL to COMMERCIAL.
    """
    tenant_id, property_id = get_property_info(property_index)

    # Current time as the update time
    now = datetime.now()
    last_modified_time = datetime_to_millis(now)

    # Original created time (from initial load)
    created_time = datetime_to_millis(datetime(2020, 6, 15))

    locality_code, locality_name = random.choice(LOCALITIES)
    city, district, state = CITIES[tenant_id]

    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)

    return {
        "tenantId": tenant_id,
        "property": {
            "propertyId": property_id,
            "propertyType": "BUILTUP",
            "usageCategory": "COMMERCIAL",  # Changed from RESIDENTIAL
            "ownershipCategory": "INDIVIDUAL",
            "status": "ACTIVE",
            "acknowldgementNumber": f"ACK-{property_id}",
            "assessmentNumber": f"ASS-{property_id}",
            "financialYear": "2020-21",
            "source": "MUNICIPAL_RECORDS",
            "channel": "CITIZEN",  # Updated via citizen portal
            "landArea": str(round(random.uniform(1000, 2000), 4)),
            "landAreaUnit": "SQ_FT",
            "units": [
                {
                    "unitId": str(uuid.uuid4()),
                    "floorNo": "0",
                    "unitType": "SHOP",  # Changed to shop
                    "usageCategory": "COMMERCIAL",
                    "occupancyType": "RENTED",
                    "occupancyDate": "2024-01-15",  # New occupancy
                    "constructedArea": round(random.uniform(800, 1500), 2),
                    "carpetArea": round(random.uniform(600, 1200), 2),
                    "builtUpArea": round(random.uniform(900, 1600), 2),
                    "arvAmount": round(random.uniform(50000, 150000), 2),
                }
            ],
            "owners": [
                {
                    "ownerId": str(uuid.uuid4()),
                    "name": f"{first_name} {last_name}",
                    "mobileNumber": f"98{random.randint(10000000, 99999999)}",
                    "email": f"{first_name.lower()}.{last_name.lower()}@example.com",
                    "gender": "MALE",
                    "fatherOrHusbandName": f"{random.choice(FIRST_NAMES)} {last_name}",
                    "relationship": "FATHER",
                    "ownerType": "CITIZEN",
                    "ownerInfoUuid": str(uuid.uuid4()),
                    "institutionId": "",
                    "documentType": "AADHAAR",
                    "documentUid": f"{random.randint(1000, 9999)} {random.randint(1000, 9999)} {random.randint(1000, 9999)}",
                    "ownershipPercentage": 100.0,
                    "isPrimaryOwner": True,
                    "status": "ACTIVE",
                }
            ],
            "address": {
                "addressId": str(uuid.uuid4()),
                "doorNo": f"{random.randint(1, 500)}/A",
                "buildingName": "Commercial Complex",
                "street": f"Main Market Street {random.randint(1, 10)}",
                "locality": {
                    "code": locality_code,
                    "name": locality_name,
                },
                "city": city,
                "district": district,
                "region": "North",
                "state": state,
                "country": "IN",
                "pinCode": f"14{random.randint(1000, 9999)}",
                "geoLocation": {
                    "latitude": str(round(random.uniform(30.5, 32.0), 7)),
                    "longitude": str(round(random.uniform(74.5, 76.5), 7)),
                },
            },
            "auditDetails": {
                "createdBy": "system",
                "createdTime": created_time,
                "lastModifiedBy": "citizen_user_001",
                "lastModifiedTime": last_modified_time,
            },
            "version": 2,  # Incremented version
        },
    }


def generate_payment_update(
    property_index: int,
    payment_percentage: float,
    version: int,
) -> dict:
    """
    Generate a demand update event with payment.
    Updates collection_amount based on payment_percentage.
    """
    tenant_id, property_id = get_property_info(property_index)
    demand_id = str(uuid.uuid4())  # This should match existing, but for demo we use new

    # Current time as the payment time
    now = datetime.now()
    last_modified_time = datetime_to_millis(now)

    # Original created time
    created_time = datetime_to_millis(datetime(2024, 5, 15))

    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)

    # Generate tax amounts
    pt_tax_amount = round(random.uniform(10000, 30000), 2)
    late_fee_amount = round(random.uniform(500, 1500), 2)

    # Calculate collection amounts based on payment percentage
    pt_collection = round(pt_tax_amount * payment_percentage, 2)
    late_fee_collection = round(late_fee_amount * payment_percentage, 2)

    total_tax = pt_tax_amount + late_fee_amount
    is_fully_paid = payment_percentage >= 1.0

    return {
        "tenantId": tenant_id,
        "demand": {
            "id": demand_id,
            "consumerCode": property_id,
            "consumerType": "PT",
            "businessService": "PT",
            "taxPeriodFrom": datetime_to_millis(FY_START),
            "taxPeriodTo": datetime_to_millis(FY_END),
            "billingPeriod": CURRENT_FY,
            "status": "ACTIVE",
            "isPaymentCompleted": is_fully_paid,
            "financialYear": CURRENT_FY,
            "minimumAmountPayable": str(round(total_tax, 2)),
            "payer": {
                "name": f"{first_name} {last_name}",
                "mobileNumber": f"98{random.randint(10000000, 99999999)}",
                "email": f"{first_name.lower()}.{last_name.lower()}@example.com",
            },
            "demandDetails": [
                {
                    "id": str(uuid.uuid4()),
                    "taxHeadMasterCode": "PT_TAX",
                    "taxHeadMasterId": str(uuid.uuid4()),
                    "taxAmount": str(pt_tax_amount),
                    "collectionAmount": str(pt_collection),
                    "taxPeriodFrom": datetime_to_millis(FY_START),
                    "taxPeriodTo": datetime_to_millis(FY_END),
                },
                {
                    "id": str(uuid.uuid4()),
                    "taxHeadMasterCode": "PT_LATE_FEE",
                    "taxHeadMasterId": str(uuid.uuid4()),
                    "taxAmount": str(late_fee_amount),
                    "collectionAmount": str(late_fee_collection),
                    "taxPeriodFrom": datetime_to_millis(FY_START),
                    "taxPeriodTo": datetime_to_millis(FY_END),
                },
            ],
            "auditDetails": {
                "createdBy": "system",
                "createdTime": created_time,
                "lastModifiedBy": "payment_gateway",
                "lastModifiedTime": last_modified_time,
            },
            "version": version,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Generate update events to Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:29092",
        help="Kafka bootstrap servers (default: localhost:29092)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print events to stdout instead of sending to Kafka",
    )
    args = parser.parse_args()

    # Property indices for updates (first 100 properties)
    property_update_indices = list(range(1, NUM_PROPERTY_UPDATES + 1))
    partial_payment_indices = list(range(101, 101 + NUM_PARTIAL_PAYMENTS))
    full_payment_indices = list(range(201, 201 + NUM_FULL_PAYMENTS))

    print("Update scenarios:")
    print(f"  - {NUM_PROPERTY_UPDATES} property updates (RESIDENTIAL -> COMMERCIAL)")
    print(f"    Properties: {property_update_indices[0]} - {property_update_indices[-1]}")
    print(f"  - {NUM_PARTIAL_PAYMENTS} partial payments (50%)")
    print(f"    Properties: {partial_payment_indices[0]} - {partial_payment_indices[-1]}")
    print(f"  - {NUM_FULL_PAYMENTS} full payments (100%)")
    print(f"    Properties: {full_payment_indices[0]} - {full_payment_indices[-1]}")
    print()

    if args.dry_run:
        print("Sample property update:")
        print(json.dumps(generate_property_update(1), indent=2))
        print("\n---\n")
        print("Sample partial payment:")
        print(json.dumps(generate_payment_update(101, 0.5, 2), indent=2))
        print("\n---\n")
        print("Sample full payment:")
        print(json.dumps(generate_payment_update(201, 1.0, 3), indent=2))
        print("\n[DRY RUN] No events sent")
        return

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    start_time = time.time()

    # Send property updates
    print(f"Sending {NUM_PROPERTY_UPDATES} property updates...")
    for idx in property_update_indices:
        event = generate_property_update(idx)
        producer.send(KAFKA_PROPERTY_TOPIC, value=event)
    producer.flush()
    print(f"  Sent property updates for indices {property_update_indices[0]}-{property_update_indices[-1]}")

    # Send partial payments
    print(f"Sending {NUM_PARTIAL_PAYMENTS} partial payments (50%)...")
    for idx in partial_payment_indices:
        event = generate_payment_update(idx, 0.5, 2)
        producer.send(KAFKA_DEMAND_TOPIC, value=event)
    producer.flush()
    print(f"  Sent partial payments for indices {partial_payment_indices[0]}-{partial_payment_indices[-1]}")

    # Send full payments
    print(f"Sending {NUM_FULL_PAYMENTS} full payments (100%)...")
    for idx in full_payment_indices:
        event = generate_payment_update(idx, 1.0, 3)
        producer.send(KAFKA_DEMAND_TOPIC, value=event)
    producer.flush()
    print(f"  Sent full payments for indices {full_payment_indices[0]}-{full_payment_indices[-1]}")

    producer.close()

    elapsed = time.time() - start_time
    total_events = NUM_PROPERTY_UPDATES + NUM_PARTIAL_PAYMENTS + NUM_FULL_PAYMENTS
    print(f"\nCompleted: {total_events} update events in {elapsed:.1f}s")
    print("\nNext steps:")
    print("  1. Trigger RMV refresh: SYSTEM REFRESH VIEW rmv_property_snapshot;")
    print("  2. Verify deduplication: Run verification/verify_deduplication.sql")
    print("  3. Check defaulters: Run verification/verify_payment_flow.sql")


if __name__ == "__main__":
    main()
