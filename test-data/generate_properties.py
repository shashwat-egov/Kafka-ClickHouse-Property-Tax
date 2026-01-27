#!/usr/bin/env python3
"""
Generate 10,000 property events and send to Kafka.

This script creates realistic property tax data distributed across:
- 4 tenants (municipalities)
- Various usage categories (RESIDENTIAL, COMMERCIAL, INDUSTRIAL, etc.)
- Various ownership categories (INDIVIDUAL, INSTITUTIONAL, GOVERNMENT)
- Properties created between 2018-2024

Each property event includes:
- 1-3 units (floors/shops)
- 1-2 owners
- 1 address

Usage:
    python generate_properties.py [--bootstrap-servers localhost:29092]
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configuration
NUM_PROPERTIES = 10000
KAFKA_TOPIC = "property-events"

# Reference data
TENANTS = ["pb.amritsar", "pb.jalandhar", "pb.ludhiana", "pb.patiala"]

USAGE_CATEGORIES = [
    "RESIDENTIAL",
    "COMMERCIAL",
    "INDUSTRIAL",
    "INSTITUTIONAL",
    "MIXED",
]

OWNERSHIP_CATEGORIES = [
    "INDIVIDUAL",
    "INSTITUTIONAL",
    "GOVERNMENT",
]

PROPERTY_TYPES = [
    "BUILTUP",
    "VACANT",
]

UNIT_TYPES = [
    "FLAT",
    "SHOP",
    "OFFICE",
    "WAREHOUSE",
    "GROUND_FLOOR",
    "FIRST_FLOOR",
    "SECOND_FLOOR",
]

OCCUPANCY_TYPES = [
    "SELFOCCUPIED",
    "RENTED",
    "UNOCCUPIED",
]

GENDERS = ["MALE", "FEMALE", "OTHER"]

LOCALITIES = [
    ("LOC001", "Model Town"),
    ("LOC002", "Civil Lines"),
    ("LOC003", "Industrial Area"),
    ("LOC004", "Old City"),
    ("LOC005", "New Colony"),
    ("LOC006", "Sector 17"),
    ("LOC007", "Market Road"),
    ("LOC008", "Station Road"),
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
    "Priya", "Sunita", "Anita", "Kavita", "Neha",
    "Pooja", "Ritu", "Meena", "Geeta", "Suman",
]

LAST_NAMES = [
    "Singh", "Sharma", "Kumar", "Gupta", "Verma",
    "Kaur", "Gill", "Sidhu", "Dhillon", "Malhotra",
]


def random_timestamp(start_year: int, end_year: int) -> int:
    """Generate random timestamp in milliseconds between years."""
    start = datetime(start_year, 4, 1)  # Financial year starts April
    end = datetime(end_year, 3, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_dt = start + timedelta(days=random_days, seconds=random.randint(0, 86400))
    return int(random_dt.timestamp() * 1000)


def generate_owner(property_id: str, is_primary: bool) -> dict:
    """Generate a property owner."""
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    gender = "MALE" if first_name in FIRST_NAMES[:10] else "FEMALE"

    return {
        "ownerId": str(uuid.uuid4()),
        "name": f"{first_name} {last_name}",
        "mobileNumber": f"98{random.randint(10000000, 99999999)}",
        "email": f"{first_name.lower()}.{last_name.lower()}@example.com",
        "gender": gender,
        "fatherOrHusbandName": f"{random.choice(FIRST_NAMES)} {last_name}",
        "relationship": "FATHER" if random.random() > 0.3 else "HUSBAND",
        "ownerType": "CITIZEN",
        "ownerInfoUuid": str(uuid.uuid4()),
        "institutionId": "",
        "documentType": "AADHAAR",
        "documentUid": f"{random.randint(1000, 9999)} {random.randint(1000, 9999)} {random.randint(1000, 9999)}",
        "ownershipPercentage": 100.0 if is_primary else random.uniform(20, 50),
        "isPrimaryOwner": is_primary,
        "status": "ACTIVE",
    }


def generate_unit(property_id: str, usage_category: str, unit_index: int) -> dict:
    """Generate a property unit."""
    floor_mapping = {0: "GROUND_FLOOR", 1: "FIRST_FLOOR", 2: "SECOND_FLOOR"}

    unit_type = floor_mapping.get(unit_index, random.choice(UNIT_TYPES))
    if usage_category == "COMMERCIAL":
        unit_type = random.choice(["SHOP", "OFFICE", "WAREHOUSE"])
    elif usage_category == "INDUSTRIAL":
        unit_type = "WAREHOUSE"

    constructed_area = random.uniform(500, 2000)

    return {
        "unitId": str(uuid.uuid4()),
        "floorNo": str(unit_index),
        "unitType": unit_type,
        "usageCategory": usage_category,
        "occupancyType": random.choice(OCCUPANCY_TYPES),
        "occupancyDate": datetime(
            random.randint(2015, 2023),
            random.randint(1, 12),
            random.randint(1, 28)
        ).strftime("%Y-%m-%d"),
        "constructedArea": round(constructed_area, 2),
        "carpetArea": round(constructed_area * 0.8, 2),
        "builtUpArea": round(constructed_area * 1.1, 2),
        "arvAmount": round(random.uniform(10000, 100000), 2),
    }


def generate_address(tenant_id: str) -> dict:
    """Generate a property address."""
    locality_code, locality_name = random.choice(LOCALITIES)
    city, district, state = CITIES[tenant_id]

    return {
        "addressId": str(uuid.uuid4()),
        "doorNo": f"{random.randint(1, 500)}/{random.choice('ABCDEFGH')}",
        "buildingName": f"Block {random.choice('ABCDEFGHIJ')}",
        "street": f"Street {random.randint(1, 50)}",
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
    }


def generate_property_event(property_index: int) -> dict:
    """Generate a complete property event."""
    # Use deterministic tenant assignment based on property_index
    # This ensures property_ids are predictable for update scenarios
    tenant_index = property_index % len(TENANTS)
    tenant_id = TENANTS[tenant_index]
    property_id = f"PT-{tenant_id.split('.')[1].upper()}-{property_index:06d}"

    usage_category = random.choices(
        USAGE_CATEGORIES,
        weights=[60, 25, 10, 3, 2],  # Mostly residential
        k=1
    )[0]

    ownership_category = random.choices(
        OWNERSHIP_CATEGORIES,
        weights=[85, 10, 5],  # Mostly individual
        k=1
    )[0]

    # Created time between 2018-2024
    created_time = random_timestamp(2018, 2024)
    last_modified_time = created_time

    # Generate 1-3 units
    num_units = random.choices([1, 2, 3], weights=[60, 30, 10], k=1)[0]
    units = [generate_unit(property_id, usage_category, i) for i in range(num_units)]

    # Generate 1-2 owners
    num_owners = random.choices([1, 2], weights=[70, 30], k=1)[0]
    owners = [generate_owner(property_id, i == 0) for i in range(num_owners)]

    # Calculate land area based on units
    total_constructed = sum(u["constructedArea"] for u in units)
    land_area = total_constructed * random.uniform(1.0, 1.5)

    # Derive financial year from created_time
    created_dt = datetime.fromtimestamp(created_time / 1000)
    if created_dt.month >= 4:
        fy_start = created_dt.year
    else:
        fy_start = created_dt.year - 1
    financial_year = f"{fy_start}-{(fy_start + 1) % 100:02d}"

    return {
        "tenantId": tenant_id,
        "property": {
            "propertyId": property_id,
            "propertyType": random.choice(PROPERTY_TYPES),
            "usageCategory": usage_category,
            "ownershipCategory": ownership_category,
            "status": "ACTIVE",
            "acknowldgementNumber": f"ACK-{property_id}",
            "assessmentNumber": f"ASS-{property_id}",
            "financialYear": financial_year,
            "source": "MUNICIPAL_RECORDS",
            "channel": "SYSTEM",
            "landArea": str(round(land_area, 4)),
            "landAreaUnit": "SQ_FT",
            "units": units,
            "owners": owners,
            "address": generate_address(tenant_id),
            "auditDetails": {
                "createdBy": "system",
                "createdTime": created_time,
                "lastModifiedBy": "system",
                "lastModifiedTime": last_modified_time,
            },
            "version": 1,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Generate property events to Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:29092",
        help="Kafka bootstrap servers (default: localhost:29092)",
    )
    parser.add_argument(
        "--num-properties",
        type=int,
        default=NUM_PROPERTIES,
        help=f"Number of properties to generate (default: {NUM_PROPERTIES})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print events to stdout instead of sending to Kafka",
    )
    args = parser.parse_args()

    if args.dry_run:
        # Print sample events
        for i in range(min(5, args.num_properties)):
            event = generate_property_event(i + 1)
            print(json.dumps(event, indent=2))
            print("---")
        print(f"[DRY RUN] Would generate {args.num_properties} property events")
        return

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    print(f"Generating {args.num_properties} property events...")
    start_time = time.time()

    for i in range(args.num_properties):
        event = generate_property_event(i + 1)
        producer.send(KAFKA_TOPIC, value=event)

        if (i + 1) % 1000 == 0:
            producer.flush()
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print(f"  Sent {i + 1:,} events ({rate:.0f} events/sec)")

    producer.flush()
    producer.close()

    elapsed = time.time() - start_time
    print(f"Completed: {args.num_properties:,} property events in {elapsed:.1f}s")
    print(f"Rate: {args.num_properties / elapsed:.0f} events/sec")


if __name__ == "__main__":
    main()
