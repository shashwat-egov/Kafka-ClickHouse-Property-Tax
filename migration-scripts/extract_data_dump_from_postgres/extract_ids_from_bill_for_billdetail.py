import os
import json
import csv
import time
import psycopg2

# ======================
# CONFIG
# ======================

TABLE_NAME = "egcl_bill"

TENANT_ID = "pb.testing"
BUSINESS_SERVICE = "PT"

START_TIME = 1680309383619
END_TIME = 1774981800000

CHUNK_SIZE = 50000
OUTPUT_DIR = "output_bill_ids"
CHECKPOINT_FILE = "checkpoint_bill_ids.json"

# DB Config
DB_HOST = ''
DB_NAME = ''
DB_USER = ''
DB_PASSWORD = ''
DB_PORT = 5432

# ======================
# Helpers
# ======================

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return None

def save_checkpoint(last_createdtime, last_id, chunk_index, total_count):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "last_createdtime": last_createdtime,
            "last_id": last_id,
            "chunk_index": chunk_index,
            "total_count": total_count
        }, f)

# ======================
# Export Function
# ======================

def export_data():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    cursor = conn.cursor()

    checkpoint = load_checkpoint()

    last_createdtime = checkpoint["last_createdtime"] if checkpoint else None
    last_id = checkpoint["last_id"] if checkpoint else None
    chunk_index = checkpoint["chunk_index"] if checkpoint else 0
    total_count = checkpoint["total_count"] if checkpoint else 0

    if checkpoint:
        print(f"Resuming from ({last_createdtime}, {last_id}) | chunk={chunk_index}")
    else:
        print("Starting fresh export...")

    while True:
        if last_createdtime is not None:
            query = f"""
                SELECT id, createdtime
                FROM {TABLE_NAME}
                WHERE tenantid != %s
                  AND businessservice = %s
                  AND createdtime > %s
                  AND createdtime < %s
                  AND (createdtime, id) > (%s, %s)
                ORDER BY createdtime, id
                LIMIT %s
            """
            params = (
                TENANT_ID, BUSINESS_SERVICE,
                START_TIME, END_TIME,
                last_createdtime, last_id,
                CHUNK_SIZE
            )
        else:
            query = f"""
                SELECT id, createdtime
                FROM {TABLE_NAME}
                WHERE tenantid != %s
                  AND businessservice = %s
                  AND createdtime > %s
                  AND createdtime < %s
                ORDER BY createdtime, id
                LIMIT %s
            """
            params = (
                TENANT_ID, BUSINESS_SERVICE,
                START_TIME, END_TIME,
                CHUNK_SIZE
            )

        # Retry logic
        retries = 5
        while retries > 0:
            try:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                break
            except psycopg2.errors.SerializationFailure:
                conn.rollback()
                retries -= 1
                print(f"Retrying... ({5 - retries}/5)")
                time.sleep(5)
                if retries == 0:
                    raise

        if not rows:
            print("Export complete.")
            break

        output_file = os.path.join(OUTPUT_DIR, f"output_{chunk_index}.csv")
        print(f"Writing {len(rows)} rows : {output_file}")

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id"])  # Only ID column
            writer.writerows([[row[0]] for row in rows])  # Only id

        # Update trackers
        last_row = rows[-1]
        last_id = last_row[0]
        last_createdtime = last_row[1]

        chunk_index += 1
        total_count += len(rows)

        print(f"Total exported so far: {total_count}")

        save_checkpoint(last_createdtime, last_id, chunk_index, total_count)

    cursor.close()
    conn.close()

    print(f"\n✅ FINAL EXPORTED COUNT: {total_count}")


# ======================
# Run
# ======================

if __name__ == "__main__":
    export_data()