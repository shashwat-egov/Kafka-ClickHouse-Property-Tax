import psycopg2
import psycopg2.errors
import csv
import os
import json
import time

# ======================
# DB connection settings
# ======================
DB_HOST = ''
DB_NAME = ''
DB_USER = ''
DB_PASSWORD = ''
DB_PORT = 5432


# ======================
# Query settings
# ======================
TABLE_NAME = 'egbs_demand_v1' # eg_pt_property,eg_pt_address,eg_pt_unit,eg_pt_owner,egbs_demand_v1,egbs_demanddetail_v1,eg_pt_asmt_assessment,egcl_payment,egcl_bill,eg_pt_property_audit

# MULTIPLE TENANTS
TENANT_ID = "pb.testing"

CREATEDTIME_LIMIT = 1774981800000
CREATEDTIME_START = 1771348375588
CHUNK_SIZE = 30000
OUTPUT_DIR = "output"
CHECKPOINT_FILE = "checkpoint.json"

# ======================
# Helper functions
# ======================
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return None

def save_checkpoint(last_id, chunk_index):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "last_id": last_id,
            "chunk_index": chunk_index
        }, f)

# ======================
# Export function
# ======================
def export_chunks():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )
    cursor = conn.cursor()

    checkpoint = load_checkpoint()
    chunk_index = checkpoint["chunk_index"] if checkpoint else 0

    while True:
        if checkpoint:
            print(f"Resuming from id={checkpoint['last_id']}")
            query = f"""
                SELECT *
                FROM {TABLE_NAME}
                WHERE tenantid != %s
                  AND id > %s
                  AND 
                  (
                        (createdtime > %s AND createdtime < %s)
                        OR 
                        (lastmodifiedtime > %s AND lastmodifiedtime < %s)
                    )
                ORDER BY id
                LIMIT %s
            """
            params = (TENANT_ID,
                      checkpoint["last_id"], CREATEDTIME_START, CREATEDTIME_LIMIT, CREATEDTIME_START, CREATEDTIME_LIMIT, CHUNK_SIZE)
        else:
            print("Starting fresh export...")
            query = f"""
                SELECT *
                FROM {TABLE_NAME}
                WHERE tenantid != %s
                AND (
                    (createdtime > %s AND createdtime < %s)
                        OR 
                        (lastmodifiedtime > %s AND lastmodifiedtime < %s)
                )
                ORDER BY id
                LIMIT %s
            """
            params = (TENANT_ID,CREATEDTIME_START, CREATEDTIME_LIMIT, CREATEDTIME_START, CREATEDTIME_LIMIT,
                      CHUNK_SIZE)

        # --- Retry loop ---
        retries = 25
        while retries > 0:
            try:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                break

            except (psycopg2.errors.SerializationFailure,
                    psycopg2.OperationalError) as e:

                print(f"Retryable DB error: {str(e)}")
                retries -= 1

                try:
                    conn.rollback()
                except:
                    pass

                # 🔥 reconnect if connection is broken
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass

                print(f"Reconnecting... ({5 - retries}/5)")
                time.sleep(5)

                conn = psycopg2.connect(
                    host=DB_HOST,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    port=DB_PORT,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
                cursor = conn.cursor()

                if retries == 0:
                    raise

        if not rows:
            print("Export complete - no more rows.")
            break

        # prepare file
        column_names = [desc[0] for desc in cursor.description]
        output_file = os.path.join(OUTPUT_DIR, f"output_{chunk_index}.csv")
        print(f"Writing {len(rows)} rows to {output_file}...")

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            writer.writerows(rows)

        # save checkpoint (last row of this chunk)
        last_id = rows[-1][column_names.index("id")]
        save_checkpoint(last_id, chunk_index + 1)

        checkpoint = {"last_id": last_id, "chunk_index": chunk_index + 1}
        chunk_index += 1

    cursor.close()
    conn.close()

# ======================
# Run
# ======================
if __name__ == "__main__":
    export_chunks()