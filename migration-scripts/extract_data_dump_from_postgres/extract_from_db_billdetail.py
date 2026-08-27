import psycopg2
import csv
import os
import time
import glob

# ======================
# DB connection settings
# ======================
DB_HOST = ''
DB_NAME = ''
DB_USER = ''
DB_PASSWORD = ''
DB_PORT = 5432

# ======================
# Config
# ======================
TABLE_NAME = 'egcl_billdetial'
TENANT_ID = "pb.testing"

INPUT_DIR = "output_bill_ids"
OUTPUT_DIR = "bill_detail_output"

BATCH_SIZE = 1000   # 🔥 reduced for replica safety

# ======================
# DB connection
# ======================
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

# ======================
# Read bill IDs
# ======================
def read_bill_ids(file_path):
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            yield row[0]

# ======================
# Fetch + Write
# ======================
def fetch_and_write(conn, bill_ids, file_index, batch_count):

    query = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE tenantid != %s
          AND billid = ANY(%s)
    """

    retries = 5

    while retries > 0:
        try:
            cursor = conn.cursor()

            # ⏱️ prevent long-running queries
            cursor.execute("SET statement_timeout = 300000")  # 5 min

            cursor.execute(query, (TENANT_ID, bill_ids))
            rows = cursor.fetchall()

            if not rows:
                cursor.close()
                return conn

            column_names = [desc[0] for desc in cursor.description]

            output_file = os.path.join(
                OUTPUT_DIR,
                f"bill_detail_{file_index}_{batch_count}.csv"
            )

            print(f"Writing {len(rows)} rows : {output_file}")

            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(column_names)
                writer.writerows(rows)

            cursor.close()
            return conn

        except Exception as e:
            print(f"Retrying due to error: {e}")
            retries -= 1

            # close broken connection
            try:
                conn.close()
            except:
                pass

            time.sleep(5)

            # 🔥 recreate connection
            conn = get_connection()

            if retries == 0:
                print(f"❌ Skipping batch {file_index}-{batch_count}")
                return conn

# ======================
# Main processing
# ======================
def export_bill_details():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = get_connection()

    csv_files = sorted(glob.glob(f"{INPUT_DIR}/*.csv"))

    file_index = 0

    for file in csv_files:
        print(f"\nProcessing file: {file}")

        batch = []
        batch_count = 0

        for bill_id in read_bill_ids(file):
            batch.append(bill_id)

            if len(batch) == BATCH_SIZE:
                conn = fetch_and_write(conn, batch, file_index, batch_count)
                batch = []
                batch_count += 1

                time.sleep(0.2)  # 🔥 reduce DB pressure

        # remaining batch
        if batch:
            conn = fetch_and_write(conn, batch, file_index, batch_count)

        file_index += 1

    conn.close()
    print("\n✅ Extraction completed successfully!")

# ======================
# Run
# ======================
if __name__ == "__main__":
    export_bill_details()