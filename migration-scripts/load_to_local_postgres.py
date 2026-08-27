import os
import time
import psycopg2
import csv
import io

# -------------------------------
# CONFIG
# -------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5435,
    "dbname": "testdb",
    "user": "postgres",
    "password": "postgres"
}

TABLE_NAME = "eg_pt_address"
CSV_ROOT = "/home/admin1/Downloads/Punjab-data-dump/17FebTo31Mar/property-address"

PROGRESS_FILE = "address_import_progress.log"
ERROR_FILE = "address_import_errors.log"

SLEEP_BETWEEN_FILES = 1


# -------------------------------
# HELPER FUNCTIONS
# -------------------------------

def get_all_csv_files(root_folder):
    csv_files = []
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    return sorted(csv_files)


def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, "r") as f:
        return set(line.strip() for line in f)


def mark_completed(filepath):
    with open(PROGRESS_FILE, "a") as f:
        f.write(filepath + "\n")


def log_error(message):
    with open(ERROR_FILE, "a") as f:
        f.write(message + "\n")


# -------------------------------
# MAIN LOADER
# -------------------------------

def main():

    if not os.path.exists(CSV_ROOT):
        print(f"❌ Folder {CSV_ROOT} does not exist")
        return

    print("🚀 Starting Resumable Address Import")
    print("------------------------------------------------")

    csv_files = get_all_csv_files(CSV_ROOT)
    completed_files = load_progress()

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False  # transactional per file

    try:
        for file_path in csv_files:

            if file_path in completed_files:
                print(f"⏭ Skipping already imported: {file_path}")
                continue

            print(f"📂 Importing: {file_path}")

            try:
                with conn.cursor() as cur:
                    with open(file_path, "r", encoding="utf-8") as infile:

                        reader = csv.DictReader(infile)
                        output = io.StringIO()
                        writer = csv.writer(output)

                        # Write header
                        writer.writerow(reader.fieldnames)

                        for row in reader:
                            writer.writerow([
                                row.get("tenantid"),
                                row.get("id"),                 # ✅ ADD THIS
                                row.get("propertyid"),
                                row.get("doorno"),
                                row.get("plotno"),
                                row.get("buildingname"),
                                row.get("street"),
                                row.get("landmark"),
                                row.get("city"),
                                row.get("pincode"),
                                row.get("locality"),
                                row.get("district"),
                                row.get("region"),
                                row.get("state"),
                                row.get("country"),
                                row.get("latitude"),
                                row.get("longitude"),
                                row.get("createdby"),          # optional
                                row.get("createdtime"),        # ✅ REQUIRED
                                row.get("lastmodifiedby"),
                                row.get("lastmodifiedtime"),
                                row.get("additionaldetails")
                            ])

                        output.seek(0)

                        copy_sql = f"""
                        COPY {TABLE_NAME} (
                            tenantid,
                            id,
                            propertyid,
                            doorno,
                            plotno,
                            buildingname,
                            street,
                            landmark,
                            city,
                            pincode,
                            locality,
                            district,
                            region,
                            state,
                            country,
                            latitude,
                            longitude,
                            createdby,
                            createdtime,
                            lastmodifiedby,
                            lastmodifiedtime,
                            additionaldetails
                        )
                        FROM STDIN WITH (
                            FORMAT csv,
                            HEADER true,
                            NULL ''
                        )
                        """

                        cur.copy_expert(copy_sql, output)

                conn.commit()
                mark_completed(file_path)

                print(f"✅ Completed: {file_path}")
                print("------------------------------------------------")

                time.sleep(SLEEP_BETWEEN_FILES)

            except Exception as e:
                conn.rollback()
                error_msg = f"❌ Error importing {file_path}: {str(e)}"
                print(error_msg)
                log_error(error_msg)
                print("Stopping execution. Fix issue and re-run to resume.")
                break

    finally:
        conn.close()

    print("🎉 Address Import Finished.")


if __name__ == "__main__":
    main()