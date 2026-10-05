import os
import re
import csv

# -------- CONFIG --------

BASE_FOLDER = "/home/admin1/Downloads/Punjab-data-dump/17FebTo31Mar/bill_details_with_ids"   # <-- change this
PATTERN = re.compile(r"output_\d+\.csv")


def count_rows(filepath):
    with open(filepath, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        return sum(1 for _ in reader)


grand_total = 0

# -------- ITERATE FOLDERS --------
# for folder in os.listdir(BASE_FOLDER):
folder_path = os.path.join(BASE_FOLDER)

# if not os.path.isdir(folder_path):
#     continue

folder_total = 0
print(f"\n📁 Folder: {folder_path}")

for file in os.listdir(folder_path):
    if not PATTERN.match(file):
        continue

    path = os.path.join(folder_path, file)
    rows = count_rows(path)

    print(f"{file}: {rows}")
    folder_total += rows

print(f"➡ Folder total: {folder_total}")
grand_total += folder_total


print("\n==============================")
print(f" GRAND TOTAL ROWS = {grand_total}")
print("==============================")
