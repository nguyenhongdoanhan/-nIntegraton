import os
import csv
import time
import shutil
from datetime import datetime
import mysql.connector

INPUT_DIR = "/app/input"
PROCESSED_DIR = "/app/processed"
POLL_INTERVAL = 5

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mysql"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "database": os.getenv("DB_NAME", "inventory_db")
}

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

def validate_row(row):
    if len(row) < 2:
        return False, "Missing columns"

    try:
        product_id = int(row[0].strip())
        quantity = int(row[1].strip())
    except Exception:
        return False, "Invalid format"

    if quantity < 0:
        return False, "Negative quantity"

    return True, (product_id, quantity)

def move_file(file_path):
    filename = os.path.basename(file_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"{os.path.splitext(filename)[0]}_{timestamp}.csv"
    dest_path = os.path.join(PROCESSED_DIR, new_name)

    shutil.move(file_path, dest_path)
    print(f"[INFO] Moved file to: {dest_path}")

def process_file(file_path):
    processed = 0
    skipped = 0

    print(f"[INFO] Found file: {file_path}")

    try:
        conn = connect_db()
        cursor = conn.cursor()

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)

            for i, row in enumerate(reader, start=1):
                valid, result = validate_row(row)

                if not valid:
                    skipped += 1
                    print(f"[WARN] Line {i}: {result} | {row}")
                    continue

                product_id, quantity = result

                try:
                    cursor.execute(
                        "UPDATE products SET quantity = %s WHERE product_id = %s",
                        (quantity, product_id)
                    )

                    if cursor.rowcount == 0:
                        skipped += 1
                        print(f"[WARN] Product {product_id} not found")
                        continue

                    processed += 1

                except Exception as e:
                    skipped += 1
                    print(f"[ERROR] DB error line {i}: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[INFO] Processed {processed} records. Skipped {skipped} invalid records.")

        move_file(file_path)

    except Exception as e:
        print(f"[ERROR] Failed to process file: {e}")

def start_polling():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    print("[INFO] Inventory Sync Service Started...")

    while True:
        try:
            files = os.listdir(INPUT_DIR)

            for file in files:
                if file.endswith(".csv"):
                    full_path = os.path.join(INPUT_DIR, file)
                    process_file(full_path)

        except Exception as e:
            print(f"[ERROR] Polling error: {e}")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    start_polling()