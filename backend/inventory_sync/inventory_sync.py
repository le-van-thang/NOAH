import os
import shutil
import csv
import time
import pymysql
import redis
import re

# Configuration
INPUT_FILE = "/app/input/inventory.csv"
PROCESSED_DIR = "/app/processed"
DB_CONFIG = {
    "host": "mysql",
    "user": "root",
    "password": "",
    "database": "webstore",
    "cursorclass": pymysql.cursors.DictCursor
}

def retry_connection(func, service_name, max_retries=30, delay=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"⏳ [SYNC] {service_name} not ready ({i+1}/{max_retries}). Error: {e}")
            time.sleep(delay)
    raise Exception(f"❌ [SYNC] Could not connect to {service_name}")

def clean_quantity(val):
    if val is None: return None
    match = re.search(r'\d+', str(val))
    return int(match.group()) if match else None

def process_inventory():
    if not os.path.exists(INPUT_FILE):
        return

    print(f"📂 [POLLING] Found {INPUT_FILE}. Batch processing started...")
    
    db = retry_connection(lambda: pymysql.connect(**DB_CONFIG), "MySQL")
    r = retry_connection(lambda: redis.Redis(host="redis", port=6379, decode_responses=True), "Redis")

    try:
        with db.cursor() as cur:
            with open(INPUT_FILE, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                processed_count = 0
                skipped_count = 0
                
                for row in reader:
                    # Module 1: Validation
                    sku = row.get("product_id") or row.get("sku") or row.get("id")
                    raw_qty = row.get("quantity") or row.get("stock")
                    
                    qty = clean_quantity(raw_qty)
                    
                    if not sku:
                        print(f"⚠️ [WARN] Skipping row: Missing Product ID")
                        skipped_count += 1
                        continue
                        
                    if qty is None:
                        print(f"⚠️ [WARN] Skipping SKU {sku}: Invalid quantity format '{raw_qty}'")
                        skipped_count += 1
                        continue
                        
                    if qty < 0:
                        print(f"⚠️ [WARN] Skipping SKU {sku}: Negative quantity {qty} detected.")
                        skipped_count += 1
                        continue
                    
                    # Cập nhật Database & Cache (Module 1 & Option 2)
                    cur.execute(
                        "INSERT INTO products (id, stock) VALUES (%s, %s) ON DUPLICATE KEY UPDATE stock=%s",
                        (sku, qty, qty)
                    )
                    r.set(f"stock:{sku}", qty)
                    processed_count += 1
        
        db.commit()
        
        # Module 1: Cleanup
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        dest_path = os.path.join(PROCESSED_DIR, f"inventory_{int(time.time())}.csv")
        shutil.move(INPUT_FILE, dest_path)
        
        print(f"✅ [SUCCESS] Module 1 Complete: Processed {processed_count} records. Skipped {skipped_count} invalid records.")
        print(f"📁 File moved to: {dest_path}")
    
    except Exception as e:
        print(f"❌ [CRITICAL] Error processing file: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    print("🚀 Inventory Sync Service started...")
    while True:
        try:
            process_inventory()
        except Exception as e:
            print(f"🚨 Service Error: {e}")
        time.sleep(10)