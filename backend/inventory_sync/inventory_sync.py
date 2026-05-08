import os
import shutil
import csv
import time
import pymysql
import redis
import re

# --- CẤU HÌNH ---
INPUT_FILE = "/app/input/inventory.csv" # File đầu vào từ hệ thống cũ
PROCESSED_DIR = "/app/processed"      # Thư mục lưu file đã xử lý xong
DB_CONFIG = {
    "host": "noah-mysql",
    "user": "root",
    "password": "",
    "database": "webstore",
    "cursorclass": pymysql.cursors.DictCursor
}

# Hàm tự động kết nối lại nếu MySQL hoặc Redis chưa sẵn sàng
def retry_connection(func, service_name, max_retries=30, delay=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"⏳ [SYNC] {service_name} chưa sẵn sàng ({i+1}/{max_retries}). Đang thử lại...")
            time.sleep(delay)
    raise Exception(f"❌ [SYNC] Không thể kết nối tới {service_name}")

def clean_quantity(val):
    if val is None: return None
    match = re.search(r'\d+', str(val))
    return int(match.group()) if match else None

# --- HÀM XỬ LÝ ĐỒNG BỘ CSV ---
def process_inventory():
    # Kiểm tra xem có file inventory.csv trong thư mục input không
    if not os.path.exists(INPUT_FILE):
        return

    print(f"📂 [POLLING] Tìm thấy file {INPUT_FILE}. Đang bắt đầu xử lý hàng loạt...")
    
    # Kết nối MySQL và Redis
    db = retry_connection(lambda: pymysql.connect(**DB_CONFIG), "MySQL")
    r = retry_connection(lambda: redis.Redis(host="redis", port=6379, decode_responses=True), "Redis")

    try:
        with db.cursor() as cur:
            with open(INPUT_FILE, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                processed_count = 0
                skipped_count = 0
                
                for row in reader:
                    # Bọc try-except cho từng dòng để xử lý dữ liệu bẩn (Dirty Data Challenge)
                    try:
                        sku = row.get("product_id") or row.get("sku") or row.get("id")
                        raw_qty = row.get("quantity") or row.get("stock")
                        
                        # Thử ép kiểu số lượng sang số nguyên - Sẽ quăng lỗi nếu là chữ (ví dụ: 'Mười')
                        qty = int(raw_qty)
                        
                        # Kiểm tra logic số lượng âm
                        if qty < 0:
                            raise ValueError(f"Số lượng không được âm: {qty}")

                        if not sku:
                            raise ValueError("Thiếu mã sản phẩm (SKU)")

                        # Nếu mọi thứ ổn, cập nhật Database & Cache
                        cur.execute(
                            "INSERT INTO products (id, stock) VALUES (%s, %s) ON DUPLICATE KEY UPDATE stock=%s",
                            (sku, qty, qty)
                        )
                        r.set(f"stock:{sku}", qty)
                        processed_count += 1

                    except Exception as row_error:
                        # Bắt lỗi cho dòng này, in ra thông báo và continue để xử lý dòng tiếp theo
                        print(f"⚠️ [DIRTY DATA] Dòng bị lỗi: {row_error}. Bỏ qua dòng này.")
                        skipped_count += 1
                        continue
        
        db.commit()
        
        # Module 1: Cleanup
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        dest_path = os.path.join(PROCESSED_DIR, f"inventory_{int(time.time())}.csv")
        shutil.move(INPUT_FILE, dest_path)
        
        print(f"✅ [SUCCESS] Hoàn tất Module 1: Đã xử lý {processed_count} dòng. Bỏ qua {skipped_count} dòng lỗi.")
        print(f"📁 File đã được di chuyển tới: {dest_path}")
    
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
        time.sleep(5)