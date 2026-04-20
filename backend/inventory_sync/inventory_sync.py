import os, shutil, csv, time, pymysql

INPUT_FILE = "/app/input/inventory.csv"
PROCESSED_DIR = "/app/processed"

while True:
    if os.path.exists(INPUT_FILE):
        try:
            db = pymysql.connect(host="mysql", user="root", password="", database="webstore")
            with db.cursor() as cur:
                with open(INPUT_FILE, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        try:
                            stock = int(r["quantity"] if "quantity" in r else r["stock"])
                            if stock < 0: continue # Bỏ qua dữ liệu âm
                        except: continue # Bỏ qua dữ liệu lỗi định dạng
                        
                        cur.execute("INSERT INTO products (sku, stock) VALUES (%s, %s) ON DUPLICATE KEY UPDATE stock=%s", 
                                    (r["product_id"] if "product_id" in r else r["sku"], stock, stock))
            db.commit()
            db.close()
            # Dọn dẹp file sau khi xong (Bắt buộc)
            os.makedirs(PROCESSED_DIR, exist_ok=True)
            shutil.move(INPUT_FILE, f"{PROCESSED_DIR}/inventory_{int(time.time())}.csv")
            print("✅ Đã xử lý và dọn dẹp file kho.")
        except Exception as e:
            print(f"❌ Lỗi: {e}")
    time.sleep(10)