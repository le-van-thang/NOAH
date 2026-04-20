import csv, time, pymysql, os, shutil

INPUT_PATH = "/app/input/inventory.csv"
PROCESSED_DIR = "/app/processed"

while True:
    if os.path.exists(INPUT_PATH):
        try:
            db = pymysql.connect(host="mysql", user="root", password="", database="webstore")
            with db.cursor() as cur:
                with open(INPUT_PATH, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        try:
                            stock = int(r["stock"])
                            if stock < 0: raise ValueError("Âm")
                        except: # Thử thách Dirty Data
                            print(f"⚠️ Bỏ qua dòng lỗi: {r}")
                            continue
                        
                        cur.execute("""
                            INSERT INTO products (sku, stock) 
                            VALUES (%s, %s) ON DUPLICATE KEY UPDATE stock=%s
                        """, (r["sku"], stock, stock))
            db.commit()
            db.close()
            
            # Di chuyển file sau khi xong (Yêu cầu bắt buộc)
            os.makedirs(PROCESSED_DIR, exist_ok=True)
            shutil.move(INPUT_PATH, f"{PROCESSED_DIR}/inventory_{int(time.time())}.csv")
            print("✅ Đã xử lý và di chuyển file thành công.")
        except Exception as e:
            print(f"❌ Lỗi: {e}")
    
    time.sleep(10) # Cơ chế Polling