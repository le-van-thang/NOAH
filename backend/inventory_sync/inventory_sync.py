import csv
import time
import pymysql

while True:
    try:
        db = pymysql.connect(
            host="mysql",
            user="root",
            password="",
            database="webstore",
            cursorclass=pymysql.cursors.DictCursor
        )

        with db.cursor() as cur:
            with open("/app/docs/inventory.csv", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    stock = int(r["stock"])
                    if stock < 0:
                        continue

                    cur.execute(
                        """
                        INSERT INTO products (sku, stock)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE stock=%s
                        """,
                        (r["sku"], stock, stock)
                    )

        db.commit()
        db.close()
        print("✅ Inventory synced")

    except Exception as e:
        print("❌ Sync error:", e)

    time.sleep(10)
