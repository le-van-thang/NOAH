from fastapi import FastAPI, HTTPException
import pymysql
import pika
import redis
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import pusher

# Option 4: Cấu hình Pusher Client (Dùng để bắn tin nhắn Real-time tới Dashboard)
pusher_client = pusher.Pusher(
  app_id='2143056',
  key='727fe40ff32e36c704c0',
  secret='625d4a4a71c17de24fa8',
  cluster='ap1',
  ssl=True
)
app = FastAPI()

# --- CẤU HÌNH CÁC DỊCH VỤ ---
MYSQL_CONFIG = {"host": "noah-mysql", "user": "root", "password": "", "database": "webstore", "cursorclass": pymysql.cursors.DictCursor}
PG_CONFIG = "host=noah-postgres dbname=finance_db user=postgres password=postgres"
RABBIT_HOST = "noah-rabbitmq"
REDIS_HOST = "noah-redis"

# Hàm tự động kết nối lại (Retry Logic)
def retry_connection(func, service_name, max_retries=20, delay=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"⏳ [API] {service_name} chưa sẵn sàng ({i+1}/{max_retries}). Đang thử lại...")
            time.sleep(delay)
    raise Exception(f"❌ [API] Không thể kết nối tới {service_name}")

def get_mysql(): return pymysql.connect(**MYSQL_CONFIG)
def get_pg(): return psycopg2.connect(PG_CONFIG, cursor_factory=RealDictCursor)
def get_redis(): return redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
def get_rabbit(): return pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))

@app.get("/")
def health(): return {"status": "ok"}

@app.post("/api/orders")
def create_order(order: dict):
    # Lấy thông tin mã sản phẩm và số lượng từ request
    pid = order.get("product_id") or order.get("sku") or order.get("id")
    qty = int(order.get("quantity", 0))

    if not pid or qty <= 0:
        raise HTTPException(status_code=400, detail="Mã sản phẩm hoặc số lượng không hợp lệ")

    # Option 2: Chống bán lố thông minh (Sử dụng Redis Atomic)
    r = retry_connection(get_redis, "Redis")
    
    # Kiểm tra xem sản phẩm có trong Cache không
    if not r.exists(f"stock:{pid}"):
        raise HTTPException(status_code=400, detail="Sản phẩm chưa có trong Cache. Hãy đồng bộ tồn kho trước.")

    try:
        # BƯỚC 0: Lấy giá sản phẩm từ MySQL để tính tổng tiền
        db = retry_connection(get_mysql, "MySQL")
        with db.cursor() as cur:
            cur.execute("SELECT price FROM products WHERE id = %s", (pid,))
            p_res = cur.fetchone()
            price = p_res['price'] if p_res else 0
        
        total_price = float(price) * qty

        # BƯỚC 1: Trừ tồn kho ngay lập tức trên Redis (Atomic Decrement)
        new_stock = r.decrby(f"stock:{pid}", qty)
        if new_stock < 0:
            # Nếu âm nghĩa là hết hàng -> Hoàn trả lại số lượng vừa trừ và báo lỗi
            r.incrby(f"stock:{pid}", qty)
            raise HTTPException(status_code=400, detail=f"Hết hàng. Chỉ còn lại {new_stock + qty} sản phẩm.")

        # BƯỚC 2: Lưu đơn hàng vào MySQL với trạng thái 'PENDING'
        with db.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (product_id, quantity, total_price, status, user_id) VALUES (%s, %s, %s, 'PENDING', %s)",
                (pid, qty, total_price, order.get("user_id", 1))
            )
            order_id = cur.lastrowid
        db.commit()
        db.close()

        # BƯỚC 3: Đẩy sự kiện đơn hàng vào RabbitMQ để Worker xử lý ngầm
        rabbit = retry_connection(get_rabbit, "RabbitMQ")
        channel = rabbit.channel()
        channel.queue_declare(queue="order_queue", durable=True)
        channel.basic_publish(
            exchange="",
            routing_key="order_queue",
            body=json.dumps({
                "order_id": order_id, 
                "product_id": pid, 
                "quantity": qty, 
                "total_price": total_price,
                "user_id": order.get("user_id", 1)
            }),
            properties=pika.BasicProperties(delivery_mode=2) # Đảm bảo tin nhắn không bị mất khi RabbitMQ sập
        )
        rabbit.close()

        # Option 4: Phát sự kiện Real-time qua Pusher để Dashboard tự reload
        try:
            pusher_client.trigger('noah-orders', 'new_order', {'message': f'Đơn hàng mới #{order_id} đã được tạo'})
        except Exception as e:
            print(f"⚠️ Lỗi Pusher: {e}")

        return {"message": "Đơn hàng đã được tiếp nhận", "order_id": order_id}

    except HTTPException: raise
    except Exception as e:
        # Nếu có lỗi bất kỳ xảy ra (ví dụ DB sập), phải hoàn trả lại tồn kho vào Redis
        r.incrby(f"stock:{pid}", qty)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/report")
def get_report():
    # Bước 1: Lấy dữ liệu tổng hợp từ MySQL (Hệ thống bán hàng)
    mysql_data = {}
    user_rev = []
    try:
        db = retry_connection(get_mysql, "MySQL", max_retries=5)
        with db.cursor() as cur:
            cur.execute("SELECT product_id, SUM(quantity) as web_total FROM orders GROUP BY product_id")
            mysql_data = {str(row['product_id']): row['web_total'] for row in cur.fetchall()}
            
            # MODULE 3 - YÊU CẦU 4: Tính doanh thu theo khách hàng
            cur.execute("SELECT user_id, SUM(total_price) as revenue FROM orders GROUP BY user_id ORDER BY revenue DESC LIMIT 5")
            user_rev = cur.fetchall()
        db.close()
    except Exception as e: print(f"⚠️ Lỗi MySQL: {e}")

    # Bước 2: Lấy dữ liệu tổng hợp từ Postgres (Hệ thống Kế toán)
    pg_data = {}
    try:
        db = retry_connection(get_pg, "PostgreSQL", max_retries=5)
        with db.cursor() as cur:
            cur.execute("SELECT product_id, SUM(quantity) as finance_total FROM finance_orders GROUP BY product_id")
            pg_data = {str(row['product_id']): row['finance_total'] for row in cur.fetchall()}
        db.close()
    except Exception as e: print(f"⚠️ Lỗi Postgres: {e}")

    # Bước 3: Gộp dữ liệu (Stitching) để Dashboard đối soát
    all_pids = set(list(mysql_data.keys()) + list(pg_data.keys()))
    reconciliation = [{
        "sku": pid,
        "web_total": mysql_data.get(pid, 0),
        "finance_total": pg_data.get(pid, 0),
        "is_synced": mysql_data.get(pid, 0) == pg_data.get(pid, 0)
    } for pid in all_pids]

    return {
        "reconciliation": reconciliation,
        "top_customers": user_rev
    }
 
@app.post("/api/sync-all")
def sync_all():
    try:
        mysql_db = retry_connection(get_mysql, "MySQL")
        pg_db = retry_connection(get_pg, "PostgreSQL")
        
        with mysql_db.cursor() as m_cur, pg_db.cursor() as p_cur:
            # Get all unsynced orders
            m_cur.execute("SELECT id, product_id, quantity FROM orders WHERE status != 'COMPLETED'")
            orders = m_cur.fetchall()
            
            if not orders:
                return {"message": "All orders already synced"}
            
            # Bulk insert to Postgres
            p_cur.executemany(
                "INSERT INTO finance_orders (order_id, product_id, quantity) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                [(o['id'], o['product_id'], o['quantity']) for o in orders]
            )
            
            # Update MySQL status
            m_cur.executemany(
                "UPDATE orders SET status = 'COMPLETED' WHERE id = %s",
                [(o['id'],) for o in orders]
            )
            
        mysql_db.commit()
        pg_db.commit()
        mysql_db.close()
        pg_db.close()
        return {"message": f"Successfully synced {len(orders)} orders"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
