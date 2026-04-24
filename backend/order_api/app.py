from fastapi import FastAPI, HTTPException
import pymysql
import pika
import redis
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

# Configs
MYSQL_CONFIG = {"host": "mysql", "user": "root", "password": "", "database": "webstore", "cursorclass": pymysql.cursors.DictCursor}
PG_CONFIG = "host=postgres dbname=finance_db user=postgres password=postgres"
RABBIT_HOST = "rabbitmq"
REDIS_HOST = "redis"

def retry_connection(func, service_name, max_retries=30, delay=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"⏳ [API] {service_name} not ready ({i+1}/{max_retries}). Error: {e}")
            time.sleep(delay)
    raise Exception(f"❌ [API] Could not connect to {service_name}")

def get_mysql(): return pymysql.connect(**MYSQL_CONFIG)
def get_pg(): return psycopg2.connect(PG_CONFIG, cursor_factory=RealDictCursor)
def get_redis(): return redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
def get_rabbit(): return pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))

@app.get("/")
def health(): return {"status": "ok"}

@app.post("/api/orders")
def create_order(order: dict):
    # Match init.sql: product_id
    pid = order.get("product_id") or order.get("sku") or order.get("id")
    qty = int(order.get("quantity", 0))

    if not pid or qty <= 0:
        raise HTTPException(status_code=400, detail="Invalid product_id or quantity")

    # Option 2: Smart Overselling Protection (Redis Atomic)
    r = retry_connection(get_redis, "Redis")
    
    # Check if key exists
    if not r.exists(f"stock:{pid}"):
        raise HTTPException(status_code=400, detail="Product not in cache. Sync inventory first.")

    # Module 2A & Option 2 Logic
    try:
        # Atomic Decrement
        new_stock = r.decrby(f"stock:{pid}", qty)
        if new_stock < 0:
            r.incrby(f"stock:{pid}", qty)
            raise HTTPException(status_code=400, detail=f"Out of stock. Only {new_stock + qty} units remaining.")

        # Persist PENDING to MySQL
        db = retry_connection(get_mysql, "MySQL")
        with db.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (product_id, quantity, status, user_id) VALUES (%s, %s, 'PENDING', %s)",
                (pid, qty, order.get("user_id", 1))
            )
            order_id = cur.lastrowid
        db.commit()
        db.close()

        # Publish to RabbitMQ
        rabbit = retry_connection(get_rabbit, "RabbitMQ")
        channel = rabbit.channel()
        channel.queue_declare(queue="order_queue", durable=True)
        channel.basic_publish(
            exchange="",
            routing_key="order_queue",
            body=json.dumps({"order_id": order_id, "product_id": pid, "quantity": qty, "user_id": order.get("user_id", 1)}),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        rabbit.close()

        return {"message": "Order accepted", "order_id": order_id}

    except HTTPException: raise
    except Exception as e:
        # Rollback Redis on failure
        r.incrby(f"stock:{pid}", qty)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/report")
def get_report():
    # MySQL
    mysql_data = {}
    try:
        db = retry_connection(get_mysql, "MySQL", max_retries=5)
        with db.cursor() as cur:
            # Query from 'orders' table
            cur.execute("SELECT product_id, SUM(quantity) as web_total FROM orders GROUP BY product_id")
            mysql_data = {str(row['product_id']): row['web_total'] for row in cur.fetchall()}
        db.close()
    except Exception as e: print(f"⚠️ MySQL error: {e}")

    # Postgres
    pg_data = {}
    try:
        db = retry_connection(get_pg, "PostgreSQL", max_retries=5)
        with db.cursor() as cur:
            cur.execute("SELECT product_id, SUM(quantity) as finance_total FROM finance_orders GROUP BY product_id")
            pg_data = {str(row['product_id']): row['finance_total'] for row in cur.fetchall()}
        db.close()
    except Exception as e: print(f"⚠️ Postgres error: {e}")

    # Stitch
    all_pids = set(list(mysql_data.keys()) + list(pg_data.keys()))
    return [{
        "sku": pid,
        "web_total": mysql_data.get(pid, 0),
        "finance_total": pg_data.get(pid, 0),
        "is_synced": mysql_data.get(pid, 0) == pg_data.get(pid, 0)
    } for pid in all_pids]
 
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
