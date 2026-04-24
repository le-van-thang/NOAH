import pika
import pymysql
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import time

# Configs
MYSQL_CONFIG = {"host": "mysql", "user": "root", "password": "", "database": "webstore", "cursorclass": pymysql.cursors.DictCursor}
PG_CONFIG = "host=postgres dbname=finance_db user=postgres password=postgres"
RABBIT_HOST = "rabbitmq"

def retry_connection(func, service_name, max_retries=30, delay=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"⏳ [WORKER] {service_name} not ready ({i+1}/{max_retries}). Error: {e}")
            time.sleep(delay)
    raise Exception(f"❌ [WORKER] Could not connect to {service_name}")

def callback(ch, method, properties, body):
    data = json.loads(body)
    order_id = data.get("order_id")
    # Handle both 'product_id' and 'sku' for safety
    pid = data.get("product_id") or data.get("sku")
    qty = data.get("quantity")

    print(f"📦 [WORKER] Processing Order #{order_id}...")

    # MODULE 2B: Simulating complex processing delay (1-2s)
    time.sleep(2)

    try:
        # 1. Save to PostgreSQL (Finance System)
        pg_conn = retry_connection(lambda: psycopg2.connect(PG_CONFIG), "PostgreSQL")
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO finance_orders (order_id, product_id, quantity) VALUES (%s, %s, %s)",
                (order_id, pid, qty)
            )
        pg_conn.commit()
        pg_conn.close()

        # 2. Update MySQL status to COMPLETED (Module 2B requirement)
        mysql_conn = retry_connection(lambda: pymysql.connect(**MYSQL_CONFIG), "MySQL")
        with mysql_conn.cursor() as cur:
            cur.execute("UPDATE orders SET status='COMPLETED' WHERE id=%s", (order_id,))
        mysql_conn.commit()
        mysql_conn.close()

        # Option 1: Notification System (Simulated SMTP/Chat)
        print(f"📧 [NOTIFY] Sending confirmation to User ID {data.get('user_id', 1)}...")
        print(f"   > 'Xin chào, đơn hàng #{order_id} của bạn đã được xác nhận thành công!'")

        print(f"✅ [SUCCESS] Order #{order_id} synced to Finance. Notification sent.")

        # 3. MANUAL ACKNOWLEDGEMENT (Module 2B requirement)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"❌ [ERROR] Processing order #{order_id}: {e}")
        # Reject and requeue if transient error
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_worker():
    rabbit = retry_connection(lambda: pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST)), "RabbitMQ")
    channel = rabbit.channel()
    channel.queue_declare(queue="order_queue", durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="order_queue", on_message_callback=callback)

    print("🎧 [WORKER] Listening for orders...")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()
