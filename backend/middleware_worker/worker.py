import pika
import psycopg2
import pymysql
import json
import time

# ===== RabbitMQ retry =====
for i in range(10):
    try:
        rabbit_conn = pika.BlockingConnection(
            pika.ConnectionParameters(host="rabbitmq")
        )
        channel = rabbit_conn.channel()
        channel.queue_declare(queue="orders")
        print("✅ RabbitMQ connected")
        break
    except:
        print("⏳ RabbitMQ not ready, retrying...")
        time.sleep(3)

# ===== PostgreSQL retry =====
for i in range(10):
    try:
        pg_conn = psycopg2.connect(
            host="postgres",
            database="finance_db",
            user="postgres",
            password="postgres"
        )
        print("✅ PostgreSQL connected")
        break
    except:
        print("⏳ PostgreSQL not ready, retrying...")
        time.sleep(3)

# ===== MySQL retry =====
for i in range(10):
    try:
        mysql_conn = pymysql.connect(
            host="mysql",
            user="root",
            password="",
            database="webstore"
        )
        print("✅ MySQL connected")
        break
    except:
        time.sleep(3)

def callback(ch, method, properties, body):
    data = json.loads(body)

    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transactions (order_id, product_id, quantity) VALUES (%s,%s,%s)",
            (data["order_id"], data["product_id"], data["quantity"])
        )
        pg_conn.commit()

    with mysql_conn.cursor() as cur:
        cur.execute(
            "UPDATE orders SET status='SYNCED' WHERE id=%s",
            (data["order_id"],)
        )
        mysql_conn.commit()

    print(f"✅ Order {data['order_id']} synced")

channel.basic_consume(
    queue="orders",
    on_message_callback=callback,
    auto_ack=True
)

print("🎧 Worker listening...")
channel.start_consuming()
