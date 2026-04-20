from fastapi import FastAPI
import pymysql
import pika
import json
import time

app = FastAPI()

# ======================
# MySQL retry
# ======================
for _ in range(10):
    try:
        mysql_conn = pymysql.connect(
            host="mysql",
            user="root",
            password="",
            database="webstore",
            cursorclass=pymysql.cursors.DictCursor
        )
        print("✅ MySQL connected")
        break
    except:
        print("⏳ MySQL not ready, retrying...")
        time.sleep(3)

# ======================
# RabbitMQ retry
# ======================
for _ in range(10):
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

# ======================
# CREATE ORDER
# ======================
@app.post("/api/orders")
def create_order(order: dict):
    with mysql_conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO orders (product_id, quantity, status)
            VALUES (%s, %s, 'PENDING')
            """,
            (order["product_id"], order["quantity"])
        )
        mysql_conn.commit()
        order_id = cursor.lastrowid

    channel.basic_publish(
        exchange="",
        routing_key="orders",
        body=json.dumps({
            "order_id": order_id,
            "product_id": order["product_id"],
            "quantity": order["quantity"]
        })
    )

    return {"status": "OK", "order_id": order_id}

# ======================
# REPORT FOR DASHBOARD
# ======================
@app.get("/api/report")
def report():
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                product_id AS sku,
                SUM(quantity) AS total_sold
            FROM orders
            GROUP BY product_id
        """)
        rows = cursor.fetchall()

    return rows
