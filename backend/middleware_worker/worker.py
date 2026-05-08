import pika
import pymysql
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- CẤU HÌNH KẾT NỐI ---
MYSQL_CONFIG = {"host": "noah-mysql", "user": "root", "password": "", "database": "webstore", "cursorclass": pymysql.cursors.DictCursor}
PG_CONFIG = "host=noah-postgres dbname=finance_db user=postgres password=postgres"
RABBIT_HOST = "noah-rabbitmq"

# Hàm tự động kết nối lại (Retry Logic) giúp hệ thống không bị sập khi Database chưa khởi động xong
def retry_connection(func, service_name, max_retries=30, delay=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"⏳ [WORKER] {service_name} chưa sẵn sàng ({i+1}/{max_retries}). Đang thử lại...")
            time.sleep(delay)
    raise Exception(f"❌ [WORKER] Không thể kết nối tới {service_name}")

# --- HÀM CALLBACK XỬ LÝ KHI CÓ TIN NHẮN TỪ RABBITMQ ---
def callback(ch, method, properties, body):
    # Bước 1: Giải mã dữ liệu JSON nhận được từ hàng đợi
    data = json.loads(body)
    order_id = data.get("order_id")
    # Lấy mã sản phẩm và số lượng mua
    pid = data.get("product_id") or data.get("sku")
    qty = data.get("quantity")

    print(f"📦 [WORKER] Đang xử lý đồng bộ Đơn hàng #{order_id}...")

    # MODULE 2B: Giả lập thời gian xử lý nghiệp vụ phức tạp (nghỉ 2 giây)
    time.sleep(2)

    try:
        # BƯỚC 1: Ghi dữ liệu vào hệ thống PostgreSQL (Hệ thống Tài chính - Finance)
        pg_conn = retry_connection(lambda: psycopg2.connect(PG_CONFIG), "Postgres (Finance)")
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO finance_orders (order_id, product_id, quantity, user_id, total_price) VALUES (%s, %s, %s, %s, %s)",
                (order_id, pid, qty, data.get("user_id"), data.get("total_price"))
            )
        pg_conn.commit()
        pg_conn.close()

        # BƯỚC 2: Cập nhật trạng thái đơn hàng bên MySQL thành 'COMPLETED' (Đã hoàn thành)
        mysql_conn = retry_connection(lambda: pymysql.connect(**MYSQL_CONFIG), "MySQL (Web)")
        with mysql_conn.cursor() as cur:
            cur.execute("UPDATE orders SET status='COMPLETED' WHERE id=%s", (order_id,))
        mysql_conn.commit()
        mysql_conn.close()

        # Option 1: Hệ thống thông báo (Gửi Email thật)
        print(f"📧 [NOTIFY] Đang gửi Email xác nhận cho khách hàng...")
        try:
            sender_email = "levanthang0166@gmail.com"
            sender_password = "xubkfpcxlylmubiv"
            receiver_email = "levanthang0166@gmail.com" # Gửi cho chính mình để test
            
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = receiver_email
            msg['Subject'] = f"✅ Xác nhận đơn hàng #{order_id} thành công!"
            
            body_text = f"Xin chào,\n\nĐơn hàng #{order_id} của bạn đã được đối soát thành công và ghi nhận vào hệ thống kế toán.\n\nCảm ơn bạn đã mua sắm tại NOAH Retail!"
            msg.attach(MIMEText(body_text, 'plain', 'utf-8'))
            
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
            server.quit()
            print("   > Đã gửi Email thành công!")
        except Exception as e:
            print(f"   > Lỗi gửi Email: {e}")

        print(f"✅ [SUCCESS] Đơn hàng #{order_id} đã đồng bộ sang Finance & Gửi thông báo xong.")

        # BƯỚC 3: XÁC NHẬN THỦ CÔNG (Manual Ack) - Báo cho RabbitMQ biết đã xong để xóa đơn này khỏi hàng đợi
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"❌ [ERROR] Lỗi khi xử lý đơn #{order_id}: {e}")
        # Nếu lỗi (ví dụ Postgres sập), đẩy đơn hàng ngược lại hàng đợi để xử lý lại (Requeue)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# --- KHỞI CHẠY WORKER ---
def start_worker():
    # Kết nối tới RabbitMQ
    rabbit = retry_connection(lambda: pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST)), "RabbitMQ")
    channel = rabbit.channel()
    # Khai báo hàng đợi (durable=True để không mất tin nhắn khi sập server)
    channel.queue_declare(queue="order_queue", durable=True)
    # Giới hạn mỗi lần chỉ xử lý đúng 1 đơn hàng (Fair Dispatch)
    channel.basic_qos(prefetch_count=1)
    # Bắt đầu lắng nghe
    channel.basic_consume(queue="order_queue", on_message_callback=callback)

    print("🎧 [WORKER] Đang lắng nghe đơn hàng từ RabbitMQ...")
    channel.start_consuming()

if __name__ == "__main__":
    start_worker()
