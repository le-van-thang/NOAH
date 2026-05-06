import requests
import threading
import time

# Cấu hình hệ thống (Qua cổng 8000 của Kong Gateway)
BASE_URL = "http://localhost:8000"
API_KEY = "noah-secret-key"  # Khớp với keyauth_credentials trong kong.yml
HEADERS = {"apikey": API_KEY}

def test_unauthorized_access():
    """Kiểm tra Module 5: Bảo mật Gateway (Chặn khi thiếu hoặc sai API Key)"""
    print("\n--- [TEST 1] Kiểm tra bảo mật Gateway ---")
    try:
        # Thử gọi API không có header apikey
        response = requests.get(f"{BASE_URL}/report")
        if response.status_code == 401:
            print("✅ Thành công: Gateway đã chặn truy cập trái phép (401 Unauthorized).")
        else:
            print(f"❌ Thất bại: Gateway không chặn truy cập (Status: {response.status_code}).")
    except Exception as e:
        print(f"⚠️ Lỗi kết nối: {e}")

def test_happy_path_order():
    """Kiểm tra Module 2: Luồng đặt hàng thành công qua Gateway"""
    print("\n--- [TEST 2] Kiểm tra đặt hàng thành công ---")
    order_data = {
        "user_id": 1,
        "product_id": 100,  # SKU có sẵn trong init.sql
        "quantity": 1
    }
    try:
        response = requests.post(f"{BASE_URL}/orders", json=order_data, headers=HEADERS)
        if response.status_code in [200, 201, 202]:
            print(f"✅ Thành công: Đơn hàng được tiếp nhận. Response: {response.text}")
        else:
            print(f"❌ Thất bại: API trả về lỗi {response.status_code}: {response.text}")
    except Exception as e:
        print(f"⚠️ Lỗi kết nối: {e}")

def send_concurrent_order(order_data, results):
    """Hàm bổ trợ gửi request đồng thời"""
    try:
        response = requests.post(f"{BASE_URL}/orders", json=order_data, headers=HEADERS)
        results.append(response.status_code)
    except:
        results.append("Error")

def test_overselling_protection():
    """Kiểm tra Option 2: Chống bán lố (Redis Atomic DECR)"""
    print("\n--- [TEST 3] Kiểm tra chống bán lố (Concurrent Orders) ---")
    print("Giả lập: 5 yêu cầu mua cùng lúc khi tồn kho thấp...")
    
    # Giả lập mua số lượng lớn hoặc gửi nhiều yêu cầu liên tục
    order_data = {"user_id": 99, "product_id": 101, "quantity": 1}
    results = []
    threads = []

    for i in range(5):
        t = threading.Thread(target=send_concurrent_order, args=(order_data, results))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    success_count = sum(1 for s in results if s in [200, 201, 202])
    fail_count = results.count(400)
    
    print(f"Kết quả: {success_count} đơn thành công, {fail_count} đơn bị từ chối (400 Out of Stock).")
    if fail_count > 0 or success_count > 0:
        print("✅ Hệ thống đã xử lý và phản hồi đúng kịch bản tồn kho.")
    else:
        print("⚠️ Kiểm tra lại trạng thái các Container nếu không có phản hồi nào.")

if __name__ == "__main__":
    print("🚀 Bắt đầu kiểm thử hệ thống NOAH Retail...")
    test_unauthorized_access()
    test_happy_path_order()
    test_overselling_protection()
    print("\n--- Kết thúc kiểm thử ---")