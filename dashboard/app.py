import streamlit as st
import pandas as pd
import requests
import time

# Cấu hình trang Dashboard
st.set_page_config(page_title="NOAH Retail Dashboard", layout="wide")
st.title("📊 NOAH Retail Reconciliation Dashboard")

# ======================
# 1. LOAD INVENTORY CSV (Legacy Data)
# ======================
# Đường dẫn này phải khớp với Volume trong docker-compose
INVENTORY_PATH = "/data/inventory.csv"

try:
    inventory = pd.read_csv(INVENTORY_PATH)
    # Chuẩn hóa tên cột: Chuyển 'product_id' hoặc 'sku' thành 'sku' để merge
    if "product_id" in inventory.columns:
        inventory = inventory.rename(columns={"product_id": "sku"})
    if "quantity" in inventory.columns:
        inventory = inventory.rename(columns={"quantity": "stock"})
except FileNotFoundError:
    st.error(f"❌ Không tìm thấy file dữ liệu kho tại: {INVENTORY_PATH}")
    st.stop()
except Exception as e:
    st.error(f"❌ Lỗi khi đọc file CSV: {e}")
    st.stop()

# ======================
# 2. LOAD REPORT FROM ORDER API (Modern Data)
# ======================
report_df = pd.DataFrame()

# Cơ chế Retry: Thử kết nối 5 lần nếu API chưa sẵn sàng (Sửa lỗi Connection Refused)
with st.spinner("🔄 Đang kết nối tới hệ thống bán hàng..."):
    for i in range(5):
        try:
            # Gọi trực tiếp qua tên service trong mạng Docker
            res = requests.get("http://order_api:8000/api/report", timeout=5)
            res.raise_for_status()
            report_df = pd.DataFrame(res.json())
            break
        except Exception:
            if i < 4:
                st.warning(f"⏳ Order API chưa sẵn sàng, đang thử lại lần {i+1}/5...")
                time.sleep(3)
            else:
                st.error("❌ Không thể kết nối tới Order API sau nhiều lần thử.")
                st.info("💡 Mẹo: Hãy kiểm tra xem container order_api đã chạy chưa bằng lệnh 'docker ps'")
                st.stop()

# Nếu API trả về rỗng, tạo DataFrame trống với các cột cần thiết
if report_df.empty:
    st.warning("⚠️ Hiện tại chưa có đơn hàng nào trong hệ thống.")
    report_df = pd.DataFrame(columns=["sku", "total_sold"])

# ======================
# 3. RECONCILIATION (Đối soát dữ liệu)
# ======================
# Ghép dữ liệu Kho (MySQL/CSV) và dữ liệu Đã bán (Postgres/API)
merged = inventory.merge(
    report_df,
    how="left",
    on="sku"
).fillna({"total_sold": 0})

# Logic kiểm tra lỗi bán lố (Overselling)
merged["status"] = merged.apply(
    lambda r: "OK" if r["stock"] >= r["total_sold"] else "MISMATCH",
    axis=1
)

# Hàm tô màu bảng dựa trên trạng thái
def color_row(row):
    if row.status == "OK":
        return ["background-color: #d4edda; color: #155724"] * len(row) # Xanh lục
    return ["background-color: #f8d7da; color: #721c24"] * len(row) # Đỏ nhạt

st.subheader("📦 Inventory vs Orders (Đối soát kho và Bán hàng)")

# Hiển thị bảng dữ liệu với định dạng màu sắc
st.dataframe(
    merged.style.apply(color_row, axis=1),
    use_container_width=True
)

# Thống kê nhanh
col1, col2 = st.columns(2)
with col1:
    st.metric("Tổng mã hàng", len(merged))
with col2:
    mismatches = len(merged[merged["status"] == "MISMATCH"])
    st.metric("Số lỗi lệch kho (Mismatch)", mismatches, delta=-mismatches, delta_color="inverse")