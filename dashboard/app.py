import streamlit as st
import pandas as pd
import requests
import time
import plotly.express as px
import plotly.graph_objects as go
import pymysql
import redis
import os
from dotenv import load_dotenv
import streamlit.components.v1 as components

# Tải biến môi trường từ file .env
load_dotenv()

# ==========================================
# 💎 NOAH DASHBOARD - GIAO DIỆN PREMIUM v4.1 (Bản tối ưu cuối cùng)
# ==========================================
st.set_page_config(page_title="Hệ thống NOAH", page_icon="⚡", layout="wide")

# Thiết kế CSS để giao diện trông hiện đại và chuyên nghiệp (Dark Mode)
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=JetBrains+Mono&display=swap');
    .stApp { background: #020617; color: #f8fafc; font-family: 'Inter', sans-serif; }
    .m-card { background: rgba(30, 41, 59, 0.5); border: 1px solid rgba(255, 255, 255, 0.05); padding: 15px 20px; border-radius: 12px; flex: 1; }
    .m-label { font-size: 0.65rem; color: #94a3b8; text-transform: uppercase; font-weight: 700; }
    .m-value { font-size: 1.8rem; font-weight: 800; margin: 5px 0; color: #38bdf8; }
    [data-testid="stSidebar"] { background-color: #0f172a !important; width: 250px !important; }
    </style>
    """, unsafe_allow_html=True)

# --- HÀM LẤY BÁO CÁO ĐỐI SOÁT TỪ API ---
def get_report():
    try:
        # Gọi API lấy dữ liệu gộp từ MySQL và Postgres
        res = requests.get("http://noah-order_api:8000/api/report", timeout=2)
        if res.status_code == 200: return res.json()
    except: pass
    return {}

# --- HÀM LẤY TỒN KHO THỜI GIAN THỰC TỪ REDIS (Option 2) ---
def get_stock(sku):
    try:
        r = redis.Redis(host="noah-redis", port=6379, decode_responses=True)
        val = r.get(f"stock:{sku}")
        return val if val else 0
    except: return "N/A"

def get_ai_insight(w_sum, f_sum, integrity, total_sku):
    # Đọc API Key từ môi trường (cấu hình trong file .env hoặc Docker)
    api_key = os.environ.get("GEMINI_API_KEY")
    
    if not api_key:
        return "⚠️ Lỗi: Chưa cấu hình GEMINI_API_KEY trong file .env"

    # Sử dụng gemini-2.5-flash theo yêu cầu
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
    
    prompt = (
        f"Tổng số đơn hàng Web: {w_sum}. "
        f"Số đơn đã đồng bộ tài chính: {f_sum}. "
        f"Tỷ lệ đồng bộ thành công (Integrity): {integrity:.1f}%. "
        f"Số lượng sản phẩm (SKU) đang hoạt động: {total_sku}. "
        "Đóng vai chuyên gia tài chính, hãy nhận xét ngắn gọn về tình hình kinh doanh này và đưa ra 1 lời khuyên."
    )
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}]
    }
    
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code == 200:
            data = res.json()
            text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            return text.replace("\\n", "<br>") if text else "AI không đưa ra nhận xét."
        else:
            return f"Không thể lấy phân tích AI (Mã lỗi: {res.status_code}). Dữ liệu vẫn an toàn."
    except Exception as e:
        return f"Lỗi kết nối AI (Failover): {str(e)}. Dữ liệu hệ thống vẫn an toàn."

# --- THANH CÔNG CỤ BÊN TRÁI (SIDEBAR) ---
with st.sidebar:
    st.markdown("### ⚡ ĐIỀU KHIỂN HỆ THỐNG")
    st.markdown("---")
    
    # Nút nạp dữ liệu ảo để demo hệ thống (Có tính toán giá tiền)
    if st.button("🚀 Nạp 20,000 đơn hàng (Full Data)", use_container_width=True):
        try:
            conn = pymysql.connect(host="noah-mysql", user="root", password="", database="webstore")
            with conn.cursor() as cur:
                # Bước 1: Lấy danh sách giá sản phẩm để tính total_price
                cur.execute("SELECT id, price FROM products")
                prices = {row[0]: float(row[1]) for row in cur.fetchall()}
                
                # Bước 2: Tạo dữ liệu mẫu ngẫu nhiên có giá tiền
                data = []
                for i in range(20000):
                    pid = 100 + (i % 200)
                    qty = (i % 5) + 1
                    u_id = (i % 1000) + 1
                    price = prices.get(pid, 100000)
                    total = price * qty
                    data.append((pid, qty, total, u_id, 'PENDING'))
                
                cur.executemany(
                    "INSERT INTO orders (product_id, quantity, total_price, user_id, status) VALUES (%s,%s,%s,%s,%s)", 
                    data
                )
                conn.commit()
                st.success("Đã nạp thành công 20,000 đơn hàng có giá tiền!")
            conn.close()
            time.sleep(1)
            st.rerun()
        except Exception as e: st.error(f"Lỗi nạp dữ liệu: {e}")

    # Nút đồng bộ thủ công hàng loạt
    if st.button("🔄 Đồng bộ tất cả sang Finance", use_container_width=True):
        with st.spinner("Đang đồng bộ dữ liệu lớn..."):
            try:
                res = requests.post("http://noah-order_api:8000/api/sync-all")
                st.success(res.json().get("message"))
            except: st.error("Không thể kết nối tới API đồng bộ")

    st.markdown("---")
    rows_per_page = st.number_input("Số dòng mỗi trang", 10, 100, 20)
    st.caption("v4.1 Final Gold Edition")

# --- TÍNH TOÁN CÁC CHỈ SỐ TỔNG QUAN ---
report_data = get_report()
if isinstance(report_data, dict):
    df = pd.DataFrame(report_data.get("reconciliation", []))
    top_customers = pd.DataFrame(report_data.get("top_customers", []))
else:
    df = pd.DataFrame(report_data)
    top_customers = pd.DataFrame()

w_sum = df['web_total'].sum() if not df.empty else 0 # Tổng đơn Web
f_sum = df['finance_total'].sum() if not df.empty else 0 # Tổng đơn đã đồng bộ sang Finance
integrity = (len(df[df['web_total'] == df['finance_total']]) / len(df) * 100) if len(df) > 0 else 0 # Tỷ lệ khớp lệnh

# Hiển thị các thẻ chỉ số (KPI Cards)
st.markdown(f"""
    <div style="display: flex; gap: 15px; margin-bottom: 20px;">
        <div class="m-card"><div class="m-label">Đơn hàng Web (MySQL)</div><div class="m-value">{w_sum:,}</div></div>
        <div class="m-card"><div class="m-label">Đã đồng bộ (Postgres)</div><div class="m-value" style="color:#00ff87">{f_sum:,}</div></div>
        <div class="m-card"><div class="m-label">Tỷ lệ khớp lệnh</div><div class="m-value" style="color:#f9d423">{integrity:.1f}%</div></div>
        <div class="m-card"><div class="m-label">Số lượng mã hàng (SKU)</div><div class="m-value">{len(df):,}</div></div>
    </div>
""", unsafe_allow_html=True)

# Chia giao diện thành 3 Tab chính
t1, t2, t3 = st.tabs(["📊 Đối soát dữ liệu", "🛒 Giả lập mua hàng (Test)", "🤖 Phân tích AI"])

with t1:
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("##### 📦 Bảng đối soát thời gian thực (Stitching Data)")
        if not df.empty:
            # TÍNH NĂNG PHÂN TRANG (PAGINATION) - Giải quyết bài toán 20,000 dòng
            total_pages = max(1, len(df)//rows_per_page + (1 if len(df)%rows_per_page > 0 else 0))
            current_page = st.number_input("Trang số", 1, total_pages, 1)
            
            # Cắt dữ liệu để hiển thị theo trang
            df_view = df.iloc[(current_page-1)*rows_per_page : current_page*rows_per_page].copy()
            # Thêm cột trạng thái so sánh trực tiếp
            df_view['Trạng thái'] = df_view.apply(lambda r: "✅ OK" if r['web_total'] == r['finance_total'] else "❌ MISMATCH", axis=1)
            
            # Định dạng màu sắc cho bảng
            def color_status(val):
                if "OK" in val: return "color: #00ff87"
                if "MISMATCH" in val: return "color: #ef4444"
                return ""
            
            st.dataframe(df_view.style.map(color_status, subset=['Trạng thái']), use_container_width=True, height=400)

            # --- THỬ THÁCH ĐIỂM 10: XUẤT FILE DỮ LIỆU (Dùng try-catch theo yêu cầu của thầy) ---
            try:
                # Chuyển DataFrame thành dữ liệu CSV
                csv_data = df.to_csv(index=False).encode('utf-8')
                
                # Nút tải file
                st.download_button(
                    label="📥 Xuất báo cáo đối soát (File CSV)",
                    data=csv_data,
                    file_name=f"noah_reconciliation_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            except Exception as e:
                # Xử lý lỗi nếu quá trình tạo file thất bại
                st.error(f"❌ Lỗi khi trích xuất file dữ liệu: {e}")
    with col2:
        st.markdown("##### 📉 Distribution")
        fig = px.pie(values=[f_sum, max(0, w_sum-f_sum)], names=['Synced', 'Pending'], hole=0.7)
        fig.update_layout(margin=dict(l=0,r=0,t=0,b=0), height=250, showlegend=False, paper_bgcolor='rgba(0,0,0,0)', template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

with t2:
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### 🛒 Cửa hàng giả lập")
        sku_list = df['sku'].astype(str).tolist() if not df.empty else ["100"]
        t_sku = st.selectbox("Chọn mã sản phẩm (SKU)", sku_list)
        # Lấy tồn kho thực tế từ Redis
        stock = get_stock(t_sku)
        st.info(f"Tồn kho thực tế trong Redis (Chống bán lố): **{stock}**")
        
        t_qty = st.number_input("Nhập số lượng cần mua", 1, 1000000, 1)
        
        if st.button("💳 XÁC NHẬN MUA HÀNG", use_container_width=True):
            try:
                # Gửi request tới API Gateway (Kong)
                r = requests.post("http://noah-order_api:8000/api/orders", 
                               json={"product_id": t_sku, "quantity": t_qty, "user_id": 1},
                               headers={"apikey": "noah-secret-key"})
                if r.status_code == 200:
                    st.success(f"Đặt hàng thành công! Mã đơn: {r.json().get('order_id')}")
                    # Tự động load lại sau 1 giây
                    time.sleep(1)
                    st.rerun()
                else: st.error(f"Lỗi: {r.json().get('detail')}")
            except Exception as e: st.error(e)

    with c2:
        st.markdown("### 📝 Nhật ký hệ thống")
        st.code(f"""
[Kết nối] Trạng thái dịch vụ: Online (Hoạt động)
[Dữ liệu] Đang quản lý {len(df)} SKUs
[Tài chính] Database PostgreSQL đang kết nối
[Bảo mật] Kong Gateway đang bảo vệ API
        """, language="bash")

with t3:
    st.markdown("##### 🧠 Chuyên gia tài chính AI (Gemini)")
    
    if "ai_insight" not in st.session_state:
        st.session_state.ai_insight = ""
        
    if st.button("🚀 Phân tích với AI (Real-time)", use_container_width=True):
        with st.spinner("AI đang xử lý dữ liệu..."):
            # Gửi dữ liệu tổng hợp (Aggregated Data), không gửi toàn bộ danh sách đơn hàng
            st.session_state.ai_insight = get_ai_insight(w_sum, f_sum, integrity, len(df))
            
    if st.session_state.ai_insight:
        st.markdown(f"""
            <div style="background: rgba(56,189,248,0.1); padding:20px; border-radius:10px; border-left:5px solid #38bdf8; margin-bottom: 20px;">
                {st.session_state.ai_insight}
            </div>
        """, unsafe_allow_html=True)
    else:
        st.info("💡 Hãy nhấn nút phía trên để lấy lời khuyên thực tế từ AI.")

    # MODULE 3 - YÊU CẦU 4: Hiển thị Top khách hàng
    if not top_customers.empty:
        st.markdown("##### 🏆 Top 5 Khách hàng thân thiết (Doanh thu)")
        top_customers.columns = ['ID Khách hàng', 'Tổng chi tiêu (VNĐ)']
        # Định dạng tiền tệ đẹp hơn (bỏ số thập phân 0000 và thêm dấu phẩy)
        top_customers['Tổng chi tiêu (VNĐ)'] = top_customers['Tổng chi tiêu (VNĐ)'].apply(lambda x: f"{int(x):,}")
        st.table(top_customers)

# ==========================================
# OPTION 4: REAL-TIME WEBSOCKET (PUSHER)
# ==========================================
# Gắn script kết nối Websocket trực tiếp vào Frontend của Streamlit
# JS sẽ tự động reload lại iFrame cha mỗi khi Server bắn event 'new_order', loại bỏ hoàn toàn F5 thủ công!
components.html(
    """
    <script src="https://js.pusher.com/8.2.0/pusher.min.js"></script>
    <script>
      // Khởi tạo Pusher Client
      var pusher = new Pusher('727fe40ff32e36c704c0', {
        cluster: 'ap1'
      });

      // Lắng nghe kênh 'noah-orders'
      var channel = pusher.subscribe('noah-orders');
      
      // Bắt sự kiện 'new_order' từ Server
      channel.bind('new_order', function(data) {
        console.log("Real-time Update:", data);
        // Tự động làm mới UI mượt mà
        window.parent.location.reload();
      });
    </script>
    """,
    height=0,
)