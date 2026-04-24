import streamlit as st
import pandas as pd
import requests
import time
import plotly.express as px
import plotly.graph_objects as go
import pymysql
import redis

# ==========================================
# 💎 NOAH ULTRA-COMPACT PREMIUM UI v4.1 (Final Optimization)
# ==========================================
st.set_page_config(page_title="NOAH Command", page_icon="⚡", layout="wide")

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

# ==========================================
# LOGIC
# ==========================================
def get_report():
    try:
        res = requests.get("http://noah-order_api:8000/api/report", timeout=2)
        if res.status_code == 200: return pd.DataFrame(res.json())
    except: pass
    return pd.DataFrame(columns=["sku", "web_total", "finance_total"])

def get_stock(sku):
    try:
        r = redis.Redis(host="redis", port=6379, decode_responses=True)
        val = r.get(f"stock:{sku}")
        return val if val else 0
    except: return "N/A"

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### ⚡ NOAH CORE")
    st.markdown("---")
    
    if st.button("🚀 Seed 20k Orders (PENDING)", use_container_width=True):
        try:
            conn = pymysql.connect(host="mysql", user="root", password="", database="webstore")
            with conn.cursor() as cur:
                # DÙNG TRẠNG THÁI 'PENDING' ĐỂ CÓ THỂ SYNC SANG POSTGRES
                data = [(100 + (i % 200), (i % 5) + 1, (i % 1000), 'PENDING') for i in range(20000)]
                cur.executemany("INSERT INTO orders (product_id, quantity, user_id, status) VALUES (%s,%s,%s,%s)", data)
                conn.commit()
                st.success("Đã nạp 20,000 đơn hàng ở trạng thái PENDING.")
            conn.close()
        except Exception as e: st.error(e)

    if st.button("🔄 Sync All to Postgres", use_container_width=True):
        with st.spinner("Đang đồng bộ khối lượng lớn..."):
            try:
                res = requests.post("http://noah-order_api:8000/api/sync-all")
                st.success(res.json().get("message"))
            except: st.error("Sync API Unreachable")

    st.markdown("---")
    rows_per_page = st.number_input("Số dòng mỗi trang", 10, 100, 20)
    st.caption("v4.1 Final Gold Edition")

# ==========================================
# MAIN DASHBOARD
# ==========================================
df = get_report()
w_sum = df['web_total'].sum() if not df.empty else 0
f_sum = df['finance_total'].sum() if not df.empty else 0
integrity = (len(df[df['web_total'] == df['finance_total']]) / len(df) * 100) if len(df) > 0 else 0

st.markdown(f"""
    <div style="display: flex; gap: 15px; margin-bottom: 20px;">
        <div class="m-card"><div class="m-label">Web Orders</div><div class="m-value">{w_sum:,}</div></div>
        <div class="m-card"><div class="m-label">Finance Synced</div><div class="m-value" style="color:#00ff87">{f_sum:,}</div></div>
        <div class="m-card"><div class="m-label">Sync Integrity</div><div class="m-value" style="color:#f9d423">{integrity:.1f}%</div></div>
        <div class="m-card"><div class="m-label">Active SKUs</div><div class="m-value">{len(df):,}</div></div>
    </div>
""", unsafe_allow_html=True)

t1, t2, t3 = st.tabs(["📊 Đối soát", "🛒 Cửa hàng (Test Bán lố)", "🤖 AI Insight"])

with t1:
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("##### 📦 Real-time Data Reconciliation")
        if not df.empty:
            # TÍNH NĂNG PHÂN TRANG (PAGINATION) - THỬ THÁCH ĐIỂM 10
            total_pages = max(1, len(df)//rows_per_page + (1 if len(df)%rows_per_page > 0 else 0))
            current_page = st.number_input("Trang số (Thử thách Phân trang)", 1, total_pages, 1)
            
            df_view = df.iloc[(current_page-1)*rows_per_page : current_page*rows_per_page].copy()
            df_view['Status'] = df_view.apply(lambda r: "✅ OK" if r['web_total'] == r['finance_total'] else "❌ MISMATCH", axis=1)
            st.dataframe(df_view, use_container_width=True, height=400)
    with col2:
        st.markdown("##### 📉 Distribution")
        fig = px.pie(values=[f_sum, max(0, w_sum-f_sum)], names=['Synced', 'Pending'], hole=0.7)
        fig.update_layout(margin=dict(l=0,r=0,t=0,b=0), height=250, showlegend=False, paper_bgcolor='rgba(0,0,0,0)', template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

with t2:
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### 🛒 Order Terminal")
        sku_list = df['sku'].astype(str).tolist() if not df.empty else ["100"]
        t_sku = st.selectbox("Chọn SKU", sku_list)
        stock = get_stock(t_sku)
        st.info(f"Tồn kho hiện tại trong Redis: **{stock}**")
        
        t_qty = st.number_input("Số lượng mua", 1, 1000000, 1)
        
        if st.button("💳 MUA NGAY", use_container_width=True):
            try:
                r = requests.post("http://noah-order_api:8000/api/orders", 
                               json={"product_id": t_sku, "quantity": t_qty, "user_id": 1},
                               headers={"apikey": "noah-secret-key"})
                if r.status_code == 200:
                    st.success(f"Thành công! ID: {r.json().get('order_id')}")
                    # REFRESH NGAY LẬP TỨC ĐỂ CẬP NHẬT TỒN KHO
                    time.sleep(1)
                    st.rerun()
                else: st.error(f"Thất bại: {r.json().get('detail')}")
            except Exception as e: st.error(e)

    with c2:
        st.markdown("### 📝 Nhật ký đồng bộ")
        st.code(f"""
[Thời gian thực] Trạng thái dịch vụ: Online
[Sync] Đang theo dõi {len(df)} SKUs
[Postgres] Finance DB đang hoạt động
[Security] Kong Gateway đang bảo vệ
        """, language="bash")

with t3:
    st.markdown("##### 🧠 Gemini Business Advisor")
    st.markdown(f"""
        <div style="background: rgba(56,189,248,0.1); padding:20px; border-radius:10px; border-left:5px solid #38bdf8;">
            <b>Chiến lược:</b> Bạn đang có {w_sum - f_sum} đơn hàng chưa đồng bộ. 
            Hãy nhấn nút <b>'Sync All to Postgres'</b> để khớp dữ liệu tài chính ngay lập tức.
        </div>
    """, unsafe_allow_html=True)

# Auto-refresh every 10s
time.sleep(10)
st.rerun()