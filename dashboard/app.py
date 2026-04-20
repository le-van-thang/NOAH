import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="NOAH Retail Dashboard", layout="wide")
st.title("📊 NOAH Retail Reconciliation Dashboard")

# ======================
# LOAD INVENTORY CSV
# ======================
INVENTORY_PATH = "/data/inventory.csv"

try:
    inventory = pd.read_csv(INVENTORY_PATH)
except FileNotFoundError:
    st.error(f"❌ Không tìm thấy file {INVENTORY_PATH}")
    st.stop()

# ======================
# LOAD REPORT FROM ORDER API
# ======================
try:
    res = requests.get("http://order_api:8000/api/report", timeout=5)
    res.raise_for_status()
    report_df = pd.DataFrame(res.json())
except Exception as e:
    st.error(f"❌ Không gọi được Order API: {e}")
    st.stop()

if report_df.empty:
    st.warning("⚠️ Chưa có đơn hàng nào")
    report_df = pd.DataFrame(columns=["sku", "total_sold"])

# ======================
# RECONCILIATION
# ======================
merged = inventory.merge(
    report_df,
    how="left",
    on="sku"
).fillna({"total_sold": 0})

merged["status"] = merged.apply(
    lambda r: "OK" if r["stock"] >= r["total_sold"] else "MISMATCH",
    axis=1
)

def color_row(row):
    if row.status == "OK":
        return ["background-color: #d4edda"] * len(row)
    return ["background-color: #f8d7da"] * len(row)

st.subheader("📦 Inventory vs Orders")
st.dataframe(
    merged.style.apply(color_row, axis=1),
    use_container_width=True
)
