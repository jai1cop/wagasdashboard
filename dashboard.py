import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import date

# Set Streamlit page config on app start (call only once)
st.set_page_config(page_title="WA Gas Supply & Demand", layout="wide")

# Robust import of data_fetcher module
try:
    import data_fetcher as dfc
except ImportError as e:
    st.error(f"Cannot import data_fetcher module: {e}")
    st.error("Please ensure data_fetcher.py is in the same directory as dashboard.py")
    st.stop()

# Cache gas data loading for performance (refresh every hour)
@st.cache_data(ttl=3600)
def load_gas_data():
    try:
        return dfc.get_model()
    except Exception as e:
        st.error(f"Error loading gas data: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Sidebar controls
st.sidebar.header("Scenario Controls")

# Yara Pilbara Fertilisers gas consumption slider (0-100 TJ/day)
yara_val = st.sidebar.slider(
    "Yara Pilbara Fertilisers gas consumption (TJ/day)",
    min_value=0, max_value=100, value=80, step=5,
    help="Adjust Yara's gas consumption to see effect on market balance"
)

# Manual data refresh button
if st.sidebar.button("Scrape latest AEMO files"):
    dfc.fetch_csv("mto_future", force=True)
    dfc.fetch_csv("flows", force=True)
    # Clear cached data to force reload
    st.cache_data.clear()
    st.sidebar.success("Data refreshed. Charts will update on next interaction.")

# Load data from cache or source
sup, model = load_gas_data()

# Abort if no data loaded
if sup.empty or model.empty:
    st.error("No data loaded — please check data source connectivity or try refreshing.")
    st.stop()

# Apply Yara gas consumption adjustment to demand
model_adj = model.copy()
model_adj["TJ_Demand"] = model_adj["TJ_Demand"] + (yara_val - 80)  # baseline is 80 TJ/day

# Supply stack: pivot for stacked area chart
stack = sup.pivot(index="GasDay", columns="FacilityName", values="TJ_Available")

# Filter supply to show from today onwards
today_dt = pd.to_datetime(date.today())
stack = stack.loc[stack.index >= today_dt]

# Create stacked area chart for supply + demand line + shortfall markers
fig1 = px.area(
    stack,
    labels={"value": "TJ/day", "GasDay": "Date", "variable": "Facility"},
    title="Projected Supply by Facility (stacked)"
)
fig1.update_traces(hovertemplate="%{y:.0f} TJ<br>%{x|%d-%b-%Y}")

# Add demand as a bold black line
fig1.add_scatter(
    x=model_adj["GasDay"], y=model_adj["TJ_Demand"],
    mode="lines", name="Historical / Forecast Demand",
    line=dict(color="black", width=3)
)

# Highlight shortfall days with red 'x' markers where supply < demand
shortfalls = model_adj[model_adj["Shortfall"] < 0]
fig1.add_scatter(
    x=shortfalls["GasDay"], y=shortfalls["TJ_Demand"],
    mode="markers", name="Shortfall",
    marker=dict(color="red", size=7, symbol="x"),
    hovertemplate="Shortfall: %{y:.0f} TJ<br>Date: %{x|%d-%b-%Y}"
)

# Display the supply/demand stacked area chart
st.plotly_chart(fig1, use_container_width=True)

# ---------------------------
# Supply-Demand Gap Bar Chart
# ---------------------------
fig2 = px.bar(
    model_adj,
    x="GasDay",
    y="Shortfall",
    color=model_adj["Shortfall"] < 0,
    color_discrete_map={True: "red", False: "green"},
    labels={"Shortfall": "Supply-Demand Gap (TJ)", "color": ""},
    title="Daily Market Supply-Demand Balance"
)
fig2.update_layout(showlegend=False)
st.plotly_chart(fig2, use_container_width=True)

# ---------------------------
# Data Table: Daily Balance
# ---------------------------
st.subheader("Daily Supply and Demand Balance Table")
st.dataframe(
    model_adj[["GasDay", "TJ_Available", "TJ_Demand", "Shortfall"]]
    .rename(columns={
        "GasDay": "Date",
        "TJ_Available": "Available Supply (TJ)",
        "TJ_Demand": "Demand (TJ)",
        "Shortfall": "Shortfall (TJ)"
    }),
    use_container_width=True
)
# Add this right after your imports in dashboard.py
import os
st.sidebar.write("Debug: Files in data_cache folder:")
cache_path = "data_cache"
if os.path.exists(cache_path):
    for file in os.listdir(cache_path):
        file_path = os.path.join(cache_path, file)
        size = os.path.getsize(file_path)
        st.sidebar.write(f"- {file}: {size} bytes")
else:
    st.sidebar.write("No data_cache folder found")
