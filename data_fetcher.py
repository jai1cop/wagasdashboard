import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import date

try:
    import data_fetcher as dfc
except Exception as e:
    st.error(f"Failed to import data_fetcher: {e}")
    st.stop()
# ...all your other code above...

def get_model():
    sup = build_supply_profile()
    dem = build_demand_profile()
    if sup.empty or dem.empty:
        print("Warning: Empty supply or demand data - model incomplete")
        return sup, dem
    total_supply = sup.groupby("GasDay")["TJ_Available"].sum().reset_index()
    model = dem.merge(total_supply, on="GasDay", how="left")
    model["Shortfall"] = model["TJ_Available"] - model["TJ_Demand"]
    return sup, model
