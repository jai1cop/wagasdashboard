import streamlit as st, plotly.express as px, pandas as pd
from datetime import date
import data_fetcher as dfc

st.set_page_config("WA Gas Supply & Demand",layout="wide")

# ---------- sidebar ----------
st.sidebar.header("Scenario Controls")

# Yara demand toggle (0-100 TJ/d)
yara_val = st.sidebar.slider("Yara Pilbara Fertilisers gas consumption (TJ/day)",
                             0,100,80,5)

# Manual “Scrape Now”
if st.sidebar.button("Scrape latest AEMO files"):
    dfc.fetch_csv("mto_future",force=True)
    dfc.fetch_csv("flows",force=True)
    st.sidebar.success("Files updated")

sup, model = dfc.get_model()

# apply Yara adjustment
model_adj = model.copy()
model_adj["TJ_Demand"] += (yara_val-80)   # baseline 80 TJ/d

# ---------- SUPPLY STACK CHART ----------
stack = sup.pivot(index="GasDay",columns="FacilityName",values="TJ_Available")
stack = stack.loc[stack.index>=date.today()]

fig1 = px.area(stack,
               labels={"value":"TJ/day","GasDay":"Date","variable":"Facility"},
               title="Projected Supply by Facility (stacked)")
fig1.update_traces(hovertemplate="%{y:.0f} TJ<br>%{x|%d-%b-%Y}")

# ---------- DEMAND & SHORTFALL ----------
fig1.add_scatter(x=model_adj["GasDay"], y=model_adj["TJ_Demand"],
                 mode="lines", name="Historical / Forecast Demand",
                 line=dict(color="black",width=3))

# highlight shortfalls
short = model_adj[model_adj["Shortfall"]<0]
fig1.add_scatter(x=short["GasDay"], y=short["TJ_Demand"],
                 mode="markers", name="Shortfall",
                 marker=dict(color="red",size=6,symbol="x"))

st.plotly_chart(fig1,use_container_width=True)

# ---------- GAP BAR ----------
fig2 = px.bar(model_adj, x="GasDay", y="Shortfall",
              color=model_adj["Shortfall"]<0,
              color_discrete_map={True:"red",False:"green"},
              labels={"Shortfall":"Supply-Demand Gap (TJ)","color":""},
              title="Daily Market Balance")
fig2.update_layout(showlegend=False)
st.plotly_chart(fig2,use_container_width=True)

# ---------- DATA TABLE ----------
st.subheader("Daily Balance Table")
st.dataframe(model_adj[["GasDay","TJ_Available","TJ_Demand","Shortfall"]]
             .rename(columns={"GasDay":"Date",
                              "TJ_Available":"Available Supply",
                              "TJ_Demand":"Demand"}))
