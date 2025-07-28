import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import date

try:
    import data_fetcher as dfc
except Exception as e:
    st.error(f"Failed to import data_fetcher: {e}")
    st.stop()
