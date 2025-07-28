import os
import requests
import pandas as pd
from datetime import datetime

# Base URL for AEMO Gas Bulletin Board reports (current)
GBB_BASE = "https://nemweb.com.au/Reports/Current/GBB/"

# CSV filenames for key datasets
FILES = {
    "flows": "GasBBActualFlowStorageLast31.CSV",              # Historical daily flows and storage
    "mto_future": "GasBBMediumTermCapacityOutlookFuture.csv",  # Medium-term capacity outlook (future)
    "nameplate": "GasBBNameplateRatingCurrent.csv",            # Nameplate ratings of facilities
}

# Local cache directory for downloads
CACHE_DIR = "data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _download(fname):
    """
    Download CSV file from AEMO GBB website and save it locally.

    :param fname: filename string to download
    :return: local path to downloaded file
    """
    try:
        url = GBB_BASE + fname
        response = requests.get(url, timeout=40)
        response.raise_for_status()
        path = os.path.join(CACHE_DIR, fname)
        with open(path, "wb") as f:
            f.write(response.content)
        return path
    except Exception as e:
        print(f"[ERROR] Failed to download {fname}: {e}")
        raise

def _stale(path):
    """
    Check if a cached file is stale (older than 1 day).

    :param path: local file path
    :return: True if stale or does not exist, False otherwise
    """
    if not os.path.exists(path):
        return True
    last_modified = datetime.utcfromtimestamp(os.path.getmtime(path))
    return (datetime.utcnow() - last_modified).days > 0

def fetch_csv(key, force=False):
    """
    Retrieve CSV data by key, downloading if missing, stale, or force-refresh requested.

    :param key: dataset key in FILES dict ("flows", "mto_future", or "nameplate")
    :param force: if True, force download ignoring cache
    :return: pandas DataFrame with CSV data
    """
    try:
        fname = FILES[key]
        fpath = os.path.join(CACHE_DIR, fname)
        if force or _stale(fpath):
            fpath = _download(fname)
        df = pd.read_csv(fpath)
        # Normalize column names to lowercase for consistent access
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"[ERROR] Could not load data for '{key}': {e}")
        # Return empty DataFrame with appropriate columns to avoid downstream errors
        if key == "nameplate":
            return pd.DataFrame(columns=["facilityname", "facilitytype", "nameplaterating"])
        if key == "mto_future":
            return pd.DataFrame(columns=["facilityname", "facilitytype", "gasday", "capacity"])
        if key == "flows":
            return pd.DataFrame(columns=["gasday", "zonetype", "zonename", "quantity"])
        return pd.DataFrame()

def clean_nameplate(df):
    """
    Filter and clean nameplate rating data for production facilities.

    :param df: raw DataFrame from nameplate CSV
    :return: cleaned DataFrame with columns ["FacilityName", "TJ_Nameplate"]
    """
    required_cols = {"facilityname", "facilitytype", "nameplaterating"}
    if not required_cols.issubset(set(df.columns)):
        print(f"[WARNING] Missing columns in nameplate data: {required_cols - set(df.columns)}")
        return pd.DataFrame(columns=["FacilityName", "TJ_Nameplate"])
    prod = df[df["facilitytype"] == "production"].copy()
    prod = prod[["facilityname", "nameplaterating"]]
    prod.rename(columns={"facilityname": "FacilityName", "nameplaterating": "TJ_Nameplate"}, inplace=True)
    return prod

def clean_mto(df):
    """
    Filter and clean medium-term capacity outlook data for production facilities.

    :param df: raw DataFrame from medium-term capacity outlook CSV
    :return: cleaned DataFrame with columns ["FacilityName", "GasDay", "TJ_Available"]
    """
    required_cols = {"facilityname", "facilitytype", "gasday", "capacity"}
    if not required_cols.issubset(set(df.columns)):
        print(f"[WARNING] Missing columns in medium-term capacity data: {required_cols - set(df.columns)}")
        return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available"])
    
    df["gasday"] = pd.to_datetime(df["gasday"], errors="coerce")
    prod = df[df["facilitytype"] == "production"].copy()
    prod = prod[["facilityname", "gasday", "capacity"]]
    prod = prod.dropna(subset=["gasday"])
    prod.rename(columns={"facilityname": "FacilityName", "gasday": "GasDay", "capacity": "TJ_Available"}, inplace=True)
    return prod

def build_supply_profile():
    """
    Build the supply profile by merging nameplate ratings with medium-term outages.

    :return: DataFrame with FacilityName, GasDay, TJ_Available, TJ_Nameplate columns
    """
    nameplate = clean_nameplate(fetch_csv("nameplate"))
    mto = clean_mto(fetch_csv("mto_future"))
    if nameplate.empty or mto.empty:
        print("[WARNING] Empty nameplate or medium-term capacity outlook data.")
        return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available", "TJ_Nameplate"])
    supply = mto.merge(nameplate, on="FacilityName", how="left")
    # Fill missing available capacities with nameplate rating as fallback
    supply["TJ_Available"] = supply["TJ_Available"].fillna(supply["TJ_Nameplate"])
    return supply

def build_demand_profile():
    """
    Aggregate historical daily demand from flow data for whole WA demand zone.

    :return: DataFrame with GasDay and TJ_Demand columns
    """
    flows = fetch_csv("flows")
    required_cols = {"gasday", "zonetype", "zonename", "quantity"}
    if not required_cols.issubset(set(flows.columns)):
        print(f"[WARNING] Missing columns in flow data: {required_cols - set(flows.columns)}")
        return pd.DataFrame(columns=["GasDay", "TJ_Demand"])
    flows["gasday"] = pd.to_datetime(flows["gasday"], errors="coerce")
    # Filter demand zones for whole WA
    demand_zone = flows[(flows["zonetype"] == "demand") & (flows["zonename"] == "whole wa")]
    demand = demand_zone.groupby("gasday")["quantity"].sum().reset_index()
    demand.rename(columns={"gasday": "GasDay", "quantity": "TJ_Demand"}, inplace=True)
    demand = demand.dropna(subset=["GasDay"])
    return demand

def get_model():
    """
    Produce combined supply-demand DataFrame with shortfall calculation.

    :return: tuple (supply DataFrame, combined model DataFrame)
    """
    sup = build_supply_profile()
    dem = build_demand_profile()
    if sup.empty or dem.empty:
        print("[WARNING] Supply or demand data incomplete.")
        return sup, dem
    total_supply = sup.groupby("GasDay")["TJ_Available"].sum().reset_index()
    model = dem.merge(total_supply, on="GasDay", how="left")
    model["Shortfall"] = model["TJ_Available"] - model["TJ_Demand"]
    return sup, model
