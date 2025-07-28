import os
import requests
import pandas as pd
from datetime import datetime

# Base URL for AEMO Gas Bulletin Board reports
GBB_BASE = "https://nemweb.com.au/Reports/Current/GBB/"

FILES = {
    "flows": "GasBBActualFlowStorageLast31.CSV",
    "mto_future": "GasBBMediumTermCapacityOutlookFuture.csv",
    "nameplate": "GasBBNameplateRatingCurrent.csv",
}

CACHE_DIR = "data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _download(fname):
    try:
        url = GBB_BASE + fname
        response = requests.get(url, timeout=40)
        response.raise_for_status()

        text = response.text.strip().lower()
        if text.startswith("<!doctype html") or text.startswith("<html"):
            raise ValueError(f"{url} returned HTML page, not CSV data")

        path = os.path.join(CACHE_DIR, fname)
        with open(path, "wb") as f:
            f.write(response.content)
        return path

    except Exception as e:
        print(f"[ERROR] Failed to download {fname}: {e}")
        error_path = os.path.join(CACHE_DIR, fname)
        if os.path.exists(error_path):
            os.remove(error_path)
        raise

def _stale(path):
    if not os.path.exists(path):
        return True
    last_modified = datetime.utcfromtimestamp(os.path.getmtime(path))
    return (datetime.utcnow() - last_modified).days > 0

def fetch_csv(key, force=False):
    try:
        fname = FILES[key]
        fpath = os.path.join(CACHE_DIR, fname)
        
        if force or _stale(fpath):
            fpath = _download(fname)

        df = pd.read_csv(fpath)
        df.columns = df.columns.str.lower()
        return df

    except Exception as e:
        print(f"[ERROR] Could not load {key}: {e}")
        if key == "nameplate":
            return pd.DataFrame(columns=["facilityname", "facilitytype", "capacityquantity"])
        elif key == "mto_future":
            return pd.DataFrame(columns=["facilityname", "facilitytype", "fromgasdate", "outlookquantity"])
        elif key == "flows":
            return pd.DataFrame(columns=["gasdate", "facilityname", "facilitytype", "supply", "demand"])
        return pd.DataFrame()

def clean_nameplate(df):
    # Updated for actual column names: capacityquantity instead of nameplaterating
    required = {"facilityname", "facilitytype", "capacityquantity"}
    if not required.issubset(df.columns):
        print(f"[WARNING] Missing nameplate columns: {required - set(df.columns)}")
        return pd.DataFrame(columns=["FacilityName", "TJ_Nameplate"])

    prod = df[df["facilitytype"] == "production"].copy()
    prod = prod[["facilityname", "capacityquantity"]]
    prod.rename(columns={
        "facilityname": "FacilityName", 
        "capacityquantity": "TJ_Nameplate"
    }, inplace=True)
    return prod

def clean_mto(df):
    # Updated for actual column names: fromgasdate, outlookquantity
    required = {"facilityname", "facilitytype", "fromgasdate", "outlookquantity"}
    if not required.issubset(df.columns):
        print(f"[WARNING] Missing MTO columns: {required - set(df.columns)}")
        return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available"])

    df["fromgasdate"] = pd.to_datetime(df["fromgasdate"], errors="coerce")
    prod = df[df["facilitytype"] == "production"].copy()
    prod = prod[["facilityname", "fromgasdate", "outlookquantity"]].dropna(subset=["fromgasdate"])
    prod.rename(columns={
        "facilityname": "FacilityName",
        "fromgasdate": "GasDay", 
        "outlookquantity": "TJ_Available"
    }, inplace=True)
    return prod

def build_supply_profile():
    nameplate = clean_nameplate(fetch_csv("nameplate"))
    mto = clean_mto(fetch_csv("mto_future"))

    if nameplate.empty or mto.empty:
        print("[WARNING] Empty supply data")
        return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available", "TJ_Nameplate"])

    supply = mto.merge(nameplate, on="FacilityName", how="left")
    supply["TJ_Available"] = supply["TJ_Available"].fillna(supply["TJ_Nameplate"])
    return supply

def build_demand_profile():
    # Updated for actual flow data structure: gasdate, demand columns
    flows = fetch_csv("flows")
    required = {"gasdate", "facilityname", "demand"}
    if not required.issubset(flows.columns):
        print(f"[WARNING] Missing flow columns: {required - set(flows.columns)}")
        return pd.DataFrame(columns=["GasDay", "TJ_Demand"])

    flows["gasdate"] = pd.to_datetime(flows["gasdate"], errors="coerce")
    
    # Aggregate demand by date
    demand = flows.groupby("gasdate")["demand"].sum().reset_index()
    demand.rename(columns={"gasdate": "GasDay", "demand": "TJ_Demand"}, inplace=True)
    demand = demand.dropna(subset=["GasDay"])
    return demand

def get_model():
    sup = build_supply_profile()
    dem = build_demand_profile()

    if sup.empty or dem.empty:
        print("[WARNING] Incomplete data - returning empty")
        return sup, dem

    total_supply = sup.groupby("GasDay")["TJ_Available"].sum().reset_index()
    model = dem.merge(total_supply, on="GasDay", how="left")
    model["Shortfall"] = model["TJ_Available"] - model["TJ_Demand"]
    
    return sup, model
