import os
import io
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta

# Constants defined at the top
GBB_BASE = "https://nemweb.com.au/Reports/Current/GBB/"
FILES = {
    "flows": "GasBBActualFlowStorageLast31.CSV",
    "mto_future": "GasBBMediumTermCapacityOutlookFuture.csv",
    "nameplate": "GasBBNameplateRatingCurrent.csv",
}

CACHE_DIR = "data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _download(fname):
    """Download file from AEMO GBB with error handling"""
    try:
        url = GBB_BASE + fname
        r = requests.get(url, timeout=40)
        r.raise_for_status()
        path = os.path.join(CACHE_DIR, fname)
        with open(path, "wb") as f:
            f.write(r.content)
        return path
    except Exception as e:
        print(f"Error downloading {fname}: {e}")
        raise

def _stale(path):
    """Check if cached file is older than 1 day"""
    return (datetime.utcnow() - datetime.utcfromtimestamp(os.path.getmtime(path))).days > 0

def fetch_csv(key, force=False):
    """Fetch CSV data with caching and error handling"""
    try:
        fname = FILES[key]
        fpath = os.path.join(CACHE_DIR, fname)
        
        if force or not os.path.exists(fpath) or _stale(fpath):
            fpath = _download(fname)
        
        return pd.read_csv(fpath)
    
    except Exception as e:
        print(f"Error fetching {key}: {e}")
        # Return empty DataFrame with expected columns as fallback
        if key == "nameplate":
            return pd.DataFrame(columns=["FacilityName", "FacilityType", "NamePlateRating"])
        elif key == "mto_future":
            return pd.DataFrame(columns=["FacilityName", "FacilityType", "GasDay", "Capacity"])
        elif key == "flows":
            return pd.DataFrame(columns=["GasDay", "ZoneType", "ZoneName", "Quantity"])
        else:
            return pd.DataFrame()

# ---------- domain helpers ----------
def clean_nameplate(df):
    """Extract production facility nameplate ratings"""
    if df.empty:
        return pd.DataFrame(columns=["FacilityName", "TJ_Nameplate"])
    
    keep = df[df["FacilityType"] == "Production"]
    keep = keep[["FacilityName", "NamePlateRating"]]
    return keep.rename(columns={"NamePlateRating": "TJ_Nameplate"})

def clean_mto(df):
    """Extract medium-term capacity outlook for production facilities"""
    if df.empty:
        return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available"])
    
    df["GasDay"] = pd.to_datetime(df["GasDay"])
    prod = df[df["FacilityType"] == "Production"]
    prod = prod[["FacilityName", "GasDay", "Capacity"]]
    return prod.rename(columns={"Capacity": "TJ_Available"})

def build_supply_profile():
    """Build complete supply profile with nameplate and constraint data"""
    try:
        nameplate = clean_nameplate(fetch_csv("nameplate"))
        mto = clean_mto(fetch_csv("mto_future"))
        
        if nameplate.empty or mto.empty:
            print("Warning: Empty nameplate or MTO data")
            return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available", "TJ_Nameplate"])
        
        supply = mto.merge(nameplate, on="FacilityName", how="left")
        supply["TJ_Available"] = supply["TJ_Available"].fillna(supply["TJ_Nameplate"])
        return supply
    
    except Exception as e:
        print(f"Error building supply profile: {e}")
        return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available", "TJ_Nameplate"])

def build_demand_profile():
    """Build demand profile from flow data"""
    try:
        flows = fetch_csv("flows")
        
        if flows.empty:
            print("Warning: Empty flows data")
            return pd.DataFrame(columns=["GasDay", "TJ_Demand"])
        
        flows["GasDay"] = pd.to_datetime(flows["GasDay"])
        demand_z = flows[(flows["ZoneType"] == "Demand") & (flows["ZoneName"] == "Whole WA")]
        demand = demand_z.groupby("GasDay")["Quantity"].sum().reset_index()
        demand.rename(columns={"Quantity": "TJ_Demand"}, inplace=True)
        return demand
    
    except Exception as e:
        print(f"Error building demand profile: {e}")
        return pd.DataFrame(columns=["GasDay", "TJ_Demand"])

# ---------- master public call ----------
def get_model():
    """Get complete supply-demand model"""
    try:
        sup = build_supply_profile()
        dem = build_demand_profile()
        
        if sup.empty or dem.empty:
            print("Warning: Empty supply or demand data")
            return pd.DataFrame(), pd.DataFrame()
        
        total_sup = sup.groupby("GasDay")["TJ_Available"].sum().reset_index()
        model = dem.merge(total_sup, on="GasDay", how="left")
        model["Shortfall"] = model["TJ_Available"] - model["TJ_Demand"]
        return sup, model
    
    except Exception as e:
        print(f"Error in get_model: {e}")
        return pd.DataFrame(), pd.DataFrame()
