import os
import requests
import pandas as pd
from datetime import datetime

# Constants
GBB_BASE = "https://nemweb.com.au/Reports/Current/GBB/"
FILES = {
    "flows": "GasBBActualFlowStorageLast31.CSV",
    "mto_future": "GasBBMediumTermCapacityOutlookFuture.csv",
    "nameplate": "GasBBNameplateRatingCurrent.csv",
}

CACHE_DIR = "data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _download(fname):
    """Download file from AEMO GBB and save to cache dir"""
    try:
        url = GBB_BASE + fname
        response = requests.get(url, timeout=40)
        response.raise_for_status()
        path = os.path.join(CACHE_DIR, fname)
        with open(path, "wb") as f:
            f.write(response.content)
        return path
    except Exception as e:
        print(f"Error downloading {fname}: {e}")
        raise

def _stale(path):
    """Check if cached file is older than 1 day"""
    if not os.path.exists(path):
        return True
    last_modified = datetime.utcfromtimestamp(os.path.getmtime(path))
    return (datetime.utcnow() - last_modified).days > 0

def fetch_csv(key, force=False):
    """
    Fetch CSV for given dataset key, using cache unless stale or forced refresh.
    Returns a pandas DataFrame.
    """
    try:
        fname = FILES[key]
        fpath = os.path.join(CACHE_DIR, fname)
        if force or _stale(fpath):
            fpath = _download(fname)
        return pd.read_csv(fpath)
    except Exception as e:
        print(f"Error fetching '{key}': {e}")
        # Return empty DataFrame with expected columns to avoid downstream crashes
        if key == "nameplate":
            return pd.DataFrame(columns=["FacilityName", "FacilityType", "NamePlateRating"])
        elif key == "mto_future":
            return pd.DataFrame(columns=["FacilityName", "FacilityType", "GasDay", "Capacity"])
        elif key == "flows":
            return pd.DataFrame(columns=["GasDay", "ZoneType", "ZoneName", "Quantity"])
        else:
            return pd.DataFrame()

def clean_nameplate(df):
    """Return production facilities with facility name and nameplate rating as TJ_Nameplate"""
    if df.empty:
        return pd.DataFrame(columns=["FacilityName", "TJ_Nameplate"])
    prod = df[df["FacilityType"] == "Production"]
    prod = prod[["FacilityName", "NamePlateRating"]].copy()
    prod.rename(columns={"NamePlateRating": "TJ_Nameplate"}, inplace=True)
    return prod

def clean_mto(df):
    """Return medium-term capacity outlook data for production facilities"""
    if df.empty:
        return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available"])
    df["GasDay"] = pd.to_datetime(df["GasDay"])
    prod = df[df["FacilityType"] == "Production"]
    prod = prod[["FacilityName", "GasDay", "Capacity"]].copy()
    prod.rename(columns={"Capacity": "TJ_Available"}, inplace=True)
    return prod

def build_supply_profile():
    """Build supply profile by merging medium-term outlook with nameplate rating"""
    nameplate = clean_nameplate(fetch_csv("nameplate"))
    mto = clean_mto(fetch_csv("mto_future"))
    if nameplate.empty or mto.empty:
        print("Warning: Empty nameplate or medium-term capacity data")
        return pd.DataFrame(columns=["FacilityName", "GasDay", "TJ_Available", "TJ_Nameplate"])
    supply = mto.merge(nameplate, on="FacilityName", how="left")
    # Fill any missing available capacity with nameplate rating as fallback
    supply["TJ_Available"] = supply["TJ_Available"].fillna(supply["TJ_Nameplate"])
    return supply

def build_demand_profile():
    """Build historical demand profile aggregated by GasDay for WA
