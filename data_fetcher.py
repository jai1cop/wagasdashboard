import os, io, requests, zipfile, pandas as pd
from datetime import datetime, timedelta

GBB_BASE   = "https://nemweb.com.au/Reports/Current/GBB/"
FILES = {
    "flows"      : "GasBBActualFlowStorageLast31.CSV",
    "mto_future" : "GasBBMediumTermCapacityOutlookFuture.csv",
    "nameplate"  : "GasBBNameplateRatingCurrent.csv",
}

CACHE_DIR = "data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _download(fname):
    url = GBB_BASE + fname
    r = requests.get(url, timeout=40)
    r.raise_for_status()
    path = os.path.join(CACHE_DIR, fname)
    with open(path,"wb") as f: f.write(r.content)
    return path

def fetch_csv(key, force=False):
    fname = FILES[key]
    fpath = os.path.join(CACHE_DIR, fname)
    if force or not os.path.exists(fpath) or _stale(fpath):
        fpath = _download(fname)
    return pd.read_csv(fpath)

def _stale(path):
    return (datetime.utcnow() - datetime.utcfromtimestamp(os.path.getmtime(path))).days>0

# ---------- domain helpers ----------
def clean_nameplate(df):
    keep = df[df["FacilityType"]=="Production"]
    keep = keep[["FacilityName","NamePlateRating"]]
    return keep.rename(columns={"NamePlateRating":"TJ_Nameplate"})

def clean_mto(df):
    df["GasDay"] = pd.to_datetime(df["GasDay"])
    prod = df[df["FacilityType"]=="Production"]
    prod = prod[["FacilityName","GasDay","Capacity"]]
    return prod.rename(columns={"Capacity":"TJ_Available"})

def build_supply_profile():
    nameplate = clean_nameplate(fetch_csv("nameplate"))
    mto       = clean_mto(fetch_csv("mto_future"))
    supply    = mto.merge(nameplate,on="FacilityName",how="left")
    supply["TJ_Available"] = supply["TJ_Available"].fillna(supply["TJ_Nameplate"])
    return supply

def build_demand_profile():
    flows = fetch_csv("flows")
    flows["GasDay"] = pd.to_datetime(flows["GasDay"])
    demand_z = flows[(flows["ZoneType"]=="Demand") & (flows["ZoneName"]=="Whole WA")]
    demand   = demand_z.groupby("GasDay")["Quantity"].sum().reset_index()
    demand.rename(columns={"Quantity":"TJ_Demand"}, inplace=True)
    return demand

# ---------- master public call ----------
def get_model():
    sup = build_supply_profile()
    dem = build_demand_profile()
    total_sup = sup.groupby("GasDay")["TJ_Available"].sum().reset_index()
    model = dem.merge(total_sup,on="GasDay",how="left")
    model["Shortfall"] = model["TJ_Available"] - model["TJ_Demand"]
    return sup, model