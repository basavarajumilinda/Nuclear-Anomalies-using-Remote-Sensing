# fordo_anomaly_framework.py
# Train on past windows; evaluate on Downscaled/Constellr >= 2025-01-01.
# Constellr uses the "hires" baseline group learned from Downscaled (train <= 2024-12-31).

import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import genpareto
import warnings
import glob

# Optional rasters
try:
    import rasterio
    RASTER_OK = True
except Exception:
    RASTER_OK = False

#  USER INPUTS 
LANDSAT_XLS    = "D:/Dissertation-2542000/RP3/Thermal/Fordo_landsattabledata_updated.xlsx"    # 2015–2022
DOWNSCALED_XLS = "D:/Dissertation-2542000/RP3/Thermal/Fordo_downscaletabledata_updated.xlsx"  # 2023–2025
CONSTELLR_XLS  = "D:/Dissertation-2542000/RP3/Thermal/Fordo_constellrtabledata_updated.xlsx"  # 2025
WEATHER_CSV    = "D:/Dissertation-2542000/fordo_LST_weather_TEMPONLY_merged.csv"              # 


FORDO_DIR      = "D:/Dissertation-2542000/Fordo"


CONSTELLR_LST_COL   = "lst_path"
CONSTELLR_CLOUD_COL = "cloudmask_path"
CONSTELLR_NODATA    = 65535

# Normalization
RENAME_MAP = {
    "landsatacquisitiondate": "date",
    "sentinel2acquisitiondate": "date_s2",
    "maxtemp": "lst_max",
    "meantemp": "lst_mean",
    "mintemp": "lst_min",
    "diff_from_mean": "diff_from_mean",
}

# Train windows (no Constellr training)
SPLITS = {
    "landsat":    {"train_end": "2022-12-31"},
    "downscaled": {"train_end": "2024-12-31"},
    "constellr":  {"train_end": "1900-01-01"},  # force test-only
}

# Evaluation windows 
EVAL_FROM = {
    "downscaled": "2025-01-01",
    "constellr":  "2025-01-01",
}

# Baseline groups: Constellr shares the Downscaled (hires) baseline
BASELINE_GROUPS = {
    "landsat":    "landsat",
    "downscaled": "hires",
    "constellr":  "hires",
}

# Thresholds/options
Z_THRESHOLD     = 3.0
Z_GAP_THRESHOLD = 3.0
USE_EVT         = True


_to_dt = lambda x: pd.to_datetime(x, errors="coerce")

def _ensure_cols(df, cols):
    for c in cols: 
        if c not in df.columns: df[c] = np.nan
    return df

def _month(x):
    try: return _to_dt(x).month
    except: return np.nan

def _date_folder(base_dir, dt):
    d = _to_dt(dt)
    return Path(base_dir) / f"{d.day:02d}-{d.month:02d}-{d.year:04d}"

def _first_match(patterns):
    for p in patterns:
        hits = sorted(glob.glob(p))
        if hits: return hits[0]
    return None

def find_constellr_paths_for_date(obs_date):
    folder = _date_folder(FORDO_DIR, obs_date)
    if not folder.exists(): return (None, None)
    lst   = _first_match([str(folder/"*lst.tif"), str(folder/"*lst.tiff"), str(folder/"*_lst.tif*")])
    cloud = _first_match([
        str(folder/"*cloud_mask.tif"), str(folder/"*cloud_mask.tiff"),
        str(folder/"*cloud_mask*.tif*"), str(folder/"*cloud_mask.png"),
        str(folder/"*cloud_mask.jpg"), str(folder/"*cloud_mask.jpeg"),
    ])
    return (lst, cloud)

def p95_minus_median_from_raster(lst_path, cloudmask_path=None, nodata=None):
    if not RASTER_OK or not lst_path or not Path(lst_path).exists(): return np.nan
    with rasterio.open(lst_path) as src:
        a = src.read(1).astype("float32")
        nd = src.nodata if nodata is None else nodata
        if nd is not None: a = np.where(a == nd, np.nan, a)
    if cloudmask_path and Path(cloudmask_path).exists():
        try:
            with rasterio.open(cloudmask_path) as m: cm = m.read(1)
            a = np.where((cm != 0) | ~np.isfinite(a), np.nan, a)
        except Exception:
            pass
    if not np.isfinite(a).any(): return np.nan
    return float(np.nanpercentile(a, 95) - np.nanmedian(a))

# 1) Load 
def load_and_unify():
    dfs = []

    if Path(LANDSAT_XLS).exists():
        l8 = pd.read_excel(LANDSAT_XLS)
        l8.columns = [c.strip().lower().replace(" ","_") for c in l8.columns]
        l8 = l8.rename(columns={k:v for k,v in RENAME_MAP.items() if k in l8.columns})
        if "date" not in l8.columns and "date_s2" in l8.columns: l8["date"] = l8["date_s2"]
        l8["date"] = _to_dt(l8["date"])
        l8["sensor"] = "landsat"
        dfs.append(l8)

    if Path(DOWNSCALED_XLS).exists():
        ds = pd.read_excel(DOWNSCALED_XLS)
        ds.columns = [c.strip().lower().replace(" ","_") for c in ds.columns]
        ds = ds.rename(columns={k:v for k,v in RENAME_MAP.items() if k in ds.columns})
        if "date" not in ds.columns or ds["date"].isna().all():
            ds["date"] = _to_dt(ds.get("date_s2", np.nan))
        else:
            ds["date"] = _to_dt(ds["date"]).fillna(_to_dt(ds.get("date_s2", np.nan)))
        ds["sensor"] = "downscaled"
        dfs.append(ds)

    if Path(CONSTELLR_XLS).exists():
        cs = pd.read_excel(CONSTELLR_XLS)
        cs.columns = [c.strip().lower().replace(" ","_") for c in cs.columns]
        cs = cs.rename(columns={k:v for k,v in RENAME_MAP.items() if k in cs.columns})
        if "date" not in cs.columns:
            dcols = [c for c in cs.columns if "date" in c]
            if dcols: cs["date"] = _to_dt(cs[dcols[0]])
        else:
            cs["date"] = _to_dt(cs["date"])
        cs["sensor"] = "constellr"
        dfs.append(cs)

    if not dfs: raise FileNotFoundError("No input tables found.")
    df = pd.concat(dfs, ignore_index=True)
    df = _ensure_cols(df, ["date","date_s2","sensor","lst_max","lst_mean","lst_min","diff_from_mean"])

    # Observation date (for weather + seasonality)
    df["obs_date"] = df["date"]
    mask_ds = df["sensor"].eq("downscaled") & df["date_s2"].notna()
    df.loc[mask_ds, "obs_date"] = _to_dt(df.loc[mask_ds, "date_s2"])

    df["month"] = df["obs_date"].apply(_month)

    # Baseline group
    df["baseline_group"] = df["sensor"].map(BASELINE_GROUPS).fillna(df["sensor"])
    return df

#  2) Weather merge 
def attach_weather(df):
    if not Path(WEATHER_CSV).exists(): return df
    wx = pd.read_csv(WEATHER_CSV)
    wx.columns = [c.strip().lower().replace(" ","_") for c in wx.columns]

    # Try to locate the two date columns
    l8_date_col = next((c for c in ("landsatacquisitiondate","landsat_date","date") if c in wx.columns), None)
    s2_date_col = next((c for c in ("sentinel2acquisitiondate","s2_date") if c in wx.columns), None)

    out = df.copy()

    if l8_date_col and "air_tmax_c" in wx.columns:
        t = wx[[l8_date_col,"air_tmax_c"]].copy()
        t["obs_date"] = _to_dt(t[l8_date_col]); t = t.dropna(subset=["obs_date"])
        out = out.merge(t[["obs_date","air_tmax_c"]].drop_duplicates(), on="obs_date", how="left", suffixes=("",""))

    if s2_date_col and "air_tmax_c_s2" in wx.columns:
        t = wx[[s2_date_col,"air_tmax_c_s2"]].copy()
        t["obs_date"] = _to_dt(t[s2_date_col]); t = t.dropna(subset=["obs_date"])
        out = out.merge(t[["obs_date","air_tmax_c_s2"]].drop_duplicates(), on="obs_date", how="left", suffixes=("",""))

    
    # - Downscaled → prefer S2 temp, else Landsat temp
    # - Landsat & Constellr → Landsat temp
    out["air_tmax_c_used"] = np.where(
        out["sensor"].eq("downscaled"),
        out["air_tmax_c_s2"].fillna(out["air_tmax_c"]),
        out["air_tmax_c"]
    )
    return out

#  3) Train/Test flags
def sensor_split(df):
    df = df.copy()
    df["is_train"] = False
    for sensor, cfg in SPLITS.items():
        te = pd.Timestamp(cfg["train_end"])
        df.loc[(df["sensor"].eq(sensor)) & (df["obs_date"] <= te), "is_train"] = True
    # Constellr is already forced to False by train_end=1900-01-01
    # Eval mask: only Downscaled & Constellr from 2025-01-01+
    df["is_eval"] = False
    for s, start in EVAL_FROM.items():
        df.loc[(df["sensor"].eq(s)) & (df["obs_date"] >= pd.Timestamp(start)), "is_eval"] = True
    return df

#  4) ΔTrob from rasters (Constellr)
def compute_scene_metrics(df):
    df = df.copy()
    fallback = df["diff_from_mean"].astype(float) if "diff_from_mean" in df.columns else np.nan
    df["delt_rob"] = fallback

    cons = df["sensor"].eq("constellr")
    if cons.any():
        # Use provided path columns if present; else resolve from folders
        if CONSTELLR_LST_COL in df.columns:
            it = df.loc[cons, [CONSTELLR_LST_COL, CONSTELLR_CLOUD_COL]].itertuples(index=True, name=None)
            for idx, lst_p, cloud_p in it:
                v = p95_minus_median_from_raster(lst_p, cloud_p, CONSTELLR_NODATA)
                if np.isfinite(v): df.at[idx, "delt_rob"] = v
        else:
            for idx, row in df.loc[cons].iterrows():
                lst_p, cloud_p = find_constellr_paths_for_date(row["obs_date"])
                v = p95_minus_median_from_raster(lst_p, cloud_p, CONSTELLR_NODATA)
                if np.isfinite(v): df.at[idx, "delt_rob"] = v
    return df

#  Robust baselines by baseline_group 
def robust_baseline(df, col="delt_rob"):
    base = (df[df["is_train"]]
            .groupby(["baseline_group","month"])[col]
            .agg(med=lambda s: s.median(skipna=True),
                 mad=lambda s: (s - s.median(skipna=True)).abs().median(skipna=True))
            .reset_index()
            .rename(columns={"med":f"{col}_median_train","mad":f"{col}_mad_train"}))
    df = df.merge(base, on=["baseline_group","month"], how="left")
    df[f"z_{col}"] = (df[col] - df[f"{col}_median_train"]) / (1.4826*(df[f"{col}_mad_train"] + 1e-9))
    df["robust_z_flag"] = df[f"z_{col}"] >= Z_THRESHOLD
    return df

#  6) Weather-normalized gap (grouped by baseline_group) 
def weather_gap(df):
    if "air_tmax_c_used" not in df.columns:
        df["weather_norm_flag"] = False
        df["z_gap"] = np.nan
        return df
    df = df.copy()
    df["lst_air_gap"] = df["lst_max"] - df["air_tmax_c_used"]
    gap_base = (df[df["is_train"] & df["lst_air_gap"].notna()]
                .groupby(["baseline_group","month"])["lst_air_gap"]
                .agg(med=lambda s: s.median(skipna=True),
                     mad=lambda s: (s - s.median(skipna=True)).abs().median(skipna=True))
                .reset_index()
                .rename(columns={"med":"gap_med_train","mad":"gap_mad_train"}))
    df = df.merge(gap_base, on=["baseline_group","month"], how="left")
    df["z_gap"] = (df["lst_air_gap"] - df["gap_med_train"]) / (1.4826*(df["gap_mad_train"] + 1e-9))
    df["weather_norm_flag"] = df["z_gap"] >= Z_GAP_THRESHOLD
    return df

#  7) EVT tail modeling 
def evt_tail_flag(df, col="delt_rob", p_body=0.95, target_q=0.99):
    df = df.copy()
    thr = (df[df["is_train"]]
           .groupby(["baseline_group","month"])[col]
           .quantile(p_body).rename("u_thr").reset_index())
    df = df.merge(thr, on=["baseline_group","month"], how="left")

    thresholds = []
    for (grp, month), g in df[df["is_train"]].groupby(["baseline_group","month"]):
        u = g["u_thr"].iloc[0]
        x = g[col].dropna()
        exc = (x - u)[x > u]
        if len(exc) >= 30:
            c, loc, scale = genpareto.fit(exc, floc=0)
            p = (target_q - p_body) / (1 - p_body)
            yq = (-scale*np.log(1-p)) if abs(c) < 1e-6 else (scale/c)*((1-p)**(-c) - 1)
            q_abs = u + yq
        else:
            q_abs = np.nan
        thresholds.append({"baseline_group": grp, "month": month, f"{col}_evt_q": q_abs})

    qtab = pd.DataFrame(thresholds)
    df = df.merge(qtab, on=["baseline_group","month"], how="left")
    df["evt_tail_flag"] = (df[col] > df[f"{col}_evt_q"]).fillna(False)
    return df

# 8) Final score 
def final_score(df):
    cols = ["robust_z_flag","weather_norm_flag","evt_tail_flag"]
    for c in cols:
        if c not in df.columns: df[c] = False
    df["anomaly_score"] = df[cols].sum(axis=1)
    df["decision"] = np.where(df["anomaly_score"]>=2,"investigate",
                       np.where(df["anomaly_score"]==1,"low_interest","ignore"))
    return df

#  9) Run 
def main():
    df = load_and_unify()
    df = attach_weather(df)       # uses air_tmax_c (Landsat date) and air_tmax_c_s2 (S2 date)
    df = sensor_split(df)         # sets is_train and is_eval
    df = compute_scene_metrics(df)  # uses P95−median for Constellr when rasters available
    df = robust_baseline(df, col="delt_rob")
    df = weather_gap(df)
    if USE_EVT:
        df = evt_tail_flag(df, col="delt_rob", p_body=0.95, target_q=0.99)
    df = final_score(df)

    
    df.sort_values(["obs_date","sensor"]).to_csv("./fordo_anomaly_table_full.csv", index=False)

    df_eval = df[df["is_eval"]].copy()
    df_eval.sort_values(["obs_date","sensor"]).to_csv("./fordo_anomaly_eval_2025plus.csv", index=False)

    print("Saved: ./fordo_anomaly_table_full.csv")
    print("Saved: ./fordo_anomaly_eval_2025plus.csv")
    print("\nEVAL-ONLY (Downscaled & Constellr after 2025):")
    print(df_eval.groupby("sensor")["decision"].value_counts(dropna=False))

if __name__ == "__main__":
    main()
