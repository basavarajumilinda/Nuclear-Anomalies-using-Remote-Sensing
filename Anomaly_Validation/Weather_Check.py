
# pip install meteostat pandas requests numpy

import pandas as pd
import numpy as np
import requests
from meteostat import Point, Daily

# User inputs
# Analysis window 
WINDOW_START = pd.Timestamp("2024-07-03")
WINDOW_END   = pd.Timestamp("2024-07-14")

# Location (ZNPP / Enerhodar)
LAT, LON = 47.5067, 34.5851

# Baseline for climatology (used to compute July mean and P90 threshold)
BASE_START = pd.Timestamp("2010-07-01")   
BASE_END   = pd.Timestamp("2023-12-31")

# Percentile used for heatwave definition 
HEATWAVE_PERCENTILE = 95
HEATWAVE_MIN_CONSEC_DAYS = 3

# Output
OUT_CSV = r"D:/Dissertation-2542000/RP3/Thermal/validation_2024_July_temp_only.csv"

def fetch_meteostat_tmax(lat, lon, start, end):
    
    try:
        site = Point(lat, lon)
        df = Daily(site, start, end, model=True).fetch()
        if df is None or df.empty:
            return pd.DataFrame(columns=["Date", "air_tmax_C"])
        out = df.reset_index()[["time", "tmax"]]
        out.columns = ["Date", "air_tmax_C"]
        return out
    except Exception:
        return pd.DataFrame(columns=["Date", "air_tmax_C"])

def fetch_openmeteo_era5_tmax(lat, lon, start, end, tz="Europe/Kyiv"):
    
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": f"{start:%Y-%m-%d}",
        "end_date": f"{end:%Y-%m-%d}",
        "daily": "temperature_2m_max",
        "timezone": tz,
    }
    try:
        r = requests.get("https://archive-api.open-meteo.com/v1/era5", params=params, timeout=60)
        if r.status_code != 200:
            return pd.DataFrame(columns=["Date", "air_tmax_C"])
        d = r.json().get("daily", {})
        return pd.DataFrame({
            "Date": pd.to_datetime(d.get("time", [])),
            "air_tmax_C": d.get("temperature_2m_max", [])
        })
    except Exception:
        return pd.DataFrame(columns=["Date", "air_tmax_C"])

def get_tmax_series(lat, lon, start, end):
    
    ms = fetch_meteostat_tmax(lat, lon, start, end)
    if not ms.empty:
        return ms.sort_values("Date")
    return fetch_openmeteo_era5_tmax(lat, lon, start, end).sort_values("Date")

def july_climatology_and_threshold(lat, lon, base_start, base_end, percentile):
    
    base = get_tmax_series(lat, lon, base_start, base_end)
    if base.empty:
        return np.nan, np.nan
    base["month"] = base["Date"].dt.month
    july = base[base["month"] == 7].dropna(subset=["air_tmax_C"])
    if july.empty:
        return np.nan, np.nan
    july_mean = float(july["air_tmax_C"].mean())
    july_pxx = float(np.nanpercentile(july["air_tmax_C"], percentile))
    return july_mean, july_pxx

def consecutive_runs(mask):
    
    m = mask.values.astype(int)
    runs = np.zeros_like(m)
    run_len = 0
    for i, v in enumerate(m):
        if v:
            run_len += 1
        else:
            run_len = 0
        runs[i] = run_len
    
    out = runs.copy()
    i = len(out) - 1
    while i >= 0:
        if out[i] > 0:
            l = out[i]
            out[i-l+1:i+1] = l
            i -= l
        else:
            i -= 1
    return pd.Series(out, index=mask.index)


# 1) Fetch daily Tmax for window
window = get_tmax_series(LAT, LON, WINDOW_START, WINDOW_END)

# 2) Compute July climatology and  threshold from baseline
july_mean_C, july_pxx_C = july_climatology_and_threshold(
    LAT, LON, BASE_START, BASE_END, HEATWAVE_PERCENTILE
)

# 3) Build output with anomalies & flags
out = window.copy()
out["july_mean_C"] = july_mean_C
out["july_p95_C"] = july_pxx_C
out["anom_vs_july_mean_C"] = out["air_tmax_C"] - july_mean_C if pd.notna(july_mean_C) else np.nan
out["above_p95_flag"] = out["air_tmax_C"] > july_pxx_C if pd.notna(july_pxx_C) else False

# 4) Heatwave detection: ≥3 consecutive days above threshold
if out.empty or not np.isfinite(july_pxx_C):
    out["hw_run_len"] = np.nan
    out["heatwave_flag"] = False
else:
    runs = consecutive_runs(out["above_p95_flag"].astype(bool))
    out["hw_run_len"] = runs
    out["heatwave_flag"] = out["hw_run_len"] >= HEATWAVE_MIN_CONSEC_DAYS

# 5) Save & print concise summary
out.to_csv(OUT_CSV, index=False)

print(f"\nSaved: {OUT_CSV}")
print(out.to_string(index=False, justify='center'))

#  conclusion for the window:
if out["heatwave_flag"].any():
    max_run = int(out["hw_run_len"].max())
    first_hit = out.loc[out["hw_run_len"] >= HEATWAVE_MIN_CONSEC_DAYS, "Date"].min()
    last_hit  = out.loc[out["hw_run_len"] >= HEATWAVE_MIN_CONSEC_DAYS, "Date"].max()
    print(
        f"\nHeatwave detected (≥{HEATWAVE_MIN_CONSEC_DAYS} days above July P{HEATWAVE_PERCENTILE}). "
        f"Max consecutive run: {max_run} days. Period approx: {first_hit:%Y-%m-%d} → {last_hit:%Y-%m-%d}."
    )
else:
    print(
        f"\nNo heatwave by the chosen definition (≥{HEATWAVE_MIN_CONSEC_DAYS} days above July P{HEATWAVE_PERCENTILE})."
    )

if np.isfinite(july_mean_C) and np.isfinite(july_pxx_C):
    print(f"July mean Tmax (baseline): {july_mean_C:.1f} °C")
    print(f"July P{HEATWAVE_PERCENTILE} Tmax (baseline): {july_pxx_C:.1f} °C")
