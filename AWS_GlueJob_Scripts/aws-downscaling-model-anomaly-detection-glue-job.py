import argparse, io, json
import numpy as np
import pandas as pd
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from scipy.stats import lognorm

s3 = boto3.client("s3")


def write_csv_s3(df, bucket, key):
    s3.put_object(Bucket=bucket, Key=key, Body=df.to_csv(index=False).encode("utf-8"))

def get_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    raise KeyError(f"None of {names} in columns {list(df.columns)}")


def pdt(series, **kw):
    # short & safe datetime parse
    return pd.to_datetime(series, errors="coerce", **kw)

def compute_threshold_from_lognorm(delta_series):
    
    v = pd.to_numeric(delta_series, errors="coerce").astype(float)
    v = v[np.isfinite(v)]
    if v.size == 0:
        return np.nan, "no_valid_baseline"

    emp99 = float(np.nanpercentile(v, 99))
    pos = v[v > 0]
    if pos.size < 5:
        return emp99, "empirical_99"

    logs = np.log(pos)
    sigma = float(np.nanstd(logs))
    if not np.isfinite(sigma) or sigma <= 1e-12:
        return emp99, "empirical_99"

    mu = float(np.nanmean(logs))
    p99 = float(lognorm.ppf(0.99, s=sigma, scale=np.exp(mu)))
    if not np.isfinite(p99):
        return emp99, "empirical_99"
    return p99, "lognormal_99"


def send_anomaly_email_via_ses(df_anom, location, threshold, from_addr, to_addrs, ses_region):


    # Map desired output header -> actual column name in df 
    colmap = {}

    # 1) Date (S2 date from merged df)
    if "Date" in df_anom.columns:
        colmap["Date"] = "Date"

    # 2) ΔT columns
    for c in ["DeltaT_GEE", "DeltaT_Constellr", "DeltaT_Fusion"]:
        if c in df_anom.columns:
            colmap[c] = c

    # 3) Landsat date for S2 pairing
    if "L8_Date_for_S2" in df_anom.columns:
        colmap["L8_Date_for_S2"] = "L8_Date_for_S2"

    # 4) Image IDs (tolerate underscore variants but output canonical headers)
    if "Landsat Image ID" in df_anom.columns:
        colmap["Landsat Image ID"] = "Landsat Image ID"
    elif "Landsat_Image_ID" in df_anom.columns:
        colmap["Landsat Image ID"] = "Landsat_Image_ID"

    if "Sentinel Image ID" in df_anom.columns:
        colmap["Sentinel Image ID"] = "Sentinel Image ID"
    elif "Sentinel_Image_ID" in df_anom.columns:
        colmap["Sentinel Image ID"] = "Sentinel_Image_ID"

    # Build table in the desired order
    desired_order = [
        "Date", "DeltaT_GEE", "DeltaT_Constellr", "DeltaT_Fusion",
        "L8_Date_for_S2", "Landsat Image ID", "Sentinel Image ID"
    ]
    actual_order = [h for h in desired_order if h in colmap]
    if actual_order:
        source_cols = [colmap[h] for h in actual_order]
        table = df_anom.loc[:, source_cols].copy()
        table.columns = actual_order       # rename to canonical headers
    else:
        table = df_anom.copy()             

    #  format dates & round deltas 
    if "Date" in table.columns:
        table["Date"] = pd.to_datetime(table["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    if "L8_Date_for_S2" in table.columns:
        table["L8_Date_for_S2"] = pd.to_datetime(table["L8_Date_for_S2"], errors="coerce").dt.strftime("%Y-%m-%d")

    for c in ["DeltaT_GEE", "DeltaT_Constellr", "DeltaT_Fusion"]:
        if c in table.columns:
            table[c] = pd.to_numeric(table[c], errors="coerce").round(2)

    # 
    html_table = table.to_html(index=False, justify="left", border=0)
    subject = f"[Anomalies] {location}: {len(df_anom)} rows above ΔT ≥ {threshold:.2f}°C"
    html_body = f"""
    <html><body>
      <p>Hi,</p>
      <p>ΔT anomalies for <b>{location}</b> (threshold <b>{threshold:.2f} °C</b>):</p>
      {html_table}
      <p>CSV is attached.</p>
    </body></html>
    """
    text_body = f"{subject}\n\nSee attached CSV."

    # 
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)

    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    csv_bytes = table.to_csv(index=False).encode("utf-8")
    part = MIMEApplication(csv_bytes, _subtype="csv")
    part.add_header("Content-Disposition", "attachment",
                    filename=f"{location}_anomalies.csv")
    msg.attach(part)

    ses = boto3.client("ses", region_name=ses_region)
    ses.send_raw_email(
        Source=from_addr,
        Destinations=to_addrs,
        RawMessage={"Data": msg.as_string().encode("utf-8")},
    )


def main():
    ap = argparse.ArgumentParser()
    # keep your pipeline args, but only a few are used
    ap.add_argument("--gee_secret_name", required=True)
    ap.add_argument("--location", required=True)
    ap.add_argument("--lat", required=True)
    ap.add_argument("--lon", required=True)
    ap.add_argument("--start_date", required=True)
    ap.add_argument("--end_date", required=True)
    ap.add_argument("--send_email", action="store_true", default=False)
    ap.add_argument("--ses_region", default="eu-west-2")          
    ap.add_argument("--email_from", default=None)                
    ap.add_argument("--email_to",   default=None)                


    ap.add_argument("--normal_year", required=True)      
    ap.add_argument("--downscaled_year", required=True)  

    ap.add_argument("--s3_bucket", required=True)
    ap.add_argument("--s3_prefix", required=True)         
    ap.add_argument("--out_prefix", required=True)        
    ap.add_argument("--override_threshold", default=None) 


    args, _ = ap.parse_known_args()

    location   = args.location.strip()
    bucket     = args.s3_bucket.strip()
    prefix     = args.s3_prefix.strip("/")
    out_prefix = args.out_prefix.rstrip("/") + "/"
    nyear      = str(args.normal_year).strip()
    dyear      = str(args.downscaled_year).strip()

    # 
    landsat_uri = f"s3://{bucket}/{prefix}/{location}/{location}_stats_normal_{nyear}_merged.csv"
    down_uri    = f"s3://{bucket}/{prefix}/{location}/{location}_stats_downscale_{dyear}_merged.csv"
    const_uri   =  f"s3://{bucket}/{prefix}/{location}/{location}_LST_summary.csv"

    fusion_uri =f"s3://{bucket}/{prefix}/{location}/lst-fusion_zaporizhia_2024_metadata_summary.csv"


    print("Reading:")
    print(" ", landsat_uri)
    print(" ", down_uri)
    print(" ", const_uri)
    print(" ", fusion_uri)

    # 
    landsat = pd.read_csv(landsat_uri)
    down    = pd.read_csv(down_uri)
    const   = pd.read_csv(const_uri)
    fusion  = pd.read_csv(fusion_uri)

    # 
    landsat["Date"] = pdt(landsat[get_col(landsat, ["Landsat acquisition date","Landsat_8_acquisition_date"])])
    down["Date"]    = pdt(down[get_col(down, ["Sentinel 2 acquisition date","Sentinel_2_acquisition_date"])])
    # Constellr "Date Folder" sometimes saved as YYYY-MM-DD or DD-MM-YYYY
    d1 = pdt(const[get_col(const, ["Date Folder"])], format="%Y-%m-%d")
    if d1.isna().all():
        d1 = pdt(const["Date Folder"], dayfirst=True)  # handles DD-MM-YYYY
    const["Date"] = d1
    fusion["Date"] = pdt(fusion["Date"], utc=True).dt.tz_convert(None)

    # ΔT columns
    landsat["DeltaT_Landsat"]   = pd.to_numeric(landsat[get_col(landsat, ["Max Temp","Max_Temp"])], errors="coerce") \
                                - pd.to_numeric(landsat[get_col(landsat, ["Mean Temp","Mean_Temp"])], errors="coerce")
    down["DeltaT_GEE"]          = pd.to_numeric(down[get_col(down, ["Max Temp","Max_Temp"])], errors="coerce") \
                                - pd.to_numeric(down[get_col(down, ["Mean Temp","Mean_Temp"])], errors="coerce")
    const["DeltaT_Constellr"]   = pd.to_numeric(const[get_col(const, ["Max Temp"])], errors="coerce") \
                                - pd.to_numeric(const[get_col(const, ["Mean Temp"])], errors="coerce")
    fusion["DeltaT_Fusion"]     = pd.to_numeric(fusion["lst_max"], errors="coerce") \
                                - pd.to_numeric(fusion["lst_mean"], errors="coerce")

    # Downscaled L8 date + days diff (if Landsat date present)
    if "Landsat acquisition date" in down.columns:
        down["L8_Date_for_S2"] = pdt(down["Landsat acquisition date"])
    elif "Landsat_8_acquisition_date" in down.columns:
        down["L8_Date_for_S2"] = pdt(down["Landsat_8_acquisition_date"])
    else:
        down["L8_Date_for_S2"] = pd.NaT
    down["S2_L8_days_diff"] = (down["Date"] - down["L8_Date_for_S2"]).dt.total_seconds() / 86400.0

    landsat_subset = landsat[["Date"]]
    keep_ids = [c for c in ["Landsat Image ID","Sentinel Image ID",
                            "Landsat_Image_ID","Sentinel_Image_ID"] if c in down.columns]
    down_subset    = down[["Date","DeltaT_GEE","L8_Date_for_S2","S2_L8_days_diff", *keep_ids]]
    const_subset   = const[["Date","DeltaT_Constellr"]]
    fusion_subset  = fusion[["Date","DeltaT_Fusion"]]

    df = (landsat_subset
            .merge(down_subset,   on="Date", how="outer")
            .merge(const_subset,  on="Date", how="outer")
            .merge(fusion_subset, on="Date", how="outer")
            .sort_values("Date").reset_index(drop=True))

    
    desired = ["Date","DeltaT_GEE","DeltaT_Constellr","DeltaT_Fusion",
               "L8_Date_for_S2","S2_L8_days_diff","Landsat Image ID","Sentinel Image ID",
               "Landsat_Image_ID","Sentinel_Image_ID"]
    df = df[[c for c in desired if c in df.columns]]

    
    if args.override_threshold is not None:
        threshold = float(args.override_threshold)
        method = "override"
    else:
        threshold, method = compute_threshold_from_lognorm(landsat["DeltaT_Landsat"])
        if not np.isfinite(threshold):
            
            x = pd.to_numeric(landsat["DeltaT_Landsat"], errors="coerce")
            threshold = float(np.nanpercentile(x[np.isfinite(x)], 99)) if x.notna().any() else np.nan
            method = "empirical_99"

    print(f"\nBaseline ΔT count (non-NaN): {landsat['DeltaT_Landsat'].notna().sum()}")
    print(f"Threshold (ΔT) = {threshold:.4f} °C  | method={method}")

   
    all_key  = out_prefix + "All_Anomalies_Comparison_WithFusion.csv"
    write_csv_s3(df, bucket, all_key)

    
    conds = []
    if "DeltaT_Constellr" in df.columns:
        conds.append(pd.to_numeric(df["DeltaT_Constellr"], errors="coerce") > threshold)
    if "DeltaT_GEE" in df.columns:
        conds.append(pd.to_numeric(df["DeltaT_GEE"], errors="coerce") > threshold)
    if "DeltaT_Fusion" in df.columns:
        conds.append(pd.to_numeric(df["DeltaT_Fusion"], errors="coerce") > threshold)

    if conds:
        mask = conds[0]
        for c in conds[1:]:
            mask = mask | c
    else:
        mask = pd.Series(False, index=df.index)

    df_anom = df.loc[mask].sort_values("Date").reset_index(drop=True)

    anom_key = out_prefix + "Only_Anomalies_Validated_WithFusion.csv"
    write_csv_s3(df_anom, bucket, anom_key)

    
    print(f"\nRows in full merged: {len(df)}")
    print(f"Rows flagged as anomalies: {len(df_anom)}")

    
    thr_key = out_prefix + "threshold_99p.json"
    s3.put_object(
        Bucket=bucket, Key=thr_key,
        Body=json.dumps({
            "threshold_deltaT_celsius": threshold,
            "method": method,
            "inputs": {
                "landsat_uri": landsat_uri,
                "downscaled_uri": down_uri,
                "constellr_uri": const_uri,
                "fusion_uri": fusion_uri
            },
            "outputs": {"all": all_key, "anomalies": anom_key}
        }, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    print("\nWrote:")
    print(f" - s3://{bucket}/{all_key}")
    print(f" - s3://{bucket}/{anom_key}")
    print(f" - s3://{bucket}/{thr_key}")
    
        
    if args.send_email and args.email_from and args.email_to:
        to_list = [e.strip() for e in args.email_to.split(",") if e.strip()]
        try:
            send_anomaly_email_via_ses(
                df_anom=df_anom,
                location=location,
                threshold=threshold,
                from_addr=args.email_from,
                to_addrs=to_list,
                ses_region=args.ses_region,
            )
            print(f"Email sent to: {to_list}")
        except Exception as e:
            print(f"Email send failed: {e}")


if __name__ == "__main__":
    main()
