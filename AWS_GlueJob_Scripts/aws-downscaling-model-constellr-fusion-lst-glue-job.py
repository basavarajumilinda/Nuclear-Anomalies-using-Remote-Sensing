import argparse, os, boto3, json,io
import pandas as pd

full = argparse.ArgumentParser()
full.add_argument("--gee_secret_name", required=True)
full.add_argument("--location", required=True)
full.add_argument("--lat", type=float, required=True)
full.add_argument("--lon", type=float, required=True)
full.add_argument("--start_date", required=True)
full.add_argument("--end_date", required=True)
full.add_argument("--year", required=True)
full.add_argument("--s3_bucket", required=True)
full.add_argument("--s3_prefix", default="lst")
args, _ = full.parse_known_args()

lat=args.lat
lon=args.lon
start_date=args.start_date
end_date=args.end_date
location=args.location
year=args.year                          # pass year into run
s3_bucket=args.s3_bucket
s3_prefix=args.s3_prefix

BUCKET = s3_bucket
PREFIX = "Constellr_FusionLST/lst-fusion_zaporizhia_2024/"  
OUT_CSV = "lst-fusion_zaporizhia_2024_metadata_summary.csv"
NEEDLE = "metadata"   


s3 = boto3.client("s3")

def iter_keys(bucket, prefix):
    
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def main():
    rows = []
    print(f"Scanning s3://{BUCKET}/{PREFIX}")

    for key in iter_keys(BUCKET, PREFIX):
        fname = key.split("/")[-1]
        if not (fname.endswith(".json") and NEEDLE.lower() in fname.lower()):
            continue

        try:
            resp = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(resp["Body"].read().decode("utf-8"))

            stats = data.get("scene_statistics", {}) or {}
            date_str = data.get("scene_datetime")

            
            def k2c(v):
                return (float(v) - 273.15) if v is not None else None

            lst_min_c  = k2c(stats.get("lst_min"))
            lst_max_c  = k2c(stats.get("lst_max"))
            lst_mean_c = k2c(stats.get("lst_mean"))

            rows.append({
                "Date": pd.to_datetime(date_str) if date_str else pd.NaT,
                "lst_min":  lst_min_c,
                "lst_max":  lst_max_c,
                "lst_mean": lst_mean_c,
                "std_min":  stats.get("std_min"),
                "std_max":  stats.get("std_max"),
                "std_mean": stats.get("std_mean"),
                "SourceFile": fname,
            })
            print(f"  processed {fname}")
        except Exception as e:
            print(f"  error {fname}: {e}")

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Date").reset_index(drop=True)

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    out_key = f"{s3_prefix}/{location}/{OUT_CSV}"
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=csv_bytes)
    print(f"\n saved summary to s3://{BUCKET}/{out_key}")

if __name__ == "__main__":
    main()
