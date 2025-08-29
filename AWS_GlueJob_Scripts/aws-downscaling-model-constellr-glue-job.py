import argparse, os, boto3, json,io
import numpy as np
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
BASE_PREFIX = "Constellr_LST/Zaporizhzhia/"   
FILENAME_NEEDLE = "Z_lst"                     


s3 = boto3.client("s3")


try:
    import rasterio
    from rasterio.io import MemoryFile
    HAVE_RASTERIO = True
except Exception:
    import tifffile as tiff
    HAVE_RASTERIO = False

def list_subfolders(bucket, base_prefix):

    folders = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            folders.append(cp["Prefix"])
    
    def parse_date(pfx):
        name = pfx.rstrip("/").split("/")[-1]
        try: return pd.to_datetime(name, format="%d-%m-%Y")
        except: return pd.NaT
    folders.sort(key=parse_date)
    return folders

def list_matching_tiffs(bucket, folder_prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):
        for obj in page.get("Contents", []):
            fn = obj["Key"].split("/")[-1]
            if fn.lower().endswith(".tiff") and FILENAME_NEEDLE.lower() in fn.lower():
                keys.append(obj["Key"])
    return keys

def read_first_band(bucket, key):
    
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    if HAVE_RASTERIO:
        with MemoryFile(body) as mem, mem.open() as src:
            arr = src.read(1).astype("float32")
            return arr, src.nodata
    else:
        with tiff.TiffFile(io.BytesIO(body)) as tf:
            arr = tf.asarray().astype("float32")
            nodata = None
            try:
                tag = tf.pages[0].tags.get("GDAL_NODATA")
                if tag and tag.value not in (None, ""):
                    nodata = float(tag.value)
            except:
                pass
            return arr, nodata

def main():
    
    out_key = f"{s3_prefix}/{location}/{location}_LST_summary.csv"

    results = []
    folders = list_subfolders(BUCKET, BASE_PREFIX)
    print(f"Scanning s3://{BUCKET}/{BASE_PREFIX} — {len(folders)} folders")

    for folder in folders:
        date_folder = folder.rstrip("/").split("/")[-1]
        print(f"\nChecking: s3://{BUCKET}/{folder}")
        keys = list_matching_tiffs(BUCKET, folder)
        if not keys:
            print("  (no matching TIFFs)")
            continue

        for key in keys:
            fname = key.split("/")[-1]
            print(f"  Processing: {fname}")
            try:
                arr, nodata = read_first_band(BUCKET, key)
                if nodata is not None:
                    arr[arr == nodata] = np.nan
                lst_c = (arr * 0.01) - 273.15  # Kelvin scale factor 0.01 → °C

                results.append({
                    "Date Folder": date_folder,
                    "Filename": fname,
                    "Min Temp": float(np.nanmin(lst_c)),
                    "Max Temp": float(np.nanmax(lst_c)),
                    "Mean Temp": float(np.nanmean(lst_c)),
                })
            except Exception as e:
                print(f"  Error: {e}")
                results.append({
                    "Date Folder": date_folder,
                    "Filename": fname,
                    "Error": str(e),
                })

    df = pd.DataFrame(results)
    if not df.empty:
        df["Date Folder"] = pd.to_datetime(df["Date Folder"], format="%d-%m-%Y", errors="coerce")
        df = df.dropna(subset=["Date Folder"]).sort_values("Date Folder")

    # Write CSV to S3
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=csv_bytes)
    print(f"\n wrote s3://{BUCKET}/{out_key}")

if __name__ == "__main__":
    main()
