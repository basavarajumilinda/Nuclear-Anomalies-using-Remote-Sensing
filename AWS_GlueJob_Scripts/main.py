# main.py
import argparse, json, os, boto3
import numpy as np
import pandas as pd
import ee

from s2_clouds import mainS2
from landsat_clouds import mainl8l9
from recent_collections import getRecent, sentinel_only_temperature_stats
from allmodel import model

ee.Initialize(project='high-keel-462317-i5')

def run(lat, lon, start_date, end_date, location, s3_bucket, s3_prefix="lst", tmp="/tmp"):
    point = [lon, lat]

    # fetch collections with your new parameterized functions
    l8_coll, l8_ids, l9_coll, l9_ids, extent = mainl8l9(point, start_date, end_date)
    s2_coll, s2_ids, _ = mainS2(point, start_date, end_date)

    l9_list, l8_list, c8_list, c9_list = getRecent(l8_ids, l9_ids, s2_ids)

    if len(l8_list) == 0 and len(l9_list) == 0:
        print("No Landsat matches. Fallback to Sentinel-2 only.")
        sentinel_only_temperature_stats(s2_ids, extent,
            filename=os.path.join(tmp, f'{location}_S2_only_stats.csv'))
        _upload_dir(tmp, s3_bucket, f"{s3_prefix}/{location}")
        return

    # L8 pairs
    stats8_downscale, stats8_normal = [], []
    for ls_id, s2_id in zip(l8_list, c8_list):
        down, normal = model(ls_id, s2_id, extent)
        stats8_downscale.append(down); stats8_normal.append(normal)

    # L9 pairs
    stats9_downscale, stats9_normal = [], []
    for ls_id, s2_id in zip(l9_list, c9_list):
        down, normal = model(ls_id, s2_id, extent)
        stats9_downscale.append(down); stats9_normal.append(normal)

    # Save CSVs to /tmp then upload to S3
    def _save(arr_list, name):
        if not arr_list: return None
        df = pd.DataFrame(np.array(arr_list).reshape(len(arr_list), 7),
                          columns=['Landsat Image ID','Sentinel Image ID',
                                   'Landsat acquisition date','Sentinel 2 acquisition date',
                                   'Max Temp','Min Temp','Mean Temp'])
        path = os.path.join(tmp, f"{location}_{name}.csv")
        df.to_csv(path, index=False)
        return path

    out_paths = []
    for nm, arr in [
        ("stats8_downscale", stats8_downscale),
        ("stats8_normal",    stats8_normal),
        ("stats9_downscale", stats9_downscale),
        ("stats9_normal",    stats9_normal),
    ]:
        p = _save(arr, nm)
        if p: out_paths.append(p)

    _upload_files(out_paths, s3_bucket, f"{s3_prefix}/{location}")

def _upload_files(paths, bucket, prefix):
    s3 = boto3.client("s3")
    for p in paths:
        key = f"{prefix}/{os.path.basename(p)}"
        s3.upload_file(p, bucket, key)
        print(f"Uploaded s3://{bucket}/{key}")

def _upload_dir(directory, bucket, prefix):
    s3 = boto3.client("s3")
    for fn in os.listdir(directory):
        if fn.endswith(".csv"):
            s3.upload_file(os.path.join(directory, fn), bucket, f"{prefix}/{fn}")

if __name__ == "__main__":
    # Glue/Step Functions pass a JSON string to --params
    parser = argparse.ArgumentParser()
    parser.add_argument("--params", required=True)
    args = parser.parse_args()
    p = json.loads(args.params)
    run(
        lat=p["lat"],
        lon=p["lon"],
        start_date=p["start_date"],
        end_date=p["end_date"],
        location=p["location"],
        s3_bucket=p["s3_bucket"],
        s3_prefix=p.get("s3_prefix", "lst")
    )
