# main.py
import argparse, os, boto3, json
import numpy as np
import pandas as pd
# ee.Authenticate()
# ee.Initialize(project='high-keel-462317-i5')


def init_gee_from_secret(secret_name):
    sm = boto3.client("secretsmanager")
    secret_str = sm.get_secret_value(SecretId=secret_name)["SecretString"]
    sa = json.loads(secret_str)
    # Write to /tmp for EE to use
    with open("/tmp/gee_sa.json", "w") as f:
        f.write(secret_str)
    import ee
    creds = ee.ServiceAccountCredentials(sa["client_email"], "/tmp/gee_sa.json")
    ee.Initialize(creds, project=sa.get("project_id"))

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--gee_secret_name", required=True)
partial_args, _ = parser.parse_known_args()
init_gee_from_secret(partial_args.gee_secret_name)


# init_gee_from_secret(os.environ["GEE_SECRET_NAME"])
import ee
from s2_clouds import *
from landsat_clouds import *
from recent_collections import *
from allmodel import *

def run(lat, lon, start_date, end_date, location, year, s3_bucket, s3_prefix="lst", tmp="/tmp"):
    point = [lon, lat]

    
    final_landsat8_collection, suitablel8_images, final_landsat9_collection, suitablel9_images, extent = mainl8l9(point, start_date, end_date)
    final_S2_collection, suitableS2_images = mainS2(point, start_date, end_date)

    l9_list, l8_list, c8_list, c9_list = getRecent(suitablel8_images, suitablel9_images, suitableS2_images)

    out_paths = []
    if len(l8_list) == 0 and len(l9_list) == 0:
        print("No Landsat matches. Proceeding with Sentinel-2 only fallback.")
        fallback_path = os.path.join(tmp, f"{location}_S2_only_stats_{year}.csv")
        sentinel_only_temperature_stats(suitableS2_images, extent, filename=fallback_path)
        out_paths.append(fallback_path)
        upload_files(out_paths, s3_bucket, f"{s3_prefix}/{location}")
        return

   
    stats8_downscale = np.array([])
    stats8_normal = np.array([])
    for i in range(len(l8_list)):
        stat_values, more_values = model(l8_list[i], c8_list[i], extent)
        print(stat_values)
        print(more_values)
        stats8_downscale = np.append(stats8_downscale, stat_values)
        stats8_normal = np.append(stats8_normal, more_values)
    #Write landsat 8 downscale
    stats = stats8_downscale.reshape(len(l8_list), 7)
    df = pd.DataFrame(stats, columns=['Landsat_Image_ID','Sentinel_Image_ID', 'Landsat_8_acquisition_date','Sentinel_2_acquisition_date','Max_Temp','Min_Temp','Mean_Temp'])
    path = os.path.join(tmp, f"{location}_stats8_downscale_{year}.csv")
    df.to_csv(path, index=False); out_paths.append(path)
    
    #Write landsat 8 normal
    stats = stats8_normal.reshape(len(l8_list), 7)
    df = pd.DataFrame(stats, columns=['Landsat Image ID','Sentinel Image ID','Landsat 8 acquisition date','Sentinel 2 acquisition date','Max Temp','Min Temp','Mean Temp'])
    path = os.path.join(tmp, f"{location}_stats8_normal_{year}.csv")
    df.to_csv(path, index=False); out_paths.append(path)

    
    stats9_downscale = np.array([])
    stats9_normal = np.array([])
    for i in range(len(l9_list)):
        stat_values, more_values = model(l9_list[i], c9_list[i], extent)
        print(stat_values)
        print(more_values)
        stats9_downscale = np.append(stats9_downscale, stat_values)
        stats9_normal = np.append(stats9_normal, more_values)

  
    print("stats8_downscale length:", stats8_downscale.size, "expected:", len(l8_list)*7)
    stats = stats9_downscale.reshape(len(l9_list), 7)
    df = pd.DataFrame(stats, columns=['Landsat Image ID','Sentinel Image ID', 'Landsat 9 acquisition date','Sentinel 2 acquisition date', 'Max Temp','Min Temp','Mean Temp'])

    path = os.path.join(tmp, f"{location}_stats9_downscale_{year}.csv")
    df.to_csv(path, index=False); out_paths.append(path)

    stats = stats9_normal.reshape(len(l9_list), 7)
    df = pd.DataFrame(stats, columns=['Landsat Image ID','Sentinel Image ID','Landsat 9 acquisition date','Sentinel 2 acquisition date','Max Temp','Min Temp','Mean Temp'])
    path = os.path.join(tmp, f"{location}_stats9_normal_{year}.csv")  # <- fixed name
    df.to_csv(path, index=False); out_paths.append(path)

    # upload to S3
    upload_files(out_paths, s3_bucket, f"{s3_prefix}/{location}")

def upload_files(paths, bucket, prefix):
    s3 = boto3.client("s3")
    for p in paths:
        key = f"{prefix}/{os.path.basename(p)}"
        s3.upload_file(p, bucket, key)
        print(f"Uploaded s3://{bucket}/{key}")

if __name__ == "__main__":
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
    
    



    run(
        lat=args.lat,
        lon=args.lon,
        start_date=args.start_date,
        end_date=args.end_date,
        location=args.location,
        year=args.year,                          # pass year into run
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix
    )