import pandas as pd

# Load previously prepared datasets
landsat = pd.read_csv("D:/Dissertation-2542000/RP3/Thermal/Notebooks/stats_normal_2015_merged.csv")
downscaled = pd.read_csv("D:/Dissertation-2542000/RP3/Thermal/Notebooks/stats_downscale_2023_merged.csv")
constellr = pd.read_csv("D:/Dissertation-2542000/RP3/Thermal/Notebooks/zaporizhzhia_LST_summary.csv")
fusion = pd.read_csv("D:/Dissertation-2542000/RP3/Thermal/Notebooks/fusion_LST_metadata_summary.csv")  

# Parse dates
landsat['Date'] = pd.to_datetime(landsat['Landsat acquisition date'])
downscaled['Date'] = pd.to_datetime(downscaled['Sentinel 2 acquisition date'])
constellr['Date'] = pd.to_datetime(constellr['Date Folder'], format="%Y-%m-%d")
fusion['Date'] = pd.to_datetime(fusion['Date'], utc=True).dt.tz_localize(None)


print("Landsat Date dtype:", landsat['Date'].dtype)
print("Downscaled Date dtype:", downscaled['Date'].dtype)
print("Constellr Date dtype:", constellr['Date'].dtype)
print("Fusion Date dtype:", fusion['Date'].dtype)

# Compute ΔT for each dataset
landsat['DeltaT_Landsat'] = landsat['Max Temp'] - landsat['Mean Temp']
downscaled['DeltaT_GEE'] = downscaled['Max Temp'] - downscaled['Mean Temp']
constellr['DeltaT_Constellr'] = constellr['Max Temp'] - constellr['Mean Temp']
fusion['DeltaT_Fusion'] = fusion['lst_max'] - fusion['lst_mean']  # 

downscaled['L8_Date_for_S2']   = pd.to_datetime(downscaled['Landsat acquisition date'], errors='coerce')
downscaled['S2_L8_days_diff']  = (downscaled['Date'] - downscaled['L8_Date_for_S2']).dt.total_seconds() / 86400.0

# Keep only date and delta columns
landsat_subset = landsat[['Date']]
downscaled_subset = downscaled[['Date', 'DeltaT_GEE','L8_Date_for_S2', 'S2_L8_days_diff','Landsat Image ID','Sentinel Image ID']]
constellr_subset = constellr[['Date', 'DeltaT_Constellr']]
fusion_subset = fusion[['Date', 'DeltaT_Fusion']]  #

# Merge all datasets
df = pd.merge(landsat_subset, downscaled_subset, on='Date', how='outer')
df = pd.merge(df, constellr_subset, on='Date', how='outer')
df = pd.merge(df, fusion_subset, on='Date', how='outer')  # 
df = df.sort_values(by='Date').reset_index(drop=True)

desired_order = [
    'Date',
    'DeltaT_GEE',
    'DeltaT_Constellr',
    'DeltaT_Fusion',
    'L8_Date_for_S2', 
    'S2_L8_days_diff',
    'Landsat Image ID',
    'Sentinel Image ID'
]

# Keep only those that actually exist in df (prevents KeyError if missing)
df = df[[col for col in desired_order if col in df.columns]]

#   Save full merged dataset 
df.to_csv("D:/Dissertation-2542000/RP3/Thermal/All_Anomalies_Comparison_WithFusion.csv", index=False)

#   Filter anomalies (any ΔT exceeding threshold)  
threshold = 16.9296  
df_anomalies = df[
    (df['DeltaT_Constellr'] > threshold) |
    (df['DeltaT_GEE'] > threshold) |
    (df['DeltaT_Fusion'] > threshold)  # 
].sort_values(by='Date').reset_index(drop=True)

#   Save anomalies  
df_anomalies.to_csv("D:/Dissertation-2542000/RP3/Thermal/Only_Anomalies_Validated_WithFusion.csv", index=False)

#   Optional print preview  
print("  ALL TEMPERATURES (With Fusion)  ")
print(df.to_string(index=False))

print("\n  ONLY ANOMALIES EXCEEDING THRESHOLD (With Fusion)  ")
print(df_anomalies.to_string(index=False))
