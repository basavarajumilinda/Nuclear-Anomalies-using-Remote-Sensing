from s2_clouds import *
from landsat_clouds import *
import ee
import numpy as np
from datetime import datetime
import pandas as pd
ee.Initialize(project='high-keel-462317-i5')

def getRecent(landsat8, landsat9, copernicus):
    landsat8_dates = [int(img.split('_')[-1]) for img in landsat8]
    landsat9_dates = [int(img.split('_')[-1]) for img in landsat9]
    copernicus_dates = [int(img.split('/')[2][0:8]) for img in copernicus]
    max_date_diff = 7
    

    suitablel8_pairs = []
    suitablel8_dates = []
    for c_date in copernicus_dates:
        c_date = datetime.strptime(str(c_date), '%Y%m%d').date()
        
        for l8_date in landsat8_dates:
            l8_date = datetime.strptime(str(l8_date), '%Y%m%d').date()
 
            date_diff = abs((l8_date - c_date)).days

            
            if date_diff < max_date_diff or max_date_diff == 0:
                land8_date = l8_date.strftime('%Y%m%d')
                cop_date = c_date.strftime('%Y%m%d')

                # find the index of l_date and c_date in landsat_dates and copernicus_dates
                l8_index = landsat8_dates.index(int(land8_date))
                c8_index = copernicus_dates.index(int(cop_date))
                
                # push the corresponding landsat and copernicus images to suitable_pairs
                suitablel8_pairs.append([landsat8[l8_index], copernicus[c8_index]])
                suitablel8_dates.append([l8_date, cop_date])

    suitablel9_pairs = []
    suitablel9_dates = []
    for c_date in copernicus_dates:
        c_date = datetime.strptime(str(c_date), '%Y%m%d').date()

        for l9_date in landsat9_dates: 
            l9_date = datetime.strptime(str(l9_date), '%Y%m%d').date()
            date_diff = abs((l9_date - c_date)).days

            
            if date_diff < max_date_diff or max_date_diff == 0:
                land9_date = l9_date.strftime('%Y%m%d')
                cop_date = c_date.strftime('%Y%m%d')

                # find the index of l_date and c_date in landsat_dates and copernicus_dates
                l9_index = landsat9_dates.index(int(land9_date))
                c9_index = copernicus_dates.index(int(cop_date))
                #print(l9_index, c_index)
                # push the corresponding landsat and copernicus images to suitable_pairs
                suitablel9_pairs.append([landsat9[l9_index], copernicus[c9_index]])
                suitablel9_dates.append([l9_date, cop_date])

    l8_list = [pair[0] for pair in suitablel8_pairs]
    c8_list = [pair[1] for pair in suitablel8_pairs]
    l9_list = [pair[0] for pair in suitablel9_pairs]
    c9_list = [pair[1] for pair in suitablel9_pairs]

    print(l8_list)
    print("                        ")
    print(c8_list)
    print("                        ")
    print(l9_list)
    print("                        ")
    print(c9_list)

    return l9_list, l8_list, c8_list, c9_list



def sentinel_only_temperature_stats(s2_image_ids, extent, filename='Iran_S2_only_stats.csv'):
    stats_s2_only = []
    for img_id in s2_image_ids:
        img = ee.Image(img_id).clip(extent)
        band = img.select('B11')

        stats = band.reduceRegion(
            reducer=ee.Reducer.minMax().combine(ee.Reducer.mean(), '', True),
            geometry=extent,
            scale=10,
            maxPixels=1e9
        ).getInfo()

        acquisition_date = img_id.split('/')[-1][:8]
        stats_s2_only.append([
            acquisition_date,
            stats.get('B11_max'),
            stats.get('B11_min'),
            stats.get('B11_mean')
        ])

    df = pd.DataFrame(stats_s2_only, columns=['Sentinel 2 acquisition date', 'Max Temp (SWIR)', 'Min Temp (SWIR)', 'Mean Temp (SWIR)'])
    df.to_csv(filename, index=False)
    print(f" Saved fallback stats to {filename}")

    #print('L8: ',suitablel8_dates)
    #print('L9: ',suitablel9_dates)
    ###########################################################################################
    # if len(suitablel8_pairs) == 0 and len(suitablel9_pairs) == 0:
    #     print('No suitable Landsat8/9 images found')
    #     return None, None, None
    # elif len(suitablel8_pairs) == 0 and len(suitablel9_pairs) != 0:
    #     recent_pair_l9 = suitablel9_pairs[-1]
    #     print(suitablel8_pairs)
    #     l9_collection_recentimg = ee.ImageCollection.fromImages(ee.Image(recent_pair_l9[0]))
    #     s2_collection_recentimg = ee.ImageCollection.fromImages(ee.Image(recent_pair_l9[1]))
        
    #     return l9_collection_recentimg,  s2_collection_recentimg, recent_pair_l9
    # elif len(suitablel8_pairs) != 0 and len(suitablel9_pairs) == 0:
    #     recent_pair_l8 = suitablel8_pairs[-1]
    #     print(suitablel9_pairs)
    #     l8_collection_recentimg = ee.ImageCollection.fromImages(ee.Image(recent_pair_l8[0]))
    #     s2_collection_recentimg = ee.ImageCollection.fromImages(ee.Image(recent_pair_l8[1]))
    #     return l8_collection_recentimg,  s2_collection_recentimg, recent_pair_l8
    # else:

    #     recent_pair_l8 = suitablel8_pairs[-1]
    #     recent_pair_l9 = suitablel9_pairs[-1]
    #     date8 = datetime.strptime(recent_pair_l8[0].split('_')[-1], '%Y%m%d').date()
    #     date9 = datetime.strptime(recent_pair_l9[0].split('_')[-1], '%Y%m%d').date()
        
    #     if date8 < date9:
    #         l9_collection_recentimg = ee.ImageCollection.fromImages(ee.Image(recent_pair_l9[0]))
    #         s2_collection_recentimg = ee.ImageCollection.fromImages(ee.Image(recent_pair_l9[1]))
    #         #print(suitablel9_pairs)
    #         l9_list = [pair[0] for pair in suitablel9_pairs]
    #         c_list = [pair[1] for pair in suitablel9_pairs]

    #         return l9_collection_recentimg,  s2_collection_recentimg, recent_pair_l9, suitablel9_pairs, l9_list, c_list
        
    #     if date8 > date9:
    #         l8_collection_recentimg = ee.ImageCollection.fromImages(ee.Image(recent_pair_l8[0]))
    #         s2_collection_recentimg = ee.ImageCollection.fromImages(ee.Image(recent_pair_l8[1]))
    #         #print(suitablel8_pairs)
    #         l8_list = [pair[0] for pair in suitablel8_pairs]
    #         c_list = [pair[1] for pair in suitablel8_pairs]
    #         #print(l8_list)
    #         #print(c_list)
    #         return l8_collection_recentimg,  s2_collection_recentimg, recent_pair_l8, suitablel8_pairs, l8_list, c_list
        

        ############################################################################################
        # split suitablel8_pairs into two lists, one for landsat8 and one for copernicus. Place at line 59
        #l8_list = [pair[0] for pair in suitablel8_pairs]
        #c_list = [pair[1] for pair in suitablel8_pairs]
        #l9_list = [pair[0] for pair in suitablel9_pairs]
        #c_list = [pair[1] for pair in suitablel9_pairs]
