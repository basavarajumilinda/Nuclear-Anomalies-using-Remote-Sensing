import ee
import cv2
import os
import io
import requests
import time
import datetime
import numpy as np
#imports for cloud stuff 
ee.Initialize(project='high-keel-462317-i5')
# call the cloud masking landsat and sentinel functions and proceed with remaining single image collections
#Landsat, sentinel, geometry = clouds()


def model(Landsat_selected_dataset, selected_S2_collection, selected_geometry):
    # inputs of the function are the paired image sets.

    def applyScaleFactors(image):
        opticalBands = image.select('SR_B.').multiply(0.0000275).add(-0.2).multiply(10000)
        thermalBands = image.select('ST_B.*').multiply(0.00341802).add(149.0).subtract(273.15)
        return image.addBands(opticalBands, None, True) \
                    .addBands(thermalBands, None, True)
      
    L8_image = ee.Image(Landsat_selected_dataset)
    acquisition_date_landsat = (ee.Date(L8_image.get('system:time_start')).getInfo())['value']
    acquisition_date_landsat = datetime.datetime.fromtimestamp(acquisition_date_landsat/1000.0)
    acquisition_date_landsat = acquisition_date_landsat.strftime('%Y-%m-%d')
    


    # access the 'value' of {'type': 'Date', 'value': 1656750330567}




    print(acquisition_date_landsat)
    L8_image = applyScaleFactors(L8_image)

    L8_image = L8_image.clip(selected_geometry)

    #Landsat 8 NDVI (30m spatial resolution)
    ndvi = L8_image.normalizedDifference(['SR_B5', 'SR_B4']).rename('ndvi')
    ndviParams = {'min': -1, 'max': 1, 'palette': ['purple', 'pink', 'green']}
    ndviclipped = ndvi.clip(selected_geometry)

    ndwi = L8_image.normalizedDifference(['SR_B3', 'SR_B5']).rename('ndwi')
    ndwiParams = {'min': -1, 'max': 1, 'palette': ['green', 'yellow', 'red', 'blue', 'navy']}
    ndwiclipped = ndwi.clip(selected_geometry)

    ndbi = L8_image.normalizedDifference(['SR_B6', 'SR_B5']).rename('ndbi')
    ndbiParams = {'min': -1, 'max': 1, 'palette': ['blue', 'yellow', 'red']}
    ndbiclipped = ndbi.clip(selected_geometry)

    
    #Calculate Landsat 8 LST in Celsius Degrees (30m spatial resolution))
    L8_LST_30m = L8_image.select('ST_B10').rename('L8_LST_30m')

    #min max L8_LST_30m
    min_normal = ee.Number(L8_LST_30m.reduceRegion(**{
                'reducer': ee.Reducer.min(),
                'scale': 30,
                'maxPixels': 1e9
                }).values().get(0))

    max_normal = ee.Number(L8_LST_30m.reduceRegion(**{
                'reducer': ee.Reducer.max(),
                'scale': 30,
                'maxPixels': 1e9
                }).values().get(0))
    
    mean_normal = ee.Number(L8_LST_30m.reduceRegion(**{
            'reducer': ee.Reducer.mean(),
            'scale': 30,
            'maxPixels': 1e9
            }).values().get(0))


    min = min_normal.getInfo()
    max = max_normal.getInfo()
    mean = mean_normal.getInfo()

    medianpixels = ee.Image(selected_S2_collection)
    acquisition_date_sentinel = (ee.Date(medianpixels.get('system:time_start')).getInfo())['value']
    acquisition_date_sentinel = datetime.datetime.fromtimestamp(acquisition_date_sentinel/1000.0)
    acquisition_date_sentinel = acquisition_date_sentinel.strftime('%Y-%m-%d')

    S2_image = medianpixels.clip(selected_geometry).divide(10000)

    dictionary_normal = {
        'Landsat Acquisition Date': acquisition_date_landsat,
        'Sentinel Acquisition Date': acquisition_date_sentinel,
        'L_LST Max Temp': max,
        'L_LST Min Temp': min,
        'L_LST Mean Temp': mean,
    }

    
    # feature = ee.Feature(None, dictionary)

    # #Wrap the Feature in a FeatureCollection for export.
    # featureCollection = ee.FeatureCollection([feature])

    # # Export the FeatureCollection to a KML file.
    # task = ee.batch.Export.table.toDrive(**{
    # 'collection': featureCollection,
    # 'description':'L_LST_{}_{}'.format(acquisition_date_landsat, acquisition_date_sentinel),
    # 'folder': 'Landsat_LST_Folder',
    # 'fileFormat': 'CSV'
    # })

    # task.start()
    # while task.active():
    #     print('Polling for task (id: {}).'.format(task.id))
    #     time.sleep(5)

    
    
    
    #Calculate Sentinel 2 spectral indices NDVI, NDWI and NDBI

    #Sentinel 2 NDVI (10m spatial resolution).
    S2_ndvi = S2_image.normalizedDifference(['B8', 'B4']).rename('S2_NDVI')
    S2_ndviParams4 = {'min': -1, 'max': 1, 'palette': ['purple', 'pink', 'green']}
    S2_ndviclipped = S2_ndvi.clip(selected_geometry)

    #Map.addLayer(S2_ndviclipped, S2_ndviParams4, 'S2_ndvi');

    #Sentinel 2 NDWI (10m spatial resolution).
    S2_ndwi = S2_image.normalizedDifference(['B3', 'B11']).rename('S2_NDWI')
    S2_ndwiParams4 = {'min': -1, 'max': 1, 'palette': ['green', 'yellow', 'red', 'blue', 'navy']}
    S2_ndwiclipped = S2_ndwi.clip(selected_geometry)

    #Map.addLayer(S2_ndwiclipped, S2_ndwiParams4, 'S2_ndwi');

    #Sentinel 2 NDBI (10m spatial resolution).
    S2_ndbi = S2_image.normalizedDifference(['B11', 'B8']).rename('S2_NDBI')
    S2_ndbiParams4 = {'min': -1, 'max': 1, 'palette': ['blue', 'yellow', 'purple']}
    S2_ndbiclipped = S2_ndbi.clip(selected_geometry)

    #Regression Calculation 
    
    #preparing bands
    bands = ee.Image(1).addBands(ndvi).addBands(ndbi).addBands(ndwi).addBands(L8_LST_30m).rename(["constant", "ndvi", "ndbi", "ndwi", "L8"])

    # run the multiple regression analysis
    imageRegression = bands.reduceRegion(**{
                        'reducer': ee.Reducer.linearRegression(**{'numX':4, 'numY':1}),
                        'geometry': selected_geometry,
                        'scale': 30,
                        })

    coefList2 = ee.Array(imageRegression.get("coefficients")).toList()
    intercept2 = ee.Image(ee.Number(ee.List(coefList2.get(0)).get(0)))
    intercept2_list = ee.List(coefList2.get(0)).get(0)
    slopeNDVI2 = ee.Image(ee.Number(ee.List(coefList2.get(1)).get(0)))
    slopeNDVI2_list =  ee.List(coefList2.get(1)).get(0)
    slopeNDBI2 = ee.Image(ee.Number(ee.List(coefList2.get(2)).get(0)))
    slopeNDBI2_list =  ee.List(coefList2.get(2)).get(0)
    slopeNDWI2 = ee.Image(ee.Number(ee.List(coefList2.get(3)).get(0)))
    slopeNDWI2_list =  ee.List(coefList2.get(3)).get(0)

    #calculate the final downscaled image
    downscaled_LST_10m = ee.Image(intercept2).add(slopeNDVI2.multiply(S2_ndvi)) \
                .add(slopeNDBI2.multiply(S2_ndbi)).add(slopeNDWI2.multiply(S2_ndwi))

    # L8-LST 30 m model calculation

    L8_LST_MODEL = intercept2.add(slopeNDVI2.multiply(ndvi)) \
                .add(slopeNDBI2.multiply(ndbi)) \
                .add(slopeNDWI2.multiply(ndwi)).clip(selected_geometry)

    L8_RESIDUALS = L8_LST_30m.subtract(L8_LST_MODEL)
    
    palette = ['040274', '040281', '0502a3', '0502b8', '0502ce', '0502e6', \
                '0602ff', '235cb1', '307ef3', '269db1', '30c8e2', '32d3ef', \
                #'3be285', '3ff38f', '86e26f', '3ae237', 'b5e22e', 
                'd6e21f', 'fff705', 'ffd611', 'ffb613', 'ff8b13', 'ff6e08', 'ff500d', \
                'ff0000', 'de0101', 'c21301', 'a71001', '911003']

    # Gaussian convolution

    # Define a gaussian kernel
    gaussian = ee.Kernel.gaussian(**{
        'radius': 1.5, 
        'units': 'pixels'
    })

    # Smooth the image by convolving with the gaussian kernel.
    L8_RESIDUALS_gaussian = L8_RESIDUALS.resample("bicubic").convolve(gaussian)

    visParam_residuals = {
        'min': -10,
        'max': 9,
        'palette': ['blue', 'yellow', 'red']
    }

    # Calculate the final downscaled LSTs
    downscaled_LST_10m2 = ee.Image(intercept2).add(slopeNDVI2.multiply(S2_ndvi)) \
                .add(slopeNDBI2.multiply(S2_ndbi)).add(slopeNDWI2.multiply(S2_ndwi))
    
    #Map.addLayer(downscaled_LST_10m2, lstParams2, 'S2-LST 10m (no residuals)');
    
    S2_LST_10_w_Residuals = downscaled_LST_10m2.add(L8_RESIDUALS_gaussian)

    S2_LST_10_w_Residuals = ee.Image(S2_LST_10_w_Residuals)

  
    #img = ee.ImageCollection.fromImages([S2_LST_10_w_Residuals]).first()

    # Convert img to ImageCollection
    #img_collection = ee.ImageCollection.fromImages(img)

    # Get the first image from the ImageCollection
    #img = img_collection.first()
    #print('bands: ', img.bandNames().getInfo())
    
    min_legend = ee.Number(S2_LST_10_w_Residuals.reduceRegion(**{
            'reducer': ee.Reducer.min(),
            'scale': 10,
            'maxPixels': 1e12,
            'crs': 'EPSG:4326'
            }).values().get(0))

    max_legend = ee.Number(S2_LST_10_w_Residuals.reduceRegion(**{
            'reducer': ee.Reducer.max(),
            'scale': 10,
            'maxPixels': 1e12,
            'crs': 'EPSG:4326'
            }).values().get(0))
    
    mean_legend = ee.Number(S2_LST_10_w_Residuals.reduceRegion(**{
        'reducer': ee.Reducer.mean(),
        'scale': 10,
        'maxPixels': 1e9
        }).values().get(0))

    min_temp = min_legend.getInfo()
    max_temp = max_legend.getInfo()
    mean_temp = mean_legend.getInfo()

    dictionary_downscale = {
        'Landsat Acquisition Date': acquisition_date_landsat,
        'Sentinel Acquisition Date': acquisition_date_sentinel,
        'Downscale Max Temp': max_temp,
        'Downscale Min Temp': min_temp,
        'Downscale Mean Temp': mean_temp,
    }

    arr_downscale = np.array([acquisition_date_landsat, acquisition_date_sentinel, max_temp, min_temp, mean_temp])
    arr_normal = np.array([acquisition_date_landsat, acquisition_date_sentinel, max, min, mean])

    return arr_downscale, arr_normal

    # feature = ee.Feature(None, dictionary)

    # #Wrap the Feature in a FeatureCollection for export.
    # featureCollection = ee.FeatureCollection([feature])

    # # Export the FeatureCollection to a CSV file.
    # task = ee.batch.Export.table.toDrive(**{
    # 'collection': featureCollection,
    # 'description':'DownscaledLST_{}_{}'.format(acquisition_date_landsat, acquisition_date_sentinel),
    # 'folder': 'Downscaled_LST_Folder',
    # 'fileFormat': 'CSV'
    # })
    # task.start()
    # while task.active():
    #     print('Polling for task (id: {}).'.format(task.id))
    #     time.sleep(5)





    # export_params = {
    #     "image": img,
    #     "description": 'Downscaled_LST_usingS2_10m_vscode',
    #     "folder": "image EE",
    #     "scale": 10,
    #     "region": selected_geometry,
    #     "crs": 'EPSG:4326',
    #     "maxPixels": 1e12,
    #     "fileFormat": 'GeoTIFF',
    #     "formatOptions": {
    #     "cloudOptimized": True
    #     }
    # }

    # # Export the image to Google Drive
    # task = ee.batch.Export.image.toDrive(**export_params)
    # task.start()

    # while task.active():
    #     print(task)
    #     print('Polling for task (id: {}).'.format(task.id))
    #     time.sleep(5)
    # img = ee.Image('COPERNICUS/S2/20220715T083609_20220715T083658_T36TXT')
    
    # # Single-band GeoTIFF files wrapped in a zip file.
    # url = img.getDownloadUrl({
    #     'name': 'single_band',
    #     'region': selected_geometry,
    # })
    # response = requests.get(url)
    # with open('single_band.zip', 'wb') as fd:
    #     fd.write(response.content)


    
 
    # response = requests.get(path)
    # with open('custom_single_band.zip', 'wb') as fd:
    #     fd.write(response.content)

