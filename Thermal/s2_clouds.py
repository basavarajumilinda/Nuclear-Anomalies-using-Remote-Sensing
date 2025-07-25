import ee
from datetime import datetime
#import geemap
#ee.Authenticate()
ee.Initialize(project='high-keel-462317-i5')

def makeRectangle(point):
    # Define a Point object.
    roi = ee.Geometry.Point(point[0], point[1])
    roiBuffer = roi.buffer(**{'distance': 2500}).bounds()
    return roiBuffer


# Define inputs for making a rectangle from center point.
point = [34.6118, 47.4984] # Zaporizhzhia NPP, Ukraine

#point=[34.8845, 50.9981] #Fordo NPP, Iran

# Map = geemap.Map(center=point, zoom=6)
# Map
today = datetime.today().strftime('%Y-%m-%d')
# Make a rectangle from center point.
extent = makeRectangle(point)

def cloudper(image):
    return image.reduceRegion(**{
    'reducer': ee.Reducer.mean(),
    'geometry': extent,
    'scale': 30,
    'maxPixels': 1e9
    })


def rename(image, newName):
    return image.select([0], [newName]).bitwiseAnd(1024).rightShift(10)
    

def maskS2clouds(image):
    qa = image.select('QA60')

    # Bits 10 and 11 are clouds and cirrus, respectively.
    # The << is the left shift bitwise operator in JavaScript.
    # It shifts the bits of a binary number to the left by a specified number of positions.
    cloudBitMask = 1 << 10
    cirrusBitMask = 1 << 11

    #Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloudBitMask).eq(0) \
            .And(qa.bitwiseAnd(cirrusBitMask).eq(0))
    image.updateMask(mask).divide(10000)
    
    clouds = rename(image, 'Clouds')

    return clouds


def mainS2():

    startdate = '2015-07-15'
    enddate= '2022-12-31'
    #Map the function over one month of data and take the median.
    #Load Sentinel-2 TOA reflectance data.
    dataset = ee.ImageCollection('COPERNICUS/S2') \
    .filterDate(startdate, enddate) \
    .filterBounds(extent) \
    .map(maskS2clouds)

    #rgbVis = {
    #'min': 0.0,
    #'max': 0.3,
    #'bands': ['B4', 'B3', 'B2'],
    #}

    #Map.setCenter(-9.1695, 38.6917, 12)
    #Map.addLayer(dataset.median(), rgbVis, 'RGB')

    L_List = dataset.toList(dataset.size())
    print('Before dataset size: ', dataset.size().getInfo())

    suitable_images = []
    for i in range(dataset.size().getInfo()):
        im = ee.Image(L_List.get(int(i)))
        cloudpercentage = cloudper(im)
        cloud_percent = cloudpercentage.get('Clouds').getInfo()
        if cloud_percent is not None and cloud_percent < 0.1:
            im_id = im.get('system:index').getInfo()
            im_id = 'COPERNICUS/S2/{}'.format(im_id)
            print(im_id)
            suitable_images.append(str(im_id))
    print(suitable_images)

    # suitable_images = []
    # for i in range(dataset.size().getInfo()):
    #     try:
    #         im = ee.Image(L_List.get(int(i)))
    #         cloudpercentage = cloudper(im)
    #         cloud_percent = cloudpercentage.get('Clouds').getInfo()
    #         print(f"[S2] Image {i} cloud %: {cloud_percent}")

    #         if cloud_percent is not None and cloud_percent < 0.5:
    #             im_id = im.get('system:index').getInfo()
    #             full_id = f'COPERNICUS/S2/{im_id}'
    #             suitable_images.append(full_id)
    #             print(f"[S2] Added: {full_id}")
    #     except Exception as e:
    #         print(f"[S2] Skipped image {i} due to error: {e}")
    #print(suitable_images)

    # sort the images by date and format them as ["","",""]
    suitable_images = sorted(suitable_images, key=lambda x: x.split('/')[2])
    

    final_S2_collection = ee.ImageCollection.fromImages(suitable_images)

    return final_S2_collection, suitable_images

