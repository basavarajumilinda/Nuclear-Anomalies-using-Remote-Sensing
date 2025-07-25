import ee
from datetime import datetime
#ee.Authenticate()
ee.Initialize(project='high-keel-462317-i5')



def makeRectangle(point):

    roi = ee.Geometry.Point(point[0], point[1])
    roiBuffer = roi.buffer(**{'distance': 2500}).bounds()
    return roiBuffer

# Here a point would be specified by a user/company which is directly on the main NPP facility.
# point = [34.6118, 47.4984] #Zaporizhzhia NPP,Ukraine
point = [34.6118, 47.4984]


#point=[50.9981,34.8845] # Fordo, Iran 


    # Make a rectangle from center point.
extent = makeRectangle(point)
# Make a rectangle from center point.
# extent = makeRectangle(point)
today = datetime.today().strftime('%Y-%m-%d')
startdate = '2015-07-15'
enddate= '2022-12-31'

# Applies scaling factors to the imagery
# Exact scaling factors are found from GEE documentation
def applyScaleFactors(image):
    opticalBands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    thermalBands = image.select('ST_B.*').multiply(0.00341802).add(149.0)
    return image.addBands(opticalBands, None, True).addBands(thermalBands, None, True)


def getQABits(image, start, end, newName):
    # Compute the bits we need to extract.
    pattern = 0
    for i in range(start, end+1):
        pattern += 2**i

    #Return a single band image of the extracted QA bits
    return image.select([0], [newName]).bitwiseAnd(pattern).rightShift(start)

def clouds(image):
  # Select the QA band.
  QA = image.select(['QA_PIXEL'])
  # Get the internal_cloud_algorithm_flag bit.
  return getQABits(QA, 6,6, 'Clouds').eq(0)


def cloudper(image):
    return image.reduceRegion(**{
    'reducer': ee.Reducer.mean(),
    'geometry': extent,
    'scale': 30,
    'maxPixels': 1e9
    })

def main(image):
    clipped = image.clip(extent)
    c = clouds(clipped)
    c1 = cloudper(c)
    return c1

def mainl8l9():
    # Here a point would be specified by a user/company which is directly on the main NPP facility.
    point = [34.6118, 47.4984] # Zaporizhzhia NPP, Ukraine

    #point=[50.9981,34.8845] # Fordo, Iran 


    # Make a rectangle from center point.
    extent = makeRectangle(point)
    
 
    

    startdate = '2015-07-15'
    enddate= '2022-12-31'

    dataset = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2') \
        .filterDate(startdate, enddate) \
        .filterBounds(extent) \
        .map(applyScaleFactors)


    L_List = dataset.toList(dataset.size())
    print('Before dataset size, landsat8: ', dataset.size().getInfo())

    suitablel8_images = []
    for i in range(dataset.size().getInfo()):
        im = ee.Image(L_List.get(int(i)))
        cloudpercentage = main(im)
        cloud_percent = cloudpercentage.get('Clouds').getInfo()
        if cloud_percent is not None and cloud_percent < 0.1:
            im_id = im.get('system:id').getInfo()
            print(im_id)
            suitablel8_images.append(str(im_id))

    suitablel8_images = sorted(suitablel8_images, key=lambda x: x.split('/')[-1].split('_')[2])
    print(suitablel8_images)
    final_landsat8_collection = ee.ImageCollection.fromImages(suitablel8_images)


    # suitablel8_images = []
    # for i in range(dataset.size().getInfo()):
    #     try:
    #         im = ee.Image(L_List.get(i))
    #         cloudpercentage = main(im)
    #         # cloud_percent = cloudpercentage.get('Clouds').getInfo()
    #         # print(f"[L8] Image {i} cloud %: {cloud_percent}")
    #         if cloudpercentage is None:
    #             print(f"[L8] Image {i} cloud percentage is missing (None)")
    #             continue

    #         cloud_percent = cloudpercentage.get('Clouds')
    #         if cloud_percent is None:
    #             print(f"[L8] Image {i} has no 'Clouds' key")
    #             continue
    #         cloud_percent = cloud_percent.getInfo()
    #         print(f"[L8] Image {i} cloud %: {cloud_percent}")

    #         if cloud_percent is not None and cloud_percent < 0.5:
    #             im_id = im.get('system:id').getInfo()
    #             suitablel8_images.append(str(im_id))
    #             print(f"[L8] Added: {im_id}")
    #             print(f"[L8] Image {i} cloud %: {cloud_percent}")
    #     except Exception as e:
    #         print(f"[L8] Skipped image {i} due to error: {e}")
            
    suitablel8_images = sorted(suitablel8_images, key=lambda x: x.split('/')[-1].split('_')[2])
    print(suitablel8_images)
    final_landsat8_collection = ee.ImageCollection.fromImages(suitablel8_images)




    dataset = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2') \
        .filterDate(startdate, enddate) \
        .filterBounds(extent) \
        .map(applyScaleFactors)

    L_List = dataset.toList(dataset.size())
    print('Before dataset size, landsat9: ', dataset.size().getInfo())

    suitablel9_images = []
    for i in range(dataset.size().getInfo()):
        im = ee.Image(L_List.get(int(i)))
        cloudpercentage = main(im)
        cloud_percent = cloudpercentage.get('Clouds').getInfo()
        im_id = im.get('system:id').getInfo()
        if cloud_percent is not None and cloud_percent < 0.1:
            im_id = im.get('system:id').getInfo()
            print(im_id)
            suitablel9_images.append(str(im_id))


    # suitablel9_images = []
    # for i in range(dataset.size().getInfo()):
    #     try:
    #         im = ee.Image(L_List.get(i))
    #         cloudpercentage = main(im)
    #         cloud_percent = cloudpercentage.get('Clouds').getInfo()
    #         print(f"[L9] Image {i} cloud %: {cloud_percent}")

    #         if cloud_percent is not None and cloud_percent < 0.1:
    #             im_id = im.get('system:id').getInfo()
    #             suitablel9_images.append(str(im_id))
    #             print(f"[L9] Added: {im_id}")
    #     except Exception as e:
    #         print(f"[L9] Skipped image {i} due to error: {e}")

    # order suitable images by date and present them as ["", "", ""] format
    suitablel9_images = sorted(suitablel9_images, key=lambda x: x.split('/')[-1].split('_')[2])
    print(suitablel9_images)


    final_landsat9_collection = ee.ImageCollection.fromImages(suitablel9_images)

    
    return final_landsat8_collection, suitablel8_images, final_landsat9_collection, suitablel9_images, extent

