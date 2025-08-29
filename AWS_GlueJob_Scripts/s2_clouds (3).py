# s2_clouds.py
import ee
from datetime import datetime

#ee.Initialize(project='high-keel-462317-i5')

def make_rectangle(point, buffer_m=2500):
    roi = ee.Geometry.Point(point[0], point[1])
    return roi.buffer(distance=buffer_m).bounds()

def cloudper(image, extent):
    return image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=extent,
        scale=30,
        maxPixels=1e9
    )


def rename(image, newName):
    # Use the QA60 band (uint16) and be explicit about integer type
    qa = image.select('QA60').toInt()
    return qa.bitwiseAnd(1024).rightShift(10).rename(newName).uint8()


def clouds_from_image(image):
    bands = image.bandNames()
    has_QA60 = bands.contains('QA60')

    def from_QA60():
        qa = image.select('QA60').toInt()
        cloud = qa.bitwiseAnd(1 << 10).neq(0)
        cirrus = qa.bitwiseAnd(1 << 11).neq(0)
        return cloud.Or(cirrus).rename('Clouds').uint8()

    def from_class_masks():
        # these are 0/1 masks when present in some tiles
        opaque = image.select('MSK_CLASSI_OPAQUE').gt(0)
        cirrus = image.select('MSK_CLASSI_CIRRUS').gt(0)
        return opaque.Or(cirrus).rename('Clouds').uint8()

    return ee.Image(ee.Algorithms.If(has_QA60, from_QA60(), from_class_masks()))

def mask_s2_clouds(image):
    # return just the Clouds band; don't scale anything here
    return clouds_from_image(image)

#def mask_s2_clouds(image):
    #qa = image.select('QA60')
    #cloudBitMask = 1 << 10
    #cirrusBitMask = 1 << 11
    #mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(qa.bitwiseAnd(cirrusBitMask).eq(0))
    #image = image.updateMask(mask).divide(10000)
    #clouds=rename(image,'Clouds')
    #image = image.updateMask(mask).divide(10000)
    #return clouds  # returns a single-band "Clouds" image

def mainS2(point, startdate, enddate, buffer_m=2500, cloud_thresh=0.1):
    """
    point: [lon, lat]
    startdate/enddate: 'YYYY-MM-DD'
    returns: (ee.ImageCollection, [image_ids])
    """
    extent = make_rectangle(point, buffer_m)
    def _prep(image):
        return mask_s2_clouds(image).clip(extent)

    dataset = (ee.ImageCollection('COPERNICUS/S2')
               .filterDate(startdate, enddate)
               .filterBounds(extent)
               .map(_prep))

    L_List = dataset.toList(dataset.size())
    print('S2 dataset size:', dataset.size().getInfo())

    suitable_images = []
    for i in range(dataset.size().getInfo()):
        im = ee.Image(L_List.get(int(i)))
        cloudpercentage = cloudper(im, extent)
        cloud_percent = cloudpercentage.get('Clouds').getInfo()
        if cloud_percent is not None and cloud_percent < cloud_thresh:
            im_id = im.get('system:index').getInfo()
            suitable_images.append(f'COPERNICUS/S2/{im_id}')
    print(suitable_images)

    suitable_images = sorted(suitable_images, key=lambda x: x.split('/')[2])
    final_S2_collection = ee.ImageCollection.fromImages([ee.Image(i) for i in suitable_images])
    return final_S2_collection, suitable_images
