# landsat_clouds.py
import ee
from datetime import datetime
#ee.Initialize(project='high-keel-462317-i5')

def make_rectangle(point, buffer_m=2500):
    roi = ee.Geometry.Point(point[0], point[1])
    return roi.buffer(distance=buffer_m).bounds()

def apply_scale_factors(image):
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


def cloudper(image,extent):
    return image.reduceRegion(**{
    'reducer': ee.Reducer.mean(),
    'geometry': extent,
    'scale': 30,
    'maxPixels': 1e9
    })
    
def main(image,extent):
    clipped = image.clip(extent)
    c = clouds(clipped)
    c1 = cloudper(c,extent)
    return c1

def mainl8l9(point, startdate, enddate, buffer_m=2500, cloud_thresh=0.1):
    """
    Returns:
      final_landsat8_collection, suitablel8_ids,
      final_landsat9_collection, suitablel9_ids,
      extent
    """
    extent = make_rectangle(point, buffer_m)
    def _prep(image):
        img = apply_scale_factors(image)
        return img.clip(extent)

    def filter_and_collect(collection_id):
        dataset = (ee.ImageCollection(collection_id)
              .filterDate(startdate, enddate)
              .filterBounds(extent)
              .map(_prep))
        L_List = dataset.toList(dataset.size())
        print('Before Dataset size', collection_id, ':', dataset.size().getInfo())
        suitable_images = []
        for i in range(dataset.size().getInfo()):
            im = ee.Image(L_List.get(int(i)))
            cloudpercentage = main(im,extent)
            cloudpercent = cloudpercentage.get('Clouds').getInfo()
            if cloudpercent is not None and cloudpercent < cloud_thresh:
                im_id = im.get('system:id').getInfo()
                print(im_id)
                suitable_images.append(str(im_id))
        suitable_images = sorted(suitable_images, key=lambda x: x.split('/')[-1].split('_')[2])
        return ee.ImageCollection.fromImages(suitable_images), suitable_images

    final_landsat8_collection, suitablel8_images = filter_and_collect('LANDSAT/LC08/C02/T1_L2')
    final_landsat9_collection, suitablel9_images = filter_and_collect('LANDSAT/LC09/C02/T1_L2')
    return final_landsat8_collection, suitablel8_images, final_landsat9_collection, suitablel9_images, extent
