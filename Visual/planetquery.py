import json
import requests
from requests.auth import HTTPBasicAuth
import os

# Examples provided in official documentation


# Converting the data from "YYYY-MM-DD" to "YYYY-MMM-00T00:00:00.000Z"
def convert_date(date):
    date = date + "T00:00:00.000Z"
    return date

# Convert downloaded TIF imagery to jpeg using gdal
def convert_to_jpeg(filename):
    os.system('gdal_translate -of JPEG ' + filename + ' ' + filename[:-4] + '.jpg')

def query(geometry, cloudcover, startdate, enddate, key):
 
    geometry_filter = {
    "type": "GeometryFilter",
    "field_name": "geometry",
    "config": geometry
    }

    date_range_filter = {
    "type": "DateRangeFilter",
    "field_name": "acquired",
    "config": {
        "gte": startdate,
        "lte": enddate
    }
    }

    cloud_cover_filter = {
    "type": "RangeFilter",
    "field_name": "cloud_cover",
    "config": {
        "lte": cloudcover
    }
    }

    combined_filter = {
    "type": "AndFilter",
    "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }


    item_type = "PSScene"

    search_request = {
    "item_types": [item_type], 
    "filter": combined_filter
    }


    search_result = \
    requests.post(
        'https://api.planet.com/data/v1/quick-search',
        auth=HTTPBasicAuth(key, ''),
        json=search_request)

    geojson = search_result.json()
    print(json.dumps(geojson, indent=2))  # Debug print

    if 'features' not in geojson or len(geojson['features']) == 0:
        raise ValueError("No imagery features returned. Check AOI, date range, or API key.")

    image_ids = [feature['id'] for feature in geojson['features']]
    print('Image ids: \n', image_ids)

    id0 = image_ids[0]
    id1 = image_ids[1]
    id0_url = 'https://api.planet.com/data/v1/item-types/{}/items/{}/assets'.format(item_type, id0)

    result = \
    requests.get(
        id0_url,
        auth=HTTPBasicAuth(key, '')
    )
    asset_types = result.json().keys()
    print('Asset Types: \n', asset_types)
    print(json.dumps(result.json(), indent=2))


    return [id0, id1,asset_types]