import json
import requests
from requests.auth import HTTPBasicAuth
import os
from planet import Planet
from planet.order_request import (
    build_request,
    product,
)

import requests
from requests.auth import HTTPBasicAuth

API_KEY = 'PLAKeabae84a0c124dd3bf732da80d449aec'
ITEM_TYPE = 'PSScene'
ITEM_ID = '20220327_005848_1040'
# url = f"https://api.planet.com/data/v1/item-types/{ITEM_TYPE}/items/{ITEM_ID}/assets"
url= f"https://api.planet.com/compute/ops/orders/v2"
response = requests.get(url, auth=HTTPBasicAuth(API_KEY, ''))

print("Status Code:", response.status_code)
print("URL:", url)

try:
    assets = response.json()
    print("Assets:", json.dumps(assets, indent=2))
    print("Headers:", response.headers)
    print("Raw:", response.text)

except Exception as e:
    print("Failed to decode JSON:", e)
    print("Raw response:", response.text)
    print("Headers:", response.headers)
    print("Raw:", response.text)



