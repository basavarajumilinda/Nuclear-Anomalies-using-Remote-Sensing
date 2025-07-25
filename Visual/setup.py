import os
from planetquery import *

# def setup():
#     #import planet api key from environment variable
#     #PLANET_API_KEY = os.environ.get('PLAKeabae84a0c124dd3bf732da80d449aec') #'PLAKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
#     PLANET_API_KEY='PLAK7dd668dc57224ec7be1c709369795278'
#     cloudcover = 0.1
#     startdate = convert_date('2024-06-01')
#     enddate = convert_date('2024-07-30')
#     aoi = {
#     "type": "Polygon",
#     "coordinates": [
#         [ 
#                 [
#                 141.02552103573942,
#                 37.42523798714251
#                 ],
#                 [
#                 141.02552103573942,
#                 37.41697182701044
#                 ],
#                 [
#                 141.0353183469465,
#                 37.41697182701044
#                 ],
#                 [
#                 141.0353183469465,
#                 37.42523798714251
#                 ],
#                 [
#                 141.02552103573942,
#                 37.42523798714251
#                 ]
#         ]
#     ]
#     }

#     return [aoi, cloudcover, startdate, enddate, PLANET_API_KEY]

def setup():
    os.environ["PL_API_KEY"] = "PLAK7dd668dc57224ec7be1c709369795278"
    PLANET_API_KEY = os.getenv("PL_API_KEY")

    # PL_API_KEY = 'PLAK7dd668dc57224ec7be1c709369795278'
    # PLANET_API_KEY = os.getenv('PL_API_KEY')

    cloudcover = 0.1
    startdate = convert_date('2022-07-07')
    enddate = convert_date('2022-08-30')

    aoi = {
        "type": "Polygon",
        "coordinates": [
            [
                [34.5780, 47.5130],
                [34.5780, 47.5020],
                [34.5925, 47.5020],
                [34.5925, 47.5130],
                [34.5780, 47.5130]
            ]
        ]
    }

    return [aoi, cloudcover, startdate, enddate, PLANET_API_KEY]

# from planet import Session, OrdersClient
# import asyncio, os

# os.environ['PL_API_KEY'] = 'PLAK7dd668dc57224ec7be1c709369795278'  

# async def test_auth():
#     async with Session() as sess:
#         client = OrdersClient(sess)
#         orders = client.list_orders()
#         async for order in orders:
#             print(order["id"])

# asyncio.run(test_auth())

