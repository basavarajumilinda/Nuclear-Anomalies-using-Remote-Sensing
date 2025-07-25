import asyncio
import os

import planet
from planet import OrdersClient
from planetquery import *
from post_download import postdownload

from post_download import *
from setup import *

# Example provided from Planet API documentation: https://github.com/planetlabs/planet-client-python/blob/main/examples/orders_create_and_download_multiple_orders.py
# which has been adapted for the purpose of this study.

#DOWNLOAD_DIR = os.path.join(os.getcwd(), 'D:/Dissertation-2542000/RP3/Visual/downloads/')
DOWNLOAD_DIR= r'D:/Dissertation-2542000/RP3/Visual/downloads/'

[aoi, cloudcover, startdate, enddate, PLANET_API_KEY] = setup()

[id0 , id1, asset_types] = query(aoi, cloudcover, startdate, enddate, PLANET_API_KEY)

items = [id0, id1]
print('Available item ids: ', items)

order = planet.order_request.build_request(
    name='order',
    products=[
        planet.order_request.product(item_ids=items,
                                     product_bundle='visual',
                                     item_type='PSScene')

    ],
    tools=[planet.order_request.clip_tool(aoi=aoi)]
)


async def create_and_download(client, order_detail, directory):
    """Make an order, wait for completion, download files as a single task."""
    with planet.reporting.StateBar(state='creating') as reporter:
        order = await client.create_order(order_detail)
        orderid = order['id']
        reporter.update(state='created', order_id=order['id'])
        await client.wait(order['id'], callback=reporter.update_state)

    await client.download_order(order['id'], directory, progress_bar=True)
    postdownload(orderid)


async def main():
    async with planet.Session() as sess:
        client = OrdersClient(sess)
        await asyncio.gather(
            create_and_download(client, order, DOWNLOAD_DIR)
        )
        


if __name__ == '__main__':
    asyncio.run(main())
