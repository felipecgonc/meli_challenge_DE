import json
import requests
import aiohttp
import asyncio
import csv

BASE_URL = "https://api.mercadolibre.com"
# Schema is being passed explicitly for simplicity due to csv and async conflict
FIELD_NAMES = ['id', 'site_id', 'title', 'seller_id', 'category_id',
               'official_store_id', 'price', 'base_price', 'original_price',
               'currency_id', 'initial_quantity', 'sale_terms', 'buying_mode',
               'listing_type_id', 'condition', 'permalink', 'thumbnail_id',
               'thumbnail', 'pictures', 'video_id', 'descriptions',
               'accepts_mercadopago', 'non_mercado_pago_payment_methods',
               'shipping', 'international_delivery_mode', 'seller_address',
               'seller_contact', 'location', 'coverage_areas', 'attributes',
               'listing_source', 'variations', 'status', 'sub_status', 'tags',
               'warranty', 'catalog_product_id', 'domain_id', 'parent_item_id',
               'deal_ids', 'automatic_relist', 'date_created', 'last_updated',
               'health', 'catalog_listing']

def search_for_ids(term):
    """
    Searches a specified query term on MercadoLivre and retrieves the IDs for the products.

    Args:
        term (str): The search term to look for.

    Returns:
        list: A list of product IDs retrieved from the search results.
    """
    id_list = list()
    for i in range(5):
        response = requests.get(f'{BASE_URL}/sites/MLA/search?q={term}&limit=50&offset={i*50}#json')
        results = response.json().get('results')
        id_list += [result["id"] for result in results]
    return id_list

async def fetch_item(session, item_id):
    """
    Asynchronously fetches full information of a single item from MercadoLivre.

    Args:
        session (aiohttp.ClientSession): An aiohttp ClientSession object.
        item_id (str): The ID of the item to fetch details for.

    Returns:
        dict: A dictionary containing the complete data of the item.
    """
    url = f'{BASE_URL}/items/{item_id}'
    async with session.get(url) as response:
        return await response.json()

async def write_data(output_file, data):
    """
    Asynchronously writes data to a CSV file.

    Args:
        output_file (file object): The file object to write data to.
        data (dict): A dictionary containing a row of data to be written to the CSV file.
    """
    writer = csv.DictWriter(output_file, fieldnames=data.keys())
    writer.writerow(data)

async def search_to_file(term):
    """
    Generates a CSV file with the data from MercadoLivre products related to the search term.

    Args:
        term (str): The search term for products.

    Returns:
        None
    """    
    output_file = open(f'{term.replace(" ", "_")}_results.csv', 'a')
    writer = csv.DictWriter(output_file, fieldnames=FIELD_NAMES)
    writer.writeheader()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for id in search_for_ids(term):
            task = asyncio.create_task(fetch_item(session, id))
            tasks.append(task)

        # Wait for all tasks to complete concurrently
        for task in asyncio.as_completed(tasks):
            data = await task
            await write_data(output_file, data)

    output_file.close()

if __name__ == '__main__':
  for product in ['nintendo switch', 'ps5', 'xbox series s']:
    asyncio.run(search_to_file(product))