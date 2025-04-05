# This file scrapes data from the most popular real-estate website in Kazakhstan
# After data is scraped it is saved to output folder as a parquet file of specified format
# Data is saved in the separate folder every month. If data for the current month is downloaded
# then previously downloaded data is deleted.
# The scrapper uses multiprocessing, e.g. loads several urls simultaneously
# Note that result might contain duplicate rows!!!
# 
# 2025-04-03
# Vitaliy Ponomaryov

import os
import re
import logging
import json
from itertools import product
from datetime import datetime
from typing import List, Dict, Any
from time import sleep
from uuid import uuid4
from multiprocessing import Pool
import requests
from requests import HTTPError
from pydantic import BaseModel, ValidationError
import pandas as pd
from shapely.geometry import shape, Point


"""
Defining parameter for the scrapper
Might be stored in a separate file and env variables if data is sensitive 
"""
sleep_time = .05
chunk_size = 3000
processes = 1
output_folder = 'output'

start_urls = [
    {
        "url": "https://krisha.kz/a/ajax-map-list/map/arenda/kvartiry/", 
        "property_type": "Appartment",
        "deal_type": "Rent", 
        "duration": "Monthly"
    }, 
    {
        "url": "https://krisha.kz/a/ajax-map-list/map/arenda/kvartiry-posutochno/",
        "property_type": "Appartment",
        "deal_type": "Rent",
        "duration": "Daily"
    },
    {
        "url": "https://krisha.kz/a/ajax-map-list/map/prodazha/kvartiry/",
        "property_type": "Appartment",
        "deal_type": "Sale"
    },
    {
        "url": "https://krisha.kz/a/ajax-map-list/map/arenda/doma-dachi/",
        "property_type": "House",
        "deal_type": "Rent",
        "duration": "Monthly"
    },
    {
        "url": "https://krisha.kz/a/ajax-map-list/map/arenda/doma-dachi-posutochno/",
        "property_type": "House",
        "deal_type": "Rent",
        "duration": "Daily"
    },
    {
        "url": "https://krisha.kz/a/ajax-map-list/map/prodazha/doma-dachi/",
        "property_type": "House",
        "deal_type": "Sale"
    }
]

cities = [
    {
        "city": "Almaty", 
        "url_path": "almaty",
        "params": {
            "bounds": "43.411676088976265,76.68081381835931,43.149807254297706,77.14498618164056"
        }
    }
]

headers = {
    'authority': 'krisha.kz',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}

"""
Configure basic logger
"""
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger('main')


"""
Pydantic models definigs configurations and outputs
"""
class URLModel(BaseModel):
    url: str
    city: str
    url_path: str
    property_type: str
    deal_type: str
    duration: str = None
    headers: Dict[str, str]
    params: Dict[str, str]

    def build_url(self) -> str:
        return os.path.join(self.url, self.url_path)


class MapModel(BaseModel):
    lat: float
    lon: float


class AdvertModel(BaseModel):
    id: int
    title: str
    addressTitle: str
    rooms: int = None
    square: float
    price: float
    photos: List[Dict[str, Any]]
    userType: str
    status: str
    storage: str
    map: MapModel
    url: URLModel


"""
Functions that are used in the process of scrapping
"""
def get_district(lat, lon) -> str:
    with open('almaty-geo-map.json', 'r', encoding='utf8') as f:
        geo_data = json.loads(f.read())
    point = Point(lon, lat)

    for feature in geo_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            return feature['properties'].get('id', 'Unknown district')
    return 'District not found'


def prepare_output_directory() -> None:
    """
    Check if output directory for monthly data exists.
    If exists then delete all files from it
    If not then create a new directory with name %Y%m
    """
    logger.info('Preparing output directory')
    dirname = f'{output_folder}/{datetime.now().strftime("%Y%m")}'
    if not os.path.isdir(dirname):
        os.mkdir(dirname)
    _ = [os.remove(os.path.join(dirname, file)) for file in os.listdir(dirname)]


def build_urls() -> List[URLModel]:
    """
    Building a list of urls from `start_urls`, `cities` and `headers` configurations
    """
    logger.info('Building URLs')
    return [
        URLModel(**start_url, **district, headers=headers)
        for start_url, district in product(start_urls, cities)
    ]


def download_data(url: URLModel):
    """
    Since the number of adverts is unknown the pager for this generatot was implemented.
    `while` loop ends once length of returned data is 0 
    """
    logger.info(f'Download {url.build_url()} started')
    url.params['page'] = 1
    
    session = requests.Session()
    
    while True:
        logger.debug(f"Download {url.build_url()} page number: {url.params['page']} started")
        try:
            response = session.get(url.build_url(), headers=url.headers, params=url.params)
            response.raise_for_status()
        except HTTPError as e:
            logger.error(f"HTTP Error: {str(e)}")
            break
        response.raise_for_status()
        _data = response.json()['adverts']
        logger.debug(f"Download {url.build_url()} page number: {url.params['page']} length of data: {len(_data)} finished")
        if len(_data) == 0:
            break
        for adv in _data.values():
            try:
                yield AdvertModel(**adv, url=url)
            except ValidationError as e:
                errors = json.loads(e.json())
                for error in errors:
                    logger.error(f"Validation Error: {error['loc'][0]} - {error['msg']}")
                    logger.error(f"Error Values: id {adv['id']} - {error['loc'][0]} - {adv[error['loc'][0]]}")
            sleep(sleep_time)
        url.params['page'] += 1

    session.close()
    logger.info(f'Download {url.build_url()} is done')


def transform_advert(advert: AdvertModel) -> pd.Series:
    """
    Transforms each advert to a specified format
    """
    logger.debug(f"Flatten results for {advert.url.build_url()} page number: {advert.url.params['page']}")
    floor_pattern = re.search('(\d+)/(\d+)', advert.title)
    district =  get_district(advert.map.lat, advert.map.lon)
    return pd.Series({
        "id": advert.id,
        "city": advert.url.city,
        "district": district,
        "address": advert.addressTitle,
        "title": advert.title,
        "property": advert.url.property_type,
        "deal_type": advert.url.deal_type,
        "duration": advert.url.duration,
        "rooms": advert.rooms,
        "square": advert.square,
        "price": advert.price,
        "number_of_photos": len(advert.photos),
        "seller": advert.userType,
        "latitude": advert.map.lat,
        "longitude": advert.map.lon,
        "floor": int(floor_pattern.group(1)) if floor_pattern else None,
        "total_floors": int(floor_pattern.group(2)) if floor_pattern else None,
        "url": advert.url.build_url(),
        "params": advert.url.params,
        "extract_datetime": pd.to_datetime('now')
    })


def save_data_to_parquet(data: List[pd.Series]) -> None:
    """
    Saves chunks of data into parquet file of the defined folder
    """
    output_filename = os.path.join(
        output_folder, 
        datetime.now().strftime('%Y%m'), 
        f'output_{uuid4().hex}_{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet'
    ) 
    logger.info(f'Saving output to {output_filename} rows count: {len(data)}')
    df = pd.concat(data, axis=1).T
    df.to_parquet(output_filename)


def pipeline(url: URLModel) -> None:
    data = download_data(url)
    rows = 0
    processed_data = []
    for i, adv in enumerate(data, 1):
        processed_data.append(transform_advert(adv))
        if i % chunk_size == 0:
            save_data_to_parquet(processed_data)
            rows += len(processed_data)
            processed_data.clear()
    if processed_data:
        save_data_to_parquet(processed_data)
        rows += len(processed_data)
    logger.info(f'Data {url.build_url()} rows count: {rows} is saved')


def main() -> None:
    logger.info('Parsing is started')
    prepare_output_directory()
    urls = build_urls()
    with Pool(processes=processes) as p:
        _ = p.map(pipeline, urls)
    logger.info('Parsing is done')


if __name__ == '__main__':
    main()
