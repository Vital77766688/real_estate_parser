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
from typing import List
from time import sleep
from uuid import uuid4
from multiprocessing import Pool
import requests
from requests import HTTPError
from pydantic import ValidationError
import pandas as pd
from shapely.geometry import shape, Point

import settings
from models import URLModel, AdvertModel


"""
Configure basic logger
"""
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger('main')


"""
Functions that are used in the process of scrapping
"""
def get_district(lat, lon) -> str:
    with open(settings.GEOMAP_PATH, 'r', encoding='utf8') as f:
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
    dirname = f'{settings.OUTPUT_FOLDER}/{datetime.now().strftime("%Y%m")}'
    if not os.path.isdir(dirname):
        os.mkdir(dirname)
    _ = [os.remove(os.path.join(dirname, file)) for file in os.listdir(dirname)]


def build_urls() -> List[URLModel]:
    """
    Building a list of urls from `start_urls`, `cities` and `headers` configurations
    """
    logger.info('Building URLs')
    with open(settings.URLS_PATH, 'r', encoding='utf8') as f:
        _urls = json.loads(f.read())
        headers = _urls['headers']
        start_urls = _urls['urls']
        cities = _urls['cities']
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
            sleep(settings.SLEEP_TIME)
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
        settings.OUTPUT_FOLDER, 
        f"{datetime.now().strftime('%Y%m')}", 
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
        if i % settings.CHUNK_SIZE == 0:
            save_data_to_parquet(processed_data)
            rows += len(processed_data)
            processed_data.clear()
    if processed_data:
        save_data_to_parquet(processed_data)
        rows += len(processed_data)
    logger.info(f'Data {url.build_url()} rows count: {rows} is saved')


def main() -> None:
    logger.info('Parsing is started')
    urls = build_urls()
    prepare_output_directory()
    with Pool(processes=settings.PROCESSES) as p:
        _ = p.map(pipeline, urls)
    logger.info('Parsing is done')


if __name__ == '__main__':
    main()
