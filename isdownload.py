"""Main file to download and parse weather data"""

import argparse
import warnings

from loguru import logger

import scripts.get_weather_data as get_weather
import scripts.isd_parser as parser
from scripts.config import INPUT_DIR


if __name__ == '__main__':
    s = 2002
    e = 2021
    stations_file = INPUT_DIR / 'weather_stations.csv'
    get_weather.main_downloader(s, e, input_file=stations_file)
    parser.main_parser(s, e)
