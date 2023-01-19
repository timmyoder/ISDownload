"""Main file to download and parse weather data"""

import argparse
import warnings

from loguru import logger

import scripts.get_weather_data as get_weather
import scripts.isd_parser as parser
from scripts.config import INPUT_DIR, RAW_DATA_DIR, WEATHER_OUT_DIR, log_name

logger.add(log_name(__file__))


def main():
    # Create argument arg_parser
    arg_parser = argparse.ArgumentParser(
        description='Download and parse NOAA ISD hourly weather data')
    arg_parser.add_argument("--id", type=str,
                            help="Provide the USAF WBAN station identifier "
                                 "Example: --id 727935-24234")
    arg_parser.add_argument('--file', type=str,
                            help='optional input csv file with station identifiers. '
                                 'CSV must have "USAF" and "WBAN" columns. '
                                 "File must be in the 'input_data/' dir. "
                                 "Do not include the path to the directory in the name. "
                                 "Example: --file input.csv")
    arg_parser.add_argument("--year", type=int, required=True,
                            help="Year of data to pull. "
                                 "Example: --year 2020")
    arg_parser.add_argument("--to-year", type=int, dest='to_year',
                            help="If provided, pulls data for all years between 'year' and "
                                 "'to_year' (e.g., pulls from 2020 to 2021: --year 2020 "
                                 "--to_year 2021). "
                                 "Example: --to_year 2021")
    arg_parser.add_argument('--isd-full', dest='isd_full', action="store_true",
                            help='Use the full ISD dataset instead of ISD lite. ')
    arg_parser.add_argument('--no-overwrite', dest='no_overwrite', action="store_true",
                            help='Do not overwrite existing files. ')
    args = arg_parser.parse_args()

    isd_lite = not args.isd_full
    if not isd_lite:
        logger.warning('Current implementation only includes mandatory data '
                       'fields from full ISD file')
    overwrite = not args.no_overwrite

    year = args.year
    end_year = args.to_year
    if end_year is not None and end_year < year:
        raise ValueError("The end year ('to_year') must be greater than first year ('year').")

    if args.id is None and args.file is None:
        raise arg_parser.error('A station identifier or input file with '
                               'stations ids must be given')

    if args.id is not None and args.file is not None:
        raise arg_parser.error('Specify either a station identifier or input file with '
                               'stations ids. NOT BOTH!')

    # Download files
    if args.id is not None:
        usaf = args.id.split("-")[0]
        wban = args.id.split("-")[1]
        if end_year is None:
            get_weather.main_consecutive_cli_stn(year=year, usaf=usaf,
                                                 wban=wban, isd_lite=isd_lite,
                                                 overwrite=overwrite)
        else:
            get_weather.main_downloader_cli_stn(start_yr=year, end_yr=end_year,
                                                usaf=usaf, wban=wban, isd_lite=isd_lite,
                                                overwrite=overwrite)
    if args.file is not None:
        file_name = args.file
        if end_year is None:
            get_weather.main_consecutive_file(year=year, input_file=file_name,
                                              isd_lite=isd_lite, overwrite=overwrite)
        else:
            get_weather.main_downloader_file(start_yr=year, end_yr=end_year,
                                             input_file=file_name, isd_lite=isd_lite,
                                             overwrite=overwrite)

    logger.success(f'All raw txt files downloaded to {RAW_DATA_DIR}')

    # Parse files
    if end_year is None:
        parser.main_parser(year, year, isd_lite=isd_lite, overwrite=overwrite)
    else:
        parser.main_parser(start=year, end=end_year,
                           isd_lite=isd_lite, overwrite=overwrite)

    logger.success(f'All raw files have been parsed here: {WEATHER_OUT_DIR}')


if __name__ == '__main__':
    # used for testing
    # s = 2020
    # e = 2021
    # stations_file = INPUT_DIR / 'sample_input.csv'
    # get_weather.main_downloader_file(s, e, input_file=stations_file)
    # parser.main_parser(s, e)

    main()
