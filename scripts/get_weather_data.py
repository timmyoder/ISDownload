"""Parallel download of NOAA ISD hourly data"""

import concurrent.futures
import ftplib
import gzip
from io import BytesIO
import pandas as pd

from loguru import logger

from scripts.config import RAW_DATA_DIR, INPUT_DIR


def read_stn_info(file_path):
    """
    read mdms station info into a pd.df.

    good_value is a 5 digit WBAN_ID that is used to replace
    incorrect 4 digit ID when leading 0 is not correctly interpreted
    """
    stations_data = pd.read_csv(file_path, dtype=str)
    # ignore case of file names
    stations_data.columns = [x.lower() for x in stations_data.columns]
    try:
        assert 'wban' in stations_data.columns
        assert 'usaf' in stations_data.columns
    except AssertionError:
        raise ValueError("Input file must have 'USAF' and 'WBAN' columns. ")

    stations_data['wban'] = stations_data['wban'].str.zfill(5)

    return stations_data


def get_isd_file(usaf_id, wban_id, year, isd_lite=True, overwrite=True):
    """
    Function used for parallel download from NOAA FTP site.
    NOAA files are .gz zips, so decompress and save as .txt files on the share drive

    """
    if isd_lite:
        isd = 'isd-lite/'
    else:
        isd = ''

    ftp_host = "ftp.ncdc.noaa.gov"

    current_station = f'{usaf_id}-{wban_id}-{year}'
    data_out_dir = RAW_DATA_DIR / f'{year}'

    current_station_file = data_out_dir / f'{current_station}.txt'

    if not overwrite and current_station_file.exists():
        logger.debug(f'{current_station} already downloaded; skipping download')
    else:
        with ftplib.FTP(host=ftp_host) as ftpconn:
            ftpconn.login()

            ftp_file = f"pub/data/noaa/{isd}{year}/{current_station}.gz"

            logger.debug(f'downloading: {ftp_file}')

            # read the whole file and save it to a BytesIO (stream)
            response = BytesIO()
            try:
                ftpconn.retrbinary(f'RETR {ftp_file}', response.write)

            except ftplib.error_perm as err:
                if str(err).startswith('550 '):
                    print('ERROR:', err)
                    logger.info(f'{current_station}: error')
                else:
                    raise

            # decompress and parse each line
            response.seek(0)  # jump back to the beginning of the stream
            content = gzip.decompress(response.read())
            with open(current_station_file, 'wb') as outFile:
                outFile.write(content)
                outFile.close()

            logger.debug(f'{current_station}.txt file saved')


def parallel_downloader(input_list):
    """runs the parallel download function in parallel,
    NOAA FTP fails with more than 5 processes"""

    # for some reason, starmap throws an EOFError when accessing the ISD lite data, too fast?
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda arg: get_isd_file(*arg), input_list)


def main_consecutive_file(year, input_file, isd_lite, overwrite=True):
    """Runs the script for a given year with a list of stations"""
    data_dump_dir = RAW_DATA_DIR / f'{year}'
    if not data_dump_dir.exists():
        data_dump_dir.mkdir()

    logger.info(f'Downloading files for {year}')

    station_file = INPUT_DIR / input_file
    stations = read_stn_info(station_file)

    input_list = zip(stations['usaf'], stations['wban'],
                     [year] * len(stations),
                     [isd_lite] * len(stations),
                     [overwrite] * len(stations),
                     )
    parallel_downloader(input_list)

    logger.info(f'Download finished for {year}')


def main_downloader_file(start_yr, end_yr, input_file,
                         isd_lite=True, overwrite=True):
    for run_year in range(start_yr, end_yr+1):
        main_consecutive_file(run_year, input_file=input_file,
                              isd_lite=isd_lite, overwrite=overwrite)


def main_consecutive_cli_stn(year, usaf, wban, isd_lite=True, overwrite=True):
    """Runs the script for a given year with a specific station"""
    data_dump_dir = RAW_DATA_DIR / f'{year}'
    if not data_dump_dir.exists():
        data_dump_dir.mkdir()

    logger.info(f'Downloading files for {year}')
    get_isd_file(usaf_id=usaf, wban_id=wban, year=year,
                 isd_lite=isd_lite, overwrite=overwrite)
    logger.info(f'Download finished for {year}')


def main_downloader_cli_stn(start_yr, end_yr, usaf, wban, isd_lite, overwrite=True):
    for run_year in range(start_yr, end_yr+1):
        main_consecutive_cli_stn(year=run_year, usaf=usaf, wban=wban,
                                 isd_lite=isd_lite, overwrite=overwrite)


if __name__ == '__main__':
    s = 2021
    e = 2022
    stations_file = INPUT_DIR / 'weather_stations.csv'
    main_downloader_file(s, e, input_file=stations_file, isd_lite=True)
