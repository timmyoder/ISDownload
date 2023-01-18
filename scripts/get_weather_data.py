"""Parallel download of NOAA ISD hourly data"""

import concurrent.futures
import ftplib
import gzip
from io import BytesIO
import pandas as pd

from loguru import logger

from config import RAW_DATA_DIR, log_name


def read_stn_info(file_path):
    """
    read mdms station info into a pd.df.

    good_value is a 5 digit WBAN_ID that is used to replace
    incorrect 4 digit ID when leading 0 is not correctly interpreted
    """
    stations_data = pd.read_csv(file_path, dtype=str)
    stations_data['wban'] = stations_data['wban'].str.zfill(5)

    return stations_data


def get_isd_file(usaf_id, wban_id, year, isd_lite=True):
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

    if current_station_file.exists():
        logger.info(f'{current_station} already downloaded; skipping download')
        pass
    else:
        with ftplib.FTP(host=ftp_host) as ftpconn:
            ftpconn.login()

            ftp_file = f"pub/data/noaa/{isd}{year}/{current_station}.gz"

            logger.info(f'downloading: {ftp_file}')

            # read the whole file and save it to a BytesIO (stream)
            response = BytesIO()
            try:
                ftpconn.retrbinary(f'RETR {ftp_file}', response.write)
                logger.info(f'{current_station}: success')

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

            logger.success(f'{current_station}.txt file saved')


def parallel_downloader(input_list):
    """runs the parallel download function in parallel,
    NOAA FTP fails with more than 5 processes"""

    # for some reason, starmap throws an EOFError when accessing the ISD lite data, too fast?
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda arg: get_isd_file(*arg), input_list)


def main_consecutive(year, input_file):
    """Runs the script for a given year"""
    data_dump_dir = RAW_DATA_DIR / f'{year}'
    if not data_dump_dir.exists():
        data_dump_dir.mkdir()

    logger.info(f'Run started for year: {year}')

    stations = read_stn_info(input_file)

    input_list = zip(stations['usaf'], stations['wban'], [year] * len(stations))
    parallel_downloader(input_list)

    logger.info('Download Finished')
    logger.info('End of Run')


def main_downloader(start_yr, end_yr, input_file):
    for run_year in range(start_yr, end_yr):
        main_consecutive(run_year, input_file=input_file)


if __name__ == '__main__':

    main_downloader(2021, 2022)
