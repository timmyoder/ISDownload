"""Parse the raw txt data files from NOAA ISD into csv files with imperial units"""

import pandas as pd
import numpy as np
import os
from loguru import logger
import multiprocessing as mp
import warnings

from config import RAW_DATA_DIR, WEATHER_OUT_DIR


def is_zero_file(file):
    """Returns True if the file exists and is 0 bytes"""
    return os.path.isfile(file) and os.path.getsize(file) == 0


def weather_parser_full(file):
    """loads each txt file and parses raw data into dataframes"""

    if is_zero_file(file):
        return None

    current_file = pd.read_csv(file, delimiter='\n', index_col=False, header=None)
    station_data = pd.DataFrame()
    try:
        usaf_id = current_file[0].str[4:10].astype(int)
        wban = current_file[0].str[10:15].astype(str)
        year = current_file[0].str[15:19].astype(int)
        month = current_file[0].str[19:21].astype(int)
        day = current_file[0].str[21:23].astype(int)
        hour = current_file[0].str[23:25].astype(int)
        temp_c = current_file[0].str[87:92].astype(float) / 10
        temp_f = current_file[0].str[87:92].astype(float) * 9 / 50 + 32
    except IndexError:
        return None

    station_data['USAF_ID'] = usaf_id
    station_data['WBAN_ID'] = wban
    station_data['YEAR'] = year
    station_data['MONTH'] = month
    station_data['DAY'] = day
    station_data['HOUR'] = hour
    station_data['TEMP_C'] = temp_c
    station_data['TEMP_F'] = temp_f

    warnings.warn('Current implementation only parses temp from full ISD file')

    # missing observations are coded as 9999, remove those
    station_data.loc[station_data['TEMP_C'] == 999.9] = np.nan
    return station_data


def weather_parser_lite(input_file):
    """loads each txt file and parses raw data into dataframes.

    missing pressure measurements are linearly interpolated between adjacent points
    """

    if is_zero_file(input_file):
        logger.warning(f'{input_file.name} was empty')
        return None

    name = input_file.name[:-len(input_file.suffix)]
    year_dir = WEATHER_OUT_DIR / name[-4:]

    # create directory for output file
    if not year_dir.exists():
        year_dir.mkdir()

    widths = [4, 3, 3, 3, 6, 6, 6, 6, 6, 6, 6, 6]

    current_file = pd.read_fwf(input_file,
                               index_col=False,
                               header=None,
                               widths=widths)
    current_file.columns = ['year',
                            'month',
                            'day',
                            'hour',
                            'temp_c',
                            'dew_point_c',
                            'pressure_hpa',
                            'wind_direction',
                            'wind_speed_m/s',
                            'sky_coverage',
                            'precip_depth_1hr_mm',
                            'precip_depth_6hr_mm']
    # replace missing measurements with nan
    current_file.replace(-9999, np.nan, inplace=True)

    scaled_columns = ['temp_c',
                      'dew_point_c',
                      'pressure_hpa',
                      'wind_speed_m/s',
                      'precip_depth_1hr_mm',
                      'precip_depth_6hr_mm']
    try:
        current_file[scaled_columns] = current_file[scaled_columns] / 10
    except TypeError as error:
        logger.error(name)
        logger.error(error)
        return None

    # add columns with IP units
    current_file['temp_f'] = current_file['temp_c'].interpolate() * 9 / 5 + 32
    current_file['dew_point_f'] = current_file['dew_point_c'].interpolate() * 9 / 5 + 32

    # fill missing pressure measurements with linearly interpolated values and convert to IP
    current_file['pressure_psi'] = current_file['pressure_hpa'].interpolate() * 0.0145038
    current_file['pressure_psi'] = current_file['pressure_psi'].fillna(14.696)

    current_file.loc[current_file['dew_point_f'] > current_file['temp_f'],
                     'dew_point_f'] = current_file.loc[current_file['dew_point_f'] > current_file['temp_f'],
                                                       'temp_f']
    # save to csv file
    current_file.to_csv(year_dir / f'{name}.csv', index=False)
    logger.success(f'{name} file parsed to csv')


def parallel_parser_lite(input_list, parallel=False):
    if parallel:
        with mp.Pool(mp.cpu_count()) as pool:
            pool.map(weather_parser_lite, input_list)
    else:
        for input_ in input_list:
            weather_parser_lite(input_)


def get_file_list(start_yr, end_yr, base_dir, file_ext):
    file_list = []
    for year in range(start_yr, end_yr + 1):
        year_dir = base_dir / str(year)
        if not year_dir.exists():
            logger.info(f'Weather data not found for {year} in {base_dir}')
            continue
        all_files = [file for file in year_dir.glob(f'*.{file_ext}')]
        file_list.extend(all_files)
    return file_list


def main_parser(start, end, base_dir=RAW_DATA_DIR, file_ext='txt'):
    parallel_parser_lite(get_file_list(start_yr=start,
                                       end_yr=end,
                                       base_dir=base_dir,
                                       file_ext=file_ext))


if __name__ == '__main__':
    start_year = 2020
    end_year = 2021

    parallel_parser_lite(get_file_list(start_year,
                                       end_year,
                                       base_dir=RAW_DATA_DIR,
                                       file_ext='txt'))
