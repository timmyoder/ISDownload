"""Parse the raw txt data files from NOAA ISD into csv files with imperial units"""

import pandas as pd
import numpy as np
import os
from loguru import logger
import multiprocessing as mp
import warnings

from scripts.config import RAW_DATA_DIR, WEATHER_OUT_DIR


def is_zero_file(file):
    """Returns True if the file exists and is 0 bytes"""
    return os.path.isfile(file) and os.path.getsize(file) == 0


def weather_parser_full(file, overwrite=True):
    """loads each txt file and parses raw data into dataframes"""

    if is_zero_file(file):
        return None

    name = file.name[:-len(file.suffix)]
    year_dir = WEATHER_OUT_DIR / name[-4:]
    parsed_file_name = year_dir / f'{name}.csv'

    if not overwrite and parsed_file_name.exists():
        logger.debug(f'{parsed_file_name} already parsed; skipping parsing')
        return None

    columns = {'tot_char': 4,
               'usaf': 6,
               'wban': 5,
               'year': 4,
               'month': 2,
               'day': 2,
               'hour': 2,
               'minute': 2,
               'source': 1,
               'latitude': 6,
               'longitude': 7,
               'report_code': 5,
               'elevation': 5,
               'weather_stn': 5,
               'qa': 4,
               'wind_angle': 3,
               'wind_qa': 1,
               'wind_type': 1,
               'wind_speed': 4,
               'wind_speed_qa': 1,
               'sky_ceiling': 5,
               'sky_ceiling_qa': 1,
               'sky_determination': 1,
               'sky_covak': 1,
               'vis_distance': 6,
               'vis_distance_qa': 1,
               'vis_variable': 1,
               'vis_variable_qa': 1,
               'temp_c': 5,
               'temp_qa': 1,
               'dew_point_c': 5,
               'dew_point_qa': 1,
               'pressure_hpa': 5,
               'pressure_qa': 1}

    widths = []
    column_names = []
    for column, width in columns.items():
        widths.append(width)
        column_names.append(column.upper())

    station_data = pd.read_fwf(file, widths=widths, names=column_names,
                               index_col=False, header=None)
    station_data['WBAN'] = station_data['WBAN'].astype(str).str.zfill(5)

    # missing observations are coded as 9999, remove those
    station_data.replace(999, np.nan, inplace=True)
    station_data.replace(9999, np.nan, inplace=True)
    station_data.replace(99999, np.nan, inplace=True)
    station_data.replace(999999, np.nan, inplace=True)

    # convert scaled columns
    station_data['LATITUDE'] = station_data['LATITUDE'] / 1000
    station_data['LONGITUDE'] = station_data['LONGITUDE'] / 1000
    station_data['PRESSURE_HPA'] = station_data['PRESSURE_HPA'] / 10
    station_data['TEMP_C'] = station_data['TEMP_C'] / 10
    station_data['TEMP_F'] = station_data['TEMP_C'] * 9 / 5 + 32
    station_data['DEW_POINT_C'] = station_data['DEW_POINT_C'] / 10
    station_data['DEW_POINT_F'] = station_data['DEW_POINT_C'] * 9 / 5 + 32

    station_data['DATETIME'] = pd.to_datetime(station_data[['YEAR', 'MONTH',
                                                            'DAY', 'HOUR', 'MINUTE']])

    station_data.to_csv(parsed_file_name, index=False)

    logger.debug(f'{file.name} file parsed to csv')

    return station_data


def weather_parser_lite(input_file, overwrite=True):
    """loads each txt file and parses raw data into dataframes.

    missing pressure measurements are linearly interpolated between adjacent points
    """

    if is_zero_file(input_file):
        logger.warning(f'{input_file.name} was empty')
        return None

    name = input_file.name[:-len(input_file.suffix)]
    year_dir = WEATHER_OUT_DIR / name[-4:]

    parsed_file_name = year_dir / f'{name}.csv'

    if not overwrite and parsed_file_name.exists():
        logger.debug(f'{parsed_file_name.name} already parsed; skipping parsing')
        return None

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
    current_file['datetime'] = pd.to_datetime(current_file[['year', 'month', 'day', 'hour']])
    # save to csv file
    current_file.to_csv(parsed_file_name, index=False)
    logger.debug(f'{name} file parsed to csv')


def parallel_parser(input_list, parallel=True, isd_lite=True, overwrite=True):
    if isd_lite:
        parser_function = weather_parser_lite
    else:
        parser_function = weather_parser_full

    inputs = zip(input_list, [overwrite]*len(input_list))

    if parallel:
        with mp.Pool(mp.cpu_count() - 1) as pool:
            pool.starmap(parser_function, inputs)

    else:
        for file in input_list:
            parser_function(file, overwrite)


def get_file_list(start_yr, end_yr, base_dir, file_ext):
    file_list = []
    for year in range(start_yr, end_yr + 1):
        year_dir = base_dir / str(year)
        if not year_dir.exists():
            logger.warning(f'Weather data not found for {year} in {base_dir}')
            continue
        all_files = [file for file in year_dir.glob(f'*.{file_ext}')]
        file_list.extend(all_files)

        # create directory for output file
        year_output_dir = WEATHER_OUT_DIR / str(year)
        if not year_output_dir.exists():
            year_output_dir.mkdir()

    return file_list


def main_parser(start, end, isd_lite=True, overwrite=True,
                base_dir=RAW_DATA_DIR, file_ext='txt'):
    file_list = get_file_list(start_yr=start, end_yr=end,
                              base_dir=base_dir, file_ext=file_ext)
    if isd_lite:
        parallel_parser(file_list, isd_lite=isd_lite, overwrite=overwrite)
    else:
        parallel_parser(file_list, isd_lite=isd_lite, overwrite=overwrite)


if __name__ == '__main__':
    start_year = 2020
    end_year = 2021

    parallel_parser(get_file_list(start_year,
                                  end_year,
                                  base_dir=RAW_DATA_DIR,
                                  file_ext='txt'))
