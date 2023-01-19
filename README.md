# ISDownload
## Download and parse NOAA ISD Weather Data 

Download data from [NOAA ISD](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) weather stations. Just provide an input CSV file with 'USAF', 'WBAN' columns to download raw hourly weather data files from NOAA and parse them to CSVs. 

If you don't know the USAF and WBAN of the weather station, you can use [this tool](https://github.com/timmyoder/isd_station_lookup) to look up the closest weather station to a Lat/Lon point (or list points).  

## Using the tool

The program can be run using the command line interface (CLI). Parsed weather files are saved to the ```weather_data_output/``` directory with a child directory for each year.

You MUST specify the first year to download data for (multiple years can optionally be downloaded using the --to-year option) as well as either a station identifier or an input file that contains one or more station identifiers.![](![]())

By default, the tool uses the ISD Lite dataset and overwrites existing files. These options can be changed with the corresponding flags.

If using the full ISD dataset, beware that there is much more data as there are multiple readings per hour and many additional readings compared to the ISD Lite dataset. The ISD Lite dataset also has some cleaning and validation performed.

```
usage: isdownload.py [-h] [--id ID] [--file FILE] --year YEAR [--to-year TO_YEAR] [--isd-full] [--no-overwrite]

Download and parse NOAA ISD hourly weather data

options:
  -h, --help         show this help message and exit
  --id ID            Provide the USAF WBAN station identifier Example: --id 727935-24234
  --file FILE        optional input csv file with station identifiers. CSV must have "USAF" and "WBAN" columns. 
                     File must be in the 'input_data/' dir. Do not include the path to the directory in
                     the name. Example: --file input.csv
  --year YEAR        Year of data to pull. Example: --year 2020
  --to-year TO_YEAR  If provided, pulls data for all years between 'year' and 'to_year' 
                     (e.g., pulls from 2020 to 2021: --year 2020 --to_year 2021). Example: --to_year 2021
  --isd-full         Use the full ISD dataset instead of ISD lite.
  --no-overwrite     Do not overwrite existing files.
```

### Specify the station with the CLI

You can specify the station to download data for with the USAF and WBAN identifiers.

usage

*  ` python isdownload.py --id USAF-WBAN --year YEAR`

example

* ` python isdownload.py --id 727935-24234 --year 2020`


### Download data for multiple stations at once

Place a csv file with desired station  into the `input_data/` directory. **The csv file must contain columns named 'USAF' and 'WBAN'.** Additional columns can be included in the file, but are not used. For an example of an input file see ```input/sample_input.csv```. 

usage

* `python lookup.py --file FILE_NAME --year YEAR`

example

* `python lookup.py --file sample_input.csv --year 2020`

### Download data for a range of years

The tool can be used to download data from multiple years at once (and for multiple stations if using the --file input file option).

usage (shown with file input, but can also be used with --id station input)

* `python lookup.py --file FILE_NAME --year YEAR, --to-year TO_YEAR`

example

* `python lookup.py --file sample_input.csv --year 2020 --to-year 2022`


