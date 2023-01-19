# ISDownload
## Download and parse NOAA ISD Weather Data 

Download data from [NOAA ISD](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) weather stations. Just provide an input CSV file with 'USAF', 'WBAN' columns to download raw hourly weather data files from NOAA and parse them to CSVs. 

If you don't know the USAF and WBAN of the weather station, you can use [this tool](https://github.com/timmyoder/isd_station_lookup) to look up the closest weather station to a Lat/Lon point (or list points).  

## Using the tool

The program can be run using the command line interface (CLI). Parsed weather files are saved to the ```weather_data_output/``` directory with a child directory for each year.

You MUST specify the first year to download data for (multiple years can optionally be downloaded using the --to-year option) as well as either a station identifier or an input file that contains one or more station identifiers.![](![]())

By default, the tool uses the ISD Lite dataset and overwrites existing files. These options can be changed with the corresponding flags.

If using the full ISD dataset, beware that there is much more data as there are multiple readings per hour and many additional readings compared to the ISD Lite dataset.

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

* `python isdownload.py --file FILE_NAME --year YEAR`

example

* `python isdownload.py --file sample_input.csv --year 2020`

### Download data for a range of years

The tool can be used to download data from multiple years at once (and for multiple stations if using the --file input file option).

usage (shown with file input, but can also be used with --id station input)

* `python isdownload.py --file FILE_NAME --year YEAR, --to-year TO_YEAR`

example

* `python isdownload.py --file sample_input.csv --year 2020 --to-year 2022`


## ISD Lite Info

Below are some relevant excerpts from the [ISD Lite documentation](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-technical-document.pdf) highlighting some differences between it and the full dataset: 

### Suitability of Data

The ISD-Lite product is not intended to replace the full ISD for situations where exact observation time and/or observation density is critical. Instead, it is designed to be a
smaller and easier to work with format which may be suitable for investigating trends,
larger scale patterns or rough climatological averages.

It should also be noted that inter-comparison of fields across an observational hour may
not be recommended due to the possibility of slight differences in actual observation
time between elements. This feature is most pronounced between the “mandatory”
elements and the “additional” elements, for example air temperature versus precipitation.
In many cases the observation time for these elements can differ by as much as ten
minutes. 

### Suitability of Data
The ISD-Lite product is not intended to replace the full ISD for situations where exact observation time and/or observation density is critical. Instead, it is designed to be a smaller and easier to work with format which may be suitable for investigating trends, larger scale patterns or rough climatological averages. 

It should also be noted that inter-comparison of fields across an observational hour may not be recommended due to the possibility of slight differences in actual observation time between elements. This feature is most pronounced between the “mandatory” elements and the “additional” elements, for example air temperature versus precipitation. In many cases the observation time for these elements can differ by as much as ten minutes. 
