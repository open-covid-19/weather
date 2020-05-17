# NOAA Weather Data
This repo contains very simple code used to preprocess weather data from NOAA for it to be used in
the [Open COVID-19 project](https://github.com/open-covid-19/data). This preprocessing performs the
following steps:
1. Downloads a list of all NOAA stations which provide at least max/min temperature measurements
1. Downloads all daily data available since 2010 for max/min temperature and, if available, precipitation and snowfall measurements

The output table is located at https://open-covid-19.github.io/weather/stations.csv and updated
daily. It contains the following columns:

| Name | Type | Description | Example |
| ---- | ---- | ----------- | ------- |
| **date** | `string` | ISO 8601 date (YYYY-MM-DD) of the datapoint | 2020-03-30 |
| **station** | `string` | Identifier for the weather station | USC00206080 |
| **minimum_temperature** | `double` [celsius] | Recorded hourly minimum temperature | 1.7 |
| **maximum_temperature** | `double` [celsius] | Recorded hourly maximum temperature | 19.4 |
| **rainfall** | `double` [millimeters] | Rainfall during the entire day | 51.0 |
| **snowfall** | `double` [millimeters] | Snowfall during the entire day | 0.0 |

## Sources of data
All data in this repository is retrieved automatically. When possible, data is retrieved directly
from the relevant authorities, like a country's ministry of health.

| Data | Source | License |
| ---- | ------ | ------- |
| Weather | [NOAA](https://www.ncei.noaa.gov) | Custom (unrestricted for non-commercial use) |

## License
The license of the data is subject to NOAA license terms (unrestricted for non-commercial use).
Please see the [LICENSE](LICENSE.md) file for more information about the license for everything else
in this repo.
