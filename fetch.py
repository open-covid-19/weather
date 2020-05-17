import sys
from functools import partial
from typing import Any, Optional, TextIO
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool as Pool

from tqdm import tqdm
from pandas import Series, read_csv

OUTPUT_COLUMNS = [
    "date",
    "station",
    "latitude",
    "longitude",
    "elevation",
    "minimum_temperature",
    "maximum_temperature",
    "rainfall",
    "snowfall",
]


def int_cast(value: Optional[Any]) -> Optional[int]:
    try:
        value = int(value)
        return value
    except:
        return None


def fix_temp(value: Optional[Any]) -> Optional[int]:
    value = int_cast(value)
    if value is not None:
        return "{0:.1f}".format(value / 10.0)
    else:
        return None


def process_station(fd: TextIO, record: Series) -> None:

    # Read the records from the nearest station
    station_url = (
        "https://www.ncei.noaa.gov/data"
        "/global-historical-climatology-network-daily/access/{}.csv"
    ).format(record.station)
    column_mapping = {
        "DATE": "date",
        "STATION": "station",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "ELEVATION": "elevation",
        "TMIN": "minimum_temperature",
        "TMAX": "maximum_temperature",
        "PRCP": "rainfall",
        "SNOW": "snowfall",
    }
    data = read_csv(station_url, usecols=lambda column: column in column_mapping.keys())
    data = data.rename(columns=column_mapping)

    # Convert temperature to correct values
    data["minimum_temperature"] = data["minimum_temperature"].apply(fix_temp)
    data["maximum_temperature"] = data["maximum_temperature"].apply(fix_temp)

    # Properly format coordinates and elevation
    data["latitude"] = data["latitude"].apply(lambda x: "{0:.06g}".format(x))
    data["longitude"] = data["longitude"].apply(lambda x: "{0:.06g}".format(x))
    data["elevation"] = data["elevation"].apply(int_cast).astype("Int64")

    # Get only data starting on 2015
    data = data[data.date > "2014-12-31"]

    # Make sure that all columns are in the record, even if null
    for column in filter(lambda x: x not in data.columns, OUTPUT_COLUMNS):
        data[column] = None

    # Put the output columns in the appropriate order and get rid of empty records
    data = data[OUTPUT_COLUMNS].dropna(subset=OUTPUT_COLUMNS[-4:], how="all")

    # Write the processed station data
    data.sort_values("date").to_csv(fd, header=False, index=False)
    fd.flush()


if __name__ == "__main__":

    # Open the output file for writing and add header
    output_path = sys.argv[1]
    fd = open(output_path, "a")
    fd.truncate(0)
    fd.write(",".join(OUTPUT_COLUMNS) + "\n")

    # Get all the weather stations with data up until 2020
    stations_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
    stations = read_csv(
        stations_url,
        sep=r"\s+",
        names=("station", "latitude", "longitude", "measurement", "year_start", "year_end"),
    )
    stations = stations[stations.year_end == 2020][
        ["station", "latitude", "longitude", "measurement"]
    ]

    # Filter stations that at least provide max and min temps
    measurements = ["TMIN", "TMAX"]
    stations = stations.groupby(["station", "latitude", "longitude"]).agg(lambda x: "|".join(x))
    stations = stations[
        stations.measurement.apply(lambda x: all(m in x.split("|") for m in measurements))
    ].reset_index()

    # Make sure that the process function receives our file handle
    map_func = partial(process_station, fd)

    # We can skip the index when iterating over the records
    map_iter = (record for _, record in stations.iterrows())

    # Bottleneck is network so we can use lots of threads in parallel
    records = list(
        tqdm(Pool(cpu_count() * 4).imap_unordered(map_func, map_iter), total=len(stations),)
    )

    # Close the output file
    fd.close()
