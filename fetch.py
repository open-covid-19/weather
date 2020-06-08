import sys
from pathlib import Path
from functools import partial
from typing import Any, Optional, TextIO

import requests
from tqdm import tqdm
from pandas import Series, read_csv
from tqdm.contrib import concurrent

KEEP_COUNT = 700


def process_ghcn(output_folder: Path, station: Series) -> None:

    # Download the last KEEP_COUNT lines of the station's records
    try:
        res = requests.get(
            "https://www.ncei.noaa.gov/data"
            f"/global-historical-climatology-network-daily/access/{station.id}.csv"
        )
        if res.status_code != 200:
            raise RuntimeError(f"Unexpected status code {res.status_code}")

        output = "\n".join(res.text.split("\n")[-KEEP_COUNT:])
        with open(output_folder / f"{station.id}.csv", "w") as fd:
            fd.write(output)

    except Exception as exc:
        print(exc, file=sys.stderr)


if __name__ == "__main__":

    # Get all the weather stations with data up until 2020
    stations_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
    stations = read_csv(
        stations_url,
        sep=r"\s+",
        names=("id", "latitude", "longitude", "measurement", "year_start", "year_end"),
    )

    # Filter only active stations
    stations = stations[stations.year_end == 2020]

    # Filter stations that at least provide max and min temps
    measurements = ["TMIN", "TMAX"]
    stations = stations.groupby(["id", "latitude", "longitude"]).agg(lambda x: "|".join(x))
    stations = stations[
        stations.measurement.apply(lambda x: all(m in x.split("|") for m in measurements))
    ].reset_index()

    # Make sure that the process function receives output folder
    output_folder = Path("output") / "ghcn"
    map_func = partial(process_ghcn, output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    # We can skip the index when iterating over the records
    map_iter = (record for _, record in stations.iterrows())

    # Bottleneck is network so we can use lots of threads in parallel
    concurrent.thread_map(map_func, map_iter, total=len(stations))
