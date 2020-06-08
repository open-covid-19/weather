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

        lines = res.text.split("\n")
        output = "\n".join([lines[0]] + lines[-KEEP_COUNT:])
        with open(output_folder / f"{station.id}.csv", "w") as fd:
            fd.write(output)

    except Exception as exc:
        print(exc, file=sys.stderr)


if __name__ == "__main__":

    ghcn_output = Path("output") / "ghcn"
    ghcn_output.mkdir(parents=True, exist_ok=True)

    # Get all the weather stations with data up until 2020
    ghcn_inventory_path = ghcn_output / "ghcnd-inventory.txt"
    stations_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
    ghcn_inventory_data = requests.get(stations_url).content
    with open(ghcn_inventory_path, "wb") as fd:
        fd.write(ghcn_inventory_data)
    stations = read_csv(
        ghcn_inventory_path,
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
    map_func = partial(process_ghcn, ghcn_output)

    # We can skip the index when iterating over the records
    map_iter = (record for _, record in stations.iterrows())

    # Bottleneck is network so we can use lots of threads in parallel
    concurrent.thread_map(map_func, map_iter, total=len(stations))
