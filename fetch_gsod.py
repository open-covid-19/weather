import sys
import datetime
from pathlib import Path
from functools import partial
from typing import Any, Optional, TextIO

import requests
from tqdm import tqdm
from pandas import Series, read_csv
from tqdm.contrib import concurrent


def process_gsod(output_folder: Path, station: Series) -> None:

    # Download the last KEEP_COUNT lines of the station's records
    try:
        res = requests.get(
            f"https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/2020/{station.id}.csv"
        )
        if res.status_code != 200:
            raise RuntimeError(f"Unexpected status code {res.status_code}")

        with open(output_folder / f"{station.id}.csv", "wb") as fd:
            fd.write(res.content)

    except Exception as exc:
        print(exc, file=sys.stderr)


if __name__ == "__main__":

    gsod_output = Path("output") / "gsod"
    gsod_output.mkdir(parents=True, exist_ok=True)

    # Get all the weather stations with data up until last month
    today = datetime.date.today()
    min_date = (today - datetime.timedelta(days=30)).strftime("%Y%m%d")
    stations = read_csv("ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv")
    stations.to_csv(gsod_output / "isd-history.csv", index=False)
    stations = stations[stations.END > int(min_date)]
    stations["id"] = stations["USAF"] + stations["WBAN"].apply(lambda x: f"{x:05d}")

    # Make sure that the process function receives output folder
    map_func = partial(process_gsod, gsod_output)

    # We can skip the index when iterating over the records
    map_iter = (record for _, record in stations.iterrows())

    # Bottleneck is network so we can use lots of threads in parallel
    concurrent.thread_map(map_func, map_iter, total=len(stations))
