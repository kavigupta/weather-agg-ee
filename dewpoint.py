import multiprocessing
from datetime import datetime, timedelta

import ee
from permacache import permacache

from download import download_ee_image
from sample import compute_date_strs


@permacache("weather-agg-ee/dewpoint/high_dewpoint_for_date_5", multiprocess_safe=True)
def high_dewpoint_for_date(date_str):
    start = datetime.now()
    print(f"{start} - Start {date_str}")
    ee.Initialize()
    date = ee.Date(date_str)
    era5_land = ee.ImageCollection("ECMWF/ERA5/HOURLY")

    day_collection = era5_land.filter(ee.Filter.date(date, date.advance(1, "day")))

    result = download_ee_image(
        day_collection.max(),
        "dewpoint_temperature_2m",
        resolution=0.25,
        degree_size=45,
        pbar=False,
    )
    end = datetime.now()
    print(f"{end} - Finished {date_str}; took {end - start}")
    return result


@permacache("weather-agg-ee/dewpoint/high_temp_for_date", multiprocess_safe=True)
def high_temp_for_date(date_str):
    start = datetime.now()
    proc_id = multiprocessing.current_process().pid
    print(f"{start} - Start {date_str} [{proc_id}]")
    ee.Initialize()
    date = ee.Date(date_str)
    era5_land = ee.ImageCollection("ECMWF/ERA5/DAILY")
    day_collection = era5_land.filter(ee.Filter.date(date, date.advance(1, "day")))
    result = download_ee_image(
        day_collection.first(),
        "maximum_2m_air_temperature",
        resolution=0.25,
        degree_size=45,
        pbar=False,
    )
    end = datetime.now()
    print(f"{end} - Finished {date_str}; took {end - start} [{proc_id}]")
    return result


def high_dewpoint_for_date_for_parallel(date_str):
    return high_dewpoint_for_date(date_str)


def high_temp_for_date_for_parallel(date_str):
    return high_temp_for_date(date_str)


def populate_caches():
    with multiprocessing.Pool(processes=8) as pool:
        pool.map(high_dewpoint_for_date_for_parallel, compute_date_strs()[:2000])
        pool.map(high_temp_for_date_for_parallel, compute_date_strs()[:2000])


if __name__ == "__main__":
    populate_caches()
