import multiprocessing
from datetime import datetime

import ee
from permacache import permacache

from download import download_ee_image
from sample import compute_date_strs, sampled_values

ten_mph_in_mps = 4.4704


@permacache(
    "weather-agg-ee/wind_speed/mean_wind_speed_for_date_3", multiprocess_safe=True
)
def mean_wind_speed_for_date(date_str):
    start = datetime.now()
    ee.Initialize()
    print(f"{start} - Start {date_str}")
    date = ee.Date(date_str)
    era5_land = ee.ImageCollection("ECMWF/ERA5/HOURLY")

    day_collection = era5_land.filter(ee.Filter.date(date, date.advance(1, "day")))

    day_collection = day_collection.map(
        lambda x: x.expression(
            "wind_speed = sqrt(u_component_of_wind_10m * u_component_of_wind_10m + v_component_of_wind_10m * v_component_of_wind_10m)",
            {
                "u_component_of_wind_10m": x.select("u_component_of_wind_10m"),
                "v_component_of_wind_10m": x.select("v_component_of_wind_10m"),
            },
        )
    )
    result = download_ee_image(
        day_collection.mean(), "wind_speed", resolution=0.25, degree_size=60, pbar=False
    )
    end = datetime.now()
    print(f"{end} - Finished {date_str}; took {end - start}")
    return result


@permacache("weather-agg-ee/wind_speed/high_wind_dates_2", multiprocess_safe=True)
def mean_high_wind_dates(count):
    sum_vals = sum(
        value > ten_mph_in_mps
        for value in sampled_values(mean_wind_speed_for_date, count)
    )
    return sum_vals / count


def high_wind_days():
    return mean_high_wind_dates(2000)


def mean_wind_speed_for_date_for_parallel(date_str):
    return mean_wind_speed_for_date(date_str)


def populate_caches():
    with multiprocessing.Pool(processes=8) as pool:
        pool.map(mean_wind_speed_for_date_for_parallel, compute_date_strs()[:2000])


if __name__ == "__main__":
    populate_caches()
