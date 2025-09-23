import multiprocessing
from datetime import datetime

import ee
import numpy as np
from permacache import permacache

from constants import date_end_str, date_start_str
from download import download_ee_image
from mean_daily_stats import decrement

rain_snow_expressions = {
    "rain": "rain=(pt <= 4 ? 1 : (pt == 7 ? 0.5 : 0)) * tp",
    "snow": "snow=(pt <= 4 ? 0 : (pt == 7 ? 0.5 : 1)) * tp",
}


@permacache(
    "weather-agg-ee/precipitation/compute_precipitation_for_month",
    multiprocess_safe=True,
)
def compute_precipitation_for_month(rain_or_snow, start_date, end_date):
    print(
        f"{datetime.now()} Precipitation {rain_or_snow} from {start_date} to {end_date}"
    )
    ee.Initialize()
    era5 = ee.ImageCollection("ECMWF/ERA5/HOURLY")
    collection = era5.filter(ee.Filter.date(ee.Date(start_date), ee.Date(end_date)))
    collection = collection.map(
        lambda x: x.expression(
            rain_snow_expressions[rain_or_snow],
            {
                "pt": x.select("precipitation_type"),
                "tp": x.select("total_precipitation"),
            },
        )
    )
    result = download_ee_image(
        collection.sum(), rain_or_snow, resolution=0.25, degree_size=45, pbar=False
    )
    print(f"{datetime.now()} Done {rain_or_snow} from {start_date} to {end_date}")
    return result


def compute_all_months(date_end):
    dates = [
        f"{year}-{month:02d}-01"
        for year in range(1990, 2021, 1)
        for month in range(1, 13)
    ]
    dates = [
        date for date in dates if date >= date_start_str and decrement(date) <= date_end
    ]
    assert dates[0] == date_start_str and decrement(dates[-1]) == date_end, (
        dates[-1],
        date_end,
    )
    all_months = zip(dates[:-1], dates[1:])
    return all_months


@permacache(
    "weather-agg-ee/precipitation/compute_precipitation",
    multiprocess_safe=True,
)
def compute_precipitation(date_end=date_end_str):
    all_months = compute_all_months(date_end)
    snow_total = [0] * 12
    rain_total = [0] * 12
    for start, end in all_months:
        _, month, _ = start.split("-")
        month_idx = int(month) - 1
        snow_total[month_idx] += compute_precipitation_for_month("snow", start, end)
        rain_total[month_idx] += compute_precipitation_for_month("rain", start, end)
    return {"snow": np.array(snow_total), "rain": np.array(rain_total)}


def compute_precipitation_for_month_for_parallel(rain_or_snow, start_date, end_date):
    return compute_precipitation_for_month(rain_or_snow, start_date, end_date)


def precipitation_stats_dict():
    precip = compute_precipitation()
    delta = datetime.strptime(date_end_str, "%Y-%m-%d") - datetime.strptime(
        date_start_str, "%Y-%m-%d"
    )
    years = delta.days / 365.2425
    results = {
        f"precipitation_{ros}_{mo + 1:02d}": (precip[ros][mo] / years, "m")
        for ros in ["rain", "snow"]
        for mo in range(12)
    }
    return results


def populate_caches():
    parameters = [
        (ros, start, end)
        for start, end in compute_all_months(date_end_str)
        for ros in ["rain", "snow"]
    ]
    with multiprocessing.Pool(8) as pool:
        list(pool.starmap(compute_precipitation_for_month_for_parallel, parameters))


if __name__ == "__main__":
    populate_caches()
