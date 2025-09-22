import ee

import numpy as np
from permacache import permacache

from download import download_ee_image

from constants import date_start_str, date_end_str
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
    print(f"Precipitation {rain_or_snow} from {start_date} to {end_date}")
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
    return download_ee_image(
        collection.sum(), rain_or_snow, resolution=0.25, degree_size=45
    )


def compute_precipitation(date_end=date_end_str):
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
    snow_total = [0] * 12
    rain_total = [0] * 12
    for start, end in zip(dates[:-1], dates[1:]):
        _, month, _ = start.split("-")
        month_idx = int(month) - 1
        snow_total[month_idx] += compute_precipitation_for_month("snow", start, end)
        rain_total[month_idx] += compute_precipitation_for_month("rain", start, end)
    return {"snow": np.array(snow_total), "rain": np.array(rain_total)}


if __name__ == "__main__":
    compute_precipitation()
