import ee

from permacache import permacache

from download import download_ee_image

from constants import date_start_str, date_end_str
from mean_daily_stats import decrement

rain_snow_expressions = {
    "rain": "rain=(pt <= 4 ? 1 : (pt == 7 ? 0.5 : 0)) * tp",
    "snow": "snow=(pt <= 4 ? 0 : (pt == 7 ? 0.5 : 1)) * tp",
}


@permacache(
    "weather-agg-ee/precipitation/compute_precipitation_for_range", multiprocess_safe=True
)
def compute_precipitation_for_range(start_date, end_date, rain_or_snow):
    print(f"Precipitation {rain_or_snow} {start_date} to {end_date}")
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


def compute_precipitation():
    dates = [f"{year}-01-01" for year in range(1990, 2021)]
    assert dates[0] == date_start_str and decrement(dates[-1]) == date_end_str
    snow_total = 0
    rain_total = 0
    for start, end in zip(dates[:-1], dates[1:]):
        snow_total += compute_precipitation_for_range(start, end, "snow")
        rain_total += compute_precipitation_for_range(start, end, "rain")
    return {"snow": snow_total, "rain": rain_total}

if __name__ == "__main__":
    compute_precipitation()
