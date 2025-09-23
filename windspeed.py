import ee
from permacache import permacache

from download import download_ee_image
from sample import sampled_values

ten_mph_in_mps = 4.4704


@permacache(
    "weather-agg-ee/wind_speed/mean_wind_speed_for_date", multiprocess_safe=True
)
def mean_wind_speed_for_date(date_str):
    ee.Initialize()
    date = ee.Date(date_str)
    era5_land = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")

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
    return download_ee_image(
        day_collection.mean(), "wind_speed", resolution=0.25, degree_size=60
    )


@permacache("weather-agg-ee/wind_speed/high_wind_dates", multiprocess_safe=True)
def mean_high_wind_dates(count):
    sum_vals = sum(
        value > ten_mph_in_mps
        for value in sampled_values(mean_wind_speed_for_date, count)
    )
    return sum_vals / count


def high_wind_days():
    return mean_high_wind_dates(2000)
