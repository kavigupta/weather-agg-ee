from datetime import datetime, timedelta

import ee
from mean_daily_stats import decrement
from permacache import drop_if_equal, permacache

from constants import date_end_str, date_start_str
from download import download_ee_image

# def high_temp_over_90f():
#     ee.Initialize()
#     era5 = ee.ImageCollection("ECMWF/ERA5/DAILY")
#     data = era5.filter(ee.Filter.date(ee.Date(date_start_str), ee.Date(date_end_str)))
#     data = data.filter(ee.Filter.eq("maximum_2m_air_temperature", 90))
#     return data.mean()


@permacache(
    "weather-agg-ee/cloud_cover/cloud_cover_for_segment",
)
def cloud_cover_for_segment(date_start_str, date_end_str):
    ee.Initialize()
    era5 = ee.ImageCollection("ECMWF/ERA5/HOURLY")
    day = era5.filter(
        ee.Filter.date(ee.Date(date_start_str), ee.Date(date_end_str))
    ).filter(ee.Filter.date(ee.Date(date_start_str), ee.Date(date_end_str)))
    day = day.map(
        lambda x: x.expression(
            "sun = flux > 0.001 ? (1 - cloud) : 0",
            {
                "flux": x.select(
                    "mean_surface_direct_short_wave_radiation_flux_clear_sky"
                ),
                "cloud": x.select("total_cloud_cover"),
            },
        )
    )
    return download_ee_image(day.mean(), "sun", resolution=0.25, degree_size=45)


def compute_cloud_segment_overall():
    dates = ["1990-01-01", "2000-01-01", "2010-01-01", "2020-01-01"]
    assert dates[0] == date_start_str and decrement(dates[-1]) == date_end_str
    results = 0
    total_weight = 0
    for start, end in zip(dates[:-1], dates[1:]):
        result = cloud_cover_for_segment(start, end)
        weight = (
            datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")
        ).days
        results += result * weight
        total_weight += weight
    return results / total_weight


if __name__ == "__main__":
    compute_cloud_segment_overall()
