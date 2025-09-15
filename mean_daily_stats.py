import ee
from datetime import datetime
from permacache import permacache

from constants import date_start_str, date_end_str
from download import download_ee_image

# def high_temp_over_90f():
#     ee.Initialize()
#     era5 = ee.ImageCollection("ECMWF/ERA5/DAILY")
#     data = era5.filter(ee.Filter.date(ee.Date(date_start_str), ee.Date(date_end_str)))
#     data = data.filter(ee.Filter.eq("maximum_2m_air_temperature", 90))
#     return data.mean()


@permacache("weather-agg-ee/mean_daily_stats/mean_daily_stats_for_segment")
def mean_daily_stats_for_segment(band, filter_spec):
    ee.Initialize()
    era5 = ee.ImageCollection("ECMWF/ERA5/DAILY")
    data = era5.filter(ee.Filter.date(ee.Date(date_start_str), ee.Date(date_end_str)))

    if filter_spec is not None:
        filter_spec = filter_spec.copy()
        assert filter_spec.pop("type") == "calendarRange"
        data = data.filter(ee.Filter.calendarRange(**filter_spec))

    mean_temp_for_segment = data.mean()

    return download_ee_image(
        mean_temp_for_segment, band, resolution=0.25, degree_size=60
    )


def for_breaks(band, year_zero, breaks):
    breaks_days = [(x - year_zero).days for x in breaks]
    segments = [
        dict(type="calendarRange", start=start, end=end)
        for start, end in zip(breaks_days, breaks_days[1:])
    ]
    winter_1, spring, summer, fall, winter_2 = [
        mean_daily_stats_for_segment(band, filter_spec) for filter_spec in segments
    ]
    winter_1_length = segments[0]["end"] - segments[0]["start"]
    winter_2_length = segments[4]["end"] - segments[4]["start"]
    winter = (winter_1 * winter_1_length + winter_2 * winter_2_length) / (
        winter_1_length + winter_2_length
    )
    return winter, spring, summer, fall


def astronomical_seasonal_summary(band):
    year_zero = datetime(2020, 12, 31)
    breaks = [
        datetime(2021, 1, 1),
        datetime(2021, 3, 20),
        datetime(2021, 6, 21),
        datetime(2021, 9, 22),
        datetime(2021, 12, 21),
        datetime(2021, 12, 31),
    ]

    return for_breaks(band, year_zero, breaks)


def month_seasonal_summary(band):
    year_zero = datetime(2020, 12, 31)
    # winter = DJF, spring = MAM, summer = JJA, fall = SON
    month_breaks = [
        datetime(2021, 1, 1),
        datetime(2021, 3, 1),
        datetime(2021, 6, 1),
        datetime(2021, 9, 1),
        datetime(2021, 12, 1),
    ]
    return for_breaks(band, year_zero, month_breaks)


def populate_caches():
    for band in "minimum_2m_air_temperature", "maximum_2m_air_temperature":
        mean_daily_stats_for_segment(band, None)
        astronomical_seasonal_summary(band)
        month_seasonal_summary(band)


if __name__ == "__main__":
    populate_caches()
