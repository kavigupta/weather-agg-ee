import ee
from datetime import datetime, timedelta
from permacache import permacache, drop_if_equal

from constants import date_start_str, date_end_str
from download import download_ee_image

# def high_temp_over_90f():
#     ee.Initialize()
#     era5 = ee.ImageCollection("ECMWF/ERA5/DAILY")
#     data = era5.filter(ee.Filter.date(ee.Date(date_start_str), ee.Date(date_end_str)))
#     data = data.filter(ee.Filter.eq("maximum_2m_air_temperature", 90))
#     return data.mean()


@permacache(
    "weather-agg-ee/mean_daily_stats/mean_daily_stats_for_segment",
    key_function=dict(mapping_fn=drop_if_equal(None)),
)
def mean_daily_stats_for_segment_and_timespan(
    band, filter_spec, date_start_str, date_end_str, mapping_fn=None
):
    print(band, filter_spec, mapping_fn, date_start_str, date_end_str)
    if mapping_fn is not None:
        mapping_fn = mapping_fn.replace("$x", band)
        mapping = lambda x: x.expression(mapping_fn, {band: x.select(band)})
    else:
        mapping = None
    return compute_daily(
        band, filter_spec, date_start_str, date_end_str, mapping=mapping
    )


def compute_daily(band, filter_spec, date_start_str, date_end_str, *, mapping=None):
    ee.Initialize()
    era5 = ee.ImageCollection("ECMWF/ERA5/DAILY")
    data = era5.filter(ee.Filter.date(ee.Date(date_start_str), ee.Date(date_end_str)))

    if filter_spec is not None:
        filter_spec = filter_spec.copy()
        assert filter_spec.pop("type") == "calendarRange"
        data = data.filter(ee.Filter.calendarRange(**filter_spec))

    if mapping is not None:
        data = data.map(mapping)

    mean_temp_for_segment = data.mean()

    return download_ee_image(
        mean_temp_for_segment, band, resolution=0.25, degree_size=45
    )


def decrement(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    date -= timedelta(days=1)
    return date.strftime("%Y-%m-%d")


def mean_daily_stats_for_segment(band, filter_spec, mapping_fn):
    dates = ["1990-01-01", "2000-01-01", "2010-01-01", "2020-01-01"]
    assert dates[0] == date_start_str and decrement(dates[-1]) == date_end_str
    results = 0
    total_weight = 0
    for start, end in zip(dates[:-1], dates[1:]):
        result = mean_daily_stats_for_segment_and_timespan(
            band, filter_spec, start, end, mapping_fn=mapping_fn
        )
        weight = (
            datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")
        ).days
        results += result * weight
        total_weight += weight

    return results / total_weight


def for_breaks(band, year_zero, breaks):
    breaks_days = [(x - year_zero).days for x in breaks]
    segments = [
        dict(type="calendarRange", start=start, end=end)
        for start, end in zip(breaks_days, breaks_days[1:])
    ]
    winter_1, spring, summer, fall, winter_2 = [
        mean_daily_stats_for_segment(band, filter_spec, None)
        for filter_spec in segments
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


def statistics_by_month(band):
    return [
        mean_daily_stats_for_segment(
            band,
            dict(type="calendarRange", start=month, end=month, field="month"),
            None,
        )
        for month in range(1, 1 + 12)
    ]


def month_seasonal_summary(band):
    year_zero = datetime(2020, 12, 31)
    # winter = DJF, spring = MAM, summer = JJA, fall = SON
    month_breaks = [
        datetime(2021, 1, 1),
        datetime(2021, 3, 1),
        datetime(2021, 6, 1),
        datetime(2021, 9, 1),
        datetime(2021, 12, 1),
        datetime(2021, 12, 31),
    ]
    return for_breaks(band, year_zero, month_breaks)


def temperature_histogram(band):
    return [
        mean_daily_stats_for_segment(
            band,
            None,
            "$x = $x > 273.15 + 5/9 * ($TEMP - 32)".replace("$TEMP", str(temp)),
        )
        for temp in range(-40, 150, 10)
    ]


def populate_caches():
    for band in "maximum_2m_air_temperature", "minimum_2m_air_temperature":
        mean_daily_stats_for_segment(band, None, None)
        astronomical_seasonal_summary(band)
        month_seasonal_summary(band)
        temperature_histogram(band)
        statistics_by_month(band)


if __name__ == "__main__":
    populate_caches()
