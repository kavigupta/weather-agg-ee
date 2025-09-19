import numpy as np
from download import download_ee_image
import ee
from datetime import datetime, timedelta


from constants import date_start_str, date_end_str

from permacache import permacache


@permacache("weather-agg-ee/dewpoint/high_dewpoint_for_date", multiprocess_safe=True)
def high_dewpoint_for_date(date_str):
    ee.Initialize()
    date = ee.Date(date_str)
    era5_land = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")

    day_collection = era5_land.filter(ee.Filter.date(date, date.advance(1, "day")))

    return download_ee_image(
        day_collection.max(), "dewpoint_temperature_2m", resolution=0.25, degree_size=60
    )


def sampled_dewpoints(num_samples, quiet=True):
    start_date = datetime.strptime(date_start_str, "%Y-%m-%d").date()
    num_dates = (
        datetime.strptime(date_end_str, "%Y-%m-%d").date() - start_date
    ).days + 1
    shuffled_dates = np.random.RandomState(0).permutation(num_dates)

    results = []
    for i in range(num_samples):
        date = start_date + timedelta(days=int(shuffled_dates[i]))
        date_str = date.strftime("%Y-%m-%d")
        if not quiet:
            print(f"Processing date #{i} of {num_samples}: {date_str}")
        results.append(high_dewpoint_for_date(date_str))

    return np.array(results)

    # results = []
    # for i in range(0, num_samples, chunk_size):
    #     print(f"Processing chunk {i} of {num_samples}")
    #     offset_numbers = shuffled_offset_numbers[i:i+chunk_size]

    #     results.append(dewpoint_for_date(date_start_str, [int(x) for x in offset_numbers]))

    # return np.array(results)


if __name__ == "__main__":
    print(sampled_dewpoints(2000, quiet=False))
