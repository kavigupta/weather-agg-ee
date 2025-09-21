from datetime import datetime, timedelta

import ee
from permacache import permacache

from download import download_ee_image
from sample import sampled_values


@permacache("weather-agg-ee/dewpoint/high_dewpoint_for_date", multiprocess_safe=True)
def high_dewpoint_for_date(date_str):
    ee.Initialize()
    date = ee.Date(date_str)
    era5_land = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")

    day_collection = era5_land.filter(ee.Filter.date(date, date.advance(1, "day")))

    return download_ee_image(
        day_collection.max(), "dewpoint_temperature_2m", resolution=0.25, degree_size=60
    )

    # results = []
    # for i in range(0, num_samples, chunk_size):
    #     print(f"Processing chunk {i} of {num_samples}")
    #     offset_numbers = shuffled_offset_numbers[i:i+chunk_size]

    #     results.append(dewpoint_for_date(date_start_str, [int(x) for x in offset_numbers]))

    # return np.array(results)


if __name__ == "__main__":
    for _ in sampled_values(high_dewpoint_for_date, 2000, quiet=False):
        pass
