import multiprocessing
from datetime import datetime, timedelta

import ee
from permacache import permacache

from download import download_ee_image
from sample import compute_date_strs


@permacache("weather-agg-ee/dewpoint/high_dewpoint_for_date", multiprocess_safe=True)
def high_dewpoint_for_date(date_str):
    start = datetime.now()
    print(f"{start} - Start {date_str}")
    ee.Initialize()
    date = ee.Date(date_str)
    era5_land = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")

    day_collection = era5_land.filter(ee.Filter.date(date, date.advance(1, "day")))

    result = download_ee_image(
        day_collection.max(),
        "dewpoint_temperature_2m",
        resolution=0.25,
        degree_size=60,
        pbar=False,
    )
    end = datetime.now()
    print(f"{end} - Finished {date_str}; took {end - start}")

    # results = []
    # for i in range(0, num_samples, chunk_size):
    #     print(f"Processing chunk {i} of {num_samples}")
    #     offset_numbers = shuffled_offset_numbers[i:i+chunk_size]

    #     results.append(dewpoint_for_date(date_start_str, [int(x) for x in offset_numbers]))

    # return np.array(results)


def high_dewpoint_for_date_for_parallel(date_str):
    return high_dewpoint_for_date(date_str)


def populate_caches():
    with multiprocessing.Pool(processes=8) as pool:
        pool.map(high_dewpoint_for_date_for_parallel, compute_date_strs())


if __name__ == "__main__":
    populate_caches()
