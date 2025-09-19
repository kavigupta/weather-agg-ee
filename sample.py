from datetime import datetime, timedelta

import numpy as np

from constants import date_end_str, date_start_str


def sampled_values(fn, num_samples, quiet=True):
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
        results.append(fn(date_str))

    return np.array(results)
