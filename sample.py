from datetime import datetime, timedelta

import numpy as np
import tqdm

from constants import date_end_str, date_start_str


def sampled_values(fn, num_samples, quiet=True):
    start_date = datetime.strptime(date_start_str, "%Y-%m-%d").date()
    num_dates = (
        datetime.strptime(date_end_str, "%Y-%m-%d").date() - start_date
    ).days + 1
    shuffled_dates = np.random.RandomState(0).permutation(num_dates)

    for i in tqdm.trange(num_samples):
        date = start_date + timedelta(days=int(shuffled_dates[i]))
        date_str = date.strftime("%Y-%m-%d")
        if not quiet:
            print(f"Processing date #{i} of {num_samples}: {date_str}")
        yield fn(date_str)
