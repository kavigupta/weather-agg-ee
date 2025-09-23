import os
import shutil
from PIL import Image
import matplotlib
import numpy as np

from all_stats import draw_title
from precipitation import compute_precipitation


def monthly_data_image(monthly, min_val, max_val):
    monthly = np.clip((monthly - min_val) / (max_val - min_val), 0, 1)
    v = monthly.max(0)
    c = monthly.max(0) - monthly.min(0)
    s = c / v
    # s = s / s.max()
    theta = np.arange(12) / 12 * 2 * np.pi
    locations = monthly.transpose(1, 2, 0) @ np.array([np.cos(theta), np.sin(theta)]).T
    h = np.rad2deg(np.atan2(locations[..., 1], locations[..., 0]) % (2 * np.pi)) / 360
    img = (
        (matplotlib.colors.hsv_to_rgb(np.dstack([h, s, v])) * 255)
        .round()
        .astype(np.uint8)
    )
    return img


def precipitation_plot(snow, rain):
    color = snow[..., None] * [3, 82, 252] + rain[..., None] * [252, 157, 3]
    color = np.clip(color, 0, 255).astype(np.uint8)
    return Image.fromarray(color)


def precipitation_by_month_video(**kwargs):
    precip = compute_precipitation(**kwargs)
    snow = precip["snow"] / np.percentile(precip["snow"], 95)
    rain = precip["rain"] / np.percentile(precip["rain"], 95)
    shutil.rmtree("precipitation_video", ignore_errors=True)
    try:
        os.makedirs("precipitation_video")
    except FileExistsError:
        pass
    for mo_idx in range(12):
        img = precipitation_plot(snow[mo_idx], rain[mo_idx])
        draw_title("Precipitation Month {}\nRain=Orange, Snow=Blue".format(mo_idx + 1), img)

        img.save("precipitation_video/month_{}.png".format(mo_idx))
    os.system(
        "ffmpeg -framerate 2 -i precipitation_video/month_%d.png "
        "-vf 'scale=1920:1080' "
        "-c:v libx264 -r 30 -pix_fmt yuv420p precipitation_video/precipitation_video.mp4"
    )
