import os
import shutil
import numpy as np
from PIL import Image, ImageDraw, ImageFont
import matplotlib as mpl
import tqdm

from cloud_cover import compute_cloud_segment_overall
from mean_daily_stats import temperature_stats_dict
from windspeed import high_wind_days


output_folder = "output"
images_folder = "images"


def all_stats():
    return {
        "sunniness": (compute_cloud_segment_overall(), "%"),
        "windspeed_over_10mph": (high_wind_days(), "%"),
        **temperature_stats_dict(),
    }


def save_to_npz(statname, stat):
    stat = stat.astype(np.float32)
    try:
        os.makedirs(output_folder, exist_ok=True)
    except FileExistsError:
        pass
    np.savez_compressed(f"{output_folder}/{statname}.npz", arr=stat)


def save_image(statname, stat, unit):
    # plot the given stat as an image, with a title. Do not have any axes or other padding
    # use viridis to color the image
    try:
        os.makedirs(images_folder, exist_ok=True)
    except FileExistsError:
        pass
    low, hi = {"K": (273.15 - 10, 273.15 + 40), "%": (0, 1)}[unit]
    stat = (stat - low) / (hi - low)
    img = Image.fromarray((mpl.cm.viridis(stat) * 255).astype(np.uint8))
    draw_title(statname, img)
    img.save(f"{images_folder}/{statname}.png")


def draw_title(statname, img):
    draw = ImageDraw.Draw(img)
    # make the text large and centered at the top
    font = ImageFont.truetype("Arial.ttf", 48)
    bbox = draw.textbbox((0, 0), statname, font=font)
    text_width = bbox[2] - bbox[0]
    draw.text(
        ((img.width - text_width) / 2, 10),
        statname.replace("_", " ").title(),
        fill=(255, 255, 255, 255),
        font=font,
    )


def run_ffmpeg_monthly():
    # Create a slow, high-res MP4 from monthly images
    os.system(
        "ffmpeg -y -framerate 2 -i images/maxdaily_temp_month_%02d.png "
        "-vf 'scale=1920:1080,format=yuv420p' "
        "-c:v libx264 -pix_fmt yuv420p "
        "images/maxdaily_temp_month.mp4"
    )


def main():
    shutil.rmtree(output_folder, ignore_errors=True)
    shutil.rmtree(images_folder, ignore_errors=True)
    stats = all_stats()
    for statname, (stat, unit) in tqdm.tqdm(stats.items()):
        save_to_npz(statname, stat)
        save_image(statname, stat, unit)
    run_ffmpeg_monthly()


if __name__ == "__main__":
    main()
