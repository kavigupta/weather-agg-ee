#!/usr/bin/env python3
"""
Download high temperature data from Google Earth Engine for 2019-05-02 at 2pm
and export as TIFF file.
"""

import ee

from download import download_ee_image


def create_temperature_image(start_date="2019-01-01", end_date="2019-01-31"):
    """Create the Earth Engine temperature image object with mean high temperature over time period.

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        ee.Image: Mean high temperature image in Celsius
    """
    # Initialize Earth Engine
    ee.Initialize()

    print(f"Creating mean high temperature image from {start_date} to {end_date}")

    # Load ERA5-Land hourly dataset
    # ERA5-Land provides temperature at 2m above ground
    era5_land = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")

    # Filter by date range
    temperature_collection = era5_land.filterDate(start_date, end_date)

    # Check if we have data for the period
    collection_size = temperature_collection.size().getInfo()
    if collection_size == 0:
        raise ValueError(
            f"No temperature data found for period {start_date} to {end_date}"
        )

    print(f"Found {collection_size} hourly images in the collection")

    # Select the temperature band (2m temperature in Kelvin) and convert to Celsius
    temperature_celsius_collection = temperature_collection.select(
        "temperature_2m"
    ).map(lambda image: image.subtract(273.15))

    # Calculate daily maximum temperatures properly
    # Get list of unique dates in the collection
    def get_daily_max_for_date(date_str):
        """Get maximum temperature for a specific date."""
        day_collection = temperature_celsius_collection.filter(
            ee.Filter.date(ee.Date(date_str), ee.Date(date_str).advance(1, "day"))
        )
        return day_collection.max()

    # Get list of dates in the range
    start_date_ee = ee.Date(start_date)
    end_date_ee = ee.Date(end_date)
    num_days = end_date_ee.difference(start_date_ee, "day").getInfo()

    # Create list of dates
    date_list = []
    for i in range(int(num_days) + 1):
        date = start_date_ee.advance(i, "day")
        date_list.append(date.format("YYYY-MM-dd").getInfo())

    # Get daily maximum temperatures
    daily_max_images = []
    for date_str in date_list:
        day_max = get_daily_max_for_date(date_str)
        # Check if the day has data by checking if it has bands
        day_info = day_max.getInfo()
        if day_info.get("bands"):  # Only add if the image has bands
            daily_max_images.append(day_max)
            print(f"Added daily max for {date_str}")
        else:
            print(f"No data for {date_str}")

    if not daily_max_images:
        raise ValueError(
            "No daily maximum temperature data found for any day in the period"
        )

    # Create image collection from daily maximums
    daily_max_collection = ee.ImageCollection.fromImages(daily_max_images)

    # Calculate mean of daily maximum temperatures over the period
    mean_daily_max_temp = daily_max_collection.mean()

    # Rename the band for clarity
    mean_daily_max_temp = mean_daily_max_temp.rename(
        "mean_daily_max_temperature_celsius"
    )

    print(
        f"Calculated mean daily maximum temperature over the period {start_date} to {end_date}"
    )

    return mean_daily_max_temp


if __name__ == "__main__":
    # Example: Get mean daily max temperature for January 2019
    temperature_image = create_temperature_image("2019-01-01", "2019-01-31")

    # Debug: Check the image properties
    image_info = temperature_image.getInfo()
    print(f"Image info: {image_info}")
    print(f"Image bands: {list(image_info.get('bands', []))}")

    # Download with configurable tile size (45° x 45° tiles = 8 tiles total)
    download_ee_image(
        temperature_image,
        "temp.npy",
        "mean_daily_max_temperature_celsius",
        resolution=0.25,
        degree_size=45,
    )
