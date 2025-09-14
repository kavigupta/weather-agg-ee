#!/usr/bin/env python3
"""
Download high temperature data from Google Earth Engine for 2019-05-02 at 2pm
and export as TIFF file.
"""

from download import download_ee_image
import ee


def create_temperature_image():
    """Create the Earth Engine temperature image object."""
    # Initialize Earth Engine
    ee.Initialize()

    # Define the date and time (2019-05-02 at 2pm UTC)
    target_date = "2019-01-02"
    target_time = "14:00:00"  # 2pm in 24-hour format

    # Create datetime object for filtering
    start_time = f"{target_date}T{target_time}"
    end_time = f"{target_date}T14:00:01"  # 1 second later for exact time

    print(f"Creating temperature image for {start_time}")

    # Load ERA5-Land hourly dataset
    # ERA5-Land provides temperature at 2m above ground
    era5_land = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")

    # Filter by date and time
    temperature_collection = era5_land.filterDate(start_time, end_time)

    # Get the first (and only) image from the filtered collection
    temperature_image = temperature_collection.first()

    if temperature_image is None:
        raise ValueError(f"No temperature data found for {start_time}")

    # Select the temperature band (2m temperature in Kelvin)
    # Convert from Kelvin to Celsius
    temperature_celsius = temperature_image.select("temperature_2m").subtract(273.15)

    # Rename the band for clarity
    temperature_celsius = temperature_celsius.rename("temperature_2m_celsius")

    # Get the image properties
    image_info = temperature_image.getInfo()
    print(f"Image ID: {image_info.get('id', 'Unknown')}")
    print(f"Image bands: {list(image_info.get('bands', []))}")

    return temperature_celsius

if __name__ == "__main__":
    download_ee_image(create_temperature_image(), "temp.npy")
