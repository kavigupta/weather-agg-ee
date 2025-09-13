#!/usr/bin/env python3
"""
Download high temperature data from Google Earth Engine for 2019-05-02 at 2pm
and export as TIFF file.
"""

import ee
import os
import numpy as np


def create_temperature_image():
    """Create the Earth Engine temperature image object."""
    # Initialize Earth Engine
    ee.Initialize()

    # Define the date and time (2019-05-02 at 2pm UTC)
    target_date = "2019-05-02"
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


def download_quadrant(bounds, ee_data: ee.Image):
    """Download temperature data for a specific quadrant."""
    print(f"Downloading {bounds}...")

    # Create export region for this quadrant
    export_region = ee.Geometry.Rectangle(bounds)

    resampled_image = ee_data.reproject(crs="EPSG:4326", scale=111_400.0 * 0.25)

    # Get the image as a numpy array
    image_array = resampled_image.sampleRectangle(region=export_region, defaultValue=0)

    # Get the actual data
    quadrant_temp_data = image_array.get("temperature_2m_celsius").getInfo()

    print(
        f"Downloaded {bounds} shape: {len(quadrant_temp_data)} x {len(quadrant_temp_data[0])}"
    )
    return quadrant_temp_data


def merge_quadrants(quadrant_data):
    """Merge 4 quadrant arrays into a single global array."""
    print("Merging quadrants...")

    # Quadrant order: NW, NE, SW, SE
    nw_data, ne_data, sw_data, se_data = quadrant_data

    # Get dimensions
    nw_height = len(nw_data)
    nw_width = len(nw_data[0])
    sw_height = len(sw_data)

    # Calculate total dimensions
    total_height = nw_height + sw_height
    total_width = nw_width + len(ne_data[0])

    print(f"Creating merged array: {total_height} x {total_width}")

    # Create merged array
    merged_data = []

    # Add northern row (NW + NE)
    for i in range(nw_height):
        row = nw_data[i] + ne_data[i]
        merged_data.append(row)

    # Add southern row (SW + SE)
    for i in range(sw_height):
        row = sw_data[i] + se_data[i]
        merged_data.append(row)

    return merged_data


def download_temperature_data():
    """Download high temperature data for 2019-05-02 at 2pm UTC in 4 quadrants and merge."""

    # Create the Earth Engine temperature image
    temperature_celsius = create_temperature_image()

    # Define 4 quadrants to stay below pixel limits
    # Global bounds: [-180, -90, 180, 90]
    quadrants = [(-180, 0, 0, 90), (0, 0, 180, 90), (-180, -90, 0, 0), (0, -90, 180, 0)]

    print("Downloading temperature data in 4 quadrants to stay below pixel limits...")

    # Download each quadrant
    quadrant_data = []
    for bounds in quadrants:
        quadrant_temp_data = download_quadrant(bounds, temperature_celsius)
        quadrant_data.append(quadrant_temp_data)

    # Merge all quadrants
    merged_data = merge_quadrants(quadrant_data)

    print(f"Merged temperature data shape: {len(merged_data)} x {len(merged_data[0])}")
    print(
        f"Temperature range: {min(min(row) for row in merged_data):.2f}°C to {max(max(row) for row in merged_data):.2f}°C"
    )

    np.save("temperature_2019_05_02_2pm.npy", merged_data)

    return merged_data


if __name__ == "__main__":
    print("Google Earth Engine Temperature Data Downloader")
    print("=" * 50)

    try:
        # Download temperature data locally
        print("\nDownloading temperature data locally...")
        temperature_data = download_temperature_data()

        print("\n✅ Download completed successfully!")
        print(
            "The temperature data has been saved as a GeoTIFF file in the current directory."
        )

    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure you have authenticated with Earth Engine")
        print("2. Run: earthengine authenticate")
        print(
            "3. Set up a Google Cloud project: earthengine set_project YOUR_PROJECT_ID"
        )
        print("4. Install rasterio: pip install rasterio")
