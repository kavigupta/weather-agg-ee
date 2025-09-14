import ee
import numpy as np
import tqdm


def download_point(point, ee_data: ee.Image, band_name: str, resolution=0.25):
    """Download temperature data for a specific point."""
    print(f"Downloading {point}...")

    # Create export region for this point
    export_region = ee.Geometry.Point(point)

    resampled_image = ee_data.reproject(crs="EPSG:4326", scale=111_400.0 * resolution)

    # Get the image as a numpy array using sampleRectangle
    image_array = resampled_image.sampleRectangle(region=export_region, defaultValue=0)

    # Get the actual data - the band name should match what we set in the temperature function
    point_temp_data = image_array.get(band_name).getInfo()

    if point_temp_data is None:
        # Debug: Check what bands are available
        image_info = image_array.getInfo()
        print(f"Available bands: {list(image_info.keys())}")
        print(f"Image info structure: {image_info}")

        # Try to get the properties to see what's actually in the image
        properties = image_info.get("properties", {})
        print(f"Properties: {properties}")

        # Check if the band data is in properties
        if band_name in properties:
            point_temp_data = properties[band_name]
        else:
            raise ValueError(
                f"Could not retrieve temperature data for {point}. Available bands: {list(image_info.keys())}, Properties: {list(properties.keys())}"
            )

    print(
        f"Downloaded {point} shape: {len(point_temp_data)} x {len(point_temp_data[0])}"
    )
    return point_temp_data


def download_quadrant(bounds, ee_data: ee.Image, band_name: str, resolution=0.25):
    """Download temperature data for a specific quadrant."""
    print(f"Downloading {bounds}...")

    # Create export region for this quadrant
    export_region = ee.Geometry.Rectangle(bounds)

    resampled_image = ee_data.reproject(crs="EPSG:4326", scale=111_300.0 * resolution)

    # Get the image as a numpy array using sampleRectangle
    image_array = resampled_image.sampleRectangle(
        region=export_region, defaultValue=0, properties=[band_name]
    )

    # Get the actual data - the band name should match what we set in the temperature function
    quadrant_temp_data = image_array.get(band_name).getInfo()

    if quadrant_temp_data is None:
        # Debug: Check what bands are available
        image_info = image_array.getInfo()
        print(f"Available bands: {list(image_info.keys())}")
        print(f"Image info structure: {image_info}")

        # Try to get the properties to see what's actually in the image
        properties = image_info.get("properties", {})
        print(f"Properties: {properties}")

        # Check if the band data is in properties
        if band_name in properties:
            quadrant_temp_data = properties[band_name]
        else:
            raise ValueError(
                f"Could not retrieve temperature data for {bounds}. Available bands: {list(image_info.keys())}, Properties: {list(properties.keys())}"
            )

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


def merge_quadrants_8(quadrant_data):
    """Merge 8 quadrant arrays into a single global array."""
    print("Merging 8 quadrants...")

    # Quadrant order: NW, N, NE, NE2, SW, S, SE, SE2
    nw_data, n_data, ne_data, ne2_data, sw_data, s_data, se_data, se2_data = (
        quadrant_data
    )

    # Get dimensions
    nw_height = len(nw_data)
    nw_width = len(nw_data[0])
    sw_height = len(sw_data)

    # Calculate total dimensions
    total_height = nw_height + sw_height
    total_width = nw_width + len(n_data[0]) + len(ne_data[0]) + len(ne2_data[0])

    print(f"Creating merged array: {total_height} x {total_width}")

    # Create merged array
    merged_data = []

    # Add northern row (NW + N + NE + NE2)
    for i in range(nw_height):
        row = nw_data[i] + n_data[i] + ne_data[i] + ne2_data[i]
        merged_data.append(row)

    # Add southern row (SW + S + SE + SE2)
    for i in range(sw_height):
        row = sw_data[i] + s_data[i] + se_data[i] + se2_data[i]
        merged_data.append(row)

    return merged_data


def merge_tiles(tile_data, degree_size):
    """Merge tile arrays into a single global array.

    Args:
        tile_data: List of tile data arrays
        degree_size: Size of each tile in degrees

    Returns:
        list: Merged global array
    """
    num_rows = 180 // degree_size
    num_cols = 360 // degree_size

    assert len(tile_data) == num_rows * num_cols

    rows = [
        np.hstack(tile_data[row_idx * num_cols : (row_idx + 1) * num_cols])
        for row_idx in range(num_rows)
    ]

    return np.vstack(rows[::-1])


def generate_tiles(degree_size=45, *, resolution):
    """Generate tiles covering the entire globe.

    Args:
        degree_size (int): Size of each tile in degrees (longitude and latitude)

    Returns:
        list: List of (min_lon, min_lat, max_lon, max_lat) tuples
    """
    tiles = []

    # Calculate number of tiles needed
    lon_tiles = int(360 / degree_size)  # 360° longitude
    lat_tiles = int(180 / degree_size)  # 180° latitude

    for lat_idx in range(lat_tiles):
        for lon_idx in range(lon_tiles):
            min_lat = -90 + lat_idx * degree_size
            max_lat = -90 + (lat_idx + 1) * degree_size
            min_lon = -180 + lon_idx * degree_size
            max_lon = -180 + (lon_idx + 1) * degree_size

            tiles.append((min_lon, min_lat, max_lon - resolution, max_lat - resolution))

    return tiles


def download_ee_image(
    ee_data: ee.Image,
    band_name: str = "mean_daily_max_temperature_celsius",
    resolution=0.25,
    degree_size=45,
):
    """Download mean daily maximum temperature data in tiles and merge.

    Args:
        ee_data: Earth Engine image
        filename: Output filename
        band_name: Name of the band to extract
        resolution: Resolution multiplier (default 0.25)
        degree_size: Size of each tile in degrees (default 45°)
    """

    # Generate tiles based on degree size
    tiles = generate_tiles(degree_size, resolution=resolution)

    # Download each tile
    tile_data = []
    for bounds in tqdm.tqdm(tiles):
        tile_temp_data = download_quadrant(bounds, ee_data, band_name, resolution)
        tile_data.append(tile_temp_data)

    # Merge all tiles
    merged_data = merge_tiles(tile_data, degree_size)

    return merged_data
