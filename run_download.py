#!/usr/bin/env python3
"""
Simple runner script that checks authentication before downloading temperature data.
"""

import sys

def check_earth_engine_auth():
    """Check if Earth Engine is properly authenticated."""
    try:
        import ee
        ee.Initialize()
        return True
    except Exception:
        return False

def main():
    """Main runner function."""
    print("Earth Engine Temperature Downloader")
    print("=" * 40)
    
    # Check if Earth Engine is authenticated
    if not check_earth_engine_auth():
        print("❌ Earth Engine not authenticated!")
        print("\nPlease run the following commands first:")
        print("1. pip install earthengine-api")
        print("2. earthengine authenticate")
        print("\nOr run: python setup.py")
        sys.exit(1)
    
    print("✅ Earth Engine authenticated successfully!")
    print("\nStarting temperature data download...")
    
    # Import and run the main download function
    try:
        from download_temperature import download_temperature_data
        download_temperature_data()
    except ImportError as e:
        print(f"❌ Error importing download module: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error during download: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
