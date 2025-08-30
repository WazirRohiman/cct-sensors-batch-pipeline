#!/usr/bin/env python3
"""Test script for fetch functionality."""

import os
import sys

sys.path.insert(0, "src")

from pipelines.io_arcgis import fetch_item


def test_wind_download():
    """Test downloading wind data (direct URL)."""
    print("Testing Wind data download (2020)...")
    url = "https://www.arcgis.com/sharing/rest/content/items/31ef242a23484e79bbb19d6b29203179/data"
    dest_path = "data/raw/wind_2020_test.xlsx"

    result = fetch_item(url, dest_path)
    if result:
        print(f"✅ Wind download successful: {result}")
        print(f"   File size: {os.path.getsize(result)} bytes")
    else:
        print("❌ Wind download failed")

    return result is not None


def test_air_quality_download():
    """Test downloading air quality data (item page URL)."""
    print("\nTesting Air Quality data download (2021)...")
    url = "https://cctegis.maps.arcgis.com/home/item.html?id=97b0fe851c6f4d86b2492eb09fe42935"
    dest_path = "data/raw/air_quality_2021_test.zip"

    result = fetch_item(url, dest_path)
    if result:
        print(f"✅ Air Quality download successful: {result}")
        print(f"   File size: {os.path.getsize(result)} bytes")
    else:
        print("❌ Air Quality download failed")

    return result is not None


def main():
    """Run fetch tests."""
    print("🧪 Testing fetch functionality...\n")

    # Ensure data directory exists
    os.makedirs("data/raw", exist_ok=True)

    wind_ok = test_wind_download()
    air_ok = test_air_quality_download()

    print("\n📊 Results:")
    print(f"   Wind data: {'✅' if wind_ok else '❌'}")
    print(f"   Air Quality data: {'✅' if air_ok else '❌'}")

    if wind_ok and air_ok:
        print("\n🎉 All tests passed! Fetch functionality working.")
        return 0
    else:
        print("\n💥 Some tests failed. Check error messages above.")
        return 1


if __name__ == "__main__":
    exit(main())
