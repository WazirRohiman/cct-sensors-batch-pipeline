"""ArcGIS downloads with retry logic and error handling.

Functions here handle programmatic download of ArcGIS items
using direct data URLs and item page parsing with
retries, content-type checks, and quarantine on failures.
"""

import os
import re
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def _download_file(url: str, dest_path: str) -> None:
    """Download file with retry logic."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    response = requests.get(url, headers=headers, stream=True, timeout=30)
    response.raise_for_status()

    # Verify content type
    content_type = response.headers.get("content-type", "").lower()
    if not any(ct in content_type for ct in ["excel", "spreadsheet", "zip", "octet-stream"]):
        print(f"Warning: Unexpected content-type: {content_type}")

    # Ensure destination directory exists
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    # Download with progress
    total_bytes = 0
    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                total_bytes += len(chunk)

    print(f"Downloaded: {dest_path} ({total_bytes} bytes)")


def _extract_download_url_from_page(item_page_url: str) -> Optional[str]:
    """Extract direct download URL from ArcGIS item page."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        response = requests.get(item_page_url, headers=headers, timeout=30)
        response.raise_for_status()

        # Extract item ID from URL
        item_id_match = re.search(r"id=([a-f0-9]+)", item_page_url)
        if not item_id_match:
            print(f"Could not extract item ID from: {item_page_url}")
            return None

        item_id = item_id_match.group(1)

        # Construct direct download URL
        base_url = "https://cctegis.maps.arcgis.com/sharing/rest/content/items"
        download_url = f"{base_url}/{item_id}/data"

        print(f"Extracted download URL: {download_url}")
        return download_url

    except Exception as e:
        print(f"Error extracting download URL from {item_page_url}: {e}")
        return None


def fetch_item(item_url_or_page: str, dest_path: str) -> Optional[str]:
    """Download an ArcGIS item (direct data URL or item page) to dest_path.

    Args:
        item_url_or_page: Either direct data URL or ArcGIS item page URL
        dest_path: Path where file should be saved

    Returns:
        The path to the saved file or None on failure (to be quarantined).
    """
    try:
        download_url = item_url_or_page

        # If this is an item page URL, extract the direct download URL
        if "/home/item.html" in item_url_or_page:
            download_url = _extract_download_url_from_page(item_url_or_page)
            if not download_url:
                return None

        # Download the file
        _download_file(download_url, dest_path)

        # Verify file was created and has content
        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
            print(f"Successfully downloaded: {dest_path}")
            return dest_path
        else:
            print(f"Download failed: {dest_path} is empty or missing")
            return None

    except Exception as e:
        print(f"Error downloading {item_url_or_page}: {e}")
        # Clean up partial file if it exists
        if os.path.exists(dest_path):
            os.remove(dest_path)
        return None
