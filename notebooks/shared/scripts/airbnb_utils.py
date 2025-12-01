"""
AirBnB-specific utility functions for InsideAirbnb data
"""
import requests
import pandas as pd
from pathlib import Path


def download_insideairbnb(city, date_snapshot, file_type, output_path, timeout=15):
    """
    Download from InsideAirbnb with consistent URL pattern

    Args:
        city: 'chicago' or 'los_angeles'
        date_snapshot: '2025-06-17' format
        file_type: 'listings' or 'reviews'
        output_path: Path object for output
        timeout: request timeout in seconds

    Returns:
        tuple: (success: bool, message: str)

    Usage:
        success, msg = download_insideairbnb('chicago', '2025-06-17', 'reviews', output_path)
        print(msg)
    """
    city_map = {
        'chicago': 'united-states/il/chicago',
        'los_angeles': 'united-states/ca/los-angeles'
    }

    if city not in city_map:
        return False, f"Unknown city: {city}. Supported: {list(city_map.keys())}"

    url = f"https://data.insideairbnb.com/{city_map[city]}/{date_snapshot}/data/{file_type}.csv.gz"

    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        return True, f"[SKIP] File exists ({size_mb:.1f} MB)"

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            f.write(response.content)

        size_mb = output_path.stat().st_size / (1024 * 1024)
        return True, f"Downloaded successfully ({size_mb:.1f} MB)"

    except requests.exceptions.RequestException as e:
        return False, f"Download failed: {str(e)}"


def merge_listings_reviews(listings_path, reviews_path, year_filter=None):
    """
    Merge AirBnB listings and reviews with optional year filter

    Args:
        listings_path: Path to listings.csv.gz
        reviews_path: Path to reviews.csv.gz
        year_filter: int or list of ints (e.g., 2022 or [2022, 2024])

    Returns:
        DataFrame: Merged reviews + listing details

    Usage:
        merged_df = merge_listings_reviews(
            listings_path,
            reviews_path,
            year_filter=[2022, 2023, 2024, 2025§]
        )
    """
    listings = pd.read_csv(listings_path, compression='gzip')
    reviews = pd.read_csv(reviews_path, compression='gzip')

    # Apply year filter if specified
    if year_filter:
        reviews['date'] = pd.to_datetime(reviews['date'])
        if isinstance(year_filter, int):
            year_filter = [year_filter]
        reviews = reviews[reviews['date'].dt.year.isin(year_filter)]

    # Merge on listing_id with suffixes to avoid confusion
    merged = reviews.merge(
        listings,
        left_on='listing_id',
        right_on='id',
        how='left',
        suffixes=('_review', '_listing')
    )

    return merged

def validate_insideairbnb_structure(reviews_df, listings_df):
    """
    Validate InsideAirbnb files have minimum required columns
    Future-proof: only checks fields stable across all versions

    Returns: (valid: bool, missing_cols: dict)
    """
    # Reviews - 100% stable across all versions
    required_reviews = ['listing_id', 'date', 'comments']

    # Listings - stable core for merge + basic analysis
    required_listings = ['id', 'property_type', 'room_type', 'latitude', 'longitude']

    missing_reviews = [col for col in required_reviews if col not in reviews_df.columns]
    missing_listings = [col for col in required_listings if col not in listings_df.columns]

    valid = len(missing_reviews) == 0 and len(missing_listings) == 0

    return valid, {
        'reviews': missing_reviews,
        'listings': missing_listings
    }
