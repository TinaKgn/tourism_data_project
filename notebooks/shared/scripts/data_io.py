"""
Data I/O functions for file operations and directory management
"""
import pandas as pd
from pathlib import Path


def setup_extraction_directories(project_root, dataset, city=None):
    """
    Create standardized bronze → silver directory structure

    Args:
        project_root: Path to project root
        dataset: 'tripadvisor', 'airbnb', 'yelp'
        city: Optional city name (e.g., 'chicago', 'los_angeles', 'new_orleans')

    Returns:
        dict with directory paths:
        {
            'bronze_original': Path,
            'bronze_conversion': Path,
            'bronze_primary_filter': Path,
            'silver_staging': Path
        }

    Usage:
        dirs = setup_extraction_directories(project_root, 'airbnb', city='chicago')
        original_dir = dirs['bronze_original']
    """
    # Build base paths
    bronze_base = project_root / "data" / "bronze" / dataset
    silver_base = project_root / "data" / "silver" / dataset

    if city:
        bronze_base = bronze_base / city
        silver_base = silver_base / city

    # Create bronze subdirectories
    dirs = {
        'bronze_original': bronze_base / "00_original_download",
        'bronze_conversion': bronze_base / "01_raw_conversion",
        'bronze_primary_filter': bronze_base / "02_primary_filter",
        'silver_staging': silver_base / "staging"
    }

    # Create all directories
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)

    return dirs


def check_existing_file(file_path, file_type='parquet', show_info=True):
    """
    Check if file exists and optionally display info

    Args:
        file_path: Path object to check
        file_type: 'parquet', 'csv', 'csv.gz', or 'xlsx'
        show_info: If True, print file details

    Returns:
        tuple: (exists: bool, info: dict or None)
    """
    if not file_path.exists():
        return False, None

    size_mb = file_path.stat().st_size / (1024 * 1024)

    info = {
        'path': file_path,
        'size_mb': size_mb
    }

    if show_info:
        print(f"[Skip] File already exists: {file_path}")
        print(f"Size: {size_mb:.1f} MB")

        # For non-data files (like xlsx), just show size
        if file_type == 'xlsx':
            print()
            return True, info

    # Load and get basic stats for data files
    try:
        if file_type == 'parquet':
            df = pd.read_parquet(file_path)
        elif file_type == 'csv.gz':
            df = pd.read_csv(file_path, compression='gzip', nrows=1000)
        elif file_type == 'csv':
            df = pd.read_csv(file_path, nrows=1000)
        else:
            df = None

        if df is not None:
            info['rows'] = len(df)
            info['columns'] = len(df.columns)

            # Check for common fields
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                years = sorted(df['date'].dt.year.dropna().unique())
                info['years'] = years

            # Check for unique identifiers
            if 'hotel_name' in df.columns:
                info['unique_hotels'] = df['hotel_name'].nunique()
            if 'listing_id' in df.columns:
                info['unique_listings'] = df['listing_id'].nunique()

    except Exception as e:
        info['load_error'] = str(e)

    if show_info and df is not None:
        if 'rows' in info:
            print(f"Rows: {info['rows']:,}")
        if 'years' in info:
            print(f"Years: {info['years']}")
        if 'unique_hotels' in info:
            print(f"Hotels: {info['unique_hotels']}")
        if 'unique_listings' in info:
            print(f"Listings: {info['unique_listings']}")
        print()

    return True, info


def check_existing_chunks(directory, pattern="*.parquet", show_info=True):
    """
    Check if chunked files exist in directory

    Args:
        directory: Path to check
        pattern: Glob pattern for chunks
        show_info: If True, print chunk info

    Returns:
        tuple: (exists: bool, chunk_count: int)

    Usage:
        exists, count = check_existing_chunks(conversion_dir, pattern="chicago_*.parquet")
        if not exists:
            # Run chunking logic
    """
    if not directory.exists():
        return False, 0

    chunks = list(directory.glob(pattern))

    if not chunks:
        return False, 0

    if show_info:
        total_size = sum(f.stat().st_size for f in chunks) / (1024 * 1024)
        print(f"[SKIP] Chunks already exist: {directory}")
        print(f"Found {len(chunks)} chunks, {total_size:.1f} MB total")
        print()

    return True, len(chunks)
