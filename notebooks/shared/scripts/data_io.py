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
        print(f"\n[SKIP] Chunks already exist: {directory}")
        print(f"Found {len(chunks)} chunks, {total_size:.1f} MB total")
        print()

    return True, len(chunks)

def convert_json_dataset_to_chunks(json_file, output_dir, file_prefix, chunk_size=10000):
    """
    Convert JSON dataset to parquet chunks with adaptive progress indicators

    Args:
        json_file: Path to input JSON file
        output_dir: Path to output directory
        file_prefix: Prefix for chunk files (e.g., 'yelp_business')
        chunk_size: Records per chunk

    Returns:
        tuple: (success: bool, chunk_count: int, total_records: int)
    """
    import json
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Check if conversion already completed
    exists, count = check_existing_chunks(output_dir, pattern=f"{file_prefix}_chunk_*.parquet")

    if exists:
        # Estimate total records from existing chunks
        sample_chunk = next(output_dir.glob(f"{file_prefix}_chunk_*.parquet"))
        sample_df = pd.read_parquet(sample_chunk)
        estimated_total = count * len(sample_df)
        return True, count, estimated_total

    print(f"Converting {json_file.name} to parquet chunks...")
    print(f"\nChunk size: {chunk_size:,} records")

    # Determine progress interval based on file size
    file_size_mb = json_file.stat().st_size / (1024 * 1024)
    if file_size_mb < 500:
        progress_interval = 10  # Small files: update every 10 chunks
    else:
        progress_interval = 50  # Large files: update every 50 chunks

    records = []
    chunk_count = 0
    total_records = 0

    with open(json_file, 'r') as f:
        for line_num, line in enumerate(f):
            try:
                record = json.loads(line.strip())
                records.append(record)
                total_records += 1

                # Write chunk when full
                if len(records) >= chunk_size:
                    df = pd.DataFrame(records)
                    chunk_filename = f"{file_prefix}_chunk_{chunk_count:05d}.parquet"
                    output_path = output_dir / chunk_filename

                    pq.write_table(
                        pa.Table.from_pandas(df),
                        output_path,
                        compression="snappy"
                    )

                    records = []
                    chunk_count += 1

                    # Adaptive progress indicator
                    if chunk_count % progress_interval == 0:
                        print(f"  Processed {chunk_count} chunks ({total_records:,} records)...")

            except json.JSONDecodeError as e:
                print(f"Warning: Skipping malformed JSON at line {line_num + 1}: {e}")
                continue

    # Write remaining records
    if records:
        df = pd.DataFrame(records)
        chunk_filename = f"{file_prefix}_chunk_{chunk_count:05d}.parquet"
        output_path = output_dir / chunk_filename

        pq.write_table(
            pa.Table.from_pandas(df),
            output_path,
            compression="snappy"
        )
        chunk_count += 1

    print(f"\nConversion complete:")
    print(f"  Total chunks: {chunk_count}")
    print(f"  Total records: {total_records:,}")
    print(f"  Output: {output_dir}")

    return True, chunk_count, total_records
