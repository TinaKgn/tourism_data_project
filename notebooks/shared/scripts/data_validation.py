"""
Data validation and summary functions
"""
import pandas as pd
from pathlib import Path


def validate_years_in_data(file_path, expected_years, date_column='date', sample_size=5000):
    """
    Check if expected years exist in dataset

    Args:
        file_path: Path to data file
        expected_years: int or list of ints (e.g., 2022 or [2022, 2024])
        date_column: Name of date column to check
        sample_size: Number of rows to sample for validation

    Returns:
        tuple: (valid: bool, info: dict)

    Usage:
        valid, info = validate_years_in_data(file_path, [2022, 2024])
        if not valid:
            print(f"Missing years: {info['missing_years']}")
    """
    if isinstance(expected_years, int):
        expected_years = [expected_years]

    try:
        # Detect file type and read
        if str(file_path).endswith('.gz'):
            df = pd.read_csv(file_path, compression='gzip', nrows=sample_size)
        elif str(file_path).endswith('.csv'):
            df = pd.read_csv(file_path, nrows=sample_size)
        elif str(file_path).endswith('.parquet'):
            df = pd.read_parquet(file_path)
            if len(df) > sample_size:
                df = df.head(sample_size)
        else:
            return False, {'error': 'Unsupported file format'}

        if date_column not in df.columns:
            return False, {'error': f"Column '{date_column}' not found"}

        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        available_years = sorted(df[date_column].dt.year.dropna().unique())
        missing_years = set(expected_years) - set(available_years)

        info = {
            'available_years': available_years,
            'missing_years': list(missing_years) if missing_years else None,
            'valid': len(missing_years) == 0
        }

        return info['valid'], info

    except Exception as e:
        return False, {'error': str(e)}


def print_final_summary(file_path, dataset_name, file_type='parquet'):
    """
    Print comprehensive final dataset summary (multi-source compatible)

    Args:
        file_path: Path to final output file
        dataset_name: Display name (e.g., "TripAdvisor NYC", "AirBnB Chicago")
        file_type: 'parquet', 'csv', or 'csv.gz'

    Usage:
        print_final_summary(output_file, "TripAdvisor NYC 2022-2025")
    """
    print("=" * 60)
    print(f"Final Dataset Summary: {dataset_name}")
    print("=" * 60)

    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        return

    # Load data
    try:
        if file_type == 'parquet':
            df = pd.read_parquet(file_path)
        elif file_type == 'csv.gz':
            df = pd.read_csv(file_path, compression='gzip')
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
        else:
            print(f"Unsupported file type: {file_type}")
            return
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    # File info
    size_mb = file_path.stat().st_size / (1024 * 1024)
    print(f"\nFile Information:")
    print(f"   Location: {file_path}")
    print(f"   Size: {size_mb:.1f} MB")

    # Data shape
    print(f"\nDataset Shape:")
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")

    # Key identifiers - check what exists
    print(f"\nKey Identifiers:")
    identifier_found = False

    # TripAdvisor
    if 'hotel_name' in df.columns:
        print(f"   Unique Hotels: {df['hotel_name'].nunique():,}")
        identifier_found = True

    # AirBnB
    if 'listing_id' in df.columns:
        print(f"   Unique Listings: {df['listing_id'].nunique():,}")
        identifier_found = True

    # Yelp
    if 'business_id' in df.columns:
        print(f"   Unique Businesses: {df['business_id'].nunique():,}")
        identifier_found = True

    # Reviewer/User IDs (multiple possible column names)
    for col in ['reviewer_id', 'user_id', 'review_id']:
        if col in df.columns:
            print(f"   Unique {col.replace('_', ' ').title()}: {df[col].nunique():,}")
            identifier_found = True
            break

    if not identifier_found:
        print(f"   No standard identifier columns found")

    # Date range - check multiple possible date columns
    date_col = None
    for potential_date_col in ['date', 'review_date', 'created_date', 'timestamp']:
        if potential_date_col in df.columns:
            date_col = potential_date_col
            break

    if date_col:
        # Suppress date parsing warnings and use coerce for flexibility
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        print(f"\nDate Range:")
        print(f"   Earliest: {df[date_col].min()}")
        print(f"   Latest: {df[date_col].max()}")
        years = sorted(df[date_col].dt.year.dropna().unique())
        print(f"   Years: {years}")

    # Year distribution (if 'year' column exists)
    if 'year' in df.columns:
        print(f"\nYear Distribution:")
        year_counts = df['year'].value_counts().sort_index()
        for year, count in year_counts.items():
            print(f"   {year}: {count:,} reviews")

    # Column quality check
    print(f"\nData Quality:")
    null_cols = df.isnull().sum()
    high_null = null_cols[null_cols > len(df) * 0.1].sort_values(ascending=False)
    if len(high_null) > 0:
        print(f"   Columns with >10% nulls:")
        for col, null_count in high_null.head(5).items():
            pct = (null_count / len(df)) * 100
            print(f"      - {col}: {pct:.1f}% null")
    else:
        print(f"   No columns with >10% missing data")

    # Known issues - check for dummy columns
    dummy_cols = [col for col in df.columns if 'Unnamed:' in str(col) or col in ['col_0']]
    if dummy_cols:
        print(f"\nKnown Issues:")
        print(f"   Dummy columns to clean in gold layer: {dummy_cols}")

    print(f"\n" + "=" * 60)
    print(f"Workflow complete - Ready for gold layer processing")
    print("=" * 60)


def print_storage_summary(bronze_base, silver_staging, dataset_name):
    """
    Print storage breakdown for bronze and silver files

    Args:
        bronze_base: Path to bronze directory base
        silver_staging: Path to silver staging directory
        dataset_name: Display name

    Usage:
        print_storage_summary(bronze_base, silver_staging, "TripAdvisor NYC")
    """
    print(f"\nStorage Summary: {dataset_name}")
    print("=" * 60)

    def get_dir_size(directory):
        """Get total size of all files in directory"""
        if not directory.exists():
            return 0
        return sum(f.stat().st_size for f in directory.rglob('*') if f.is_file())

    # Bronze subdirectories
    original_size = get_dir_size(bronze_base / "00_original_download") / (1024 * 1024)
    conversion_size = get_dir_size(bronze_base / "01_raw_conversion") / (1024 * 1024)
    primary_size = get_dir_size(bronze_base / "02_primary_filter") / (1024 * 1024)

    # Silver
    silver_size = get_dir_size(silver_staging) / (1024 * 1024)

    total_size = original_size + conversion_size + primary_size + silver_size

    print(f"Bronze Layer:")
    print(f"   00_original_download: {original_size:.1f} MB")
    print(f"   01_raw_conversion: {conversion_size:.1f} MB")
    print(f"   02_primary_filter: {primary_size:.1f} MB")
    print(f"\nSilver Layer:")
    print(f"   staging: {silver_size:.1f} MB")
    print(f"\nTotal: {total_size:.1f} MB")

    # Cleanup suggestions
    if conversion_size > 0 or primary_size > 0:
        print(f"\nOptional Cleanup:")
        print(f"   To save {conversion_size + primary_size:.1f} MB, delete intermediate files:")
        print(f"   rm -rf {bronze_base / '01_raw_conversion'}")
        print(f"   rm -rf {bronze_base / '02_primary_filter'}")
        print(f"   Keeps: original + final ({original_size + silver_size:.1f} MB)")
