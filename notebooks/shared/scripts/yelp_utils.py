"""
Yelp-specific utility functions for Kaggle downloads and JSON processing
Consolidated to remove redundancies with shared utility functions
"""
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from collections import Counter

# Import shared utilities to avoid redundant functionality
from data_io import check_existing_file, check_existing_chunks


# Configuration for consistent target files
TARGET_FILES = [
    'yelp_academic_dataset_business.json',
    'yelp_academic_dataset_review.json',
    'yelp_academic_dataset_user.json'
]


def test_kaggle_authentication():
    """Test Kaggle authentication without downloading anything"""
    try:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except:
            pass

        import kaggle

        try:
            kaggle.api.dataset_list(search="test")
            return True, "✓ Kaggle authentication successful"
        except Exception as api_error:
            if "401" in str(api_error) or "unauthorized" in str(api_error).lower():
                return False, "✗ Authentication failed - check your KAGGLE_USERNAME and KAGGLE_KEY"
            else:
                return True, "✓ Kaggle authentication successful"

    except OSError as e:
        if "Could not find kaggle.json" in str(e):
            return False, "✗ Authentication failed - no credentials found"
        else:
            return False, f"✗ Authentication error: {str(e)}"
    except Exception as e:
        return False, f"✗ API test failed: {str(e)}"


def download_yelp_with_complete_handling(download_dir, target_files=None, validate=True):
    """
    Download Yelp dataset with integrated validation

    Args:
        download_dir: Path object for download directory
        target_files: List of specific files to download
        validate: If True, validate structure after download/skip

    Returns:
        tuple: (success: bool, message: str, status: str, validation_results: dict or None)
    """
    if target_files is None:
        target_files = [
            'yelp_academic_dataset_business.json',
            'yelp_academic_dataset_review.json',
            'yelp_academic_dataset_user.json'
        ]

    # Step 1: Check existing files
    print("\nChecking for existing files...")
    files_to_download = []

    for filename in target_files:
        file_path = download_dir / filename
        exists, info = check_existing_file(file_path, file_type='json', show_info=True)
        if not exists:
            files_to_download.append(filename)

    files_exist = len(files_to_download) == 0

    # Step 2: Download if needed
    if not files_exist:
        print(f"\nFiles to download: {files_to_download}")

        # Load environment variables
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        # Attempt download
        try:
            import kaggle
            print("\nStarting download (this may take 10-20 minutes for 8GB)...")
            print("Progress: Download in progress...")

            import time
            start_time = time.time()

            kaggle.api.dataset_download_files(
                'yelp-dataset/yelp-dataset',
                path=str(download_dir),
                unzip=True
            )

            elapsed = time.time() - start_time
            print(f"\n✓ Download completed in {elapsed/60:.1f} minutes")

            # Verify downloaded files
            print("\nVerifying downloaded files...")
            still_missing = []

            for filename in target_files:
                file_path = download_dir / filename
                if not file_path.exists():
                    still_missing.append(filename)
                else:
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    print(f"  ✓ {filename}: {size_mb:.1f} MB")

            if still_missing:
                return False, (
                    f"Download incomplete. Files not found after extraction: {still_missing}\n"
                    "See manual download instructions above"
                ), "extraction_failed", None

            download_status = "downloaded"
            download_message = f"Download completed successfully in {elapsed/60:.1f} minutes"

        except OSError as e:
            if "Could not find kaggle.json" in str(e):
                return False, (
                    "Kaggle authentication failed - no credentials found\n"
                    "See manual download instructions above"
                ), "auth_failed", None
            else:
                return False, (
                    f"Kaggle authentication error: {str(e)}\n"
                    "See manual download instructions above"
                ), "auth_failed", None

        except Exception as e:
            return False, (
                f"\nDownload failed: {str(e)}\n"
                "See manual download instructions above"
            ), "download_failed", None
    else:
        download_status = "files_exist"
        download_message = "All files already present"

    # Step 3: Validate if requested
    if validate:
        print("\n" + "="*60)

        file_paths = {
            'business': download_dir / 'yelp_academic_dataset_business.json',
            'review': download_dir / 'yelp_academic_dataset_review.json',
            'user': download_dir / 'yelp_academic_dataset_user.json'
        }

        all_valid, validation_results = validate_yelp_structure(file_paths)

        if not all_valid:
            return False, (
                f"{download_message}\n"
                "However, validation failed - files may be corrupted or incomplete"
            ), "validation_failed", validation_results

        # Success with validation
        final_message = f"{download_message}\nAll files validated successfully ✓"
        return True, final_message, download_status, validation_results

    # Success without validation
    return True, download_message, download_status, None

def validate_yelp_structure(file_paths, sample_seed=42):
    """
    Validate Yelp JSON files structure in one pass

    Args:
        file_paths: dict with keys 'business', 'review', 'user' and Path values
        sample_seed: Random seed for consistent sampling

    Returns:
        tuple: (all_valid: bool, validation_results: dict)
    """
    import random
    random.seed(sample_seed)

    expected_columns = {
        'business': ['business_id', 'name', 'city', 'state', 'categories'],
        'review': ['review_id', 'business_id', 'user_id', 'date', 'text', 'stars'],
        'user': ['user_id', 'name', 'review_count', 'yelping_since']
    }

    sample_fields = {
        'business': ['business_id', 'name', 'city', 'categories'],
        'review': ['business_id', 'date', 'stars', 'text'],
        'user': ['user_id', 'name', 'review_count', 'yelping_since']
    }

    results = {}
    all_valid = True
    total_size_gb = 0

    for file_type, file_path in file_paths.items():
        print(f"\n{file_type.title()}: {file_path.name}", end="")

        if not file_path.exists():
            print(" ✗")
            print(f"  File not found")
            results[file_type] = {'valid': False, 'error': 'File not found'}
            all_valid = False
            continue

        size_mb = file_path.stat().st_size / (1024 * 1024)
        total_size_gb += size_mb / 1024
        print(f" ({size_mb:.1f} MB) ✓")

        # Validate structure
        try:
            with open(file_path, 'r') as f:
                # Get random line for more diverse sampling
                lines = []
                for i, line in enumerate(f):
                    if i >= 100:  # Sample from first 100 records
                        break
                    lines.append(line)

                if lines:
                    sample_line = random.choice(lines)
                    record = json.loads(sample_line)
                    actual_columns = list(record.keys())

                    # Check required columns
                    expected = expected_columns.get(file_type, [])
                    missing = [col for col in expected if col not in actual_columns]

                    if missing:
                        print(f"  ✗ Missing columns: {missing}")
                        all_valid = False
                        results[file_type] = {'valid': False, 'missing': missing}
                    else:
                        print(f"  Dataset validated: {len(actual_columns)} Total Columns ✓")

                        # Format sample (stored in results, not printed)
                        sample_parts = []
                        for field in sample_fields.get(file_type, []):
                            if field in record:
                                value = str(record[field])
                                if len(value) > 50:
                                    value = value[:47] + "..."
                                sample_parts.append(f"{field}='{value}'")

                        results[file_type] = {
                            'valid': True,
                            'size_mb': size_mb,
                            'total_columns': len(actual_columns),
                            'sample': ", ".join(sample_parts)
                        }
                else:
                    print(f"  ✗ Empty file")
                    all_valid = False
                    results[file_type] = {'valid': False, 'error': 'Empty file'}

        except Exception as e:
            print(f"  ✗ Validation failed: {str(e)}")
            all_valid = False
            results[file_type] = {'valid': False, 'error': str(e)}

    results['total_size_gb'] = total_size_gb
    return all_valid, results

def classify_tourism_business(categories_string, category_groups):
    """
    Classify business into tourism groups using normalized matching

    Args:
        categories_string: Comma-separated categories (e.g., "Restaurants, Cajun/Creole, Bars")
        category_groups: Dictionary mapping group names to category keywords

    Returns:
        list: Tourism group names matched (e.g., ['restaurant', 'nightlife'])

    Usage:
        category_groups = {
            'restaurant': ['restaurant', 'food', 'coffee & tea'],
            'nightlife': ['bar', 'pub', 'wine bar']
        }
        groups = classify_tourism_business("Restaurants, Bars", category_groups)
        # Returns: ['restaurant', 'nightlife']
    """
    if pd.isna(categories_string):
        return []

    # Normalize input categories (strip, lowercase, remove trailing 's')
    input_cats = [
        cat.strip().lower().rstrip('s')
        for cat in str(categories_string).split(',')
    ]

    matched_groups = []

    # Check each category group
    for group_name, keywords in category_groups.items():
        # Normalize keywords (lowercase, remove trailing 's')
        normalized_keywords = [kw.lower().rstrip('s') for kw in keywords]

        # Check if any input category matches any keyword
        for input_cat in input_cats:
            if input_cat in normalized_keywords:
                matched_groups.append(group_name)
                break  # Only add group once

    return matched_groups
