"""
Functions for extracting data from Yelp JSON files
"""

import json
import pandas as pd
from collections import Counter


def count_records(filename, record_type="records"):
    """
    Count total number of records in a Yelp JSON file.

    Parameters:
    -----------
    filename : str
        Path to JSON file (e.g., 'yelp_academic_dataset_business.json')
    record_type : str, optional (default='records')
        Type of record for display purposes (e.g., 'businesses', 'reviews', 'users')

    Returns:
    --------
    int : Total number of records in the file

    Examples:
    ---------
    >>> count_records('yelp_academic_dataset_business.json', 'businesses')
    Counted 100,000 businesses so far...
    Total businesses: 150,346
    150346

    >>> count_records('yelp_academic_dataset_review.json', 'reviews')
    Total reviews: 6,990,280
    6990280

    Notes:
    ------
    -Each line in Yelp JSON files is a complete JSON object (one record)
    -Large files may take several minutes to count
    - Uncomment the if condition to print progress every 100,000 records
    """
    count = 0
    with open(filename, 'r') as f:
        for line in f:
            count += 1
            #if count % 100000 == 0:
                #print(f"Counted {count:,} {record_type} so far...")

    print(f"\nTotal {record_type}: {count:,}")
    return count

def peek_json(filename, n=5):
    """
    Display first n records from a JSON file

    Parameters:
    -----------
    filename : str
        Path to JSON file
    n : int
        Number of records to display (default: 5)
    """
    with open(filename, 'r') as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            data = json.loads(line)
            print(f"\n--- Record {i+1} ---")
            print(json.dumps(data, indent=2))


def extract_all_businesses(filename):
    """
    Extract all business data from Yelp business JSON file

    Parameters:
    -----------
    filename : str
        Path to yelp_academic_dataset_business.json

    Returns:
    --------
    pd.DataFrame : DataFrame with all business data
    """
    businesses = []

    with open(filename, 'r') as f:
        for i, line in enumerate(f):
            biz = json.loads(line)
            businesses.append({
                'business_id': biz['business_id'],
                'name': biz['name'],
                'city': biz.get('city'),
                'state': biz.get('state'),
                'postal_code': biz.get('postal_code'),
                'latitude': biz.get('latitude'),
                'longitude': biz.get('longitude'),
                'stars': biz.get('stars'),
                'review_count': biz.get('review_count'),
                'is_open': biz.get('is_open'),
                'categories': biz.get('categories', '')
            })

            if (i + 1) % 50000 == 0:
                print(f"Processed {i + 1:,} businesses...")

    df = pd.DataFrame(businesses)
    print(f"\nTotal businesses loaded: {len(df):,}")

    return df


def get_all_city_states(filename):
    """
    Get distribution of cities and states from business file

    Parameters:
    -----------
    filename : str
        Path to yelp_academic_dataset_business.json

    Returns:
    --------
    Counter : Counter object with (city, state) counts
    """
    locations = []

    with open(filename, 'r') as f:
        for i, line in enumerate(f):
            data = json.loads(line)
            city = data.get('city', 'Unknown')
            state = data.get('state', 'Unknown')
            locations.append(f"{city}, {state}")

            if (i + 1) % 50000 == 0:
                print(f"Processed {i + 1:,} businesses...")

    location_counts = Counter(locations)
    print(f"\nTotal businesses: {len(locations):,}")
    print(f"Unique locations: {len(location_counts):,}\n")

    return location_counts


def extract_city_dataset(
    business_df,
    review_file,
    user_file,
    target_years=None,
    city='New Orleans',
    state='LA'
):
    """
    Extract complete city-level review dataset.

    Parameters:
    -----------
    business_df : pd.DataFrame
        Combined business DataFrame (restaurants, hotels, etc.)
        Must contain business_id, city, and state columns.

    review_file : str
        Path to Yelp review JSON file (yelp_academic_dataset_review.json)

    user_file : str
        Path to Yelp user JSON file (yelp_academic_dataset_user.json)

    target_years : list or None, optional
        • If a list is provided (e.g., [2013, 2016, 2018]), only reviews from those years are included.
        • If None (default), reviews from **all years** are included.

    city : str
        City name to filter businesses (default: 'New Orleans')

    state : str
        State code to filter businesses (default: 'LA')

    Returns:
    --------
    pd.DataFrame
        Fully merged dataset containing business, review, user, and seasonal fields.
    """

    # Filter businesses for the city + state
    business_df_city = business_df[
        (business_df["city"] == city) & (business_df["state"] == state)
    ].copy()

    business_ids = set(business_df_city["business_id"])

    print(f"Extracting data for {city}, {state}")
    if target_years is None:
        print("Target years: ALL YEARS")
    else:
        print(f"Target years: {target_years}")
    print(f"Businesses to track: {len(business_ids):,}")

    # STEP 1 ───────── EXTRACT REVIEWS
    print("\n" + "=" * 80)
    print("STEP 1: Extracting Reviews")
    print("=" * 80)

    reviews = []
    user_ids_needed = set()

    with open(review_file, "r") as f:
        for i, line in enumerate(f):
            review = json.loads(line)
            business_id = review["business_id"]
            year = int(review["date"].split("-")[0])

            # Filter by business + optional years
            if business_id in business_ids:
                if (target_years is None) or (year in target_years):

                    reviews.append({
                        "review_id": review["review_id"],
                        "business_id": business_id,
                        "user_id": review["user_id"],
                        "review_stars": review["stars"],
                        "review_date": review["date"],
                        "review_text": review["text"],
                        "useful": review.get("useful", 0),
                        "funny": review.get("funny", 0),
                        "cool": review.get("cool", 0),
                    })

                    user_ids_needed.add(review["user_id"])

            if (i + 1) % 500000 == 0:
                print(
                    f"  Processed {i + 1:,} reviews... "
                    f"Found {len(reviews):,} relevant reviews"
                )

    df_reviews = pd.DataFrame(reviews)
    print(f"\n✓ Extracted {len(df_reviews):,} reviews")

    # STEP 2 ───────── EXTRACT USERS
    print("\n" + "=" * 80)
    print("STEP 2: Extracting User Data")
    print("=" * 80)

    users = []
    with open(user_file, "r") as f:
        for i, line in enumerate(f):
            user = json.loads(line)
            if user["user_id"] in user_ids_needed:
                users.append({
                    "user_id": user["user_id"],
                    "user_name": user.get("name"),
                    "user_review_count": user.get("review_count", 0),
                    "user_yelping_since": user.get("yelping_since"),
                    "user_average_stars": user.get("average_stars", 0),
                })

            if (i + 1) % 100000 == 0:
                print(f"  Processed {i + 1:,} users...")

    df_users = pd.DataFrame(users)
    print(f"\n✓ Extracted {len(df_users):,} users")

    # STEP 3 ───────── MERGE
    print("\n" + "=" * 80)
    print("STEP 3: Merging Data")
    print("=" * 80)

    df_merged = df_reviews.merge(df_users, on="user_id", how="left")

    business_df_clean = business_df_city[
        [
            "business_id",
            "name",
            "city",
            "state",
            "postal_code",
            "latitude",
            "longitude",
            "stars",
            "review_count",
            "is_open",
            "categories",
        ]
    ].copy()

    business_df_clean = business_df_clean.rename(
        columns={
            "name": "business_name",
            "stars": "business_avg_stars",
            "review_count": "business_total_reviews",
        }
    )

    df_final = df_merged.merge(business_df_clean, on="business_id", how="left")

    # Add temporal fields
    df_final["review_date"] = pd.to_datetime(df_final["review_date"])
    df_final["year"] = df_final["review_date"].dt.year
    df_final["month"] = df_final["review_date"].dt.month

    # Add season
    def get_season(month):
        if month in [12, 1, 2]:
            return "Winter"
        elif month in [3, 4, 5]:
            return "Spring"
        elif month in [6, 7, 8]:
            return "Summer"
        else:
            return "Fall"

    df_final["season"] = df_final["month"].apply(get_season)

    print(f"\n✓ Final dataset: {len(df_final):,} rows")

    return df_final


def check_dataset_date_range(review_file, business_df=None, city=None, state=None):
    """
    Check the overall date range and total number of reviews in the Yelp review dataset.
    Optionally filter by a specific city (and state).

    Parameters:
    -----------
    review_file : str
        Path to yelp_academic_dataset_review.json
    business_df : pd.DataFrame, optional
        Business dataset containing at least ['business_id', 'city', 'state'].
        Required if city/state filtering is used.
    city : str, optional
        City name to filter reviews (e.g., 'New Orleans')
    state : str, optional
        State code to filter reviews (e.g., 'LA')

    Returns:
    --------
    dict :
        {
            'min_date': earliest review date (str),
            'max_date': latest review date (str),
            'total_reviews': total number of matching reviews (int)
        }

    Notes:
    ------
    - If city/state are provided, only reviews for businesses in that location are scanned.
    - Progress updates printed every 1,000,000 records.
    """

    # Optional filtering setup
    if city is not None or state is not None:
        if business_df is None:
            raise ValueError("business_df must be provided when filtering by city/state.")

        city_filter = business_df.copy()

        if city is not None:
            city_filter = city_filter[city_filter["city"] == city]
        if state is not None:
            city_filter = city_filter[city_filter["state"] == state]

        business_ids = set(city_filter["business_id"])
        filter_active = True
    else:
        business_ids = None
        filter_active = False

    min_date = None
    max_date = None
    total_reviews = 0

    print("\n" + "="*80)
    if filter_active:
        print(f"Scanning Review Dataset for Date Range in {city}, {state}")
        print(f"Businesses matched: {len(business_ids):,}")
    else:
        print("Scanning Entire Review Dataset for Global Date Range")
    print("="*80)

    with open(review_file, 'r') as f:
        for i, line in enumerate(f):
            review = json.loads(line)

            # Apply city-level business filter
            if filter_active:
                if review["business_id"] not in business_ids:
                    continue

            date = review.get("date")
            if date:
                if min_date is None or date < min_date:
                    min_date = date
                if max_date is None or date > max_date:
                    max_date = date

            total_reviews += 1

            if (i + 1) % 1_000_000 == 0:
                print(
                    f"  Processed {i + 1:,} lines..."
                    f" (Current range: {min_date} → {max_date}, matches: {total_reviews:,})"
                )

    print("\n✓ Completed scan")
    print("-" * 80)
    print(f"Earliest review: {min_date}")
    print(f"Latest review:   {max_date}")
    #print(f"Total reviews:   {total_reviews:,}")
    print("-" * 80)

    return {
        "min_date": min_date,
        "max_date": max_date,
        "total_reviews": total_reviews
    }