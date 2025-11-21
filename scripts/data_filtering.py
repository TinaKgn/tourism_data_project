"""
Functions for filtering and querying data
"""

import pandas as pd


def filter_by_city_and_category(df, city, state, category):
    """
    Filter businesses by city, state, and category

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with business data
    city : str
        City name (e.g., 'Philadelphia')
    state : str
        State code (e.g., 'PA')
    category : str
        Category to search for (e.g., 'Restaurants')

    Returns:
    --------
    pd.DataFrame : Filtered DataFrame
    """
    # Filter by city and state
    df_city = df[(df['city'] == city) & (df['state'] == state)].copy()

    # Filter by category
    def has_category(categories_str):
        if pd.isna(categories_str):
            return False
        cats = [cat.strip() for cat in str(categories_str).split(',')]
        return category in cats

    df_filtered = df_city[df_city['categories'].apply(has_category)].copy()

    return df_filtered


def classify_establishment(categories):
    """
    Classify establishment type from categories string for tourism analysis.

    This function specifically identifies hospitality and food service businesses
    relevant to tourism research. It categorizes businesses into two main types:
    - Hotels & Travel: Lodging and travel-related services
    - Restaurants: Food and dining establishments

    Any business not matching these categories is labeled as 'Other'.

    Parameters:
    -----------
    categories : str
        Comma-separated category string from Yelp business data.
        Example: "Mexican, Restaurants, Bars" or "Hotels, Hotels & Travel"

    Returns:
    --------
    str : Classification result
        - 'Hotels & Travel': If 'hotel' or 'travel' appears in categories
        - 'Restaurants': If 'restaurant' or 'food' appears in categories
        - 'Other': If neither tourism nor food-related
        - 'Unknown': If categories is missing/null

    Examples:
    ---------
    >>> classify_establishment("Mexican, Restaurants, Bars")
    'Restaurants'

    >>> classify_establishment("Hotels & Travel, Hotels")
    'Hotels & Travel'

    >>> classify_establishment("Shopping, Fashion")
    'Other'

    >>> classify_establishment(None)
    'Unknown'

    """
    if pd.isna(categories):
        return 'Unknown'
    cats_lower = str(categories).lower()
    if 'hotel' in cats_lower or 'travel' in cats_lower:
        return 'Hotels & Travel'
    elif 'restaurant' in cats_lower or 'food' in cats_lower:
        return 'Restaurants'
    else:
        return 'Other'


def get_categories_distribution(df_business):
    """
    Get distribution of all categories in business data

    Parameters:
    -----------
    df_business : pd.DataFrame
        DataFrame with business data containing 'categories' column

    Returns:
    --------
    pd.DataFrame : DataFrame with category counts
    """
    from collections import Counter

    all_categories = []
    for categories_str in df_business['categories'].dropna():
        cats = [cat.strip() for cat in str(categories_str).split(',')]
        all_categories.extend(cats)

    category_counts = Counter(all_categories)

    category_df = pd.DataFrame(category_counts.most_common(),
                              columns=['category', 'business_count'])

    return category_df