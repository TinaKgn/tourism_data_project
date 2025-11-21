"""
Functions for data visualization
"""

import pandas as pd
import matplotlib.pyplot as plt


def visualize_seasonal_distribution(df_reviews, city_name, figsize=(16, 5)):
    """
    Create seasonal distribution visualization

    Parameters:
    -----------
    df_reviews : pd.DataFrame
        DataFrame with 'month' and 'season' columns
    city_name : str
        Name of city for title
    figsize : tuple
        Figure size (default: (16, 5))
    """
    def get_season(month):
        if month in [12, 1, 2]: return 'Winter'
        elif month in [3, 4, 5]: return 'Spring'
        elif month in [6, 7, 8]: return 'Summer'
        else: return 'Fall'

    if 'season' not in df_reviews.columns:
        df_reviews['season'] = df_reviews['month'].apply(get_season)

    fig, axes = plt.subplots(1, 2, figsize=figsize)
    fig.suptitle(f'Seasonal Review Distribution - {city_name}',
                 fontsize=16, fontweight='bold')

    season_order = ['Winter', 'Spring', 'Summer', 'Fall']
    colors = ['#A8DADC', '#457B9D', '#E63946', '#F4A261']

    # Seasonal bar chart
    season_counts = df_reviews['season'].value_counts().reindex(season_order, fill_value=0)
    total = season_counts.sum()

    axes[0].bar(season_counts.index, season_counts.values,
                color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    axes[0].set_title('Seasonal Distribution', fontweight='bold')
    axes[0].set_ylabel('Number of Reviews', fontweight='bold')
    axes[0].grid(True, alpha=0.3, axis='y')

    # Add percentages
    for i, (season, count) in enumerate(season_counts.items()):
        pct = (count / total * 100) if total > 0 else 0
        axes[0].text(i, count, f'{count:,}\n({pct:.1f}%)',
                    ha='center', va='bottom', fontweight='bold')

    # Timeline
    df_reviews['year_month'] = pd.to_datetime(df_reviews['review_date']).dt.to_period('M')
    monthly_counts = df_reviews.groupby('year_month').size()
    monthly_counts.index = monthly_counts.index.to_timestamp()

    axes[1].plot(monthly_counts.index, monthly_counts.values,
                linewidth=2, color='#2E86AB', marker='o', markersize=4)
    axes[1].fill_between(monthly_counts.index, monthly_counts.values,
                         alpha=0.3, color='#2E86AB')
    axes[1].set_title('Timeline', fontweight='bold')
    axes[1].set_ylabel('Reviews per Month', fontweight='bold')
    axes[1].grid(True, alpha=0.3)
    axes[1].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.show()

    return fig


def print_dataset_summary(df, target_years):
    """
    Print summary statistics for dataset

    Parameters:
    -----------
    df : pd.DataFrame
        Complete dataset
    target_years : list
        List of years to summarize
    """
    print("="*80)
    print("DATASET SUMMARY")
    print("="*80)

    for year in target_years:
        df_year = df[df['year'] == year]
        print(f"\n{year}:")
        print(f"  Total reviews: {len(df_year):,}")

        if 'establishment_type' in df.columns:
            print(f"  Restaurants: {len(df_year[df_year['establishment_type'] == 'Restaurants']):,}")
            print(f"  Hotels: {len(df_year[df_year['establishment_type'] == 'Hotels & Travel']):,}")

        print(f"  Unique businesses: {df_year['business_id'].nunique()}")
        print(f"  Unique users: {df_year['user_id'].nunique()}")

        if 'review_stars' in df.columns:
            print(f"  Avg review rating: {df_year['review_stars'].mean():.2f}")

        # Seasonal breakdown
        if 'season' in df.columns:
            seasonal = df_year.groupby('season').size()
            print(f"  Seasonal distribution:")
            for season in ['Winter', 'Spring', 'Summer', 'Fall']:
                count = seasonal.get(season, 0)
                pct = (count / len(df_year) * 100) if len(df_year) > 0 else 0
                print(f"    {season:8s}: {count:4,} ({pct:5.1f}%)")