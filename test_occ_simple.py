"""
Simple test script for OCC data fetcher
"""

import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.fetchers.fetch_occ import OCCDailyDataFetcher


def test_single_month():
    """Test fetching a single month of data."""
    print("=" * 60)
    print("TEST 1: Single Month Fetch")
    print("=" * 60)
    
    fetcher = OCCDailyDataFetcher(download_dir="data/raw/occ_test")
    
    # Get previous month to ensure data exists
    today = datetime.now().date()
    if today.month == 1:
        test_date = date(today.year - 1, 12, 1)
    else:
        test_date = date(today.year, today.month - 1, 1)
    
    print(f"Fetching data for: {test_date.strftime('%B %Y')}")
    
    df = fetcher.fetch_data(test_date, test_date)
    
    if not df.empty:
        print(f"✅ Success! Fetched {len(df)} records")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"Metrics found: {df['metric'].unique()}")
        print("\nSample data:")
        print(df.head(10))
    else:
        print("❌ No data fetched")
    
    return df


def test_date_range():
    """Test fetching a range of months."""
    print("\n" + "=" * 60)
    print("TEST 2: Date Range Fetch")
    print("=" * 60)
    
    fetcher = OCCDailyDataFetcher(download_dir="data/raw/occ_test")
    
    # Fetch last 3 months
    end_date = datetime.now().date()
    start_date = end_date - relativedelta(months=3)
    
    print(f"Fetching data from {start_date} to {end_date}")
    
    df = fetcher.fetch_data(start_date, end_date)
    
    if not df.empty:
        print(f"✅ Success! Fetched {len(df)} records")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Show monthly breakdown
        df['month'] = pd.to_datetime(df['date']).dt.to_period('M')
        monthly_counts = df.groupby('month').size()
        print("\nRecords per month:")
        for month, count in monthly_counts.items():
            print(f"  {month}: {count:,} records")
    else:
        print("❌ No data fetched")
    
    return df


def test_specific_months():
    """Test fetching specific months that should have data."""
    print("\n" + "=" * 60)
    print("TEST 3: Specific Months")
    print("=" * 60)
    
    fetcher = OCCDailyDataFetcher(download_dir="data/raw/occ_test")
    
    # Test specific months
    test_ranges = [
        (date(2024, 1, 1), date(2024, 1, 31), "January 2024"),
        (date(2024, 6, 1), date(2024, 6, 30), "June 2024"),
        (date(2023, 12, 1), date(2023, 12, 31), "December 2023"),
    ]
    
    for start_date, end_date, description in test_ranges:
        print(f"\nTesting {description}...")
        df = fetcher.fetch_data(start_date, end_date)
        
        if not df.empty:
            print(f"  ✅ {description}: {len(df)} records")
        else:
            print(f"  ❌ {description}: No data")


def test_data_quality():
    """Test data quality and format."""
    print("\n" + "=" * 60)
    print("TEST 4: Data Quality Check")
    print("=" * 60)
    
    fetcher = OCCDailyDataFetcher(download_dir="data/raw/occ_test")
    
    # Fetch recent month
    end_date = datetime.now().date()
    start_date = end_date - relativedelta(months=1)
    
    df = fetcher.fetch_data(start_date, end_date)
    
    if not df.empty:
        print("Data structure:")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Shape: {df.shape}")
        print(f"  Symbol values: {df['symbol'].unique()}")
        print(f"  Metric types: {df['metric'].unique()}")
        
        # Check for nulls
        null_counts = df.isnull().sum()
        print("\nNull values:")
        for col, count in null_counts.items():
            if count > 0:
                print(f"  {col}: {count} ({count/len(df)*100:.1f}%)")
        
        # Check date format
        print("\nDate format check:")
        sample_dates = df['date'].head()
        for d in sample_dates:
            print(f"  {d}")
        
        # Value statistics
        print("\nValue statistics:")
        print(f"  Min: {df['value'].min():,.0f}")
        print(f"  Max: {df['value'].max():,.0f}")
        print(f"  Mean: {df['value'].mean():,.0f}")
        
        return True
    else:
        print("❌ No data to check")
        return False


def main():
    """Run all tests."""
    print("🚀 OCC Data Fetcher Test Suite")
    print("=" * 60)
    
    # Run tests
    test_single_month()
    test_date_range()
    test_specific_months()
    test_data_quality()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    
    # Check output files
    test_dir = Path("data/raw/occ_test")
    if test_dir.exists():
        files = list(test_dir.glob("*.parquet"))
        print(f"\n📁 Output files created: {len(files)}")
        for f in files:
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"  {f.name}: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()