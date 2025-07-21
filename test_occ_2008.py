#!/usr/bin/env python3
"""
Test script for OCC data fetcher - January and February 2008
"""

from datetime import date
from src.fetchers.fetch_occ import OCCDailyDataFetcher

def test_occ_2008():
    """Test fetching OCC data for Jan-Feb 2008."""
    print("🚀 Testing OCC Data Fetcher - January and February 2008")
    print("=" * 60)
    
    # Initialize fetcher
    fetcher = OCCDailyDataFetcher()
    
    # Set date range
    start_date = date(2008, 1, 1)
    end_date = date(2008, 2, 28)
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print(f"📁 Download directory: {fetcher.download_dir}")
    print()
    
    try:
        # Fetch data
        print("⏳ Fetching data...")
        df = fetcher.fetch_data(start_date, end_date)
        
        print()
        print("📊 Results:")
        print(f"Total records: {len(df)}")
        
        if len(df) > 0:
            print("\nFirst 10 records:")
            print(df.head(10))
            
            print(f"\nUnique metrics: {df['metric'].unique()}")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            
            # Show summary by metric
            print("\nSummary by metric:")
            summary = df.groupby('metric')['value'].agg(['count', 'sum', 'mean']).round(2)
            print(summary)
        else:
            print("❌ No data retrieved")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_occ_2008() 