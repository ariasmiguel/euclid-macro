#!/usr/bin/env python3
"""
Test script for OCC data fetcher with incremental saving
"""

from datetime import date
from src.fetchers.fetch_occ import OCCDailyDataFetcher
import os

def test_occ_incremental():
    """Test OCC fetcher with incremental saving."""
    print("🚀 Testing OCC Data Fetcher - Incremental Saving")
    print("=" * 60)
    
    # Initialize fetcher
    fetcher = OCCDailyDataFetcher()
    
    # Set date range (just January and February 2008 for testing)
    start_date = date(2008, 1, 1)
    end_date = date(2008, 2, 28)
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print(f"📁 Download directory: {fetcher.download_dir}")
    print()
    
    try:
        # Fetch data
        print("⏳ Fetching data with incremental saving...")
        df = fetcher.fetch_data(start_date, end_date)
        
        print()
        print("📊 Results:")
        print(f"Total records: {len(df)}")
        
        if len(df) > 0:
            print("\nFirst 10 records:")
            print(df.head(10))
            
            print(f"\nUnique metrics: {df['metric'].unique()}")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            
            # Check if individual month files were created
            print("\n📁 Checking individual month files:")
            for year in [2008]:
                year_dir = os.path.join(fetcher.download_dir, str(year))
                if os.path.exists(year_dir):
                    files = [f for f in os.listdir(year_dir) if f.endswith('.csv')]
                    print(f"  {year}/: {files}")
            
        else:
            print("❌ No data retrieved")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_occ_incremental() 