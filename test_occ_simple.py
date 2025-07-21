"""
Simple test to check what months are available on OCC website
"""

import logging
from datetime import datetime
from src.fetchers.fetch_occ import OCCDailyDataFetcher

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_available_months():
    """Test to see what months are actually available on the OCC website"""
    print("🧪 Testing available months on OCC website...")
    print("=" * 60)
    
    try:
        # Create fetcher
        fetcher = OCCDailyDataFetcher(download_dir="data/raw/occ_test")
        
        # Test different months to see what's available
        test_months = [
            (2025, 7, "July 2025"),
            (2025, 6, "June 2025"),
            (2025, 5, "May 2025"), 
            (2025, 4, "April 2025"),
            (2025, 3, "March 2025"),
            (2025, 2, "February 2025"),
            (2025, 1, "January 2025"),
        ]
        
        available_months = []
        
        for year, month, month_name in test_months:
            print(f"🔍 Testing {month_name}...")
            try:
                month_data = fetcher.extract_month_data_single(year, month)
                if month_data:
                    print(f"   ✅ {month_name}: Available")
                    available_months.append((year, month, month_name))
                else:
                    print(f"   ❌ {month_name}: Not available")
            except Exception as e:
                print(f"   ❌ {month_name}: Error - {str(e)}")
        
        print(f"\n📋 Available months: {[name for _, _, name in available_months]}")
        return available_months
        
    except Exception as e:
        print(f"❌ Available months test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    test_available_months() 