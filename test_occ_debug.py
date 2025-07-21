"""
Debug script for OCC fetcher - Test June 2025 extraction
"""

import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from src.fetchers.fetch_occ import OCCDailyDataFetcher

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_occ_june_2025_specific():
    """Test OCC fetcher specifically for June 2025 with detailed debugging"""
    print("🧪 Testing OCC fetcher specifically for June 2025...")
    print("=" * 60)
    
    try:
        # Create fetcher with test download directory
        fetcher = OCCDailyDataFetcher(download_dir="data/raw/occ_test")
        
        # Check current date
        current_date = datetime.now()
        print(f"📅 Current date: {current_date.strftime('%Y-%m-%d')}")
        print(f"📅 Requesting June 2025 (month 6)")
        
        # Test single month extraction with detailed logging
        print("📅 Extracting June 2025 data...")
        month_data = fetcher.extract_month_data_single(2025, 6)  # June = month 6
        
        if month_data:
            print("✅ Month data extracted successfully!")
            print(f"📊 Extracted data keys: {list(month_data.keys())}")
            print(f"📅 Requested: June 2025, Actual: {month_data.get('month_name', 'Unknown')} {month_data.get('year', 'Unknown')}")
            
            # Check what tables were extracted
            if 'occ_contract_volume' in month_data:
                occ_records = len(month_data['occ_contract_volume'])
                print(f"📊 OCC Options table: {occ_records} records")
                if occ_records > 0:
                    print(f"   Sample OCC data: {month_data['occ_contract_volume'][0]}")
            else:
                print("⚠️ No OCC options table found")
                
            if 'futures_contract_volume' in month_data:
                futures_records = len(month_data['futures_contract_volume'])
                print(f"📈 Futures table: {futures_records} records")
                if futures_records > 0:
                    print(f"   Sample futures data: {month_data['futures_contract_volume'][0]}")
            else:
                print("⚠️ No futures table found")
            
            # Convert to long format
            print("\n🔄 Converting to long format...")
            long_format_data = fetcher.convert_to_long_format([month_data])
            
            if not long_format_data.empty:
                print("✅ Long format conversion successful!")
                print(f"📊 Final data shape: {long_format_data.shape}")
                print(f"📅 Date range: {long_format_data['date'].min()} to {long_format_data['date'].max()}")
                print(f"📈 Metrics: {long_format_data['metric'].unique().tolist()}")
                print(f"🔢 Sample data:")
                print(long_format_data.head(10))
                
                # Check if we got June data or July data
                dates = pd.to_datetime(long_format_data['date'])
                unique_months = dates.dt.strftime('%Y-%m').unique()
                print(f"📅 Actual months in data: {sorted(unique_months)}")
                
                if '2025-06' in unique_months:
                    print("✅ SUCCESS: June 2025 data extracted!")
                elif '2025-07' in unique_months:
                    print("⚠️ WARNING: July 2025 data extracted instead of June 2025")
                    print("   This suggests the date picker didn't work correctly")
                else:
                    print(f"❓ UNEXPECTED: Got data for months: {unique_months}")
                
                # Save test file
                test_file = Path("data/raw/occ_test/occ_june_2025_specific.parquet")
                test_file.parent.mkdir(parents=True, exist_ok=True)
                long_format_data.to_parquet(test_file, index=False)
                print(f"💾 Test data saved to: {test_file}")
                
                return long_format_data
            else:
                print("❌ Long format conversion resulted in empty data")
                return pd.DataFrame()
        else:
            print("❌ No month data extracted")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ OCC fetcher test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def test_occ_available_months():
    """Test to see what months are actually available on the OCC website"""
    print("\n🧪 Testing available months on OCC website...")
    print("=" * 60)
    
    try:
        # Create fetcher
        fetcher = OCCDailyDataFetcher(download_dir="data/raw/occ_test")
        
        # Test different months to see what's available
        test_months = [
            (2025, 6, "June 2025"),
            (2025, 5, "May 2025"), 
            (2025, 4, "April 2025"),
            (2025, 7, "July 2025")
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

def test_occ_force_june_2025():
    """Test with modified approach to force June 2025 extraction"""
    print("\n🧪 Testing forced June 2025 extraction...")
    print("=" * 60)
    
    try:
        # Create fetcher
        fetcher = OCCDailyDataFetcher(download_dir="data/raw/occ_test")
        
        # Try to extract June 2025 with more explicit date handling
        print("📅 Attempting to force June 2025 extraction...")
        
        # Start driver manually for more control
        fetcher.start_driver()
        
        try:
            # Navigate to the page
            fetcher.driver.get(fetcher.base_url)
            fetcher.driver.implicitly_wait(5)
            
            print("🌐 Navigated to OCC website")
            
            # Find and click Daily Statistics radio button
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            wait = WebDriverWait(fetcher.driver, 10)
            radio_buttons = fetcher.driver.find_elements(By.XPATH, "//input[@type='radio']")
            
            daily_radio = None
            for radio in radio_buttons:
                if radio.get_attribute('value') == 'D':
                    daily_radio = radio
                    break
            
            if daily_radio:
                fetcher.driver.execute_script("arguments[0].click();", daily_radio)
                print("✅ Clicked Daily Statistics radio button")
            else:
                print("❌ Could not find Daily Statistics radio button")
                return pd.DataFrame()
            
            # Click date picker with more explicit handling
            date_input = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@name='report_date']")))
            fetcher.driver.execute_script("arguments[0].click();", date_input)
            print("✅ Clicked date picker")
            
            # Try to navigate to June 2025 more explicitly
            print("📅 Attempting to select June 2025...")
            
            # Look for year/month navigation
            try:
                year_month_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'month__year_btn')]")))
                year_month_btn.click()
                print("✅ Clicked year/month navigation")
            except:
                print("⚠️ Could not find year/month navigation button")
            
            # Select year 2025
            try:
                year_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'year') and text()='2025']")))
                year_element.click()
                print("✅ Selected year 2025")
            except:
                print("⚠️ Could not select year 2025")
            
            # Select month June
            try:
                month_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'month') and text()='June']")))
                month_element.click()
                print("✅ Selected month June")
            except:
                print("⚠️ Could not select month June")
            
            # Click View button
            view_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'marketData-inputBtn') and text()='View']")))
            fetcher.driver.execute_script("arguments[0].click();", view_button)
            print("✅ Clicked View button")
            
            # Wait for data to load
            import time
            time.sleep(fetcher.data_load_wait)
            
            # Extract data
            tables = fetcher.driver.find_elements(By.TAG_NAME, "table")
            print(f"📊 Found {len(tables)} tables")
            
            if len(tables) >= 2:
                extracted_data = {
                    'year': 2025,
                    'month': 6,
                    'month_name': 'June'
                }
                
                # Extract table data
                for i, table in enumerate(tables[:2]):
                    try:
                        from io import StringIO
                        table_html = table.get_attribute('outerHTML')
                        df = pd.read_html(StringIO(table_html))[0]
                        
                        if i == 0:
                            extracted_data['occ_contract_volume'] = df.to_dict('records')
                            print(f"✅ Extracted OCC table with {len(df)} rows")
                        elif i == 1:
                            extracted_data['futures_contract_volume'] = df.to_dict('records')
                            print(f"✅ Extracted Futures table with {len(df)} rows")
                            
                    except Exception as e:
                        print(f"❌ Error extracting table {i}: {str(e)}")
                
                # Convert to long format
                long_format_data = fetcher.convert_to_long_format([extracted_data])
                
                if not long_format_data.empty:
                    print("✅ Manual extraction successful!")
                    print(f"📊 Data shape: {long_format_data.shape}")
                    print(f"📅 Date range: {long_format_data['date'].min()} to {long_format_data['date'].max()}")
                    
                    # Save test file
                    test_file = Path("data/raw/occ_test/occ_june_2025_manual.parquet")
                    test_file.parent.mkdir(parents=True, exist_ok=True)
                    long_format_data.to_parquet(test_file, index=False)
                    print(f"💾 Manual test data saved to: {test_file}")
                    
                    return long_format_data
                else:
                    print("❌ Manual extraction resulted in empty data")
                    return pd.DataFrame()
            else:
                print("❌ Not enough tables found")
                return pd.DataFrame()
                
        finally:
            fetcher.close_driver()
            
    except Exception as e:
        print(f"❌ Manual June 2025 test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def main():
    """Run all OCC tests"""
    print("🚀 Starting OCC fetcher debugging for June 2025...")
    print("=" * 60)
    
    # Test 1: Specific June 2025 extraction
    print("\n1️⃣ Testing specific June 2025 extraction...")
    june_result = test_occ_june_2025_specific()
    
    # Test 2: Check available months
    print("\n2️⃣ Testing available months...")
    available_months = test_occ_available_months()
    
    # Test 3: Manual June 2025 extraction
    print("\n3️⃣ Testing manual June 2025 extraction...")
    manual_result = test_occ_force_june_2025()
    
    print("\n" + "=" * 60)
    print("📋 Summary:")
    print(f"   June 2025 specific: {'✅' if not june_result.empty else '❌'}")
    print(f"   Available months: {len(available_months)} found")
    print(f"   Manual June 2025: {'✅' if not manual_result.empty else '❌'}")
    
    if available_months:
        print(f"   Available months: {[name for _, _, name in available_months]}")
    
    # Show file sizes if data was saved
    test_dir = Path("data/raw/occ_test")
    if test_dir.exists():
        print(f"\n💾 Test files created:")
        for file in test_dir.glob("*.parquet"):
            size_mb = file.stat().st_size / (1024 * 1024)
            print(f"   • {file.name}: {size_mb:.2f} MB")
    
    if not june_result.empty or not manual_result.empty:
        print("\n🎉 At least one OCC test method worked!")
        print("💡 Next steps:")
        print("   • Check the extracted data quality")
        print("   • Verify the data format matches expectations")
        print("   • If June 2025 isn't available, check what months are available")
    else:
        print("\n❌ All OCC tests failed")
        print("💡 Troubleshooting:")
        print("   • Check if lxml and beautifulsoup4 are installed")
        print("   • Verify Chrome/ChromeDriver is available")
        print("   • Check OCC website accessibility")
        print("   • Review the detailed error messages above")

if __name__ == "__main__":
    main() 