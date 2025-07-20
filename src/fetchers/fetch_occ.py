"""
OCC (Options Clearing Corporation) Data Fetcher

This module provides functionality to fetch daily options and futures volume data 
from the OCC website and return it in a standardized long format for the data pipeline.

The fetcher extracts data for entire years and consolidates everything into a single
parquet file optimized for DuckDB ingestion and analysis.

Uses BaseDataFetcher and utility classes following the established pipeline patterns.
"""

import pandas as pd
import os
import time
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional, List, Dict, Any
from io import StringIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from ..core.base_fetcher import BaseDataFetcher
from ..utils.web_scraping_utils import WebScrapingUtils
from ..utils.transform_utils import DataTransformUtils


class OCCDailyDataFetcher(BaseDataFetcher):
    """
    OCC daily volume data fetcher using BaseDataFetcher infrastructure.
    
    Extracts daily options and futures volume data from the OCC website,
    transforms it into standardized long format, and saves as a single
    consolidated parquet file optimized for DuckDB.
    
    Output format: date, symbol, metric, value where symbol='OCC' and metric contains
    descriptive names like 'OCC_Options_Equity_Volume', 'OCC_Futures_Total_Volume'.
    """
    
    def __init__(self, download_dir: str = "data/bronze"):
        """
        Initialize OCC fetcher.
        
        Args:
            download_dir: Directory for output files
        """
        super().__init__("occ")
        
        # Initialize utility classes
        self.data_transformer = DataTransformUtils()
        
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)
        
        # OCC specific configuration
        self.base_url = "https://www.theocc.com/market-data/market-data-reports/volume-and-open-interest/historical-volume-statistics"
        self.sleep_time = 0.3  # Ultra-fast timing
        self.data_load_wait = 1.0  # Critical data loading wait
        
        # Chrome options for headless scraping
        self.chrome_options = Options()
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--disable-web-security')
        self.chrome_options.add_argument('--allow-running-insecure-content')
        self.chrome_options.add_argument('--headless=new')
        self.chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.chrome_options.add_experimental_option('useAutomationExtension', False)
        self.chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = None
    
    def fetch_data(self, start_year: int, end_year: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch OCC data for specified year range.
        
        Args:
            start_year: Starting year for data extraction
            end_year: Ending year (optional, defaults to start_year)
            
        Returns:
            DataFrame in standard long format with columns: date, symbol, metric, value
            where symbol='OCC' and metric contains names like 'OCC_Options_Equity_Volume'
        """
        if end_year is None:
            end_year = start_year
            
        self.logger.info(f"Fetching OCC data from {start_year} to {end_year}")
        
        all_data = []
        
        for year in range(start_year, end_year + 1):
            year_data = self.extract_year_with_session_management(year)
            if year_data:
                # Convert to long format and append
                long_format_data = self.convert_to_long_format(year_data)
                all_data.append(long_format_data)
                
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"Successfully fetched {len(combined_df)} records")
            
            # Save to parquet
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"stg_occ_{timestamp}.parquet"
            filepath = os.path.join(self.download_dir, filename)
            combined_df.to_parquet(filepath, index=False)
            self.logger.info(f"Saved data to {filepath}")
            
            return combined_df
        else:
            self.logger.warning("No data extracted")
            return pd.DataFrame()
    
    def extract_year_with_session_management(self, year: int, batch_size: int = 1) -> List[Dict]:
        """Extract data for all months in a year using session management"""
        all_extracted_data = []
        successful_months = []
        failed_months = []
        
        self.logger.info(f"Extracting full year {year} with session management")
        
        total_batches = (12 + batch_size - 1) // batch_size
        
        try:
            for batch_num in range(total_batches):
                batch_start = batch_num * batch_size + 1
                batch_end = min(batch_start + batch_size - 1, 12)
                
                self.logger.info(f"Processing batch {batch_num + 1}/{total_batches}: Months {batch_start}-{batch_end}")
                
                # Start fresh browser session
                self.start_driver()
                
                try:
                    for month in range(batch_start, batch_end + 1):
                        month_name = calendar.month_name[month]
                        self.logger.info(f"Processing {month_name} {year}")
                        
                        extracted_data = self.extract_month_data(year, month)
                        
                        if extracted_data:
                            successful_months.append(f"{year}-{month:02d}")
                            all_extracted_data.append(extracted_data)
                        else:
                            failed_months.append(f"{year}-{month:02d}")
                        
                        # Small delay between months
                        if month < batch_end:
                            time.sleep(1.0)
                
                finally:
                    self.close_driver()
                
                # Pause between batches
                if batch_num < total_batches - 1:
                    time.sleep(3.0)
            
            self.logger.info(f"Year {year} extraction complete: {len(successful_months)} successful, {len(failed_months)} failed")
            
        except Exception as e:
            self.logger.error(f"Session management extraction failed: {str(e)}")
            try:
                self.close_driver()
            except:
                pass
                
        return all_extracted_data
    
    def start_driver(self):
        """Start the Chrome driver"""
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.driver.maximize_window()
        
    def close_driver(self):
        """Close the Chrome driver"""
        if self.driver:
            self.driver.quit()
            
    def extract_month_data(self, year: int, month: int) -> Optional[Dict]:
        """Extract daily data for a specific month"""
        try:
            # Navigate to the page
            self.driver.get(self.base_url)
            time.sleep(self.sleep_time)
            
            # Find and click Daily Statistics radio button
            wait = WebDriverWait(self.driver, 10)
            radio_buttons = self.driver.find_elements(By.XPATH, "//input[@type='radio']")
            
            daily_radio = None
            for radio in radio_buttons:
                if radio.get_attribute('value') == 'D':
                    daily_radio = radio
                    break
            
            if not daily_radio:
                return None
                
            self.driver.execute_script("arguments[0].click();", daily_radio)
            time.sleep(self.sleep_time)
            
            # Click date picker
            date_input = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@name='report_date']")))
            self.driver.execute_script("arguments[0].click();", date_input)
            time.sleep(self.sleep_time)
            
            # Navigate to correct month/year
            try:
                year_month_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'month__year_btn')]")))
                year_month_btn.click()
                time.sleep(self.sleep_time)
            except:
                pass
            
            # Select year
            try:
                year_element = wait.until(EC.element_to_be_clickable((By.XPATH, f"//span[contains(@class, 'year') and text()='{year}']")))
                year_element.click()
                time.sleep(self.sleep_time)
            except:
                pass
            
            # Select month
            month_names = ["January", "February", "March", "April", "May", "June",
                          "July", "August", "September", "October", "November", "December"]
            month_name = month_names[month - 1]
            
            try:
                month_element = wait.until(EC.element_to_be_clickable((By.XPATH, f"//span[contains(@class, 'month') and text()='{month_name}']")))
                month_element.click()
                time.sleep(self.sleep_time)
            except:
                pass
            
            # Click View button
            view_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'marketData-inputBtn') and text()='View']")))
            self.driver.execute_script("arguments[0].click();", view_button)
            time.sleep(self.data_load_wait)
            
            # Extract data tables
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            if len(tables) >= 2:
                extracted_data = {
                    'year': year,
                    'month': month,
                    'month_name': month_name
                }
                
                # Extract table data
                for i, table in enumerate(tables[:2]):
                    try:
                        table_html = table.get_attribute('outerHTML')
                        df = pd.read_html(StringIO(table_html))[0]
                        
                        if i == 0:
                            extracted_data['occ_contract_volume'] = df.to_dict('records')
                        elif i == 1:
                            extracted_data['futures_contract_volume'] = df.to_dict('records')
                            
                    except Exception as e:
                        self.logger.warning(f"Error extracting table {i}: {str(e)}")
                
                return extracted_data
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting data for {year}-{month}: {str(e)}")
            return None
    
    def convert_to_long_format(self, year_data: List[Dict]) -> pd.DataFrame:
        """Convert extracted data to standard long format with date, symbol, metric, value schema"""
        long_data = []
        
        for month_data in year_data:
            year = month_data['year']
            month = month_data['month']
            
            # Process OCC options data
            if 'occ_contract_volume' in month_data:
                occ_df = pd.DataFrame(month_data['occ_contract_volume'])
                
                # Keep only first 3 columns for futures (avoid total column duplication)
                if 'futures_contract_volume' in month_data:
                    futures_df = pd.DataFrame(month_data['futures_contract_volume'])
                    futures_df = futures_df.iloc[:, :3].copy()
                    
                    # Set column names
                    occ_df.columns = ["date", "OCC_Options_Equity_Volume", "OCC_Options_Index_Volume", 
                                     "OCC_Options_Debt_Volume", "OCC_Futures_Total_Volume", "OCC_Total_Volume"]
                    futures_df.columns = ["date", "OCC_Futures_Equity_Volume", "OCC_Futures_Index_Volume"]
                    
                    # Merge tables
                    merged_df = pd.merge(occ_df, futures_df, on='date', how='outer')
                else:
                    merged_df = occ_df
                
                # Filter daily data only and fix dates
                daily_mask = merged_df['date'].astype(str).str.match(r'^\d{1,2}/\d{1,2}$')
                daily_df = merged_df[daily_mask].copy()
                
                # Convert dates to proper format
                daily_df['date'] = daily_df['date'].apply(
                    lambda x: f"{year}-{x.split('/')[0].zfill(2)}-{x.split('/')[1].zfill(2)}"
                )
                
                # Convert to standard long format (date, symbol, metric, value)
                for _, row in daily_df.iterrows():
                    date_val = row['date']
                    for col in daily_df.columns:
                        if col != 'date':
                            long_data.append({
                                'date': date_val,
                                'symbol': 'OCC',  # Consistent symbol
                                'metric': col,     # Metric name in metric column
                                'value': float(row[col]) if pd.notna(row[col]) else None
                            })
        
        return pd.DataFrame(long_data)

    def fetch_batch_without_saving(self, start_year: int, end_year: Optional[int] = None, 
                                  max_months: Optional[int] = 2) -> pd.DataFrame:
        """
        Fetch OCC data without saving to file (for pipeline integration).
        Includes smart date range logic to avoid fetching future months.
        
        Args:
            start_year: Starting year for data extraction
            end_year: Ending year (optional, defaults to start_year)
            max_months: Maximum number of months to fetch (None for unlimited, default 2 for incremental)
            
        Returns:
            DataFrame in standard long format without saving files
        """
        if end_year is None:
            end_year = start_year
            
        # Smart date range: don't fetch future months
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        
        max_months_str = "unlimited" if max_months is None else str(max_months)
        self.logger.info(f"Fetching OCC data from {start_year} to {end_year} (pipeline mode, max {max_months_str} months)")
        
        all_data = []
        months_fetched = 0
        
        for year in range(start_year, end_year + 1):
            # Determine month range for this year
            if year == current_year:
                # For current year, only fetch up to current month
                month_range = list(range(1, min(current_month + 1, 13)))
                self.logger.info(f"Current year {year}: fetching months 1-{current_month}")
            elif year < current_year:
                # For past years, fetch all 12 months
                month_range = list(range(1, 13))
                self.logger.info(f"Past year {year}: fetching all 12 months")
            else:
                # For future years, skip entirely
                self.logger.info(f"Future year {year}: skipping")
                continue
            
            # Apply max_months limit if specified
            if max_months is not None and months_fetched >= max_months:
                self.logger.info(f"Reached max_months limit ({max_months}), stopping")
                break
                
            # Determine which months to fetch
            if max_months is None:
                # Unlimited: fetch all months in the year
                target_months = month_range
            else:
                # Limited: fetch only recent months up to the limit
                remaining_months = max_months - months_fetched
                target_months = month_range[-min(len(month_range), remaining_months):] if remaining_months > 0 else []
            
            for month in target_months:
                if max_months is not None and months_fetched >= max_months:
                    break
                    
                month_name = calendar.month_name[month]
                progress_str = f"({months_fetched + 1}/{max_months})" if max_months else f"({months_fetched + 1})"
                self.logger.info(f"Fetching {month_name} {year} {progress_str}")
                
                month_data = self.extract_month_data_single(year, month)
                if month_data:
                    long_format_data = self.convert_to_long_format([month_data])
                    all_data.append(long_format_data)
                    
                months_fetched += 1
                
                # Small delay between months
                time.sleep(1.0)
                
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            mode_str = "HISTORICAL" if max_months is None else "INCREMENTAL"
            self.logger.info(f"✅ {mode_str} fetch complete: {len(combined_df)} records from {months_fetched} months")
            return combined_df
        else:
            self.logger.warning("⚠️ No data extracted")
            return pd.DataFrame()

    def extract_month_data_single(self, year: int, month: int) -> Optional[Dict]:
        """
        Extract data for a single month without session management.
        Faster for small requests.
        """
        try:
            self.start_driver()
            result = self.extract_month_data(year, month)
            return result
        except Exception as e:
            self.logger.error(f"Error extracting {year}-{month}: {str(e)}")
            return None
        finally:
            try:
                self.close_driver()
            except:
                pass

    def get_single_series(self, identifier: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        Implementation of abstract method for OCC fetcher.
        For OCC, we extract by year, so this method extracts data for the years
        that overlap with the date range.
        
        Args:
            identifier: Not used for OCC (always 'OCC')
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            DataFrame with OCC data in long format
        """
        start_year = start_date.year
        end_year = end_date.year
        
        return self.fetch_data(start_year, end_year)
    
    def fetch_batch(self, symbols_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Implementation of abstract method for OCC fetcher.
        Intelligently determines whether to do full historical fetch or incremental update.
        
        Args:
            symbols_df: Not used for OCC (OCC is a direct source)
            
        Returns:
            DataFrame with OCC data in standard format
        """
        # Check if this should be a full historical fetch or incremental update
        if self._should_do_full_historical_fetch():
            self.logger.info("🔄 Performing FULL HISTORICAL fetch (January 2008 to current)")
            return self.fetch_full_historical_data()
        else:
            self.logger.info("📈 Performing INCREMENTAL fetch (current month only)")
            return self.fetch_incremental_data()
    
    def _should_do_full_historical_fetch(self) -> bool:
        """
        Determine if we should do a full historical fetch or incremental update.
        
        Logic:
        - Check DuckDB stg_occ table directly for existing data
        - If no data exists, do full fetch
        - If data exists, analyze date coverage to find missing year-month combinations
        - If significant gaps exist (missing multiple months), do full fetch
        - If only recent months are missing, do incremental fetch
        
        Returns:
            True if full historical fetch is needed, False for incremental
        """
        try:
            import duckdb
            import pandas as pd
            from datetime import datetime
            from dateutil.relativedelta import relativedelta
            
            # Connect to DuckDB and check existing data
            try:
                con = duckdb.connect('bristol_gate.duckdb', read_only=True)
                
                # Check if stg_occ table exists and has data
                try:
                    result = con.execute("SELECT COUNT(*) FROM stg_occ").fetchone()
                    record_count = result[0] if result else 0
                    
                    if record_count == 0:
                        self.logger.info("💡 No existing OCC data in DuckDB - will do full historical fetch")
                        con.close()
                        return True
                    
                    # Get existing data coverage
                    existing_data = con.execute(
                        "SELECT date FROM stg_occ WHERE symbol = 'OCC'"
                    ).fetchdf()
                    
                    con.close()
                    
                    if len(existing_data) == 0:
                        self.logger.info("💡 No OCC records found in DuckDB - will do full historical fetch")
                        return True
                    
                    # Analyze date coverage
                    existing_data['date'] = pd.to_datetime(existing_data['date'])
                    existing_months = set(existing_data['date'].dt.to_period('M').astype(str))
                    
                    # Generate expected year-month combinations from 2008-01 to current month
                    start_date = datetime(2008, 1, 1)  # OCC data starts around 2008
                    current_date = datetime.now()
                    
                    expected_months = set()
                    current_period = start_date
                    while current_period <= current_date:
                        expected_months.add(current_period.strftime('%Y-%m'))
                        current_period += relativedelta(months=1)
                    
                    # Find missing months
                    missing_months = expected_months - existing_months
                    missing_count = len(missing_months)
                    total_expected = len(expected_months)
                    
                    self.logger.info(f"📊 DuckDB data coverage analysis:")
                    self.logger.info(f"   - Total records: {len(existing_data)}")
                    self.logger.info(f"   - Expected months: {total_expected} (2008-01 to {current_date.strftime('%Y-%m')})")
                    self.logger.info(f"   - Existing months: {len(existing_months)}")
                    self.logger.info(f"   - Missing months: {missing_count}")
                    
                    # Decision logic
                    if missing_count == 0:
                        self.logger.info("✅ Complete data coverage - will do incremental fetch")
                        return False
                    elif missing_count == 1 and current_date.strftime('%Y-%m') in missing_months:
                        self.logger.info("📈 Only current month missing - will do incremental fetch")
                        return False
                    elif missing_count <= 3:
                        self.logger.info(f"⚠️ Few months missing ({missing_count}) - will do incremental fetch")
                        # For small gaps, incremental fetch will catch up
                        return False
                    else:
                        # Significant gaps - need full historical fetch
                        self.logger.info(f"🔄 Significant gaps detected ({missing_count} months missing) - will do full historical fetch")
                        if missing_count <= 10:  # Show missing months if not too many
                            sorted_missing = sorted(list(missing_months))
                            self.logger.info(f"   Missing: {', '.join(sorted_missing)}")
                        return True
                        
                except Exception as table_error:
                    # Table doesn't exist or is empty
                    self.logger.info(f"💡 DuckDB stg_occ table not accessible ({table_error}) - will do full historical fetch")
                    try:
                        con.close()
                    except:
                        pass
                    return True
                    
            except Exception as db_error:
                self.logger.warning(f"⚠️ Could not access DuckDB: {db_error} - defaulting to full fetch")
                return True
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error checking existing data: {e} - defaulting to full fetch")
            return True
    
    def fetch_full_historical_data(self) -> pd.DataFrame:
        """
        Fetch full historical data from January 2008 to current date.
        
        Returns:
            DataFrame with complete historical OCC data
        """
        current_year = datetime.now().year
        start_year = 2008  # OCC data availability starts around 2008
        
        self.logger.info(f"🔄 Fetching FULL HISTORICAL OCC data: {start_year} to {current_year}")
        self.logger.info(f"📅 This will fetch approximately {(current_year - start_year + 1) * 12} months of data")
        
        # Use the existing fetch_batch_without_saving but without month limits
        return self.fetch_batch_without_saving(start_year, current_year, max_months=None)
    
    def fetch_incremental_data(self) -> pd.DataFrame:
        """
        Fetch incremental data (missing recent months) for regular updates.
        Intelligently determines which recent months are missing and fetches them.
        
        Returns:
            DataFrame with missing recent month OCC data
        """
        try:
            import glob
            
            # Determine what months we need to fetch
            missing_months = self._get_missing_recent_months()
            
            if not missing_months:
                self.logger.info("✅ No missing recent months - data is up to date")
                return pd.DataFrame()
            
            self.logger.info(f"📈 Fetching INCREMENTAL OCC data for {len(missing_months)} missing months: {', '.join(missing_months)}")
            
            # Fetch the missing months
            all_data = []
            for year_month in missing_months:
                year, month = map(int, year_month.split('-'))
                month_name = calendar.month_name[month]
                
                self.logger.info(f"Fetching missing month: {month_name} {year}")
                month_data = self.extract_month_data_single(year, month)
                if month_data:
                    long_format_data = self.convert_to_long_format([month_data])
                    all_data.append(long_format_data)
                
                # Small delay between months
                time.sleep(1.0)
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                self.logger.info(f"✅ INCREMENTAL fetch complete: {len(combined_df)} records from {len(missing_months)} months")
                return combined_df
            else:
                self.logger.warning("⚠️ No incremental data extracted")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"❌ Error in incremental fetch: {e}")
            # Fallback to simple current month fetch
            current_year = datetime.now().year
            return self.fetch_batch_without_saving(current_year, current_year, max_months=1)
    
    def _get_missing_recent_months(self, lookback_months: int = 3) -> List[str]:
        """
        Get list of missing recent months (last N months including current) from DuckDB.
        
        Args:
            lookback_months: How many recent months to check (default 3)
            
        Returns:
            List of missing year-month strings in YYYY-MM format
        """
        try:
            import duckdb
            
            # Get existing data from DuckDB
            existing_months = set()
            
            try:
                con = duckdb.connect('bristol_gate.duckdb', read_only=True)
                existing_data = con.execute(
                    "SELECT date FROM stg_occ WHERE symbol = 'OCC'"
                ).fetchdf()
                con.close()
                
                if len(existing_data) > 0:
                    existing_data['date'] = pd.to_datetime(existing_data['date'])
                    existing_months = set(existing_data['date'].dt.to_period('M').astype(str))
                    
            except Exception as db_error:
                self.logger.warning(f"⚠️ Could not access DuckDB for recent months check: {db_error}")
            
            # Generate recent months to check
            current_date = datetime.now()
            recent_months = []
            
            for i in range(lookback_months):
                target_date = current_date - relativedelta(months=i)
                recent_months.append(target_date.strftime('%Y-%m'))
            
            # Find missing recent months
            missing_months = [month for month in recent_months if month not in existing_months]
            return sorted(missing_months)  # Sort chronologically
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error determining missing months: {e}")
            # Fallback to current month
            return [datetime.now().strftime('%Y-%m')]


def fetch_occ() -> pd.DataFrame:
    """
    Fetch OCC daily volume data using the main pipeline interface.
    
    This function provides the standard interface for the data collection pipeline.
    Intelligently determines whether to do full historical fetch (2008-current) or 
    incremental update (current month only) based on existing data.
    
    Returns:
        DataFrame in standard long format with columns: date, symbol, metric, value
    """
    fetcher = OCCDailyDataFetcher()
    return fetcher.fetch_batch()


if __name__ == "__main__":
    # Example usage - test the standardized interface
    df = fetch_occ()
    print(f"Fetched {len(df)} records")
    print(df.head())
    
    # Legacy usage with specific years (still available via class)
    # fetcher = OCCDailyDataFetcher()
    # df_historical = fetcher.fetch_data(2008, 2008)
    # print(f"Historical data: {len(df_historical)} records") 