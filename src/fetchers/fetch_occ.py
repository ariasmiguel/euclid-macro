"""
OCC (Options Clearing Corporation) Data Fetcher - Simplified Version

Fetches daily options and futures volume data from the OCC website.
Simplified to use straightforward start_date and end_date parameters.
"""

import pandas as pd
import os
import time
import calendar
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import Optional, List, Dict
from io import StringIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from ..core.base_fetcher import BaseDataFetcher
from ..utils.transform_utils import DataTransformUtils


class OCCDailyDataFetcher(BaseDataFetcher):
    """
    OCC daily volume data fetcher.
    Simplified to fetch data for date ranges specified by start_date and end_date.
    """
    
    def __init__(self, download_dir: str = "data/raw/occ"):
        """Initialize OCC fetcher."""
        super().__init__("occ")
        
        self.data_transformer = DataTransformUtils()
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)
        
        # OCC configuration
        self.base_url = "https://www.theocc.com/market-data/market-data-reports/volume-and-open-interest/historical-volume-statistics"
        self.sleep_time = 0.3
        self.data_load_wait = 1.0
        
        # Chrome options
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
    
    def fetch_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Fetch OCC data for date range.
        
        Args:
            start_date: Start date (will fetch from beginning of that month)
            end_date: End date (will fetch through end of that month)
            
        Returns:
            DataFrame with columns: date, symbol, metric, value
        """
        # Generate list of year-month tuples to fetch
        months_to_fetch = []
        current = date(start_date.year, start_date.month, 1)
        end = date(end_date.year, end_date.month, 1)
        
        while current <= end:
            # Don't fetch future months
            if current <= datetime.now().date():
                months_to_fetch.append((current.year, current.month))
            current += relativedelta(months=1)
        
        if not months_to_fetch:
            self.logger.warning("No valid months to fetch")
            return pd.DataFrame()
        
        self.logger.info(f"Fetching {len(months_to_fetch)} months: {months_to_fetch[0]} to {months_to_fetch[-1]}")
        
        # Fetch all months
        all_data = []
        for year, month in months_to_fetch:
            month_name = calendar.month_name[month]
            self.logger.info(f"Fetching {month_name} {year}")
            
            month_data = self.extract_month_data_single(year, month)
            if month_data:
                long_format_data = self.convert_to_long_format([month_data])
                all_data.append(long_format_data)
            
            time.sleep(1.0)  # Be nice to the server
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"✅ Fetched {len(combined_df)} records")
            
            # Save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"occ_{timestamp}.parquet"
            filepath = os.path.join(self.download_dir, filename)
            combined_df.to_parquet(filepath, index=False)
            self.logger.info(f"💾 Saved to {filepath}")
            
            return combined_df
        else:
            self.logger.warning("No data extracted")
            return pd.DataFrame()
    
    def extract_month_data_single(self, year: int, month: int) -> Optional[Dict]:
        """Extract data for a single month."""
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
    
    def start_driver(self):
        """Start Chrome driver."""
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.driver.maximize_window()
    
    def close_driver(self):
        """Close Chrome driver."""
        if self.driver:
            self.driver.quit()
    
    def extract_month_data(self, year: int, month: int) -> Optional[Dict]:
        """Extract data for a specific month from OCC website."""
        try:
            # Navigate to page
            self.driver.get(self.base_url)
            time.sleep(self.sleep_time)
            
            wait = WebDriverWait(self.driver, 10)
            
            # Click Daily Statistics radio button
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
            
            # Navigate to year/month
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
            
            # Extract tables
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
            self.logger.error(f"Error extracting {year}-{month}: {str(e)}")
            return None
    
    def convert_to_long_format(self, month_data_list: List[Dict]) -> pd.DataFrame:
        """Convert to long format: date, symbol, metric, value."""
        long_data = []
        
        for month_data in month_data_list:
            year = month_data['year']
            month = month_data['month']
            
            if 'occ_contract_volume' in month_data:
                occ_df = pd.DataFrame(month_data['occ_contract_volume'])
                
                # Handle futures data if present
                if 'futures_contract_volume' in month_data:
                    futures_df = pd.DataFrame(month_data['futures_contract_volume'])
                    futures_df = futures_df.iloc[:, :3].copy()
                    
                    # Set column names
                    occ_df.columns = ["date", "OCC_Options_Equity_Volume", "OCC_Options_Index_Volume", 
                                     "OCC_Options_Debt_Volume", "OCC_Futures_Total_Volume", "OCC_Total_Volume"]
                    futures_df.columns = ["date", "OCC_Futures_Equity_Volume", "OCC_Futures_Index_Volume"]
                    
                    # Merge
                    merged_df = pd.merge(occ_df, futures_df, on='date', how='outer')
                else:
                    merged_df = occ_df
                
                # Filter daily data only
                daily_mask = merged_df['date'].astype(str).str.match(r'^\d{1,2}/\d{1,2}$')
                daily_df = merged_df[daily_mask].copy()
                
                # Fix dates
                daily_df['date'] = daily_df['date'].apply(
                    lambda x: f"{year}-{x.split('/')[0].zfill(2)}-{x.split('/')[1].zfill(2)}"
                )
                
                # Convert to long format
                for _, row in daily_df.iterrows():
                    date_val = row['date']
                    for col in daily_df.columns:
                        if col != 'date':
                            long_data.append({
                                'date': date_val,
                                'symbol': 'OCC',
                                'metric': col,
                                'value': float(row[col]) if pd.notna(row[col]) else None
                            })
        
        return pd.DataFrame(long_data)
    
    # Methods required by BaseDataFetcher
    def get_single_series(self, identifier: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Implementation of abstract method."""
        return self.fetch_data(start_date.date(), end_date.date())
    
    def fetch_batch(self) -> pd.DataFrame:
        """Fetch full historical data."""
        start_date = date(2008, 1, 1)
        end_date = datetime.now().date()
        return self.fetch_data(start_date, end_date)


def fetch_occ() -> pd.DataFrame:
    """Main entry point for pipeline."""
    fetcher = OCCDailyDataFetcher()
    return fetcher.fetch_batch()


if __name__ == "__main__":
    # Simple test
    fetcher = OCCDailyDataFetcher()
    
    # Fetch last 3 months
    end_date = datetime.now().date()
    start_date = end_date - relativedelta(months=3)
    
    df = fetcher.fetch_data(start_date, end_date)
    print(f"Fetched {len(df)} records")
    print(df.head())