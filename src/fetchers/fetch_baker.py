"""
Baker Hughes Rig Count Data Fetcher

This module provides functionality to fetch rig count data from Baker Hughes
and return it in a standardized format for the data pipeline.

Refactored to use BaseDataFetcher and utility classes to eliminate code duplication.
"""

import pandas as pd
import os
from datetime import datetime
import time
from selenium.webdriver.common.by import By
from typing import Optional

from ..core.base_fetcher import BaseDataFetcher
from ..utils.web_scraping_utils import WebScrapingUtils
from ..utils.file_download_utils import FileDownloadUtils
from ..utils.excel_processing_utils import ExcelProcessingUtils
from ..utils.transform_utils import DataTransformUtils


class BakerHughesFetcher(BaseDataFetcher):
    """
    Baker Hughes rig count data fetcher using BaseDataFetcher infrastructure.
    
    Combines web scraping, file download, Excel processing, and data transformation
    utilities to fetch and standardize Baker Hughes rig count data.
    """
    
    def __init__(self, download_dir: str = "data/baker_hughes"):
        """
        Initialize Baker Hughes fetcher.
        
        Args:
            download_dir: Directory for downloaded files
        """
        super().__init__("baker_hughes")
        
        # Initialize utility classes
        self.web_scraper = WebScrapingUtils()
        self.file_downloader = FileDownloadUtils(download_dir)
        self.excel_processor = ExcelProcessingUtils()
        self.data_transformer = DataTransformUtils()
        
        self.download_dir = download_dir
        
        # Baker Hughes specific configuration
        self.default_url = "https://bakerhughesrigcount.gcs-web.com/na-rig-count"
        self.file_name_pattern = "North America Rotary Rig Count ("
        self.sheet_name = "US Oil & Gas Split"
        self.skip_rows = 6
        
        self.logger.info("Baker Hughes fetcher initialized")
    
    def get_single_series(self, series_id: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        Baker Hughes doesn't use individual series - this method calls fetch_batch.
        
        Args:
            series_id: Not used for Baker Hughes (fetches all rig count data)
            start_date: Not used (gets latest available data)
            end_date: Not used (gets latest available data)
            
        Returns:
            DataFrame with all Baker Hughes rig count data
        """
        self.logger.info("Baker Hughes fetches all data at once, delegating to fetch_batch")
        # Create a dummy symbols_df for compatibility
        dummy_symbols_df = pd.DataFrame({
            'string.symbol': ['BAKER_HUGHES_RIG_COUNT'],
            'string.source': ['baker'],
            'date.series.start': [start_date.strftime('%Y-%m-%d')]
        })
        return self.fetch_batch(dummy_symbols_df)
    
    def fetch_batch(self, symbols_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Fetch Baker Hughes rig count data.
        
        Args:
            symbols_df: Not used for Baker Hughes (optional for compatibility)
            
        Returns:
            DataFrame with standardized Baker Hughes data
        """
        self.logger.info("Starting Baker Hughes rig count data collection")
        
        for attempt in range(self.max_retries):
            try:
                # Step 1: Download the Excel file using web scraping
                downloaded_file = self._download_baker_hughes_file()
                
                if not downloaded_file:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Download failed, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(self.base_wait_time)
                        continue
                    else:
                        self.logger.error("Failed to download Baker Hughes file after all retries")
                        return pd.DataFrame()
                
                # Step 2: Process the Excel file
                raw_data = self._process_baker_hughes_excel(downloaded_file)
                
                if raw_data.empty:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Excel processing failed, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        continue
                    else:
                        self.logger.error("Failed to process Baker Hughes Excel file")
                        return pd.DataFrame()
                
                # Step 3: Transform data to standard format
                standardized_data = self._transform_baker_hughes_data(raw_data)
                
                self.logger.info(f"Baker Hughes data collection completed. Total rows: {len(standardized_data)}")
                return standardized_data
                
            except Exception as e:
                if not self.handle_api_error(e, "Baker Hughes", attempt, self.max_retries):
                    return pd.DataFrame()
        
        self.logger.error("Failed to fetch Baker Hughes data after all retries")
        return pd.DataFrame()
    
    def _download_baker_hughes_file(self) -> Optional[str]:
        """
        Download Baker Hughes Excel file using web scraping.
        
        Returns:
            Path to downloaded file or None if failed
        """
        driver = None
        try:
            # Setup web driver using utility class
            driver = self.web_scraper.setup_chrome_driver(
                download_dir=self.download_dir,
                headless=True
            )
            
            # Navigate to Baker Hughes page
            self.logger.info(f"Navigating to Baker Hughes page: {self.default_url}")
            driver.get(self.default_url)
            
            # Use utility method to wait and check access
            if not self.web_scraper.wait_for_page_load(driver, timeout=10):
                self.logger.error("Page failed to load properly")
                return None
            
            # Check for access denied
            if "Access Denied" in driver.page_source or "Forbidden" in driver.page_source:
                self.logger.error("Access denied to Baker Hughes website")
                return None
            
            # Find and click download link
            target_link = self._find_download_link(driver)
            if not target_link:
                self.logger.error("Could not find download link")
                return None
            
            self.logger.info("Clicking download link...")
            target_link.click()
            
            # Wait for download completion using utility method
            downloaded_file = self.file_downloader.wait_for_download_completion(
                timeout=30,
                file_extension='.xlsb'
            )
            
            return downloaded_file
            
        except Exception as e:
            self.logger.error(f"Error downloading Baker Hughes file: {str(e)}")
            return None
            
        finally:
            if driver:
                driver.quit()
    
    def _find_download_link(self, driver):
        """
        Find the Baker Hughes rig count download link.
        
        Args:
            driver: Selenium WebDriver instance
            
        Returns:
            WebElement for the download link or None
        """
        try:
            self.logger.info("Searching for rig count spreadsheet link...")
            all_links = driver.find_elements(By.TAG_NAME, "a")
            
            for link in all_links:
                try:
                    link_text = link.text.strip()
                    if self.file_name_pattern in link_text:
                        self.logger.info(f"Found target link: {link_text}")
                        return link
                except:
                    continue
            
            self.logger.error(f"No link found containing '{self.file_name_pattern}'")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding download link: {str(e)}")
            return None
    
    def _process_baker_hughes_excel(self, file_path: str) -> pd.DataFrame:
        """
        Process Baker Hughes Excel file using ExcelProcessingUtils.
        
        Args:
            file_path: Path to downloaded Excel file
            
        Returns:
            Processed DataFrame
        """
        try:
            self.logger.info(f"Processing Excel file: {file_path}")
            
            # Read Excel file using utility class
            df = self.excel_processor.read_excel_file(
                file_path=file_path,
                sheet_name=self.sheet_name,
                skip_rows=self.skip_rows
            )
            
            if df.empty:
                self.logger.error("Excel file is empty or could not be read")
                return pd.DataFrame()
            
            # Process date column using utility method
            df = self.excel_processor.convert_excel_dates(df)
            
            self.logger.info(f"Successfully processed Excel file with {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing Excel file: {str(e)}")
            return pd.DataFrame()
    
    def _transform_baker_hughes_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Baker Hughes data to standard format using DataTransformUtils.
        
        Args:
            df: Raw DataFrame from Excel file
            
        Returns:
            Standardized DataFrame with columns: date, symbol, metric, value
        """
        try:
            # First standardize the column names (Date -> date)
            df = df.rename(columns={'Date': 'date'})
            
            # Convert ONLY the 'date' column if it contains Excel serial numbers
            # Do NOT auto-detect other columns as dates (they are rig count values)
            if 'date' in df.columns:
                df = self.excel_processor.convert_excel_dates(df, date_columns=['date'])
            
            # Add BKR_ prefix to all columns except date
            df.columns = ['date' if col == 'date' else f'BKR_{col}' for col in df.columns]
            
            # Remove percentage columns using utility method
            df = self.data_transformer.remove_percentage_columns(df)
            
            # Transform wide to long format using utility method
            value_columns = [col for col in df.columns if col.startswith('BKR_')]
            
            result_df = self.data_transformer.melt_to_long_format(
                df=df,
                id_vars=['date'],
                value_vars=value_columns,
                var_name='metric',
                value_name='value'
            )
            
            # Add symbol column (using metric name)
            result_df['symbol'] = result_df['metric']
            result_df['metric'] = 'rig_count'
            
            # Standardize column order using utility method
            result_df = self.data_transformer.standardize_column_order(
                result_df,
                expected_order=['date', 'symbol', 'metric', 'value']
            )
            
            # Ensure value column is numeric (convert any remaining date objects back to numbers)
            if 'value' in result_df.columns:
                # Convert back to numeric in case any Excel dates were mistakenly applied
                result_df['value'] = pd.to_numeric(result_df['value'], errors='coerce')
            
            # Clean data using utility method
            result_df = self.data_transformer.clean_and_validate_data(
                result_df,
                required_columns=['date', 'symbol', 'value']
            )
            
            self.logger.info(f"Data transformation completed. Final rows: {len(result_df)}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error transforming Baker Hughes data: {str(e)}")
            return pd.DataFrame()


# Legacy function wrappers for backward compatibility
def get_baker_data(baker_url: str = "https://bakerhughesrigcount.gcs-web.com/na-rig-count",
                   file_name: str = "North America Rotary Rig Count (",
                   sheet_name: str = "US Oil & Gas Split",
                   skip_rows: int = 6,
                   download_dir: str = "data/baker_hughes") -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use BakerHughesFetcher.fetch_batch() instead.
    """
    fetcher = BakerHughesFetcher(download_dir)
    return fetcher.fetch_batch()


def fetch_baker() -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use BakerHughesFetcher.fetch_batch() instead.
    """
    fetcher = BakerHughesFetcher()
    return fetcher.fetch_batch()


if __name__ == "__main__":
    # Test the refactored fetcher
    fetcher = BakerHughesFetcher()
    
    fetcher.logger.info("Testing refactored Baker Hughes fetcher...")
    test_data = fetcher.fetch_batch()
    
    print(f"Test data shape: {test_data.shape}")
    if not test_data.empty:
        print(test_data.head())
        print(f"Unique symbols: {test_data['symbol'].unique()[:10]}")  # Show first 10
        print(f'Value range: {test_data["value"].min()} to {test_data["value"].max()}')
    else:
        print("No data retrieved") 