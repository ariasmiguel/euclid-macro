"""
FINRA Margin Statistics Data Fetcher

This module provides functionality to fetch margin statistics data from FINRA
and return it in a standardized format for the data pipeline.

Refactored to use BaseDataFetcher and utility classes to eliminate code duplication.
"""

import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional
import requests
from urllib.parse import urlparse
from selenium.webdriver.common.by import By

from ..core.base_fetcher import BaseDataFetcher
from ..utils.web_scraping_utils import WebScrapingUtils
from ..utils.file_download_utils import FileDownloadUtils
from ..utils.excel_processing_utils import ExcelProcessingUtils
from ..utils.transform_utils import DataTransformUtils


class FINRAFetcher(BaseDataFetcher):
    """
    FINRA margin statistics data fetcher using BaseDataFetcher infrastructure.
    
    Combines web scraping, file download, Excel processing, and data transformation
    utilities to fetch and standardize FINRA margin statistics data.
    """
    
    def __init__(self, download_dir: str = "data/finra"):
        """
        Initialize FINRA fetcher.
        
        Args:
            download_dir: Directory for downloaded files
        """
        super().__init__("finra")
        
        # Initialize utility classes
        self.web_scraper = WebScrapingUtils()
        self.file_downloader = FileDownloadUtils(download_dir)
        self.excel_processor = ExcelProcessingUtils()
        self.data_transformer = DataTransformUtils()
        
        self.download_dir = download_dir
        
        # FINRA specific configuration
        self.default_url = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"
        self.download_link_text = "DOWNLOAD THE DATA"
        
        self.logger.info("FINRA fetcher initialized")
    
    def get_single_series(self, series_id: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        FINRA doesn't use individual series - this method calls fetch_batch.
        
        Args:
            series_id: Not used for FINRA (fetches all margin statistics)
            start_date: Not used (gets latest available data)
            end_date: Not used (gets latest available data)
            
        Returns:
            DataFrame with all FINRA margin statistics data
        """
        self.logger.info("FINRA fetches all data at once, delegating to fetch_batch")
        # Create a dummy symbols_df for compatibility
        dummy_symbols_df = pd.DataFrame({
            'string.symbol': ['FINRA_MARGIN_STATS'],
            'string.source': ['finra'],
            'date.series.start': [start_date.strftime('%Y-%m-%d')]
        })
        return self.fetch_batch(dummy_symbols_df)
    
    def fetch_batch(self, symbols_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Fetch FINRA margin statistics data.
        
        Args:
            symbols_df: Not used for FINRA (optional for compatibility)
            
        Returns:
            DataFrame with standardized FINRA data
        """
        self.logger.info("Starting FINRA margin statistics data collection")
        
        for attempt in range(self.max_retries):
            try:
                # Step 1: Find and download the Excel file
                download_url = self._find_download_url()
                
                if not download_url:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Failed to find download URL, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        continue
                    else:
                        self.logger.error("Failed to find FINRA download URL after all retries")
                        return pd.DataFrame()
                
                # Step 2: Download the file directly
                downloaded_file = self._download_finra_file(download_url)
                
                if not downloaded_file:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Download failed, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        continue
                    else:
                        self.logger.error("Failed to download FINRA file after all retries")
                        return pd.DataFrame()
                
                # Step 3: Process the Excel file
                raw_data = self._process_finra_excel(downloaded_file)
                
                if raw_data.empty:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Excel processing failed, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        continue
                    else:
                        self.logger.error("Failed to process FINRA Excel file")
                        return pd.DataFrame()
                
                # Step 4: Transform data to standard format
                standardized_data = self._transform_finra_data(raw_data)
                
                self.logger.info(f"FINRA data collection completed. Total rows: {len(standardized_data)}")
                return standardized_data
                
            except Exception as e:
                if not self.handle_api_error(e, "FINRA", attempt, self.max_retries):
                    return pd.DataFrame()
        
        self.logger.error("Failed to fetch FINRA data after all retries")
        return pd.DataFrame()
    
    def _find_download_url(self) -> Optional[str]:
        """
        Find the FINRA download URL using web scraping.
        
        Returns:
            Download URL or None if not found
        """
        driver = None
        try:
            # Setup web driver using utility class
            driver = self.web_scraper.setup_chrome_driver(
                download_dir=self.download_dir,
                headless=True
            )
            
            # Navigate to FINRA page
            self.logger.info(f"Navigating to FINRA page: {self.default_url}")
            driver.get(self.default_url)
            
            # Use utility method to wait and check access
            if not self.web_scraper.wait_for_page_load(driver, timeout=10):
                self.logger.error("Page failed to load properly")
                return None
            
            # Check for access denied
            if "Access Denied" in driver.page_source or "Forbidden" in driver.page_source:
                self.logger.error("Access denied to FINRA website")
                return None
            
            # Find download link
            download_url = self._extract_download_link(driver)
            return download_url
            
        except Exception as e:
            self.logger.error(f"Error finding FINRA download URL: {str(e)}")
            return None
            
        finally:
            if driver:
                driver.quit()
    
    def _extract_download_link(self, driver) -> Optional[str]:
        """
        Extract the download link from the FINRA page.
        
        Args:
            driver: Selenium WebDriver instance
            
        Returns:
            Download URL or None if not found
        """
        try:
            self.logger.info("Searching for download link...")
            all_links = driver.find_elements(By.TAG_NAME, "a")
            
            for link in all_links:
                try:
                    link_text = link.text.strip()
                    if self.download_link_text in link_text.upper():
                        href = link.get_attribute("href")
                        self.logger.info(f"Found download link: {href}")
                        return href
                except:
                    continue
            
            self.logger.error(f"No link found containing '{self.download_link_text}'")
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting download link: {str(e)}")
            return None
    
    def _download_finra_file(self, download_url: str) -> Optional[str]:
        """
        Download FINRA Excel file directly using requests.
        
        Args:
            download_url: URL to download from
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            self.logger.info(f"Downloading Excel file from: {download_url}")
            
            # Use utility method for HTTP download
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
            }
            
            response = requests.get(download_url, headers=headers, timeout=60)
            response.raise_for_status()
            
            # Get filename from URL
            parsed_url = urlparse(download_url)
            filename = os.path.basename(parsed_url.path)
            if not filename.endswith(('.xlsx', '.xls')):
                filename = "finra_margin_statistics.xlsx"
            
            # Save file
            file_path = os.path.join(self.download_dir, filename)
            
            # Ensure download directory exists
            os.makedirs(self.download_dir, exist_ok=True)
            
            with open(file_path, 'wb') as file:
                file.write(response.content)
            
            self.logger.info(f"File downloaded successfully to: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error downloading FINRA file: {str(e)}")
            return None
    
    def _process_finra_excel(self, file_path: str) -> pd.DataFrame:
        """
        Process FINRA Excel file using ExcelProcessingUtils.
        
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
                sheet_name=0,  # First sheet
                skip_rows=0
            )
            
            if df.empty:
                self.logger.error("Excel file is empty or could not be read")
                return pd.DataFrame()
            
            self.logger.info(f"Successfully processed Excel file with {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing Excel file: {str(e)}")
            return pd.DataFrame()
    
    def _transform_finra_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform FINRA data to standard format using DataTransformUtils.
        
        Args:
            df: Raw DataFrame from Excel file
            
        Returns:
            Standardized DataFrame with columns: date, symbol, metric, value
        """
        try:
            # Create the date column from Year-Month
            df['date'] = pd.to_datetime(df['Year-Month'], format='%Y-%m')
            df['date'] = df['date'].apply(lambda x: (x + relativedelta(months=1, days=-1))).dt.date
            
            # Delete the Year-Month column
            df = df.drop(columns=['Year-Month'])
            
            # Rename the columns with FINRA_ prefix
            df.columns = [
                "FINRA_Margin_Debt",
                "FINRA_Free_Credit_Cash", 
                "FINRA_Free_Credit_Margin",
                "date"
            ]
            
            # Transform wide to long format using utility method
            value_columns = [col for col in df.columns if col.startswith('FINRA_')]
            
            result_df = self.data_transformer.melt_to_long_format(
                df=df,
                id_vars=['date'],
                value_vars=value_columns,
                var_name='metric',
                value_name='value'
            )
            
            # Add symbol column (using metric name)
            result_df['symbol'] = result_df['metric']
            result_df['metric'] = 'value'
            
            # Standardize column order using utility method
            result_df = self.data_transformer.standardize_column_order(
                result_df,
                expected_order=['date', 'symbol', 'metric', 'value']
            )
            
            # Clean data using utility method
            result_df = self.data_transformer.clean_and_validate_data(
                result_df,
                required_columns=['date', 'symbol', 'value']
            )
            
            self.logger.info(f"Data transformation completed. Final rows: {len(result_df)}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error transforming FINRA data: {str(e)}")
            return pd.DataFrame()


# Legacy function wrappers for backward compatibility
def get_finra_data(
    url: str = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics",
    download_dir: str = "data/finra",
    headless: bool = True
) -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use FINRAFetcher.fetch_batch() instead.
    """
    fetcher = FINRAFetcher(download_dir)
    return fetcher.fetch_batch()


def fetch_finra() -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use FINRAFetcher.fetch_batch() instead.
    """
    fetcher = FINRAFetcher()
    return fetcher.fetch_batch()


if __name__ == "__main__":
    # Test the refactored fetcher
    fetcher = FINRAFetcher()
    
    fetcher.logger.info("Testing refactored FINRA fetcher...")
    test_data = fetcher.fetch_batch()
    
    print(f"Test data shape: {test_data.shape}")
    if not test_data.empty:
        print(test_data.head())
        print(f"Unique symbols: {test_data['symbol'].unique()}")
    else:
        print("No data retrieved") 