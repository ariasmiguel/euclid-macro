"""
USDA ERS (Economic Research Service) Data Fetcher

This module provides functionality to fetch agricultural data from USDA ERS
and return it in a standardized format for the data pipeline.

Refactored to use BaseDataFetcher and utility classes to eliminate code duplication.
"""

import pandas as pd
import os
from datetime import datetime
from typing import Optional
import re
import traceback
import requests
from urllib.parse import urlparse, unquote
from selenium.webdriver.common.by import By

from ..core.base_fetcher import BaseDataFetcher
from ..utils.web_scraping_utils import WebScrapingUtils
from ..utils.file_download_utils import FileDownloadUtils
from ..utils.excel_processing_utils import ExcelProcessingUtils
from ..utils.transform_utils import DataTransformUtils


class USDAFetcher(BaseDataFetcher):
    """
    USDA ERS data fetcher using BaseDataFetcher infrastructure.
    
    Combines web scraping, file download, Excel processing, and data transformation
    utilities to fetch and standardize USDA agricultural data.
    """
    
    def __init__(self, download_dir: str = "data/usda"):
        """
        Initialize USDA fetcher.
        
        Args:
            download_dir: Directory for downloaded files
        """
        super().__init__("usda")
        
        # Initialize utility classes
        self.web_scraper = WebScrapingUtils()
        self.file_downloader = FileDownloadUtils(download_dir)
        self.excel_processor = ExcelProcessingUtils()
        self.data_transformer = DataTransformUtils()
        
        self.download_dir = download_dir
        
        # USDA specific configuration - defaults for net farm income
        self.default_url = "https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/data-files-us-and-state-level-farm-income-and-wealth-statistics/"
        self.default_link_text = "U.S. farm sector financial indicators"
        self.default_metric_pattern = r'net\s+farm\s+income'
        self.default_sheet_name = 'Sheet1'
        self.default_symbol_name = "USDA_NET_FARM_INCOME"
        
        self.logger.info("USDA fetcher initialized")
    
    def get_single_series(self, series_id: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        USDA doesn't use individual series - this method calls fetch_batch.
        
        Args:
            series_id: Not used for USDA (fetches specific farm metric)
            start_date: Not used (gets latest available data)
            end_date: Not used (gets latest available data)
            
        Returns:
            DataFrame with USDA farm income data
        """
        self.logger.info("USDA fetches all data at once, delegating to fetch_batch")
        # Create a dummy symbols_df for compatibility
        dummy_symbols_df = pd.DataFrame({
            'string.symbol': [self.default_symbol_name],
            'string.source': ['usda'],
            'date.series.start': [start_date.strftime('%Y-%m-%d')]
        })
        return self.fetch_batch(dummy_symbols_df)
    
    def fetch_batch(self, symbols_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Fetch USDA farm income data.
        
        Args:
            symbols_df: Not used for USDA (optional for compatibility)
            
        Returns:
            DataFrame with standardized USDA data
        """
        return self.get_usda_ers_data()
    
    def get_usda_ers_data(self,
                         usda_page_url: Optional[str] = None,
                         target_link_text: Optional[str] = None,
                         target_metric_pattern: Optional[str] = None,
                         sheet_name: Optional[str] = None,
                         symbol_name: Optional[str] = None) -> pd.DataFrame:
        """
        Downloads and processes USDA ERS farm income data for a specific metric.
        
        Args:
            usda_page_url: URL of the USDA ERS data page
            target_link_text: Text to identify the download link
            target_metric_pattern: Regex pattern to identify the target metric
            sheet_name: Name of the sheet to process
            symbol_name: Symbol to use for the output data
            
        Returns:
            DataFrame with columns ['date', 'symbol', 'metric', 'value']
        """
        self.logger.info("Starting USDA ERS data collection")
        
        # Use defaults if not provided
        page_url = usda_page_url or self.default_url
        link_text = target_link_text or self.default_link_text
        metric_pattern = target_metric_pattern or self.default_metric_pattern
        sheet = sheet_name or self.default_sheet_name
        symbol = symbol_name or self.default_symbol_name
        
        for attempt in range(self.max_retries):
            try:
                # Step 1: Find and download the Excel file
                downloaded_file = self._download_usda_file(page_url, link_text)
                
                if not downloaded_file:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Download failed, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        continue
                    else:
                        self.logger.error("Failed to download USDA file after all retries")
                        return pd.DataFrame()
                
                # Step 2: Process the Excel file
                processed_data = self._process_usda_excel(
                    downloaded_file, sheet, metric_pattern, symbol
                )
                
                if processed_data.empty:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Excel processing failed, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        continue
                    else:
                        self.logger.error("Failed to process USDA Excel file")
                        return pd.DataFrame()
                
                self.logger.info(f"USDA data collection completed. Total rows: {len(processed_data)}")
                return processed_data
                
            except Exception as e:
                if not self.handle_api_error(e, "USDA", attempt, self.max_retries):
                    return pd.DataFrame()
        
        self.logger.error("Failed to fetch USDA data after all retries")
        return pd.DataFrame()
    
    def _download_usda_file(self, page_url: str, link_text: str) -> Optional[str]:
        """
        Download USDA Excel file using web scraping.
        
        Args:
            page_url: URL of the USDA page
            link_text: Text to identify download link
            
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
            
            # Navigate to USDA page
            self.logger.info(f"Navigating to USDA page: {page_url}")
            driver.get(page_url)
            
            # Use utility method to wait and check access
            if not self.web_scraper.wait_for_page_load(driver, timeout=10):
                self.logger.error("Page failed to load properly")
                return None
            
            # Check for access denied
            if "Access Denied" in driver.page_source or "Forbidden" in driver.page_source:
                self.logger.error("Access denied to USDA website")
                return None
            
            # Find download link
            download_url = self._find_usda_download_link(driver, link_text)
            
            if not download_url:
                self.logger.error("Could not find USDA download link")
                return None
            
            # Download file directly using requests
            downloaded_file = self._download_file_direct(download_url)
            return downloaded_file
            
        except Exception as e:
            self.logger.error(f"Error downloading USDA file: {str(e)}")
            return None
            
        finally:
            if driver:
                driver.quit()
    
    def _find_usda_download_link(self, driver, link_text: str) -> Optional[str]:
        """
        Find the USDA download link.
        
        Args:
            driver: Selenium WebDriver instance
            link_text: Text to search for in links
            
        Returns:
            Download URL or None if not found
        """
        try:
            self.logger.info("Searching for USDA download links...")
            all_links = driver.find_elements(By.TAG_NAME, "a")
            
            self.logger.info(f"Found {len(all_links)} links. Searching for '{link_text}'")
            
            for link_element in all_links:
                try:
                    link_text_content = link_element.text.strip()
                    href_content = link_element.get_attribute("href")
                    
                    if link_text.lower() in link_text_content.lower():
                        if href_content and (href_content.endswith(".xlsx") or href_content.endswith(".xls")):
                            self.logger.info(f"Found matching download link: {href_content}")
                            return href_content
                except:
                    continue
            
            self.logger.error(f"Could not find download link containing '{link_text}'")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding download link: {str(e)}")
            return None
    
    def _download_file_direct(self, download_url: str) -> Optional[str]:
        """
        Download file directly using requests.
        
        Args:
            download_url: URL to download from
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            self.logger.info(f"Downloading file from: {download_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
            }
            
            response = requests.get(download_url, headers=headers, stream=True, timeout=60)
            response.raise_for_status()
            
            # Get filename from URL
            parsed_url = urlparse(download_url)
            filename = os.path.basename(unquote(parsed_url.path))
            
            if not filename or not (filename.endswith(".xlsx") or filename.endswith(".xls")):
                filename = "usda_downloaded_data.xlsx"
            
            # Save file
            file_path = os.path.join(self.download_dir, filename)
            
            # Ensure download directory exists
            os.makedirs(self.download_dir, exist_ok=True)
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.logger.info(f"File downloaded successfully to: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error downloading file: {str(e)}")
            return None
    
    def _process_usda_excel(self, file_path: str, sheet_name: str, 
                           metric_pattern: str, symbol_name: str) -> pd.DataFrame:
        """
        Process USDA Excel file using ExcelProcessingUtils and custom logic.
        
        Args:
            file_path: Path to downloaded Excel file
            sheet_name: Sheet name to process
            metric_pattern: Regex pattern for target metric
            symbol_name: Symbol name for output
            
        Returns:
            Processed DataFrame
        """
        try:
            self.logger.info(f"Processing Excel file: {file_path}")
            
            # Read Excel file using utility class
            df_full = self.excel_processor.read_excel_file(
                file_path=file_path,
                sheet_name=sheet_name,
                skip_rows=0,
                header=None
            )
            
            if df_full.empty:
                self.logger.error("Excel file is empty or could not be read")
                return pd.DataFrame()
            
            self.logger.info(f"Full DataFrame shape: {df_full.shape}")
            
            # Find year header row using USDA-specific logic
            year_header_info = self._find_year_header_row(df_full)
            if not year_header_info:
                return pd.DataFrame()
            
            year_header_row_index, year_data_start_col_index = year_header_info
            
            # Extract and parse years
            parsed_years = self._extract_years(df_full, year_header_row_index, year_data_start_col_index)
            if parsed_years.empty:
                return pd.DataFrame()
            
            # Find target metric row
            metric_row_info = self._find_metric_row(df_full, metric_pattern)
            if not metric_row_info:
                return pd.DataFrame()
            
            target_metric_row_index, actual_metric_name = metric_row_info
            
            # Extract and process metric values
            result_df = self._extract_metric_values(
                df_full, target_metric_row_index, parsed_years, 
                symbol_name, actual_metric_name
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error processing USDA Excel file: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def _find_year_header_row(self, df_full: pd.DataFrame) -> Optional[tuple]:
        """Find the year header row in the USDA data."""
        def is_potential_year_header(val):
            if pd.isna(val): 
                return False
            s_val = str(val).strip()
            return bool(re.fullmatch(r'(19\d{2}|20\d{2})[A-Z]?\b', s_val))
        
        for i, row_series in df_full.iterrows():
            potential_year_cells_count = 0
            first_year_col_in_row = -1
            
            for col_idx, cell_val in enumerate(row_series.iloc[1:]):
                if is_potential_year_header(cell_val):
                    potential_year_cells_count += 1
                    if first_year_col_in_row == -1:
                        first_year_col_in_row = col_idx + 1
            
            if potential_year_cells_count > 3:
                self.logger.info(f"Found year header row at index: {i}, data starts at column: {first_year_col_in_row}")
                return (i, first_year_col_in_row)
        
        self.logger.error("Could not find year header row")
        return None
    
    def _extract_years(self, df_full: pd.DataFrame, year_header_row_index: int, 
                      year_data_start_col_index: int) -> pd.Series:
        """Extract and parse years from the header row."""
        year_header_values_raw = df_full.iloc[year_header_row_index, year_data_start_col_index:]
        
        def extract_year_int(value):
            if pd.isna(value): 
                return pd.NA
            s_value = str(value).strip()
            if 'change' in s_value.lower():
                return pd.NA
            
            match = re.search(r'\b(19\d{2}|20\d{2})[A-Z]?\b', s_value)
            if match:
                return int(match.group(1))
            return pd.NA
        
        parsed_years_series = year_header_values_raw.apply(extract_year_int).dropna().astype('Int64')
        
        if parsed_years_series.empty:
            self.logger.error("No valid years found in header row")
            return pd.Series()
        
        self.logger.info(f"Parsed {len(parsed_years_series)} years: {parsed_years_series.tolist()}")
        return parsed_years_series
    
    def _find_metric_row(self, df_full: pd.DataFrame, metric_pattern: str) -> Optional[tuple]:
        """Find the target metric row."""
        for i, row_series in df_full.iterrows():
            if row_series.empty or len(row_series) == 0:
                continue
                
            first_cell_val_str = str(row_series.iloc[0]).strip()
            
            is_target_metric_base = re.search(metric_pattern, first_cell_val_str, re.IGNORECASE)
            if is_target_metric_base:
                # Special handling for "net farm income" to avoid "net cash farm income"
                if "net farm income" in metric_pattern.lower() and \
                   "cash" not in metric_pattern.lower() and \
                   "cash" in first_cell_val_str.lower():
                    continue
                
                self.logger.info(f"Found target metric '{first_cell_val_str}' at row {i}")
                return (i, first_cell_val_str)
        
        self.logger.error(f"Could not find metric row for pattern: '{metric_pattern}'")
        return None
    
    def _extract_metric_values(self, df_full: pd.DataFrame, target_metric_row_index: int,
                              parsed_years: pd.Series, symbol_name: str, 
                              actual_metric_name: str) -> pd.DataFrame:
        """Extract and process metric values."""
        metric_values_raw = df_full.iloc[target_metric_row_index, parsed_years.index]
        metric_values_numeric = pd.to_numeric(metric_values_raw, errors='coerce')
        
        self.logger.info(f"Extracted raw values for '{actual_metric_name}': {metric_values_raw.tolist()}")
        
        final_dates = pd.to_datetime(parsed_years.astype(str) + "-01-01", errors='coerce').dt.date
        valid_data_mask = pd.notna(final_dates) & pd.notna(metric_values_numeric.values)
        
        if not valid_data_mask.any():
            self.logger.error("No valid date/value pairs found")
            return pd.DataFrame()
        
        result_df = pd.DataFrame({
            'date': final_dates[valid_data_mask],
            'symbol': symbol_name,
            'metric': 'value',
            'value': metric_values_numeric.values[valid_data_mask]
        })
        
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
        
        self.logger.info(f"Final USDA data: {len(result_df)} rows")
        return result_df


# Legacy function wrappers for backward compatibility
def get_usda_ers_data(
    usda_page_url: str = "https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/data-files-us-and-state-level-farm-income-and-wealth-statistics/",
    target_link_text_in_file: str = "U.S. farm sector financial indicators",
    target_metric_text_in_file: str = r'net\s+farm\s+income',
    sheet_name_in_file: str = 'Sheet1',
    download_dir: str = "data/usda",
    symbol_name: str = "USDA_NET_FARM_INCOME",
    headless: bool = True
) -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use USDAFetcher.get_usda_ers_data() instead.
    """
    fetcher = USDAFetcher(download_dir)
    return fetcher.get_usda_ers_data(
        usda_page_url=usda_page_url,
        target_link_text=target_link_text_in_file,
        target_metric_pattern=target_metric_text_in_file,
        sheet_name=sheet_name_in_file,
        symbol_name=symbol_name
    )


def fetch_usda() -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use USDAFetcher.fetch_batch() instead.
    """
    fetcher = USDAFetcher()
    return fetcher.fetch_batch()


if __name__ == "__main__":
    # Test the refactored fetcher
    fetcher = USDAFetcher()
    
    fetcher.logger.info("Testing refactored USDA fetcher...")
    test_data = fetcher.fetch_batch()
    
    print(f"Test data shape: {test_data.shape}")
    if not test_data.empty:
        print(test_data.head())
        print(f"Unique symbols: {test_data['symbol'].unique()}")
    else:
        print("No data retrieved") 