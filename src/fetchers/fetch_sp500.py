"""
S&P 500 Earnings and Estimates Data Fetcher (Silverblatt)

This module provides functionality to fetch S&P 500 earnings and estimates data
and return it in a standardized format for the data pipeline.

Refactored to use BaseDataFetcher and utility classes to eliminate code duplication.
"""

import pandas as pd
import os
from datetime import datetime
from typing import Optional
import re

from ..core.base_fetcher import BaseDataFetcher
from ..utils.web_scraping_utils import WebScrapingUtils
from ..utils.file_download_utils import FileDownloadUtils
from ..utils.excel_processing_utils import ExcelProcessingUtils
from ..utils.transform_utils import DataTransformUtils


class SP500Fetcher(BaseDataFetcher):
    """
    S&P 500 earnings and estimates data fetcher using BaseDataFetcher infrastructure.
    
    Combines web scraping, file download, Excel processing, and data transformation
    utilities to fetch and standardize S&P 500 Silverblatt data.
    """
    
    def __init__(self, download_dir: str = "data/sp500"):
        """
        Initialize S&P 500 fetcher.
        
        Args:
            download_dir: Directory for downloaded files
        """
        super().__init__("sp500")
        
        # Initialize utility classes
        self.web_scraper = WebScrapingUtils()
        self.file_downloader = FileDownloadUtils(download_dir)
        self.excel_processor = ExcelProcessingUtils()
        self.data_transformer = DataTransformUtils()
        
        self.download_dir = download_dir
        
        # S&P 500 specific configuration
        self.sp500_url = "https://www.spglobal.com/spdji/en/documents/additional-material/sp-500-eps-est.xlsx"
        self.referer_url = "https://www.spglobal.com/spdji/en/indices/equity/sp-500/"
        self.filename = "sp-500-eps-est.xlsx"
        
        self.logger.info("S&P 500 fetcher initialized")
    
    def get_single_series(self, series_id: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        S&P 500 doesn't use individual series - this method calls fetch_batch.
        
        Args:
            series_id: Not used for S&P 500 (fetches all earnings data)
            start_date: Not used (gets latest available data)
            end_date: Not used (gets latest available data)
            
        Returns:
            DataFrame with all S&P 500 earnings data
        """
        self.logger.info("S&P 500 fetches all data at once, delegating to fetch_batch")
        # Create a dummy symbols_df for compatibility
        dummy_symbols_df = pd.DataFrame({
            'string.symbol': ['SP500_SILVERBLATT'],
            'string.source': ['sp500'],
            'date.series.start': [start_date.strftime('%Y-%m-%d')]
        })
        return self.fetch_batch(dummy_symbols_df)
    
    def fetch_batch(self, symbols_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Fetch S&P 500 earnings and estimates data.
        
        Args:
            symbols_df: Not used for S&P 500 (optional for compatibility)
            
        Returns:
            DataFrame with standardized S&P 500 data
        """
        self.logger.info("Starting S&P 500 earnings data collection")
        
        for attempt in range(self.max_retries):
            try:
                # Step 1: Download the Excel file
                downloaded_file = self._download_sp500_file()
                
                if not downloaded_file:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Download failed, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        continue
                    else:
                        self.logger.error("Failed to download S&P 500 file after all retries")
                        return pd.DataFrame()
                
                # Step 2: Process the Excel file
                combined_data = self._process_sp500_excel(downloaded_file)
                
                if combined_data.empty:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Excel processing failed, retrying... (Attempt {attempt + 1}/{self.max_retries})")
                        continue
                    else:
                        self.logger.error("Failed to process S&P 500 Excel file")
                        return pd.DataFrame()
                
                self.logger.info(f"S&P 500 data collection completed. Total rows: {len(combined_data)}")
                return combined_data
                
            except Exception as e:
                if not self.handle_api_error(e, "S&P 500", attempt, self.max_retries):
                    return pd.DataFrame()
        
        self.logger.error("Failed to fetch S&P 500 data after all retries")
        return pd.DataFrame()
    
    def _download_sp500_file(self) -> Optional[str]:
        """
        Download S&P 500 Excel file using web scraping to establish session.
        
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
            
            # First, visit the referer page to establish session
            self.logger.info(f"Visiting referer page: {self.referer_url}")
            driver.get(self.referer_url)
            
            # Use utility method to wait for page load
            if not self.web_scraper.wait_for_page_load(driver, timeout=10):
                self.logger.warning("Referer page load incomplete, continuing...")
            
            # Now navigate to the download URL
            self.logger.info(f"Downloading Excel file from: {self.sp500_url}")
            driver.get(self.sp500_url)
            
            # Wait for download completion using utility method
            downloaded_file = self.file_downloader.wait_for_download_completion(
                timeout=15,
                file_extension=".xlsx"
            )
            
            return downloaded_file
            
        except Exception as e:
            self.logger.error(f"Error downloading S&P 500 file: {str(e)}")
            return None
            
        finally:
            if driver:
                driver.quit()
    
    def _process_sp500_excel(self, file_path: str) -> pd.DataFrame:
        """
        Process S&P 500 Excel file using ExcelProcessingUtils.
        
        Args:
            file_path: Path to downloaded Excel file
            
        Returns:
            Combined and processed DataFrame
        """
        try:
            self.logger.info(f"Processing Excel file: {file_path}")
            
            # Process quarterly data
            quarterly_data = self._process_quarterly_sheet(file_path)
            
            # Process estimates data
            estimates_data = self._process_estimates_sheet(file_path)
            
            # Combine both datasets
            combined_data = self._combine_and_finalize_data(quarterly_data, estimates_data)
            
            return combined_data
            
        except Exception as e:
            self.logger.error(f"Error processing S&P 500 Excel file: {str(e)}")
            return pd.DataFrame()
    
    def _process_quarterly_sheet(self, file_path: str) -> pd.DataFrame:
        """Process the quarterly data sheet."""
        try:
            # Column names for quarterly data
            column_names = [
                "date", "op_earnings_per_share", "ar_earnings_per_share",
                "cash_dividends_per_share", "sales_per_share", "book_value_per_share",
                "capex_per_share", "price", "divisor"
            ]
            
            # Read quarterly data using utility class
            df_quarterly = self.excel_processor.read_excel_file(
                file_path=file_path,
                sheet_name="QUARTERLY DATA",
                skip_rows=5,
                column_names=column_names
            )
            
            if df_quarterly.empty:
                self.logger.warning("Quarterly data sheet is empty")
                return pd.DataFrame()
            
            # Transform using utility method
            df_quarterly_melted = self.data_transformer.melt_to_long_format(
                df=df_quarterly,
                id_vars=['date'],
                value_vars=[
                    'op_earnings_per_share', 'ar_earnings_per_share',
                    'cash_dividends_per_share', 'sales_per_share',
                    'book_value_per_share', 'capex_per_share', 'price', 'divisor'
                ],
                var_name='metric',
                value_name='value'
            )
            
            # Convert values to numeric using utility method
            df_quarterly_melted = self.data_transformer.standardize_numeric_columns(
                df_quarterly_melted, 
                ['value']
            )
            
            # Convert date column
            df_quarterly_melted['date'] = pd.to_datetime(df_quarterly_melted['date']).dt.date
            
            self.logger.info(f"Processed quarterly data: {len(df_quarterly_melted)} rows")
            return df_quarterly_melted
            
        except Exception as e:
            self.logger.error(f"Error processing quarterly sheet: {str(e)}")
            return pd.DataFrame()
    
    def _process_estimates_sheet(self, file_path: str) -> pd.DataFrame:
        """Process the estimates & PEs sheet."""
        try:
            # Read raw estimates data
            df_estimates_raw = self.excel_processor.read_excel_file(
                file_path=file_path,
                sheet_name="ESTIMATES&PEs",
                skip_rows=0
            )
            
            if df_estimates_raw.empty:
                self.logger.warning("Estimates sheet is empty")
                return pd.DataFrame()
            
            # Find the ACTUALS row
            actuals_row = df_estimates_raw[df_estimates_raw.iloc[:, 0] == "ACTUALS"].index[0]
            self.logger.info(f"ACTUALS row index: {actuals_row}")
            
            # Get data after ACTUALS row
            df_estimates = df_estimates_raw.iloc[actuals_row + 1:].copy().reset_index(drop=True)
            
            # Filter valid date rows
            valid_date_mask = df_estimates.iloc[:, 0].apply(self._is_valid_date)
            df_estimates = df_estimates[valid_date_mask].copy().reset_index(drop=True)
            
            # Convert first column to date
            df_estimates.iloc[:, 0] = pd.to_datetime(
                df_estimates.iloc[:, 0].astype(str).str.strip(), 
                format='mixed'
            ).dt.date
            
            # Remove empty columns
            df_estimates = df_estimates.dropna(axis=1, how='all')
            
            # Define column names
            estimates_column_names = [
                'date', 'sp500_price', 'op_earnings_per_share', 'ar_earnings_per_share',
                'op_earnings_pe', 'ar_earnings_pe', 'op_earnings_ttm', 'ar_earnings_ttm'
            ]
            
            # Assign column names
            num_cols = len(df_estimates.columns)
            df_estimates.columns = estimates_column_names[:num_cols]
            
            # Transform using utility method
            df_estimates_melted = self.data_transformer.melt_to_long_format(
                df=df_estimates,
                id_vars=['date'],
                value_vars=[col for col in df_estimates.columns if col != 'date'],
                var_name='metric',
                value_name='value'
            )
            
            # Convert values to numeric using utility method
            df_estimates_melted = self.data_transformer.standardize_numeric_columns(
                df_estimates_melted,
                ['value']
            )
            
            self.logger.info(f"Processed estimates data: {len(df_estimates_melted)} rows")
            return df_estimates_melted
            
        except Exception as e:
            self.logger.error(f"Error processing estimates sheet: {str(e)}")
            return pd.DataFrame()
    
    def _combine_and_finalize_data(self, quarterly_data: pd.DataFrame, 
                                  estimates_data: pd.DataFrame) -> pd.DataFrame:
        """Combine and finalize the S&P 500 data."""
        try:
            # Combine datasets
            df_combined = pd.concat([quarterly_data, estimates_data], ignore_index=True)
            
            if df_combined.empty:
                self.logger.error("Combined dataset is empty")
                return pd.DataFrame()
            
            # Final numeric conversion using utility method
            df_combined = self.data_transformer.standardize_numeric_columns(df_combined, ['value'])
            
            # Clean data using utility method
            df_combined = self.data_transformer.clean_and_validate_data(
                df_combined,
                required_columns=['date', 'value']
            )
            
            # Sort by date descending
            df_combined = df_combined.sort_values('date', ascending=False)
            
            # Filter to keep only desired symbols
            keep_symbols = [
                'op_earnings_per_share', 'ar_earnings_per_share', 'cash_dividends_per_share',
                'sales_per_share', 'book_value_per_share', 'capex_per_share',
                'op_earnings_pe', 'ar_earnings_pe', 'op_earnings_ttm', 'ar_earnings_ttm'
            ]
            
            df_combined = df_combined[df_combined['metric'].isin(keep_symbols)]
            
            # Add symbol column with SILVERBLATT_ prefix
            df_combined['symbol'] = 'SILVERBLATT_' + df_combined['metric']
            
            # Standardize column order using utility method
            df_combined = self.data_transformer.standardize_column_order(
                df_combined,
                expected_order=['date', 'symbol', 'metric', 'value']
            )
            
            self.logger.info(f"Final S&P 500 data: {len(df_combined)} rows")
            self.logger.info(f"Unique symbols: {len(df_combined['symbol'].unique())}")
            
            return df_combined
            
        except Exception as e:
            self.logger.error(f"Error combining and finalizing data: {str(e)}")
            return pd.DataFrame()
    
    def _is_valid_date(self, date_str) -> bool:
        """Check if a string can be converted to datetime."""
        try:
            if pd.isna(date_str):
                return False
            pd.to_datetime(str(date_str).strip(), format='mixed')
            return True
        except:
            return False


# Legacy function wrappers for backward compatibility
def get_sp500_data(
    sp500_url: str = "https://www.spglobal.com/spdji/en/documents/additional-material/sp-500-eps-est.xlsx",
    referer_url: str = "https://www.spglobal.com/spdji/en/indices/equity/sp-500/",
    download_dir: str = "data/sp500"
) -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use SP500Fetcher.fetch_batch() instead.
    """
    fetcher = SP500Fetcher(download_dir)
    return fetcher.fetch_batch()


def fetch_sp500() -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use SP500Fetcher.fetch_batch() instead.
    """
    fetcher = SP500Fetcher()
    return fetcher.fetch_batch()


if __name__ == "__main__":
    # Test the refactored fetcher
    fetcher = SP500Fetcher()
    
    fetcher.logger.info("Testing refactored S&P 500 fetcher...")
    test_data = fetcher.fetch_batch()
    
    print(f"Test data shape: {test_data.shape}")
    if not test_data.empty:
        print(f"Value column dtype: {test_data['value'].dtype}")
        print(test_data.head())
        print(f"Unique symbols: {sorted(test_data['symbol'].unique())[:5]}")  # Show first 5
    else:
        print("No data retrieved") 