"""
FRED (Federal Reserve Economic Data) Fetcher

This module provides functionality to fetch economic data from the FRED API
and return it in a standardized format for the data pipeline.

Refactored to use BaseDataFetcher for common functionality.
"""

import pandas as pd
from fredapi import Fred
from datetime import datetime
import time
import threading
from typing import List
from ..core.base_fetcher import BaseDataFetcher


class FREDRateLimiter:
    """
    Rate limiter for FRED API to respect the 120 requests per minute limit.
    """
    def __init__(self, max_requests: int = 120, time_window: int = 60, logger=None):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = threading.Lock()
        self.logger = logger
    
    def wait_if_needed(self):
        """
        Wait if necessary to respect rate limits.
        """
        with self.lock:
            now = datetime.now()
            
            # Remove requests older than the time window
            self.requests = [req_time for req_time in self.requests 
                           if (now - req_time).total_seconds() < self.time_window]
            
            # If we're at the limit, wait until we can make another request
            if len(self.requests) >= self.max_requests:
                oldest_request = min(self.requests)
                wait_time = self.time_window - (now - oldest_request).total_seconds()
                if wait_time > 0:
                    if self.logger:
                        self.logger.info(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                    else:
                        print(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                    time.sleep(wait_time + 0.1)  # Add small buffer
                    # Clean up old requests after waiting
                    now = datetime.now()
                    self.requests = [req_time for req_time in self.requests 
                                   if (now - req_time).total_seconds() < self.time_window]
            
            # Record this request
            self.requests.append(now)


class FREDFetcher(BaseDataFetcher):
    """
    FRED data fetcher using BaseDataFetcher infrastructure.
    """
    
    def __init__(self):
        super().__init__("fred")
        self.fred_api = None
        self.rate_limiter = FREDRateLimiter(logger=self.logger)
        self._initialize_api()
    
    def _initialize_api(self):
        """Initialize FRED API client."""
        api_key = self.load_environment_variable('FRED_API_KEY', required=True)
        self.fred_api = Fred(api_key=api_key)
        self.logger.info("FRED API client initialized")
    
    def get_single_series(self, series_id: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        Fetch a single FRED time series.
        
        Args:
            series_id: FRED series ID
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            Standardized DataFrame with columns: date, series_id, value
        """
        start_str, end_str = self.format_date_range(start_date, end_date)
        
        for attempt in range(self.max_retries):
            try:
                # Apply rate limiting before making the request
                self.rate_limiter.wait_if_needed()
                
                # Make the API request
                data = self.fred_api.get_series(series_id, start_date, end_date)
                
                if data is None or data.empty:
                    self.logger.warning(f"No data returned for FRED series {series_id}")
                    return pd.DataFrame()
                
                # Convert to DataFrame
                df = pd.DataFrame(data, columns=['value'])
                
                # Standardize using base class method
                standardized_df = self.standardize_dataframe(
                    df=df,
                    expected_columns=['date', 'series_id', 'value'],
                    identifier_column='series_id',
                    identifier_value=series_id
                )
                
                self.logger.info(f"Successfully fetched {len(standardized_df)} rows for FRED series {series_id}")
                return standardized_df
                
            except Exception as e:
                if not self.handle_api_error(e, series_id, attempt, self.max_retries):
                    # Don't retry for certain errors
                    return pd.DataFrame()
        
        # If we get here, all retries failed
        raise Exception(f"Failed to fetch FRED data for {series_id} after {self.max_retries} attempts")
    
    def fetch_batch(self, symbols_df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
        """
        Fetch FRED data for all symbols in the provided DataFrame.
        
        Args:
            symbols_df: DataFrame with symbol information
            batch_size: Batch size for progress reporting
            
        Returns:
            Combined DataFrame with all FRED data
        """
        self.logger.info("Starting FRED data collection with rate limiting")
        
        # Filter for FRED symbols
        fred_symbols = symbols_df[symbols_df['string.source'].str.lower() == 'fred'].copy()
        
        if fred_symbols.empty:
            self.logger.warning("No FRED symbols found in symbols DataFrame")
            return pd.DataFrame()
        
        total_symbols = len(fred_symbols)
        self.logger.info(f"Found {total_symbols} FRED symbols to process")
        
        # Estimate time based on rate limit
        estimated_time_minutes = (total_symbols / 120) + 1
        self.logger.info(f"Estimated completion time: {estimated_time_minutes:.1f} minutes")
        
        all_data = []
        end_date = datetime.now()
        successful_fetches = 0
        failed_fetches = 0
        
        start_time = datetime.now()
        
        for idx, (_, row) in enumerate(fred_symbols.iterrows(), 1):
            series_id = row['string.symbol']
            start_date_str = row['date.series.start']
            
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                
                # Progress logging
                if idx % batch_size == 0 or idx == total_symbols:
                    elapsed_time = datetime.now() - start_time
                    avg_time_per_request = elapsed_time.total_seconds() / idx
                    remaining_requests = total_symbols - idx
                    estimated_remaining_time = remaining_requests * avg_time_per_request / 60
                    
                    self.logger.info(
                        f"Progress: {idx}/{total_symbols} ({idx/total_symbols*100:.1f}%) - "
                        f"Successful: {successful_fetches}, Failed: {failed_fetches} - "
                        f"Est. remaining: {estimated_remaining_time:.1f} minutes"
                    )
                
                data = self.get_single_series(series_id, start_date, end_date)
                
                if not data.empty:
                    all_data.append(data)
                    successful_fetches += 1
                else:
                    failed_fetches += 1
                    
            except Exception as e:
                self.logger.error(f"Error processing FRED series {series_id}: {str(e)}")
                failed_fetches += 1
                continue
        
        # Use base class summary logging
        return self.log_collection_summary(all_data, total_symbols)


# Legacy function wrappers for backward compatibility
def get_fred_data(series_id: str, start_date: datetime = datetime(1900, 1, 1), 
                 end_date: datetime = datetime.now()) -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use FREDFetcher.get_single_series() instead.
    """
    fetcher = FREDFetcher()
    return fetcher.get_single_series(series_id, start_date, end_date)


def fetch_fred(symbols_df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use FREDFetcher.fetch_batch() instead.
    """
    fetcher = FREDFetcher()
    return fetcher.fetch_batch(symbols_df, batch_size)


def fetch_fred_batch(series_list: List[str], start_date: datetime = datetime(1990, 1, 1), 
                    end_date: datetime = datetime.now()) -> pd.DataFrame:
    """
    Fetch FRED data for a list of series IDs with rate limiting.
    
    Args:
        series_list: List of FRED series IDs to fetch
        start_date: Start date for data
        end_date: End date for data
        
    Returns:
        Combined DataFrame with all FRED data
    """
    fetcher = FREDFetcher()
    
    # Convert series list to DataFrame format expected by fetch_batch
    symbols_df = pd.DataFrame({
        'string.symbol': series_list,
        'string.source': 'fred',
        'date.series.start': [start_date.strftime('%Y-%m-%d')] * len(series_list)
    })
    
    return fetcher.fetch_batch(symbols_df)


if __name__ == "__main__":
    # Test the refactored fetcher
    fetcher = FREDFetcher()
    
    test_series = "CPIAUCSL"  # Consumer Price Index
    fetcher.logger.info("Testing refactored FRED fetcher...")
    
    test_data = fetcher.get_single_series(test_series, datetime(2023, 1, 1), datetime(2023, 12, 31))
    print(f"Test data shape: {test_data.shape}")
    print(test_data.head())
    
    # Test batch functionality
    test_batch = ["CPIAUCSL", "UNRATE", "GDP"]
    fetcher.logger.info("Testing batch FRED fetcher...")
    batch_data = fetch_fred_batch(test_batch, datetime(2023, 1, 1), datetime(2023, 12, 31))
    print(f"Batch test data shape: {batch_data.shape}")
    print(f"Unique symbols in batch: {batch_data['series_id'].unique().tolist()}") 