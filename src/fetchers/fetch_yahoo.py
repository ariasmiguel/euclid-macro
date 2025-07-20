"""
Yahoo Finance Data Fetcher

This module provides functionality to fetch historical market data from Yahoo Finance
and return it in a standardized format for the data pipeline.

Refactored to use BaseDataFetcher for common functionality.
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import List, Optional
from ..core.base_fetcher import BaseDataFetcher


class YahooFetcher(BaseDataFetcher):
    """
    Yahoo Finance data fetcher using BaseDataFetcher infrastructure.
    
    Fetches OHLCV (Open, High, Low, Close, Volume) data from Yahoo Finance
    and returns it in a standardized wide format.
    """
    
    def __init__(self):
        super().__init__("yahoo")
        # Yahoo Finance doesn't require API keys
        self.logger.info("Yahoo Finance fetcher initialized")
    
    def _standardize_yahoo_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Standardize Yahoo Finance DataFrame with OHLCV columns.
        
        Args:
            df: Raw DataFrame from yfinance
            symbol: Stock symbol
            
        Returns:
            Standardized DataFrame with columns: date, symbol, open, high, low, close, volume
        """
        if df.empty:
            return df
        
        # Check if we have a MultiIndex in columns and flatten if needed
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        # Reset the index to make the date a column
        df = df.reset_index()
        
        # Rename the columns to match the desired structure
        column_mapping = {
            'Date': 'date',
            'Open': 'open', 
            'High': 'high', 
            'Low': 'low', 
            'Close': 'close', 
            'Volume': 'volume'
        }
        
        # Only rename columns that exist
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)
        
        # Convert date to date format (using our date_utils pattern)
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Add the symbol column
        df['symbol'] = symbol
        
        # Reorder columns to put symbol after date
        expected_columns = ['date', 'symbol']
        value_columns = [col for col in df.columns if col not in ['date', 'symbol']]
        column_order = expected_columns + value_columns
        
        # Ensure all expected columns exist
        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]
        
        # Remove any rows with all NaN values (except date and symbol)
        if value_columns:
            df = df.dropna(subset=[col for col in value_columns if col in df.columns], how='all')
        
        return df
    
    def get_single_series(self, symbol: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        Fetch historical market data for a single symbol from Yahoo Finance.
        
        Args:
            symbol: Stock ticker symbol
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            Standardized DataFrame with OHLCV data
        """
        start_str, end_str = self.format_date_range(start_date, end_date)
        
        for attempt in range(self.max_retries):
            try:
                # Download the data using yfinance
                df = yf.download(symbol, start=start_str, end=end_str)
                
                if df.empty:
                    self.logger.warning(f"No data returned for Yahoo symbol {symbol}")
                    return pd.DataFrame()
                
                # Standardize the DataFrame
                standardized_df = self._standardize_yahoo_dataframe(df, symbol)
                
                self.logger.info(f"Successfully fetched {len(standardized_df)} rows for Yahoo symbol {symbol}")
                return standardized_df
                
            except Exception as e:
                if not self.handle_api_error(e, symbol, attempt, self.max_retries):
                    # Don't retry for certain errors
                    return pd.DataFrame()
        
        # If we get here, all retries failed
        raise Exception(f"Failed to fetch Yahoo data for {symbol} after {self.max_retries} attempts")
    
    def fetch_batch(self, symbols_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetch Yahoo Finance data for all symbols in the provided DataFrame.
        
        Args:
            symbols_df: DataFrame with symbol information
            
        Returns:
            Combined DataFrame with all Yahoo Finance data
        """
        self.logger.info("Starting Yahoo Finance data collection")
        
        # Filter for Yahoo Finance symbols
        yahoo_symbols = symbols_df[symbols_df['string.source'].str.lower() == 'yahoo'].copy()
        
        if yahoo_symbols.empty:
            self.logger.warning("No Yahoo Finance symbols found in symbols DataFrame")
            return pd.DataFrame()
        
        total_symbols = len(yahoo_symbols)
        self.logger.info(f"Found {total_symbols} Yahoo symbols to process")
        
        all_data = []
        end_date = datetime.now()
        successful_fetches = 0
        failed_fetches = 0
        
        for idx, (_, row) in enumerate(yahoo_symbols.iterrows(), 1):
            symbol = row['string.symbol']
            start_date_str = row['date.series.start']
            
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                
                # Progress logging every 10 symbols (Yahoo is faster than APIs)
                if idx % 10 == 0 or idx == total_symbols:
                    self.logger.info(f"Progress: {idx}/{total_symbols} ({idx/total_symbols*100:.1f}%) - "
                                   f"Successful: {successful_fetches}, Failed: {failed_fetches}")
                
                data = self.get_single_series(symbol, start_date, end_date)
                
                if not data.empty:
                    all_data.append(data)
                    successful_fetches += 1
                else:
                    failed_fetches += 1
                    
            except Exception as e:
                self.logger.error(f"Error processing Yahoo symbol {symbol}: {str(e)}")
                failed_fetches += 1
                continue
        
        # Use base class summary logging
        return self.log_collection_summary(all_data, total_symbols)


# Legacy function wrappers for backward compatibility
def get_yahoo_data(symbol: str, start_date: datetime = datetime(1990, 1, 1), 
                  end_date: datetime = datetime.now()) -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use YahooFetcher.get_single_series() instead.
    """
    fetcher = YahooFetcher()
    return fetcher.get_single_series(symbol, start_date, end_date)


def fetch_yahoo(symbols_df: pd.DataFrame) -> pd.DataFrame:
    """
    Legacy wrapper for backward compatibility.
    
    DEPRECATED: Use YahooFetcher.fetch_batch() instead.
    """
    fetcher = YahooFetcher()
    return fetcher.fetch_batch(symbols_df)


if __name__ == "__main__":
    # Test the refactored fetcher
    fetcher = YahooFetcher()
    
    test_symbol = "AAPL"
    fetcher.logger.info("Testing refactored Yahoo Finance fetcher...")
    
    test_data = fetcher.get_single_series(test_symbol, datetime(2023, 1, 1), datetime(2023, 12, 31))
    print(f"Test data shape: {test_data.shape}")
    print(test_data.head())
    
    # Test with multiple symbols
    test_symbols_df = pd.DataFrame({
        'string.symbol': ['AAPL', 'GOOGL', 'MSFT'],
        'string.source': 'yahoo',
        'date.series.start': ['2023-01-01', '2023-01-01', '2023-01-01']
    })
    
    batch_data = fetcher.fetch_batch(test_symbols_df)
    print(f"\nBatch test data shape: {batch_data.shape}")
    print(f"Unique symbols in batch: {batch_data['symbol'].unique().tolist()}")
    print(f"Columns: {batch_data.columns.tolist()}") 