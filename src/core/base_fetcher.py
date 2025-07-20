"""
Base Data Fetcher

This module provides a base class for all data fetchers, centralizing common
functionality like logging, error handling, date formatting, and DataFrame
standardization.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import pandas as pd
import os
import logging
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod
from dotenv import load_dotenv


class BaseDataFetcher(ABC):
    """
    Base class for all data fetchers providing common functionality.
    
    This class centralizes:
    - Logging setup and configuration
    - Date formatting for APIs
    - Error handling with retry logic
    - DataFrame standardization
    - Environment variable loading
    """
    
    def __init__(self, source_name: str, logger_name: Optional[str] = None):
        """
        Initialize the base fetcher.
        
        Args:
            source_name: Name of the data source (e.g., 'fred', 'eia', 'yahoo')
            logger_name: Optional custom logger name, defaults to module name
        """
        self.source_name = source_name.upper()
        self.logger = self.setup_logging(logger_name)
        self.max_retries = 3
        self.base_wait_time = 30  # seconds
        
        # Load environment variables once
        load_dotenv()
    
    def setup_logging(self, logger_name: Optional[str] = None) -> logging.Logger:
        """
        Set up standardized logging configuration.
        
        Args:
            logger_name: Optional custom logger name
            
        Returns:
            Configured logger instance
        """
        name = logger_name or f"fetcher.{self.source_name.lower()}"
        logger = logging.getLogger(name)
        
        # Avoid duplicate handlers if logger already configured
        if not logger.handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        return logger
    
    def format_date_range(self, start_date: datetime, end_date: datetime) -> tuple[str, str]:
        """
        Format date range for API calls.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            Tuple of (start_date_str, end_date_str) in YYYY-MM-DD format
        """
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        self.logger.info(f"Fetching {self.source_name} data from {start_str} to {end_str}")
        
        return start_str, end_str
    
    def load_environment_variable(self, var_name: str, required: bool = True) -> Optional[str]:
        """
        Load environment variable with proper error handling.
        
        Args:
            var_name: Name of environment variable
            required: Whether variable is required
            
        Returns:
            Environment variable value or None
            
        Raises:
            ValueError: If required variable is not set
        """
        value = os.getenv(var_name)
        
        if required and not value:
            raise ValueError(f"{var_name} environment variable is not set")
        
        if value:
            self.logger.debug(f"Loaded environment variable: {var_name}")
        else:
            self.logger.warning(f"Optional environment variable not set: {var_name}")
            
        return value
    
    def handle_api_error(self, exception: Exception, identifier: str, 
                        attempt: int, max_retries: int) -> bool:
        """
        Handle API errors with standardized retry logic.
        
        Args:
            exception: The exception that occurred
            identifier: Series ID or symbol being processed
            attempt: Current attempt number (0-based)
            max_retries: Maximum number of retries
            
        Returns:
            True if should retry, False if should stop
        """
        error_msg = str(exception)
        
        # Check for rate limiting
        if any(indicator in error_msg.lower() for indicator in 
               ["too many requests", "429", "rate limit"]):
            if attempt < max_retries - 1:
                wait_time = self.base_wait_time * (2 ** attempt)  # Exponential backoff
                self.logger.warning(
                    f"Rate limit hit for {identifier}. "
                    f"Waiting {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                return True
            else:
                self.logger.error(f"Rate limit exceeded for {identifier} after {max_retries} attempts")
                return False
        
        # Check for client errors (don't retry)
        if any(indicator in error_msg.lower() for indicator in 
               ["bad request", "400", "not found", "404", "unauthorized", "401"]):
            self.logger.error(f"{self.source_name} error for {identifier}: {error_msg}")
            return False
        
        # Server errors or other issues (retry with backoff)
        if attempt < max_retries - 1:
            wait_time = self.base_wait_time * (2 ** attempt)
            self.logger.warning(
                f"Error fetching {identifier}: {error_msg}. "
                f"Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})"
            )
            time.sleep(wait_time)
            return True
        else:
            self.logger.error(f"Failed to fetch {identifier} after {max_retries} attempts: {error_msg}")
            return False
    
    def standardize_dataframe(self, df: pd.DataFrame, expected_columns: List[str], 
                            identifier_column: str, identifier_value: str) -> pd.DataFrame:
        """
        Standardize DataFrame to expected format.
        
        Args:
            df: DataFrame to standardize
            expected_columns: List of expected column names in order
            identifier_column: Name of identifier column (e.g., 'series_id', 'symbol')
            identifier_value: Value for identifier column
            
        Returns:
            Standardized DataFrame
        """
        if df.empty:
            self.logger.warning(f"Empty DataFrame for {identifier_value}")
            return df
        
        # Reset index if needed
        if df.index.name is not None or not isinstance(df.index, pd.RangeIndex):
            df = df.reset_index()
            # Rename index column to 'date' if it contains date-like data
            if 'index' in df.columns:
                df = df.rename(columns={'index': 'date'})
        
        # Add identifier column
        df[identifier_column] = identifier_value
        
        # Ensure expected columns exist
        for col in expected_columns:
            if col not in df.columns and col != identifier_column:
                self.logger.warning(f"Missing expected column '{col}' for {identifier_value}")
        
        # Reorder columns
        available_columns = [col for col in expected_columns if col in df.columns]
        df = df[available_columns]
        
        # Remove rows with NaN values in critical columns (usually 'value')
        value_columns = [col for col in df.columns if col not in ['date', identifier_column]]
        if value_columns:
            initial_rows = len(df)
            df = df.dropna(subset=value_columns, how='all')
            dropped_rows = initial_rows - len(df)
            if dropped_rows > 0:
                self.logger.debug(f"Dropped {dropped_rows} rows with NaN values for {identifier_value}")
        
        self.logger.info(f"Standardized {identifier_value}: {len(df)} rows")
        return df
    
    def log_collection_summary(self, all_data: List[pd.DataFrame], 
                             total_symbols: int) -> pd.DataFrame:
        """
        Log collection summary and combine data.
        
        Args:
            all_data: List of DataFrames collected
            total_symbols: Total number of symbols processed
            
        Returns:
            Combined DataFrame or empty DataFrame if no data
        """
        successful_fetches = len(all_data)
        failed_fetches = total_symbols - successful_fetches
        
        self.logger.info(f"{self.source_name} collection summary:")
        self.logger.info(f"  ✅ Successful: {successful_fetches}")
        self.logger.info(f"  ❌ Failed: {failed_fetches}")
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            total_rows = len(combined_data)
            date_range = ""
            
            if 'date' in combined_data.columns:
                try:
                    dates = pd.to_datetime(combined_data['date'])
                    date_range = f" ({dates.min().strftime('%Y-%m-%d')} to {dates.max().strftime('%Y-%m-%d')})"
                except:
                    pass
            
            self.logger.info(f"  📊 Total rows: {total_rows}{date_range}")
            return combined_data
        else:
            self.logger.warning(f"No {self.source_name} data collected")
            return pd.DataFrame()
    
    @abstractmethod
    def get_single_series(self, identifier: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        Fetch a single time series. Must be implemented by subclasses.
        
        Args:
            identifier: Series identifier (symbol, series_id, etc.)
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            DataFrame with the fetched data
        """
        pass
    
    @abstractmethod
    def fetch_batch(self, symbols_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetch data for multiple symbols. Must be implemented by subclasses.
        
        Args:
            symbols_df: DataFrame with symbol information
            
        Returns:
            Combined DataFrame with all fetched data
        """
        pass 