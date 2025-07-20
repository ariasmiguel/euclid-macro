"""
EIA (Energy Information Administration) Data Fetcher - REFACTORED

This module provides functionality to fetch energy data from the EIA API
using the new BaseDataFetcher infrastructure to eliminate code duplication.
"""

import pandas as pd
from datetime import datetime
from typing import List
from myeia import API

from ..core.base_fetcher import BaseDataFetcher
from ..core.logging_setup import get_logger

# Use centralized logging setup
logger = get_logger(__name__)


class EIAFetcher(BaseDataFetcher):
    """
    EIA data fetcher using the BaseDataFetcher infrastructure.
    
    Handles fetching energy data from the EIA API with standardized
    error handling, logging, and data formatting.
    """
    
    def __init__(self):
        """Initialize EIA fetcher with base configuration."""
        super().__init__(source_name="eia")
        
        # Load and validate EIA API token
        self.api_token = self.load_environment_variable("EIA_TOKEN", required=True)
        
        # Initialize EIA API instance
        self.api = API()
    
    def get_single_series(self, series_id: str, start_date: datetime, 
                         end_date: datetime) -> pd.DataFrame:
        """
        Fetch a single EIA time series.
        
        Args:
            series_id: EIA series ID to fetch
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            DataFrame with standardized format [date, series_id, value]
        """
        # Use base class date formatting
        start_str, end_str = self.format_date_range(start_date, end_date)
        
        # Retry logic handled by base class
        for attempt in range(self.max_retries):
            try:
                # Make API call
                df = self.api.get_series(
                    series_id=series_id,
                    start_date=start_str,
                    end_date=end_str
                )
                
                if df is None or df.empty:
                    self.logger.warning(f"No data returned for EIA series {series_id}")
                    return pd.DataFrame()
                
                # EIA-specific column processing (BEFORE standardize_dataframe)
                df = self._process_eia_columns(df, series_id)
                
                # Use base class DataFrame standardization
                df = self.standardize_dataframe(
                    df=df,
                    expected_columns=['date', 'series_id', 'value'],
                    identifier_column='series_id',
                    identifier_value=series_id
                )
                
                return df
                
            except Exception as e:
                # Use base class error handling
                should_retry = self.handle_api_error(e, series_id, attempt, self.max_retries)
                if not should_retry:
                    return pd.DataFrame()
        
        # All retries failed
        self.logger.error(f"Failed to fetch EIA data for {series_id} after {self.max_retries} attempts")
        return pd.DataFrame()
    
    def fetch_batch(self, symbols_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetch EIA data for multiple symbols.
        
        Args:
            symbols_df: DataFrame with symbol information containing:
                       - string.symbol: EIA series ID
                       - string.source: Data source name  
                       - date.series.start: Start date for data
            
        Returns:
            Combined DataFrame with all EIA data
        """
        self.logger.info("Starting EIA data collection")
        
        # Filter for EIA symbols
        eia_symbols = symbols_df[symbols_df['string.source'].str.lower() == 'eia'].copy()
        
        if eia_symbols.empty:
            self.logger.warning("No EIA symbols found")
            return pd.DataFrame()
        
        # Process each symbol
        all_data = []
        end_date = datetime.now()
        
        for idx, row in eia_symbols.iterrows():
            series_id = row['string.symbol']
            start_date_str = row['date.series.start']
            
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                data = self.get_single_series(series_id, start_date, end_date)
                
                if not data.empty:
                    all_data.append(data)
                    
            except Exception as e:
                self.logger.error(f"Error processing EIA series {series_id}: {str(e)}")
                continue
        
        # Use base class collection summary
        return self.log_collection_summary(all_data, len(eia_symbols))
    
    def _process_eia_columns(self, df: pd.DataFrame, series_id: str) -> pd.DataFrame:
        """
        Process EIA-specific column formatting to match expected structure.
        
        Args:
            df: Raw DataFrame from EIA API
            series_id: Series ID for the data
            
        Returns:
            DataFrame with properly formatted columns [date, series_id, value]
        """
        df_processed = df.copy()
        
        # Step 1: Handle index to date conversion (EIA API returns date as index)
        if df_processed.index.name is not None or not isinstance(df_processed.index, pd.RangeIndex):
            df_processed = df_processed.reset_index()
            # Rename the index column to 'date'
            if 'index' in df_processed.columns:
                df_processed = df_processed.rename(columns={'index': 'date'})
        
        # Step 2: Set all columns to lowercase (EIA-specific requirement)
        df_processed.columns = df_processed.columns.str.lower()
        
        # Step 3: Ensure we have the right columns
        if 'date' not in df_processed.columns:
            # If still no date column, try to identify it
            for col in df_processed.columns:
                if any(keyword in str(col).lower() for keyword in ['date', 'time', 'period']):
                    df_processed = df_processed.rename(columns={col: 'date'})
                    break
        
        # Step 4: Handle value column
        if 'value' not in df_processed.columns:
            # Find the data column (usually the first non-date column)
            value_candidates = [col for col in df_processed.columns 
                              if col not in ['date', 'series_id'] 
                              and pd.api.types.is_numeric_dtype(df_processed[col])]
            
            if value_candidates:
                # Use the first numeric column as value
                df_processed = df_processed.rename(columns={value_candidates[0]: 'value'})
            elif len(df_processed.columns) >= 2:
                # Fallback: use second column as value
                non_date_cols = [col for col in df_processed.columns if col != 'date']
                if non_date_cols:
                    df_processed = df_processed.rename(columns={non_date_cols[0]: 'value'})
        
        # Step 5: Add series_id column if not present
        if 'series_id' not in df_processed.columns:
            df_processed['series_id'] = series_id
        
        # Step 6: Debug logging to help troubleshoot
        self.logger.debug(f"EIA columns after processing for {series_id}: {df_processed.columns.tolist()}")
        
        return df_processed


# Backward compatibility functions
def get_eia_data(series_id: str, start_date: datetime = datetime(1900, 1, 1), 
                end_date: datetime = datetime.now()) -> pd.DataFrame:
    """
    Fetch single EIA series - backward compatibility wrapper.
    
    Args:
        series_id: EIA series ID
        start_date: Start date for data
        end_date: End date for data
        
    Returns:
        DataFrame with EIA data
    """
    fetcher = EIAFetcher()
    return fetcher.get_single_series(series_id, start_date, end_date)


def fetch_eia(symbols_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch EIA data for multiple symbols - backward compatibility wrapper.
    
    Args:
        symbols_df: DataFrame with symbol information
        
    Returns:
        Combined DataFrame with all EIA data
    """
    fetcher = EIAFetcher()
    return fetcher.fetch_batch(symbols_df)


if __name__ == "__main__":
    # Test the refactored fetcher
    test_series = "PET.RWTC.D"  # Example EIA series
    test_data = get_eia_data(test_series, datetime(2023, 1, 1), datetime(2023, 12, 31))
    print(f"Test data shape: {test_data.shape}")
    print(test_data.head()) 