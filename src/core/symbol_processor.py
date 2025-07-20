"""
Symbol Processor Utility

This module provides utilities for processing and preparing symbol DataFrames
for data fetching operations, centralizing common symbol manipulation patterns.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List


class SymbolProcessor:
    """
    Utility class for symbol DataFrame processing and preparation.
    
    Centralizes:
    - Symbol column renaming and standardization
    - Default date assignment
    - Symbol format validation
    - Source filtering and preparation
    """
    
    def __init__(self, logger_name: Optional[str] = None):
        """
        Initialize symbol processor.
        
        Args:
            logger_name: Optional custom logger name
        """
        self.logger = logging.getLogger(logger_name or "symbol_processor")
    
    def prepare_symbols_for_fetch(self, 
                                 symbols_df: pd.DataFrame,
                                 source_type: str,
                                 default_start_date: str = '1900-01-01') -> pd.DataFrame:
        """
        Prepare symbols DataFrame for fetching operations.
        
        This combines the common pattern of:
        1. Filtering by source
        2. Renaming columns to expected format
        3. Adding default dates if missing
        
        Args:
            symbols_df: Input symbols DataFrame
            source_type: Source to filter for (e.g., 'fred', 'eia', 'yahoo')
            default_start_date: Default start date if not present
            
        Returns:
            Prepared DataFrame ready for fetching
        """
        if symbols_df.empty:
            self.logger.warning("Empty symbols DataFrame provided")
            return pd.DataFrame()
        
        # Step 1: Filter for the specific source
        source_symbols = self._filter_by_source(symbols_df, source_type)
        
        if source_symbols.empty:
            self.logger.warning(f"No symbols found for source: {source_type}")
            return pd.DataFrame()
        
        # Step 2: Rename columns to standard format
        # prepared_symbols = self._standardize_column_names(source_symbols)
        
        # Step 3: Add default dates if missing
        prepared_symbols = self._add_default_dates(source_symbols, default_start_date)
        
        # Step 4: Validate the result
        prepared_symbols = self._validate_symbol_format(prepared_symbols, source_type)
        
        self.logger.info(f"Prepared {len(prepared_symbols)} symbols for {source_type}")
        return prepared_symbols
    
    def _filter_by_source(self, symbols_df: pd.DataFrame, source_type: str) -> pd.DataFrame:
        """
        Filter symbols DataFrame by source type.
        
        Args:
            symbols_df: Input DataFrame
            source_type: Source to filter for
            
        Returns:
            Filtered DataFrame
        """
        source_column_candidates = [
            'source', 'string_source', 'data_source'
        ]
        
        source_column = None
        for candidate in source_column_candidates:
            if candidate in symbols_df.columns:
                source_column = candidate
                break
        
        if not source_column:
            self.logger.error(f"No source column found. Available columns: {symbols_df.columns.tolist()}")
            return pd.DataFrame()
        
        # Filter case-insensitively
        filtered = symbols_df[symbols_df[source_column].str.lower() == source_type.lower()].copy()
        
        self.logger.debug(f"Filtered {len(filtered)} symbols for source '{source_type}'")
        return filtered
    
    # def _standardize_column_names(self, symbols_df: pd.DataFrame) -> pd.DataFrame:
    #    """
    #    Standardize column names to expected format.
    #    
    #    Args:
    #        symbols_df: Input DataFrame
    #            
    #    Returns:
    #        DataFrame with standardized column names
    #    """
    #    df_renamed = symbols_df.copy()
    #    
    #    # Standard column mapping
    #    column_mapping = {
    #        # 'symbol': 'symbol',
    #        'series_id': 'symbol',
    #        # 'source': 'string.source',
    #        # 'Source': 'source',
    #        'data_source': 'source',
    #        'start_date': 'date_series_start',
    #        'series_start': 'date_series_start',
    #        'start': 'date_series_start'
    #    }
    #    
    #    # Apply mapping for columns that exist
    #    for old_col, new_col in column_mapping.items():
    #        if old_col in df_renamed.columns and new_col not in df_renamed.columns:
    #            df_renamed = df_renamed.rename(columns={old_col: new_col})
    #    
    #    self.logger.debug("Standardized column names")
    #    return df_renamed
    
    def _add_default_dates(self, symbols_df: pd.DataFrame, 
                          default_start_date: str) -> pd.DataFrame:
        """
        Add default start date if not present.
        
        Args:
            symbols_df: Input DataFrame
            default_start_date: Default start date string
            
        Returns:
            DataFrame with default dates added
        """
        df_with_dates = symbols_df.copy()
        
        # Add default start date if column doesn't exist
        if 'date_series_start' not in df_with_dates.columns:
            df_with_dates['date_series_start'] = default_start_date
            self.logger.debug(f"Added default start date: {default_start_date}")
        else:
            # Fill missing values with default
            missing_count = df_with_dates['date_series_start'].isna().sum()
            if missing_count > 0:
                df_with_dates['date_series_start'] = df_with_dates['date_series_start'].fillna(default_start_date)
                self.logger.debug(f"Filled {missing_count} missing start dates with: {default_start_date}")
        
        return df_with_dates
    
    def _validate_symbol_format(self, symbols_df: pd.DataFrame, 
                               source_type: str) -> pd.DataFrame:
        """
        Validate symbol DataFrame format.
        
        Args:
            symbols_df: Input DataFrame
            source_type: Source type for context
            
        Returns:
            Validated DataFrame (may be filtered)
        """
        required_columns = ['symbol', 'source', 'date_series_start']
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in symbols_df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns for {source_type}: {missing_columns}")
            return pd.DataFrame()
        
        # Remove rows with missing symbols
        initial_count = len(symbols_df)
        validated_df = symbols_df.dropna(subset=['symbol']).copy()
        dropped_count = initial_count - len(validated_df)
        
        if dropped_count > 0:
            self.logger.warning(f"Dropped {dropped_count} rows with missing symbols")
        
        # Validate date format
        validated_df = self._validate_date_format(validated_df)
        
        return validated_df
    
    def _validate_date_format(self, symbols_df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and standardize date format.
        
        Args:
            symbols_df: Input DataFrame
            
        Returns:
            DataFrame with validated dates
        """
        df_validated = symbols_df.copy()
        
        try:
            # Try to parse dates to validate format
            test_dates = pd.to_datetime(df_validated['date_series_start'])
            
            # Convert back to string format (YYYY-MM-DD)
            df_validated['date_series_start'] = test_dates.dt.strftime('%Y-%m-%d')
            
            self.logger.debug("Validated and standardized date formats")
            
        except Exception as e:
            self.logger.warning(f"Date validation failed: {e}")
            # Keep original dates if validation fails
        
        return df_validated
    
    def get_symbol_stats(self, symbols_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistics about the symbols DataFrame.
        
        Args:
            symbols_df: Symbols DataFrame
            
        Returns:
            Dictionary with statistics
        """
        if symbols_df.empty:
            return {'total_symbols': 0, 'sources': []}
        
        stats = {
            'total_symbols': len(symbols_df),
            'columns': symbols_df.columns.tolist(),
        }
        
        # Source statistics
        if 'string.source' in symbols_df.columns:
            source_counts = symbols_df['string.source'].value_counts().to_dict()
            stats['sources'] = source_counts
        elif 'source' in symbols_df.columns:
            source_counts = symbols_df['source'].value_counts().to_dict()
            stats['sources'] = source_counts
        
        # Date range statistics
        date_columns = [col for col in symbols_df.columns if 'date' in col.lower()]
        if date_columns:
            stats['date_columns'] = date_columns
        
        return stats
    
    @classmethod
    def prepare_for_source(cls, symbols_df: pd.DataFrame, 
                          source_type: str,
                          default_start_date: str = '1900-01-01') -> pd.DataFrame:
        """
        Convenience class method for quick symbol preparation.
        
        Args:
            symbols_df: Input symbols DataFrame
            source_type: Source to prepare for
            default_start_date: Default start date
            
        Returns:
            Prepared symbols DataFrame
        """
        processor = cls()
        return processor.prepare_symbols_for_fetch(symbols_df, source_type, default_start_date) 