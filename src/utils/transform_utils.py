"""
Data Transform Utils

This module provides utility functions for common data transformation patterns
used across the data pipeline. Centralizes operations like melting DataFrames,
standardizing column orders, and date conversions.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, date
from typing import List, Optional, Union, Dict, Any


class DataTransformUtils:
    """
    Utility class for common data transformation operations.
    
    This class centralizes:
    - DataFrame melting (wide to long format)
    - Column standardization and ordering
    - Date format conversions
    - Data cleaning and validation
    """
    
    def __init__(self, logger_name: Optional[str] = None):
        """
        Initialize the transform utils.
        
        Args:
            logger_name: Optional custom logger name
        """
        self.logger = logging.getLogger(logger_name or "transform_utils")
    
    @staticmethod
    def melt_to_long_format(df: pd.DataFrame, 
                           id_vars: List[str],
                           value_vars: Optional[List[str]] = None,
                           var_name: str = 'metric',
                           value_name: str = 'value',
                           symbol_column: str = 'symbol') -> pd.DataFrame:
        """
        Convert DataFrame from wide to long format using standardized approach.
        
        Args:
            df: DataFrame to melt
            id_vars: Columns to use as identifier variables
            value_vars: Columns to use as value variables (auto-detected if None)
            var_name: Name for variable column
            value_name: Name for value column  
            symbol_column: Name for symbol column (usually derived from metric)
            
        Returns:
            Melted DataFrame in long format
        """
        if df.empty:
            return df
        
        # Get value columns (exclude id_vars or use provided value_vars)
        if value_vars is not None:
            # Use explicitly provided value columns
            value_columns = [col for col in value_vars if col in df.columns]
        else:
            # Auto-detect value columns (exclude id_vars)
            value_columns = [col for col in df.columns if col not in id_vars]
        
        if not value_columns:
            logging.warning("No value columns found for melting")
            return df
        
        # Melt the DataFrame
        result_df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=value_columns,
            var_name=var_name,
            value_name=value_name
        )
        
        # Add symbol column (typically the same as metric for most sources)
        if symbol_column not in result_df.columns:
            result_df[symbol_column] = result_df[var_name]
        
        logging.debug(f"Melted DataFrame: {df.shape} -> {result_df.shape}")
        return result_df
    
    @staticmethod
    def standardize_column_order(df: pd.DataFrame, 
                                expected_order: List[str]) -> pd.DataFrame:
        """
        Reorder DataFrame columns to match expected order.
        
        Args:
            df: DataFrame to reorder
            expected_order: List of column names in desired order
            
        Returns:
            DataFrame with reordered columns
        """
        if df.empty:
            return df
        
        # Only include columns that exist in the DataFrame
        available_columns = [col for col in expected_order if col in df.columns]
        
        # Add any extra columns not in expected_order
        extra_columns = [col for col in df.columns if col not in expected_order]
        
        if extra_columns:
            logging.debug(f"Found extra columns not in expected order: {extra_columns}")
            available_columns.extend(extra_columns)
        
        return df[available_columns]
    
    @staticmethod
    def convert_dates_to_standard_format(df: pd.DataFrame, 
                                       date_column: str = 'date') -> pd.DataFrame:
        """
        Convert date column to standard format (date objects).
        
        Args:
            df: DataFrame with date column
            date_column: Name of the date column
            
        Returns:
            DataFrame with standardized dates
        """
        if df.empty or date_column not in df.columns:
            return df
        
        try:
            # Convert to datetime first, then to date
            df[date_column] = pd.to_datetime(df[date_column]).dt.date
            logging.debug(f"Converted {date_column} to date format")
        except Exception as e:
            logging.warning(f"Failed to convert {date_column} to date format: {e}")
        
        return df
    
    @staticmethod
    def clean_and_validate_data(df: pd.DataFrame, 
                               required_columns: List[str],
                               drop_na_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Clean and validate DataFrame.
        
        Args:
            df: DataFrame to clean
            required_columns: Columns that must be present
            drop_na_columns: Columns to use for dropping NaN rows (defaults to ['value'])
            
        Returns:
            Cleaned DataFrame
        """
        if df.empty:
            return df
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()  # Return empty DataFrame if required columns missing
        
        # Drop rows with NaN values in specified columns
        if drop_na_columns is None:
            drop_na_columns = ['value'] if 'value' in df.columns else []
        
        available_drop_columns = [col for col in drop_na_columns if col in df.columns]
        if available_drop_columns:
            initial_rows = len(df)
            df = df.dropna(subset=available_drop_columns)
            dropped_rows = initial_rows - len(df)
            if dropped_rows > 0:
                logging.debug(f"Dropped {dropped_rows} rows with NaN values")
        
        return df
    
    @staticmethod  
    def add_prefix_to_columns(df: pd.DataFrame, 
                             prefix: str,
                             exclude_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Add prefix to column names, excluding specified columns.
        
        Args:
            df: DataFrame to modify
            prefix: Prefix to add
            exclude_columns: Columns to exclude from prefixing (default: ['date'])
            
        Returns:
            DataFrame with prefixed columns
        """
        if df.empty:
            return df
        
        if exclude_columns is None:
            exclude_columns = ['date']
        
        # Create new column mapping
        new_columns = {}
        for col in df.columns:
            if col in exclude_columns:
                new_columns[col] = col
            else:
                new_columns[col] = f"{prefix}{col}"
        
        df = df.rename(columns=new_columns)
        logging.debug(f"Added prefix '{prefix}' to columns (excluded: {exclude_columns})")
        
        return df
    
    @staticmethod
    def remove_percentage_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove columns that contain percentage signs.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            DataFrame without percentage columns
        """
        if df.empty:
            return df
        
        # Find percentage columns
        percent_columns = [col for col in df.columns if '%' in str(col)]
        
        if percent_columns:
            df = df.drop(columns=percent_columns)
            logging.debug(f"Removed percentage columns: {percent_columns}")
        
        return df
    
    @staticmethod
    def standardize_numeric_columns(df: pd.DataFrame,
                                  numeric_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Ensure specified columns are numeric type.
        
        Args:
            df: DataFrame to process
            numeric_columns: List of columns that should be numeric (default: ['value'])
            
        Returns:
            DataFrame with numeric columns converted
        """
        if df.empty:
            return df
        
        if numeric_columns is None:
            numeric_columns = ['value'] if 'value' in df.columns else []
        
        for col in numeric_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    logging.debug(f"Converted {col} to numeric type")
                except Exception as e:
                    logging.warning(f"Failed to convert {col} to numeric: {e}")
        
        return df
    
    @classmethod
    def apply_standard_pipeline(cls, df: pd.DataFrame,
                               id_vars: List[str],
                               expected_order: List[str],
                               prefix: Optional[str] = None,
                               date_column: str = 'date') -> pd.DataFrame:
        """
        Apply the standard transformation pipeline used by most fetchers.
        
        This combines the most common transformation steps:
        1. Add prefix to columns (if specified)
        2. Remove percentage columns
        3. Melt to long format
        4. Standardize column order
        5. Convert dates to standard format
        6. Clean and validate data
        
        Args:
            df: DataFrame to transform
            id_vars: Columns to use as identifier variables for melting
            expected_order: Expected column order for final DataFrame
            prefix: Optional prefix to add to value columns
            date_column: Name of date column
            
        Returns:
            Fully processed DataFrame
        """
        utils = cls()
        
        if df.empty:
            return df
        
        # Step 1: Add prefix if specified
        if prefix:
            df = cls.add_prefix_to_columns(df, prefix, exclude_columns=[date_column])
        
        # Step 2: Remove percentage columns
        df = cls.remove_percentage_columns(df)
        
        # Step 3: Melt to long format
        df = cls.melt_to_long_format(df, id_vars)
        
        # Step 4: Standardize column order  
        df = cls.standardize_column_order(df, expected_order)
        
        # Step 5: Convert dates
        df = cls.convert_dates_to_standard_format(df, date_column)
        
        # Step 6: Clean and validate
        df = cls.clean_and_validate_data(df, expected_order)
        
        logging.info(f"Applied standard transformation pipeline: final shape {df.shape}")
        return df 