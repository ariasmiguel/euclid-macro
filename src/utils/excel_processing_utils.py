"""
Excel Processing Utils

This module provides utilities for Excel file processing operations,
including reading files with fallback engines, date conversion, and
column manipulation commonly used in data fetching modules.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Union
from pathlib import Path


class ExcelProcessingUtils:
    """
    Utility class for Excel file processing operations.
    
    Centralizes:
    - Excel file reading with multiple engine fallbacks
    - Date detection and conversion from Excel format
    - Column prefixing and manipulation
    - Sheet handling and error recovery
    """
    
    def __init__(self, logger_name: Optional[str] = None):
        """
        Initialize Excel processing utils.
        
        Args:
            logger_name: Optional custom logger name
        """
        self.logger = logging.getLogger(logger_name or "excel_processing_utils")
    
    def read_excel_with_fallback(self,
                                file_path: str,
                                sheet_name: Union[str, int] = 0,
                                skiprows: Optional[int] = None,
                                header: Optional[int] = 0) -> pd.DataFrame:
        """
        Read Excel file with multiple engine fallbacks.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or index to read
            skiprows: Number of rows to skip
            header: Row to use as column names
            
        Returns:
            DataFrame with Excel data
        """
        file_ext = Path(file_path).suffix.lower()
        engines_to_try = []
        
        # Determine engines based on file extension
        if file_ext == '.xlsb':
            engines_to_try = ['pyxlsb', 'openpyxl']
        elif file_ext in ['.xlsx', '.xlsm']:
            engines_to_try = ['openpyxl', 'xlrd']
        elif file_ext == '.xls':
            engines_to_try = ['xlrd', 'openpyxl']
        else:
            engines_to_try = ['openpyxl', 'xlrd', 'pyxlsb']
        
        # Try each engine until one works
        for engine in engines_to_try:
            try:
                self.logger.debug(f"Trying to read {file_path} with engine: {engine}")
                
                df = pd.read_excel(
                    file_path,
                    engine=engine,
                    sheet_name=sheet_name,
                    skiprows=skiprows,
                    header=header
                )
                
                self.logger.info(f"Successfully read Excel file with {engine}: {df.shape}")
                return df
                
            except Exception as e:
                self.logger.debug(f"Engine {engine} failed: {e}")
                continue
        
        # If all engines failed
        raise Exception(f"Failed to read Excel file {file_path} with any available engine")
    
    def read_excel_file(self,
                       file_path: str,
                       sheet_name: Union[str, int] = 0,
                       skip_rows: Optional[int] = None,
                       column_names: Optional[List[str]] = None,
                       header: Optional[int] = 0) -> pd.DataFrame:
        """
        Read Excel file - compatibility method for refactored fetchers.
        
        This is an alias for read_excel_with_fallback with parameter mapping.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or index to read
            skip_rows: Number of rows to skip
            column_names: Column names to assign (will be applied after reading)
            header: Row to use as column names
            
        Returns:
            DataFrame with Excel data
        """
        # Read with fallback engines
        df = self.read_excel_with_fallback(
            file_path=file_path,
            sheet_name=sheet_name,
            skiprows=skip_rows,
            header=header
        )
        
        # Apply custom column names if provided
        if column_names and not df.empty:
            if len(column_names) <= len(df.columns):
                df.columns = column_names[:len(df.columns)]
            else:
                self.logger.warning(f"More column names provided ({len(column_names)}) than actual columns ({len(df.columns)})")
        
        return df
    
    def detect_date_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Detect columns that likely contain dates.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of column names that appear to contain dates
        """
        date_columns = []
        
        for col in df.columns:
            col_name = str(col).lower()
            
            # Check by column name
            if any(keyword in col_name for keyword in ['date', 'time', 'year', 'month']):
                date_columns.append(col)
                continue
            
            # Check by data type and values
            if pd.api.types.is_numeric_dtype(df[col]):
                # Check if numeric values are in Excel date range
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    min_val = non_null_values.min()
                    max_val = non_null_values.max()
                    
                    # Excel dates typically range from ~1 (1900-01-01) to ~50000 (2036+)
                    if 1 <= min_val <= 50000 and 1 <= max_val <= 50000:
                        date_columns.append(col)
        
        self.logger.debug(f"Detected potential date columns: {date_columns}")
        return date_columns
    
    def convert_excel_dates(self,
                           df: pd.DataFrame,
                           date_columns: Optional[List[str]] = None,
                           origin: str = '1899-12-30') -> pd.DataFrame:
        """
        Convert Excel numeric dates to proper date format.
        
        Args:
            df: DataFrame to process
            date_columns: Specific columns to convert (auto-detect if None)
            origin: Excel date origin
            
        Returns:
            DataFrame with converted dates
        """
        if df.empty:
            return df
        
        df_copy = df.copy()
        
        # Auto-detect date columns if not specified
        if date_columns is None:
            date_columns = self.detect_date_columns(df_copy)
        
        for col in date_columns:
            if col not in df_copy.columns:
                continue
            
            try:
                # Convert Excel serial dates to datetime, then to date
                df_copy[col] = pd.to_datetime(
                    df_copy[col], 
                    origin=origin, 
                    unit='D'
                ).dt.date
                
                self.logger.debug(f"Converted column '{col}' from Excel date format")
                
            except Exception as e:
                self.logger.warning(f"Failed to convert column '{col}' as Excel date: {e}")
                continue
        
        return df_copy
    
    def add_column_prefix(self,
                         df: pd.DataFrame,
                         prefix: str,
                         exclude_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Add prefix to column names, excluding specified columns.
        
        Args:
            df: DataFrame to modify
            prefix: Prefix to add
            exclude_columns: Columns to exclude from prefixing
            
        Returns:
            DataFrame with prefixed columns
        """
        if df.empty:
            return df
        
        if exclude_columns is None:
            exclude_columns = ['date']
        
        df_copy = df.copy()
        
        # Create new column mapping
        column_mapping = {}
        for col in df_copy.columns:
            if col in exclude_columns:
                column_mapping[col] = col
            else:
                # Add prefix if not already present
                new_name = f"{prefix}{col}" if not str(col).startswith(prefix) else str(col)
                column_mapping[col] = new_name
        
        df_copy = df_copy.rename(columns=column_mapping)
        
        prefixed_count = sum(1 for old, new in column_mapping.items() if old != new)
        if prefixed_count > 0:
            self.logger.debug(f"Added prefix '{prefix}' to {prefixed_count} columns")
        
        return df_copy
    
    def remove_percentage_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove columns that contain percentage signs in names.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            DataFrame without percentage columns
        """
        if df.empty:
            return df
        
        percent_columns = [col for col in df.columns if '%' in str(col)]
        
        if percent_columns:
            df_cleaned = df.drop(columns=percent_columns)
            self.logger.debug(f"Removed {len(percent_columns)} percentage columns: {percent_columns}")
            return df_cleaned
        
        return df
    
    def standardize_date_column(self,
                               df: pd.DataFrame,
                               date_column: str = 'date',
                               auto_detect: bool = True) -> pd.DataFrame:
        """
        Standardize a date column to consistent format.
        
        Args:
            df: DataFrame to process
            date_column: Name of date column to standardize
            auto_detect: Whether to auto-detect and convert Excel dates
            
        Returns:
            DataFrame with standardized date column
        """
        if df.empty or date_column not in df.columns:
            return df
        
        df_copy = df.copy()
        
        try:
            # First try direct conversion
            df_copy[date_column] = pd.to_datetime(df_copy[date_column]).dt.date
            self.logger.debug(f"Standardized date column '{date_column}' via direct conversion")
            
        except Exception:
            if auto_detect:
                # Try Excel date conversion
                try:
                    df_copy = self.convert_excel_dates(df_copy, [date_column])
                    self.logger.debug(f"Standardized date column '{date_column}' via Excel conversion")
                except Exception as e:
                    self.logger.warning(f"Failed to standardize date column '{date_column}': {e}")
            else:
                self.logger.warning(f"Failed to standardize date column '{date_column}'")
        
        return df_copy
    
    def extract_numeric_columns(self,
                               df: pd.DataFrame,
                               exclude_columns: Optional[List[str]] = None) -> List[str]:
        """
        Extract names of numeric columns from DataFrame.
        
        Args:
            df: DataFrame to analyze
            exclude_columns: Columns to exclude from selection
            
        Returns:
            List of numeric column names
        """
        if exclude_columns is None:
            exclude_columns = ['date']
        
        numeric_columns = []
        for col in df.columns:
            if col not in exclude_columns and pd.api.types.is_numeric_dtype(df[col]):
                numeric_columns.append(col)
        
        self.logger.debug(f"Found {len(numeric_columns)} numeric columns")
        return numeric_columns
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean column names by removing special characters and whitespace.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            DataFrame with cleaned column names
        """
        if df.empty:
            return df
        
        df_copy = df.copy()
        
        # Clean column names
        new_columns = []
        for col in df_copy.columns:
            # Convert to string and clean
            clean_name = str(col).strip()
            # Remove or replace problematic characters
            clean_name = clean_name.replace(' ', '_')
            clean_name = clean_name.replace('-', '_')
            clean_name = clean_name.replace('(', '').replace(')', '')
            clean_name = clean_name.replace('[', '').replace(']', '')
            clean_name = clean_name.replace('%', 'pct')
            new_columns.append(clean_name)
        
        df_copy.columns = new_columns
        
        self.logger.debug("Cleaned column names")
        return df_copy
    
    def process_excel_for_pipeline(self,
                                  file_path: str,
                                  sheet_name: Union[str, int] = 0,
                                  skiprows: Optional[int] = None,
                                  prefix: Optional[str] = None,
                                  exclude_from_prefix: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Complete Excel processing pipeline with common operations.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet to read
            skiprows: Rows to skip
            prefix: Optional prefix for columns
            exclude_from_prefix: Columns to exclude from prefixing
            
        Returns:
            Processed DataFrame ready for data pipeline
        """
        try:
            # Step 1: Read Excel file
            df = self.read_excel_with_fallback(file_path, sheet_name, skiprows)
            
            if df.empty:
                self.logger.warning("Excel file is empty")
                return df
            
            # Step 2: Clean column names
            df = self.clean_column_names(df)
            
            # Step 3: Convert Excel dates
            df = self.convert_excel_dates(df)
            
            # Step 4: Standardize date column if it exists
            if 'date' in df.columns:
                df = self.standardize_date_column(df)
            
            # Step 5: Add prefix if specified
            if prefix:
                df = self.add_column_prefix(df, prefix, exclude_from_prefix)
            
            # Step 6: Remove percentage columns
            df = self.remove_percentage_columns(df)
            
            self.logger.info(f"Excel processing pipeline completed: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"Excel processing pipeline failed: {e}")
            return pd.DataFrame() 