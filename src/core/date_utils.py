"""
Date Utils

This module provides utilities for date handling operations commonly used
across the data pipeline, centralizing date formatting, conversion, and validation.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import pandas as pd
import logging
from datetime import datetime, date, timedelta
from typing import Union, Optional, List, Tuple

class DateUtils:
    """
    Utility class for date handling operations.
    
    Centralizes:
    - Date format standardization
    - API date formatting
    - Date range validation
    - Excel date conversion
    - Pandas date operations
    """
    
    def __init__(self, logger_name: Optional[str] = None):
        """
        Initialize date utils.
        
        Args:
            logger_name: Optional custom logger name
        """
        self.logger = logging.getLogger(logger_name or "date_utils")
    
    @staticmethod
    def standardize_date_format(date_input: Union[str, datetime, date, pd.Timestamp]) -> date:
        """
        Standardize various date inputs to date objects.
        
        Args:
            date_input: Date in various formats
            
        Returns:
            Standardized date object
            
        Raises:
            ValueError: If date cannot be parsed
        """
        if date_input is None:
            raise ValueError("Date input cannot be None")
        
        if isinstance(date_input, date):
            return date_input
        
        if isinstance(date_input, datetime):
            return date_input.date()
        
        if isinstance(date_input, pd.Timestamp):
            return date_input.date()
        
        if isinstance(date_input, str):
            try:
                # Try common formats
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        return datetime.strptime(date_input.strip(), fmt).date()
                    except ValueError:
                        continue
                
                # Fallback to pandas date parsing
                return pd.to_datetime(date_input).date()
                
            except Exception as e:
                raise ValueError(f"Could not parse date string '{date_input}': {e}")
        
        raise ValueError(f"Unsupported date type: {type(date_input)}")
    
    @staticmethod
    def format_for_api(date_obj: Union[date, datetime], format_string: str = '%Y-%m-%d') -> str:
        """
        Format date for API calls.
        
        Args:
            date_obj: Date object to format
            format_string: Output format string
            
        Returns:
            Formatted date string
        """
        if isinstance(date_obj, datetime):
            return date_obj.strftime(format_string)
        elif isinstance(date_obj, date):
            return date_obj.strftime(format_string)
        else:
            # Try to convert first
            standardized = DateUtils.standardize_date_format(date_obj)
            return standardized.strftime(format_string)
    
    @staticmethod
    def validate_date_range(start_date: Union[str, date, datetime], 
                           end_date: Union[str, date, datetime]) -> Tuple[date, date]:
        """
        Validate and standardize a date range.
        
        Args:
            start_date: Start date in various formats
            end_date: End date in various formats
            
        Returns:
            Tuple of (start_date, end_date) as date objects
            
        Raises:
            ValueError: If dates are invalid or range is invalid
        """
        start = DateUtils.standardize_date_format(start_date)
        end = DateUtils.standardize_date_format(end_date)
        
        if start > end:
            raise ValueError(f"Start date ({start}) cannot be after end date ({end})")
        
        return start, end
    
    @staticmethod
    def convert_to_pandas_date(date_column: pd.Series) -> pd.Series:
        """
        Convert pandas Series to standardized date format.
        
        Args:
            date_column: Pandas Series with date data
            
        Returns:
            Series with standardized date objects
        """
        try:
            # Convert to datetime first, then to date
            return pd.to_datetime(date_column).dt.date
        except Exception as e:
            logging.getLogger("date_utils").warning(f"Failed to convert dates: {e}")
            return date_column
    
    @staticmethod
    def convert_excel_serial_date(serial_date: Union[int, float], 
                                 origin: str = '1899-12-30') -> date:
        """
        Convert Excel serial date number to date object.
        
        Args:
            serial_date: Excel serial date number
            origin: Excel date origin
            
        Returns:
            Date object
        """
        try:
            return pd.to_datetime(serial_date, origin=origin, unit='D').date()
        except Exception as e:
            raise ValueError(f"Could not convert Excel date {serial_date}: {e}")
    
    @staticmethod
    def get_month_end_date(year: int, month: int) -> date:
        """
        Get the last day of a given month and year.
        
        Args:
            year: Year
            month: Month (1-12)
            
        Returns:
            Last day of the month
        """
        if month == 12:
            return date(year + 1, 1, 1) - timedelta(days=1)
        else:
            return date(year, month + 1, 1) - timedelta(days=1)
    
    @staticmethod
    def add_business_days(start_date: date, days: int) -> date:
        """
        Add business days to a date (excluding weekends).
        
        Args:
            start_date: Starting date
            days: Number of business days to add
            
        Returns:
            Date after adding business days
        """
        return pd.bdate_range(start=start_date, periods=days + 1)[-1].date()
    
    @staticmethod
    def get_quarter_end_dates(year: int) -> List[date]:
        """
        Get quarter end dates for a given year.
        
        Args:
            year: Year
            
        Returns:
            List of quarter end dates
        """
        return [
            date(year, 3, 31),   # Q1
            date(year, 6, 30),   # Q2
            date(year, 9, 30),   # Q3
            date(year, 12, 31)   # Q4
        ]
    
    @staticmethod
    def create_date_range(start_date: Union[str, date], 
                         end_date: Union[str, date],
                         frequency: str = 'D') -> pd.DatetimeIndex:
        """
        Create a date range between two dates.
        
        Args:
            start_date: Start date
            end_date: End date
            frequency: Pandas frequency string ('D', 'M', 'Y', etc.)
            
        Returns:
            DatetimeIndex with the date range
        """
        start = DateUtils.standardize_date_format(start_date)
        end = DateUtils.standardize_date_format(end_date)
        
        return pd.date_range(start=start, end=end, freq=frequency)
    
    @staticmethod
    def get_date_bounds_from_dataframe(df: pd.DataFrame, 
                                      date_column: str = 'date') -> Tuple[date, date]:
        """
        Get the min and max dates from a DataFrame.
        
        Args:
            df: DataFrame with date column
            date_column: Name of date column
            
        Returns:
            Tuple of (min_date, max_date)
        """
        if df.empty or date_column not in df.columns:
            raise ValueError(f"DataFrame is empty or missing column '{date_column}'")
        
        dates = pd.to_datetime(df[date_column])
        return dates.min().date(), dates.max().date()
    
    @classmethod
    def format_date_range_for_logging(cls, start_date: Union[str, date, datetime], 
                                     end_date: Union[str, date, datetime]) -> str:
        """
        Format date range for consistent logging.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            Formatted date range string
        """
        try:
            start = cls.standardize_date_format(start_date)
            end = cls.standardize_date_format(end_date)
            return f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}"
        except Exception:
            return f"{start_date} to {end_date}"
    
    @staticmethod
    def generate_timestamp_string(dt: Optional[datetime] = None) -> str:
        """
        Generate a timestamp string in standardized format for file naming.
        
        Args:
            dt: Optional datetime object, defaults to current time
            
        Returns:
            Timestamp string in format YYYYMMDD_HHMMSS
        """
        if dt is None:
            dt = datetime.now()
        return dt.strftime('%Y%m%d_%H%M%S')
    
    @staticmethod
    def format_current_datetime() -> str:
        """
        Format current datetime for logging display.
        
        Returns:
            Current datetime formatted as YYYY-MM-DD HH:MM:SS
        """
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def format_datetime(dt: datetime) -> str:
        """
        Format a datetime object for display.
        
        Args:
            dt: Datetime object to format
            
        Returns:
            Formatted datetime string as YYYY-MM-DD HH:MM:SS
        """
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def format_date_only(dt: Union[datetime, pd.Timestamp]) -> str:
        """
        Format a datetime object to show only the date part.
        
        Args:
            dt: Datetime object to format
            
        Returns:
            Formatted date string as YYYY-MM-DD
        """
        if pd.isna(dt):
            return 'Unknown'
        return dt.strftime('%Y-%m-%d') 