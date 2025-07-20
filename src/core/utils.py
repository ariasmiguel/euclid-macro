"""
Utility functions for the data collection pipeline.
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Source-specific schema definitions
SOURCE_SCHEMAS = {
    'yahoo': {
        'required_columns': ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume'],
        'description': 'Yahoo Finance OHLCV data',
        'date_column': 'date',
        'identifier_column': 'symbol'
    },
    'fred': {
        'required_columns': ['date', 'series_id', 'value'],
        'description': 'FRED economic data',
        'date_column': 'date',
        'identifier_column': 'series_id'
    },
    'eia': {
        'required_columns': ['date', 'series_id', 'value'],
        'description': 'EIA energy data',
        'date_column': 'date',
        'identifier_column': 'series_id'
    },
    'baker': {
        'required_columns': ['date', 'symbol', 'metric', 'value'],
        'description': 'Baker Hughes rig count data',
        'date_column': 'date',
        'identifier_column': 'symbol'
    },
    'finra': {
        'required_columns': ['date', 'symbol', 'metric', 'value'],
        'description': 'FINRA margin statistics',
        'date_column': 'date',
        'identifier_column': 'symbol'
    },
    'sp500': {
        'required_columns': ['date', 'symbol', 'metric', 'value'],
        'description': 'S&P 500 earnings data',
        'date_column': 'date',
        'identifier_column': 'symbol'
    },
    'usda': {
        'required_columns': ['date', 'symbol', 'metric', 'value'],
        'description': 'USDA agricultural data',
        'date_column': 'date',
        'identifier_column': 'symbol'
    },
    'occ': {
        'required_columns': ['date', 'symbol', 'metric', 'value'],
        'description': 'OCC options and futures volume data',
        'date_column': 'date',
        'identifier_column': 'symbol'
    }
}

class DataValidator:
    """Handles source-specific data validation"""
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, source_name: str) -> bool:
        """
        Validates that a DataFrame has the expected structure for the specific source.
        
        Parameters:
        - df (pd.DataFrame): DataFrame to validate
        - source_name (str): Name of the data source for validation
        
        Returns:
        - bool: True if valid, False otherwise
        """
        source_name_lower = source_name.lower()
        
        if df.empty:
            logger.warning(f"{source_name} DataFrame is empty")
            return False
        
        # Get expected schema for this source
        if source_name_lower not in SOURCE_SCHEMAS:
            logger.error(f"Unknown source '{source_name}'. Available sources: {list(SOURCE_SCHEMAS.keys())}")
            return False
            
        schema = SOURCE_SCHEMAS[source_name_lower]
        expected_columns = schema['required_columns']
        description = schema['description']
        
        # Check if all required columns are present
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"{source_name} DataFrame missing required columns for {description}. "
                        f"Missing: {missing_columns}, Expected: {expected_columns}, Got: {df.columns.tolist()}")
            return False
        
        # Check for null values in critical columns
        null_counts = df[expected_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"{source_name} DataFrame has null values: {null_counts.to_dict()}")
        
        # Source-specific validation
        if source_name_lower == 'yahoo':
            # Validate that numeric columns are actually numeric
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    logger.error(f"Yahoo Finance column '{col}' should be numeric")
                    return False
                    
        elif source_name_lower in ['fred', 'eia']:
            # Validate that value column is numeric
            if not pd.api.types.is_numeric_dtype(df['value']):
                logger.error(f"{source_name} 'value' column should be numeric")
                return False
                
        elif source_name_lower in ['baker', 'finra', 'sp500', 'usda']:
            # Validate that value column is numeric
            if not pd.api.types.is_numeric_dtype(df['value']):
                logger.error(f"{source_name} 'value' column should be numeric")
                return False
        
        # Validate date column
        try:
            pd.to_datetime(df['date'])
        except Exception:
            logger.error(f"{source_name} 'date' column contains invalid dates")
            return False
        
        logger.info(f"{source_name} DataFrame validation passed for {description}. Shape: {df.shape}")
        return True

# Legacy functions for backward compatibility
def load_symbols_csv(file_path: str) -> pd.DataFrame:
    """
    Legacy function - Loads the symbols CSV file into a pandas DataFrame.
    Consider using SymbolManager.load_symbols_from_db() instead.
    """
    logger.warning("Using legacy CSV loading. Consider switching to database-based symbol loading.")
    logger.info(f"Loading symbols from {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(df)} symbols")
        return df
    except Exception as e:
        logger.error(f"Error loading symbols CSV: {str(e)}")
        raise 