"""
Utility functions for the data collection pipeline.
"""

import pandas as pd
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, Any
from .duckdb_functions import DuckDBManager

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

class SymbolManager:
    """Manages symbol data operations with DuckDB"""
    
    def __init__(self, db_path: str = 'bristol_gate.duckdb'):
        self.db_manager = DuckDBManager(db_path)
        
    def load_symbols_from_db(self, source_filter: Optional[str] = None) -> pd.DataFrame:
        """
        Load symbols from DuckDB symbols table with optional source filtering.
        
        Parameters:
        - source_filter (str, optional): Filter symbols by specific source
        
        Returns:
        - pd.DataFrame: DataFrame with symbols data
        """
        try:
            if not self.db_manager.connect():
                raise Exception("Failed to connect to DuckDB")
            
            if source_filter:
                filters = {'source': source_filter.lower()}
                df = self.db_manager.extract_data('symbols', filters)
                logger.info(f"Loaded {len(df)} symbols for source '{source_filter}'")
            else:
                df = self.db_manager.extract_data('symbols')
                logger.info(f"Loaded {len(df)} total symbols from database")
            
            if df.empty:
                logger.warning(f"No symbols found{f' for source {source_filter}' if source_filter else ''}")
                
            return df
            
        except Exception as e:
            logger.error(f"Error loading symbols from database: {e}")
            raise
        finally:
            self.db_manager.close()

class IncrementalDataManager:
    """Manages incremental data loading logic at the symbol level"""
    
    def __init__(self, db_path: str = 'bristol_gate.duckdb'):
        self.db_manager = DuckDBManager(db_path)
    
    def get_latest_dates_by_symbol(self, table_name: str, source_name: str) -> pd.DataFrame:
        """
        Get the latest date for each symbol from a staging table.
        
        Parameters:
        - table_name (str): Name of the staging table
        - source_name (str): Source name for logging
        
        Returns:
        - pd.DataFrame: DataFrame with columns [identifier, latest_date] where identifier 
                       is 'symbol' or 'series_id' depending on the source
        """
        try:
            if not self.db_manager.connect():
                logger.error(f"Failed to connect to database for {source_name}")
                return pd.DataFrame()
            
            # Check if table exists
            table_check = self.db_manager.con.execute(f"""
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """).fetchone()
            
            if table_check[0] == 0:
                logger.info(f"Table {table_name} doesn't exist yet for {source_name}")
                return pd.DataFrame()
            
            # Get the identifier column for this source
            source_schema = SOURCE_SCHEMAS.get(source_name.lower())
            if not source_schema:
                logger.error(f"Unknown source schema for {source_name}")
                return pd.DataFrame()
            
            identifier_column = source_schema['identifier_column']
            
            # Get latest date for each identifier (symbol/series_id)
            query = f"""
                SELECT {identifier_column} as identifier, MAX(date) as latest_date 
                FROM {table_name}
                GROUP BY {identifier_column}
            """
            
            result_df = self.db_manager.con.execute(query).df()
            
            if not result_df.empty:
                # Convert latest_date to date objects
                result_df['latest_date'] = pd.to_datetime(result_df['latest_date']).dt.date
                logger.info(f"Found latest dates for {len(result_df)} identifiers in {table_name} for {source_name}")
                logger.info(f"Date range: {result_df['latest_date'].min()} to {result_df['latest_date'].max()}")
            else:
                logger.info(f"No data in {table_name} for {source_name}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error getting latest dates for {source_name}: {e}")
            return pd.DataFrame()
        finally:
            self.db_manager.close()
    
    def filter_incremental_data(self, df: pd.DataFrame, source_name: str, latest_dates_df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame to only include data newer than each symbol's latest date.
        
        Parameters:
        - df (pd.DataFrame): Data to filter
        - source_name (str): Source name for logging
        - latest_dates_df (pd.DataFrame): DataFrame with columns [identifier, latest_date]
        
        Returns:
        - pd.DataFrame: Filtered DataFrame with only new data
        """
        if df.empty:
            return df
        
        if latest_dates_df.empty:
            logger.info(f"No existing data for {source_name}, loading all data")
            return df
        
        # Get the identifier column for this source
        source_schema = SOURCE_SCHEMAS.get(source_name.lower())
        if not source_schema:
            logger.error(f"Unknown source schema for {source_name}")
            return df
        
        identifier_column = source_schema['identifier_column']
        
        # Ensure date column is in date format
        df = df.copy()
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Merge with latest dates to get each symbol's cutoff date
        df_with_cutoff = df.merge(
            latest_dates_df.rename(columns={'identifier': identifier_column}),
            on=identifier_column,
            how='left'
        )
        
        initial_count = len(df)
        
        # Filter: keep rows where either:
        # 1. No previous data exists for this symbol (latest_date is NaN), OR
        # 2. The date is newer than the symbol's latest date
        mask = (df_with_cutoff['latest_date'].isna()) | (df_with_cutoff['date'] > df_with_cutoff['latest_date'])
        df_filtered = df_with_cutoff[mask].drop(columns=['latest_date'])
        
        final_count = len(df_filtered)
        
        # Log details by identifier
        if not latest_dates_df.empty:
            symbols_with_data = latest_dates_df['identifier'].nunique()
            symbols_with_new_data = df_filtered[identifier_column].nunique() if not df_filtered.empty else 0
            
            logger.info(f"Incremental filter for {source_name}:")
            logger.info(f"  • Total rows: {initial_count} -> {final_count}")
            logger.info(f"  • Symbols with existing data: {symbols_with_data}")
            logger.info(f"  • Symbols with new data: {symbols_with_new_data}")
            
            if final_count > 0 and final_count < initial_count:
                # Show some examples of what was filtered
                filtered_out_count = initial_count - final_count
                logger.info(f"  • Filtered out {filtered_out_count} existing rows")
        else:
            logger.info(f"Incremental filter for {source_name}: {initial_count} -> {final_count} rows (all new)")
        
        return df_filtered

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

class BronzeLayerManager:
    """Manages bronze layer exports with timestamp organization"""
    
    def __init__(self, base_path: str = 'data/bronze'):
        self.base_path = Path(base_path)
        
    def export_to_bronze(self, df: pd.DataFrame, source_name: str) -> bool:
        """
        Export DataFrame to bronze layer organized by source with timestamp.
        
        Parameters:
        - df (pd.DataFrame): Data to export
        - source_name (str): Name of the data source
        
        Returns:
        - bool: True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning(f"No data to export for {source_name}")
                return False
            
            # Create source directory
            source_path = self.base_path / source_name
            source_path.mkdir(parents=True, exist_ok=True)
            
            # Generate timestamp for when data was pulled
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create filename with timestamp and row count for easy identification
            row_count = len(df)
            parquet_file = source_path / f"{source_name}_{timestamp}_{row_count}rows.parquet"
            
            # Export to parquet with optimal settings for DuckDB
            df.to_parquet(
                parquet_file, 
                index=False,
                compression='snappy',  # Fast decompression for DuckDB
                engine='pyarrow'
            )
            
            logger.info(f"✅ Successfully exported {source_name} data to {parquet_file}")
            logger.info(f"   📊 {row_count} rows exported at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error exporting {source_name} to bronze layer: {e}")
            return False

class DataPipelineManager:
    """Main manager for data pipeline operations with symbol-level incremental loading"""
    
    def __init__(self, db_path: str = 'bristol_gate.duckdb'):
        self.db_manager = DuckDBManager(db_path)
        self.symbol_manager = SymbolManager(db_path)
        self.incremental_manager = IncrementalDataManager(db_path)
        self.validator = DataValidator()
        self.bronze_manager = BronzeLayerManager()
        
    def store_to_staging_table(self, df: pd.DataFrame, table_name: str, source_name: str, 
                             incremental: bool = True) -> bool:
        """
        Store data to staging table with validation, symbol-level incremental loading, and bronze layer export.
        
        Parameters:
        - df (pd.DataFrame): Data to store
        - table_name (str): Name of the staging table
        - source_name (str): Name of the data source
        - incremental (bool): Whether to use incremental loading
        
        Returns:
        - bool: True if successful, False otherwise
        """
        try:
            # Validate data first
            if not self.validator.validate_dataframe(df, source_name):
                logger.error(f"❌ Validation failed for {source_name}")
                return False
            
            # Apply symbol-level incremental filtering if requested
            if incremental:
                latest_dates_df = self.incremental_manager.get_latest_dates_by_symbol(table_name, source_name)
                df = self.incremental_manager.filter_incremental_data(df, source_name, latest_dates_df)
                
                if df.empty:
                    logger.info(f"✅ No new data for {source_name} - already up to date")
                    return True
            
            # Connect to database
            if not self.db_manager.connect():
                logger.error(f"❌ Failed to connect to database for {source_name}")
                return False
            
            # Upload to staging table
            if not self.db_manager.upload_data(table_name, df):
                logger.error(f"❌ Failed to upload {source_name} data to {table_name}")
                return False
            
            # Export to bronze layer (append-only approach)
            if not self.bronze_manager.export_to_bronze(df, source_name):
                logger.warning(f"⚠️  Failed to export {source_name} to bronze layer")
                # Don't return False here as the main storage succeeded
            
            logger.info(f"✅ Successfully stored {source_name} data ({len(df)} rows) to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error storing {source_name} data: {e}")
            return False
        finally:
            self.db_manager.close()

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