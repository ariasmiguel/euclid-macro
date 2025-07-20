"""
Simplified Data Collection Pipeline

This module contains the main DataCollectionPipeline class that orchestrates
data collection from various sources and saves to parquet files.
"""

import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Optional
import sys
from pathlib import Path

# Import individual fetch functions
from .fetchers.fetch_yahoo import fetch_yahoo
from .fetchers.fetch_fred import fetch_fred
from .fetchers.fetch_eia import fetch_eia
from .fetchers.fetch_baker import fetch_baker
from .fetchers.fetch_finra import fetch_finra
from .fetchers.fetch_sp500 import fetch_sp500
from .fetchers.fetch_usda import fetch_usda
from .fetchers.fetch_occ import fetch_occ

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleStorageManager:
    """Simple storage manager for parquet files"""
    
    def __init__(self, raw_path: str = 'data/raw'):
        self.raw_path = Path(raw_path)
        self.raw_path.mkdir(parents=True, exist_ok=True)
        
    def save_source_data(self, source: str, data: pd.DataFrame) -> Optional[Path]:
        """Save all data from a source to single parquet file"""
        if data.empty:
            logger.warning(f"No data to save for {source}")
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{source}_{timestamp}.parquet"
        file_path = self.raw_path / filename
        
        # Save with compression
        data.to_parquet(file_path, index=False, compression='snappy')
        
        logger.info(f"Saved {len(data):,} rows from {source} to {file_path}")
        return file_path
    
    def save_combined_data(self, all_data: Dict[str, pd.DataFrame]) -> Optional[Path]:
        """Combine all source data into single long-format parquet and CSV files"""
        if not all_data:
            logger.warning("No data to combine")
            return None
        
        # Combine all dataframes
        combined_dfs = []
        for source, df in all_data.items():
            if not df.empty:
                # Ensure source column exists
                if 'source' not in df.columns:
                    df['source'] = source
                
                # Ensure metric column exists (for sources that don't have it)
                if 'metric' not in df.columns:
                    # For sources like Yahoo that have multiple columns, we need to identify the metric
                    if source == 'yahoo':
                        # Yahoo data should already have been melted to long format with metric column
                        # If not, we'll use 'close' as default
                        df['metric'] = 'close'
                    else:
                        # For other sources, use a default metric name
                        df['metric'] = 'value'
                
                combined_dfs.append(df)
        
        if not combined_dfs:
            return None
            
        # Create long format dataframe
        long_df = pd.concat(combined_dfs, ignore_index=True)
        
        # Ensure consistent column order with metric included
        required_cols = ['date', 'symbol', 'metric', 'value', 'source']
        extra_cols = [col for col in long_df.columns if col not in required_cols]
        column_order = required_cols + extra_cols
        long_df = long_df[[col for col in column_order if col in long_df.columns]]
        
        # Save combined files with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as parquet
        parquet_filename = f"combined_data_{timestamp}.parquet"
        parquet_path = self.raw_path / parquet_filename
        long_df.to_parquet(parquet_path, index=False, compression='snappy')
        
        # Save as CSV
        csv_filename = f"combined_data_{timestamp}.csv"
        csv_path = self.raw_path / csv_filename
        long_df.to_csv(csv_path, index=False)
        
        # Also save as 'latest' for easy access
        latest_parquet_path = self.raw_path / "combined_data_latest.parquet"
        long_df.to_parquet(latest_parquet_path, index=False, compression='snappy')
        
        latest_csv_path = self.raw_path / "combined_data_latest.csv"
        long_df.to_csv(latest_csv_path, index=False)
        
        logger.info(f"Saved combined data: {len(long_df):,} total rows")
        logger.info(f"  • Parquet: {parquet_path}")
        logger.info(f"  • CSV: {csv_path}")
        logger.info(f"  • Latest parquet: {latest_parquet_path}")
        logger.info(f"  • Latest CSV: {latest_csv_path}")
        
        # Log metric information
        if 'metric' in long_df.columns:
            unique_metrics = long_df['metric'].unique()
            logger.info(f"  • Metrics included: {', '.join(sorted(unique_metrics))}")
        
        return parquet_path

class DataCollectionPipeline:
    """Simplified data collection pipeline orchestrator"""
    
    def __init__(self, allowed_sources: list = None):
        self.storage = SimpleStorageManager()
        self.allowed_sources = [src.lower() for src in allowed_sources] if allowed_sources else None
        
        if self.allowed_sources:
            logger.info(f"🎯 Source filtering enabled: {', '.join(self.allowed_sources)}")
        else:
            logger.info("📊 All sources enabled")
    
    def _is_source_allowed(self, source_name: str) -> bool:
        """Check if a source is allowed based on the allowed_sources filter."""
        if self.allowed_sources is None:
            return True
        return source_name.lower() in self.allowed_sources
    
    def _log_collection_stats(self, df: pd.DataFrame, source_name: str, total_symbols: int) -> None:
        """Log detailed statistics about data collection for a source."""
        if df.empty:
            logger.info(f"✅ {source_name.upper()}: No data collected")
            return
            
        # Get unique symbols/series
        symbol_col = 'symbol' if 'symbol' in df.columns else 'series_id'
        unique_symbols = df[symbol_col].nunique() if symbol_col in df.columns else 0
        
        # Date range information
        df_dates = pd.to_datetime(df['date'])
        date_range = f"{df_dates.min().strftime('%Y-%m-%d')} to {df_dates.max().strftime('%Y-%m-%d')}"
        
        logger.info(f"✅ {source_name.upper()}: {len(df):,} rows collected")
        logger.info(f"   📊 {unique_symbols}/{total_symbols} symbols/series with data")
        logger.info(f"   📅 Date range: {date_range}")
    
    def _load_symbols_from_csv(self) -> pd.DataFrame:
        """Load symbols from CSV file"""
        symbols_path = Path("data/symbols.csv")
        if not symbols_path.exists():
            raise Exception("data/symbols.csv not found. Please create this file with symbol data.")
        
        df = pd.read_csv(symbols_path)
        logger.info(f"Loaded {len(df)} symbols from data/symbols.csv")
        return df
    
    def _prepare_symbols_for_source(self, symbols_df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Prepare symbols for a specific source"""
        source_symbols = symbols_df[symbols_df['source'].str.lower() == source.lower()].copy()
        
        if source_symbols.empty:
            logger.info(f"No symbols found for source: {source}")
            return pd.DataFrame()
        
        # Standardize column names for fetchers
        if 'symbol' in source_symbols.columns:
            # For sources that use 'symbol' column
            return source_symbols[['symbol', 'source', 'description', 'date_series_start']].copy()
        else:
            # For sources that use other identifier columns
            return source_symbols.copy()
    
    def collect_symbol_based_data(self, symbols_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Collects data from sources that use the symbols DataFrame.
        
        Parameters:
        - symbols_df (pd.DataFrame): DataFrame containing symbol information
        
        Returns:
        - Dict[str, pd.DataFrame]: Dictionary mapping source names to DataFrames
        """
        results = {}
        
        # Yahoo Finance data
        logger.info("=" * 50)
        logger.info("COLLECTING YAHOO FINANCE DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('yahoo'):
            logger.info("🚫 Yahoo Finance data collection SKIPPED (not in allowed sources)")
        else:
            yahoo_symbols = self._prepare_symbols_for_source(symbols_df, 'yahoo')
            if not yahoo_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(yahoo_symbols)} Yahoo Finance symbols")
                    yahoo_data = fetch_yahoo(yahoo_symbols)
                    results['yahoo'] = yahoo_data
                    self._log_collection_stats(yahoo_data, "yahoo", len(yahoo_symbols))
                except Exception as e:
                    logger.error(f"❌ Error collecting Yahoo Finance data: {str(e)}")
                    raise Exception(f"Yahoo Finance collection failed: {str(e)}")
            else:
                logger.info("No Yahoo Finance symbols found")
        
        # FRED data
        logger.info("=" * 50)
        logger.info("COLLECTING FRED DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('fred'):
            logger.info("🚫 FRED data collection SKIPPED (not in allowed sources)")
        else:
            fred_symbols = self._prepare_symbols_for_source(symbols_df, 'fred')
            if not fred_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(fred_symbols)} FRED series")
                    fred_data = fetch_fred(fred_symbols)
                    results['fred'] = fred_data
                    self._log_collection_stats(fred_data, "fred", len(fred_symbols))
                except Exception as e:
                    logger.error(f"❌ Error collecting FRED data: {str(e)}")
                    raise Exception(f"FRED collection failed: {str(e)}")
            else:
                logger.info("No FRED symbols found")
        
        # EIA data
        logger.info("=" * 50)
        logger.info("COLLECTING EIA DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('eia'):
            logger.info("🚫 EIA data collection SKIPPED (not in allowed sources)")
        else:
            eia_symbols = self._prepare_symbols_for_source(symbols_df, 'eia')
            if not eia_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(eia_symbols)} EIA series")
                    eia_data = fetch_eia(eia_symbols)
                    results['eia'] = eia_data
                    self._log_collection_stats(eia_data, "eia", len(eia_symbols))
                except Exception as e:
                    logger.error(f"❌ Error collecting EIA data: {str(e)}")
                    raise Exception(f"EIA collection failed: {str(e)}")
            else:
                logger.info("No EIA symbols found")
        
        return results

    def collect_direct_source_data(self) -> Dict[str, pd.DataFrame]:
        """
        Collects data from sources that don't use the symbols DataFrame.
        
        Returns:
        - Dict[str, pd.DataFrame]: Dictionary mapping source names to DataFrames
        """
        results = {}
        
        # Baker Hughes data
        logger.info("=" * 50)
        logger.info("COLLECTING BAKER HUGHES DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('baker'):
            logger.info("🚫 Baker Hughes data collection SKIPPED (not in allowed sources)")
        else:
            try:
                baker_data = fetch_baker()
                results['baker'] = baker_data
                total_symbols = baker_data['symbol'].nunique() if not baker_data.empty else 0
                self._log_collection_stats(baker_data, "baker", total_symbols)
            except Exception as e:
                logger.error(f"❌ Error collecting Baker Hughes data: {str(e)}")
                raise Exception(f"Baker Hughes collection failed: {str(e)}")
        
        # FINRA data
        logger.info("=" * 50)
        logger.info("COLLECTING FINRA DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('finra'):
            logger.info("🚫 FINRA data collection SKIPPED (not in allowed sources)")
        else:
            try:
                finra_data = fetch_finra()
                results['finra'] = finra_data
                total_symbols = finra_data['symbol'].nunique() if not finra_data.empty else 0
                self._log_collection_stats(finra_data, "finra", total_symbols)
            except Exception as e:
                logger.error(f"❌ Error collecting FINRA data: {str(e)}")
                raise Exception(f"FINRA collection failed: {str(e)}")
        
        # S&P 500 data
        logger.info("=" * 50)
        logger.info("COLLECTING S&P 500 DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('sp500'):
            logger.info("🚫 S&P 500 data collection SKIPPED (not in allowed sources)")
        else:
            try:
                sp500_data = fetch_sp500()
                results['sp500'] = sp500_data
                total_symbols = sp500_data['symbol'].nunique() if not sp500_data.empty else 0
                self._log_collection_stats(sp500_data, "sp500", total_symbols)
            except Exception as e:
                logger.error(f"❌ Error collecting S&P 500 data: {str(e)}")
                raise Exception(f"S&P 500 collection failed: {str(e)}")
        
        # USDA data
        logger.info("=" * 50)
        logger.info("COLLECTING USDA DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('usda'):
            logger.info("🚫 USDA data collection SKIPPED (not in allowed sources)")
        else:
            try:
                usda_data = fetch_usda()
                results['usda'] = usda_data
                total_symbols = usda_data['symbol'].nunique() if not usda_data.empty else 0
                self._log_collection_stats(usda_data, "usda", total_symbols)
            except Exception as e:
                logger.error(f"❌ Error collecting USDA data: {str(e)}")
                raise Exception(f"USDA collection failed: {str(e)}")
        
        # OCC data
        logger.info("=" * 50)
        logger.info("COLLECTING OCC DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('occ'):
            logger.info("🚫 OCC data collection SKIPPED (not in allowed sources)")
        else:
            try:
                occ_data = fetch_occ()
                results['occ'] = occ_data
                total_symbols = occ_data['symbol'].nunique() if not occ_data.empty else 0
                self._log_collection_stats(occ_data, "occ", total_symbols)
            except Exception as e:
                logger.error(f"❌ Error collecting OCC data: {str(e)}")
                raise Exception(f"OCC collection failed: {str(e)}")
        
        return results

    def run_full_pipeline(self) -> bool:
        """
        Execute the complete data collection pipeline.
        
        Returns:
        - bool: True if successful, False if any step fails
        """
        logger.info("=" * 60)
        logger.info("STARTING DATA COLLECTION PIPELINE")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # Load symbols from CSV
            logger.info("📋 Loading symbols from data/symbols.csv...")
            symbols_df = self._load_symbols_from_csv()
            
            if symbols_df.empty:
                raise Exception("No symbols found in data/symbols.csv")
            
            # Filter out sources handled by direct calls
            symbols_df_filtered = symbols_df[
                ~symbols_df['source'].str.lower().isin(['bkr', 'finra', 'silverblatt', 'occ', 'usda'])
            ]
            
            logger.info(f"📊 Loaded {len(symbols_df)} total symbols, {len(symbols_df_filtered)} for symbol-based collection")
            
            # Collect symbol-based data
            logger.info("🔄 Starting symbol-based data collection...")
            symbol_results = self.collect_symbol_based_data(symbols_df_filtered)
            
            # Collect direct source data
            logger.info("🔄 Starting direct source data collection...")
            direct_results = self.collect_direct_source_data()
            
            # Combine all results
            all_results = {**symbol_results, **direct_results}
            
            # Save individual source files
            logger.info("💾 Saving individual source files...")
            for source, data in all_results.items():
                if not data.empty:
                    self.storage.save_source_data(source, data)
            
            # Save combined data
            logger.info("💾 Saving combined data file...")
            combined_path = self.storage.save_combined_data(all_results)
            
            # Summary statistics
            logger.info("=" * 60)
            logger.info("DATA COLLECTION SUMMARY")
            logger.info("=" * 60)
            
            total_rows = 0
            sources_with_data = 0
            
            for source_name, df in all_results.items():
                row_count = len(df)
                total_rows += row_count
                
                if row_count > 0:
                    sources_with_data += 1
            
            if total_rows > 0:
                logger.info(f"🎉 TOTAL ROWS COLLECTED: {total_rows:,}")
                logger.info(f"📈 SOURCES WITH DATA: {sources_with_data}/{len(all_results)}")
            else:
                logger.info("❌ NO DATA COLLECTED FROM ANY SOURCE")
            
            # Calculate and log timing
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"⏱️  Pipeline completed in {duration}")
            
            logger.info("=" * 60)
            logger.info("OUTPUT FILES")
            logger.info("=" * 60)
            if total_rows > 0:
                logger.info("📁 Data saved to:")
                logger.info("   • Individual source files: data/raw/{source}_{timestamp}.parquet")
                if combined_path:
                    logger.info(f"   • Combined file: {combined_path}")
                    logger.info("   • Latest combined: data/raw/all_sources_combined_latest.parquet")
            else:
                logger.info("📁 No output files created (no data collected)")
            
            return True
            
        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.error(f"❌ Pipeline failed after {duration}: {str(e)}")
            logger.error("🛑 Pipeline halted due to error. Please investigate and retry.")
            return False 