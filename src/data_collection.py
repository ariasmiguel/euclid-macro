"""
Data Collection Pipeline with Symbol-Level Incremental Loading

This module contains the main DataCollectionPipeline class that orchestrates
the data collection from various sources with symbol-level incremental loading support.
"""

import pandas as pd
from datetime import datetime
import logging
from typing import Dict
import sys

# Import individual fetch functions
from .fetchers.fetch_yahoo import fetch_yahoo
from .fetchers.fetch_fred import fetch_fred
from .fetchers.fetch_eia import fetch_eia
from .fetchers.fetch_baker import fetch_baker
from .fetchers.fetch_finra import fetch_finra
from .fetchers.fetch_sp500 import fetch_sp500
from .fetchers.fetch_usda import fetch_usda
from .fetchers.fetch_occ import fetch_occ

# Import utilities
from ..core.utils import (
    SymbolManager, 
    DataPipelineManager,
    DataValidator,
    SOURCE_SCHEMAS
)
from symbol_processor import SymbolProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCollectionPipeline:
    """Main data collection pipeline orchestrator with symbol-level incremental loading"""
    
    def __init__(self, db_path: str = 'bristol_gate.duckdb', incremental: bool = True, allowed_sources: list = None):
        self.symbol_manager = SymbolManager(db_path)
        self.pipeline_manager = DataPipelineManager(db_path)
        self.validator = DataValidator()
        self.symbol_processor = SymbolProcessor()
        self.incremental = incremental
        self.allowed_sources = [src.lower() for src in allowed_sources] if allowed_sources else None
        
        if incremental:
            logger.info("🔄 Pipeline configured for INCREMENTAL loading (symbol-level)")
        else:
            logger.info("🔃 Pipeline configured for FULL REFRESH loading")
            
        if self.allowed_sources:
            logger.info(f"🎯 Source filtering enabled: {', '.join(self.allowed_sources)}")
        else:
            logger.info("📊 All sources enabled")
        
    def _log_collection_stats(self, df: pd.DataFrame, source_name: str, total_symbols: int) -> None:
        """Log detailed statistics about data collection for a source."""
        if df.empty:
            logger.info(f"✅ {source_name.upper()}: No new data (up to date)")
            return
            
        # Get the identifier column for this source
        source_schema = SOURCE_SCHEMAS.get(source_name.lower())
        if source_schema:
            identifier_column = source_schema['identifier_column']
            unique_identifiers = df[identifier_column].nunique() if identifier_column in df.columns else 0
            identifier_name = "symbols" if identifier_column == "symbol" else "series"
            
            # Date range information
            df_dates = pd.to_datetime(df['date'])
            date_range = f"{df_dates.min().strftime('%Y-%m-%d')} to {df_dates.max().strftime('%Y-%m-%d')}"
            
            logger.info(f"✅ {source_name.upper()}: {len(df):,} NEW rows")
            logger.info(f"   📊 {unique_identifiers}/{total_symbols} {identifier_name} with new data")
            logger.info(f"   📅 Date range: {date_range}")
        else:
            logger.info(f"✅ {source_name.upper()}: {len(df):,} NEW rows processed successfully")
        
    def _is_source_allowed(self, source_name: str) -> bool:
        """Check if a source is allowed based on the allowed_sources filter."""
        if self.allowed_sources is None:
            return True
        return source_name.lower() in self.allowed_sources
        
    def collect_symbol_based_data(self, symbols_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Collects data from sources that use the symbols DataFrame with symbol-level incremental loading.
        
        Parameters:
        - symbols_df (pd.DataFrame): DataFrame containing symbol information
        
        Returns:
        - Dict[str, pd.DataFrame]: Dictionary mapping source names to DataFrames
        
        Raises:
        - Exception: If any data collection fails (halt on error)
        """
        results = {}
        
        # Yahoo Finance data
        logger.info("=" * 50)
        logger.info("COLLECTING YAHOO FINANCE DATA")
        logger.info("=" * 50)
        
        if not self._is_source_allowed('yahoo'):
            logger.info("🚫 Yahoo Finance data collection SKIPPED (not in allowed sources)")
        else:
            yahoo_symbols = symbols_df[symbols_df['source'].str.lower() == 'yahoo'].copy()
            if not yahoo_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(yahoo_symbols)} Yahoo Finance symbols")
                    
                    # Use SymbolProcessor for standardized symbol processing
                    yahoo_symbols_renamed = self.symbol_processor.prepare_symbols_for_fetch(
                        yahoo_symbols, 'yahoo'
                    )
                    
                    yahoo_data = fetch_yahoo(yahoo_symbols_renamed)
                    
                    if not self.validator.validate_dataframe(yahoo_data, "yahoo"):
                        raise Exception("Yahoo Finance data validation failed")
                    
                    results['yahoo'] = yahoo_data
                    
                    if not self.pipeline_manager.store_to_staging_table(
                        yahoo_data, "stg_yahoo", "yahoo", incremental=self.incremental
                    ):
                        raise Exception("Failed to store Yahoo Finance data")
                    
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
            fred_symbols = symbols_df[symbols_df['source'].str.lower() == 'fred'].copy()
            if not fred_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(fred_symbols)} FRED series")
                    
                    # Use SymbolProcessor for standardized symbol processing
                    fred_symbols_renamed = self.symbol_processor.prepare_symbols_for_fetch(
                        fred_symbols, 'fred'
                    )
                    
                    fred_data = fetch_fred(fred_symbols_renamed)
                    
                    if not self.validator.validate_dataframe(fred_data, "fred"):
                        raise Exception("FRED data validation failed")
                    
                    results['fred'] = fred_data
                    
                    if not self.pipeline_manager.store_to_staging_table(
                        fred_data, "stg_fred", "fred", incremental=self.incremental
                    ):
                        raise Exception("Failed to store FRED data")
                    
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
            eia_symbols = symbols_df[symbols_df['source'].str.lower() == 'eia'].copy()
            if not eia_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(eia_symbols)} EIA series")
                    
                    # Use SymbolProcessor for standardized symbol processing
                    eia_symbols_renamed = self.symbol_processor.prepare_symbols_for_fetch(
                        eia_symbols, 'eia'
                    )
                    
                    eia_data = fetch_eia(eia_symbols_renamed)
                    
                    if not self.validator.validate_dataframe(eia_data, "eia"):
                        raise Exception("EIA data validation failed")
                    
                    results['eia'] = eia_data
                    
                    if not self.pipeline_manager.store_to_staging_table(
                        eia_data, "stg_eia", "eia", incremental=self.incremental
                    ):
                        raise Exception("Failed to store EIA data")
                    
                    self._log_collection_stats(eia_data, "eia", len(eia_symbols))
                    
                except Exception as e:
                    logger.error(f"❌ Error collecting EIA data: {str(e)}")
                    raise Exception(f"EIA collection failed: {str(e)}")
            else:
                logger.info("No EIA symbols found")
        
        return results

    def collect_direct_source_data(self) -> Dict[str, pd.DataFrame]:
        """
        Collects data from sources that don't use the symbols DataFrame with symbol-level incremental loading.
        
        Returns:
        - Dict[str, pd.DataFrame]: Dictionary mapping source names to DataFrames
        
        Raises:
        - Exception: If any data collection fails (halt on error)
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
                
                if not self.validator.validate_dataframe(baker_data, "baker"):
                    raise Exception("Baker Hughes data validation failed")
                
                results['baker'] = baker_data
                
                if not self.pipeline_manager.store_to_staging_table(
                    baker_data, "stg_baker", "baker", incremental=self.incremental
                ):
                    raise Exception("Failed to store Baker Hughes data")
                
                # Count unique symbols for Baker Hughes
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
                
                if not self.validator.validate_dataframe(finra_data, "finra"):
                    raise Exception("FINRA data validation failed")
                
                results['finra'] = finra_data
                
                if not self.pipeline_manager.store_to_staging_table(
                    finra_data, "stg_finra", "finra", incremental=self.incremental
                ):
                    raise Exception("Failed to store FINRA data")
                
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
                
                if not self.validator.validate_dataframe(sp500_data, "sp500"):
                    raise Exception("S&P 500 data validation failed")
                
                results['sp500'] = sp500_data
                
                if not self.pipeline_manager.store_to_staging_table(
                    sp500_data, "stg_sp500", "sp500", incremental=self.incremental
                ):
                    raise Exception("Failed to store S&P 500 data")
                
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
                
                if not self.validator.validate_dataframe(usda_data, "usda"):
                    raise Exception("USDA data validation failed")
                
                results['usda'] = usda_data
                
                if not self.pipeline_manager.store_to_staging_table(
                    usda_data, "stg_usda", "usda", incremental=self.incremental
                ):
                    raise Exception("Failed to store USDA data")
                
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
                
                if not self.validator.validate_dataframe(occ_data, "occ"):
                    raise Exception("OCC data validation failed")
                
                results['occ'] = occ_data
                
                if not self.pipeline_manager.store_to_staging_table(
                    occ_data, "stg_occ", "occ", incremental=self.incremental
                ):
                    raise Exception("Failed to store OCC data")
                
                total_symbols = occ_data['symbol'].nunique() if not occ_data.empty else 0
                self._log_collection_stats(occ_data, "occ", total_symbols)
                
            except Exception as e:
                logger.error(f"❌ Error collecting OCC data: {str(e)}")
                raise Exception(f"OCC collection failed: {str(e)}")
        
        return results

    def run_full_pipeline(self) -> bool:
        """
        Execute the complete data collection pipeline with symbol-level incremental loading.
        
        Returns:
        - bool: True if successful, False if any step fails
        """
        logger.info("=" * 60)
        logger.info("STARTING DATA COLLECTION PIPELINE")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # Load symbols from DuckDB
            logger.info("📋 Loading symbols from DuckDB...")
            symbols_df = self.symbol_manager.load_symbols_from_db()
            
            if symbols_df.empty:
                raise Exception("No symbols found in database. Please run setup_duckdb.py --load-symbols first.")
            
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
            
            # Combine all results for summary
            all_results = {**symbol_results, **direct_results}
            
            # Summary statistics
            logger.info("=" * 60)
            logger.info("DATA COLLECTION SUMMARY")
            logger.info("=" * 60)
            
            total_rows = 0
            sources_with_new_data = 0
            
            for source_name, df in all_results.items():
                row_count = len(df)
                total_rows += row_count
                
                if row_count > 0:
                    sources_with_new_data += 1
            
            if total_rows > 0:
                logger.info(f"🎉 TOTAL NEW ROWS COLLECTED: {total_rows:,}")
                logger.info(f"📈 SOURCES WITH NEW DATA: {sources_with_new_data}/{len(all_results)}")
            else:
                logger.info("✅ ALL SOURCES UP TO DATE - No new data to collect")
                logger.info(f"🔍 SOURCES CHECKED: {len(all_results)}")
            
            # Calculate and log timing
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"⏱️  Pipeline completed in {duration}")
            
            logger.info("=" * 60)
            logger.info("BRONZE LAYER EXPORTS")
            logger.info("=" * 60)
            if total_rows > 0:
                logger.info("📁 New data exported to bronze layer:")
                logger.info("   data/bronze/{source}/{source}_{timestamp}_{rows}rows.parquet")
                logger.info("💡 You can query these files directly with DuckDB for optimal performance!")
            else:
                logger.info("📁 No new bronze layer exports (no new data)")
            
            return True
            
        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.error(f"❌ Pipeline failed after {duration}: {str(e)}")
            logger.error("🛑 Pipeline halted due to error. Please investigate and retry.")
            return False 