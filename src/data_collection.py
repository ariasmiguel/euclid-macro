"""
Simplified Data Collection Pipeline - Raw Data Only

This module handles raw data collection from various sources and saves
to parquet files without any transformation or standardization.
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

class RawDataCollector:
    """Handles raw data collection and storage without transformation"""
    
    def __init__(self, raw_path: str = 'data/raw'):
        self.raw_path = Path(raw_path)
        self.raw_path.mkdir(parents=True, exist_ok=True)
        
    def save_raw_data(self, source: str, data: pd.DataFrame) -> Optional[Path]:
        """Save raw data from a source to timestamped parquet file"""
        if data.empty:
            logger.warning(f"No data to save for {source}")
            return None
        
        # Create source-specific directory
        source_dir = self.raw_path / source
        source_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{source}_{timestamp}.parquet"
        filepath = source_dir / filename
        
        # Save raw data as-is
        data.to_parquet(filepath, index=False, compression='snappy')
        
        # Also save as 'latest' for easy access
        latest_path = source_dir / f"{source}_latest.parquet"
        data.to_parquet(latest_path, index=False, compression='snappy')
        
        logger.info(f"Saved {source} data: {len(data):,} rows to {filepath}")
        return filepath

class DataCollectionPipeline:
    """Raw data collection pipeline orchestrator"""
    
    def __init__(self, allowed_sources: list = None):
        self.collector = RawDataCollector()
        self.allowed_sources = [src.lower() for src in allowed_sources] if allowed_sources else None
        
        if self.allowed_sources:
            logger.info(f"🎯 Source filtering enabled: {', '.join(self.allowed_sources)}")
        else:
            logger.info("📊 All sources enabled")
    
    def _is_source_allowed(self, source_name: str) -> bool:
        """Check if a source is allowed based on the filter"""
        if self.allowed_sources is None:
            return True
        return source_name.lower() in self.allowed_sources
    
    def _log_collection_stats(self, df: pd.DataFrame, source_name: str) -> None:
        """Log statistics about collected data"""
        if df.empty:
            logger.info(f"✅ {source_name.upper()}: No data collected")
            return
            
        # Basic stats
        logger.info(f"✅ {source_name.upper()}: {len(df):,} rows collected")
        
        # Date range if date column exists
        if 'date' in df.columns:
            df_dates = pd.to_datetime(df['date'])
            date_range = f"{df_dates.min().strftime('%Y-%m-%d')} to {df_dates.max().strftime('%Y-%m-%d')}"
            logger.info(f"   📅 Date range: {date_range}")
        
        # Column info
        logger.info(f"   📊 Columns: {', '.join(df.columns[:10])}")
    
    def _load_symbols_from_csv(self) -> pd.DataFrame:
        """Load symbols from CSV file"""
        symbols_path = Path("data/symbols.csv")
        if not symbols_path.exists():
            raise Exception("data/symbols.csv not found. Please create this file with symbol data.")
        
        df = pd.read_csv(symbols_path)
        logger.info(f"Loaded {len(df)} symbols from data/symbols.csv")
        return df
    
    def _prepare_symbols_for_source(self, symbols_df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Filter symbols for a specific source"""
        source_symbols = symbols_df[symbols_df['source'].str.lower() == source.lower()].copy()
        
        if source_symbols.empty:
            logger.info(f"No symbols found for source: {source}")
        
        return source_symbols
    
    def collect_symbol_based_data(self, symbols_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Collect data from sources that use symbol lists"""
        results = {}
        
        # Yahoo Finance
        if self._is_source_allowed('yahoo'):
            logger.info("\n" + "="*50)
            logger.info("COLLECTING YAHOO FINANCE DATA")
            logger.info("="*50)
            
            yahoo_symbols = self._prepare_symbols_for_source(symbols_df, 'yahoo')
            if not yahoo_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(yahoo_symbols)} Yahoo symbols")
                    yahoo_data = fetch_yahoo(yahoo_symbols)
                    results['yahoo'] = yahoo_data
                    self._log_collection_stats(yahoo_data, "yahoo")
                except Exception as e:
                    logger.error(f"❌ Error collecting Yahoo data: {str(e)}")
                    results['yahoo'] = pd.DataFrame()
        
        # FRED
        if self._is_source_allowed('fred'):
            logger.info("\n" + "="*50)
            logger.info("COLLECTING FRED DATA")
            logger.info("="*50)
            
            fred_symbols = self._prepare_symbols_for_source(symbols_df, 'fred')
            if not fred_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(fred_symbols)} FRED series")
                    fred_data = fetch_fred(fred_symbols)
                    results['fred'] = fred_data
                    self._log_collection_stats(fred_data, "fred")
                except Exception as e:
                    logger.error(f"❌ Error collecting FRED data: {str(e)}")
                    results['fred'] = pd.DataFrame()
        
        # EIA
        if self._is_source_allowed('eia'):
            logger.info("\n" + "="*50)
            logger.info("COLLECTING EIA DATA")
            logger.info("="*50)
            
            eia_symbols = self._prepare_symbols_for_source(symbols_df, 'eia')
            if not eia_symbols.empty:
                try:
                    logger.info(f"📋 Processing {len(eia_symbols)} EIA series")
                    eia_data = fetch_eia(eia_symbols)
                    results['eia'] = eia_data
                    self._log_collection_stats(eia_data, "eia")
                except Exception as e:
                    logger.error(f"❌ Error collecting EIA data: {str(e)}")
                    results['eia'] = pd.DataFrame()
        
        return results

    def collect_direct_source_data(self) -> Dict[str, pd.DataFrame]:
        """Collect data from sources that don't use symbol lists"""
        results = {}
        
        # Baker Hughes
        if self._is_source_allowed('baker'):
            logger.info("\n" + "="*50)
            logger.info("COLLECTING BAKER HUGHES DATA")
            logger.info("="*50)
            
            try:
                baker_data = fetch_baker()
                results['baker'] = baker_data
                self._log_collection_stats(baker_data, "baker")
            except Exception as e:
                logger.error(f"❌ Error collecting Baker data: {str(e)}")
                results['baker'] = pd.DataFrame()
        
        # FINRA
        if self._is_source_allowed('finra'):
            logger.info("\n" + "="*50)
            logger.info("COLLECTING FINRA DATA")
            logger.info("="*50)
            
            try:
                finra_data = fetch_finra()
                results['finra'] = finra_data
                self._log_collection_stats(finra_data, "finra")
            except Exception as e:
                logger.error(f"❌ Error collecting FINRA data: {str(e)}")
                results['finra'] = pd.DataFrame()
        
        # S&P 500
        if self._is_source_allowed('sp500'):
            logger.info("\n" + "="*50)
            logger.info("COLLECTING S&P 500 DATA")
            logger.info("="*50)
            
            try:
                sp500_data = fetch_sp500()
                results['sp500'] = sp500_data
                self._log_collection_stats(sp500_data, "sp500")
            except Exception as e:
                logger.error(f"❌ Error collecting S&P 500 data: {str(e)}")
                results['sp500'] = pd.DataFrame()
        
        # USDA
        if self._is_source_allowed('usda'):
            logger.info("\n" + "="*50)
            logger.info("COLLECTING USDA DATA")
            logger.info("="*50)
            
            try:
                usda_data = fetch_usda()
                results['usda'] = usda_data
                self._log_collection_stats(usda_data, "usda")
            except Exception as e:
                logger.error(f"❌ Error collecting USDA data: {str(e)}")
                results['usda'] = pd.DataFrame()
        
        # OCC
        if self._is_source_allowed('occ'):
            logger.info("\n" + "="*50)
            logger.info("COLLECTING OCC DATA")
            logger.info("="*50)
            
            try:
                occ_data = fetch_occ()
                results['occ'] = occ_data
                self._log_collection_stats(occ_data, "occ")
            except Exception as e:
                logger.error(f"❌ Error collecting OCC data: {str(e)}")
                results['occ'] = pd.DataFrame()
        
        return results

    def run_raw_collection(self) -> bool:
        """Execute raw data collection only"""
        logger.info("="*60)
        logger.info("STARTING RAW DATA COLLECTION")
        logger.info("="*60)
        
        start_time = datetime.now()
        
        try:
            # Load symbols
            logger.info("📋 Loading symbols from data/symbols.csv...")
            symbols_df = self._load_symbols_from_csv()
            
            if symbols_df.empty:
                raise Exception("No symbols found in data/symbols.csv")
            
            # Filter symbols for direct sources
            symbols_df_filtered = symbols_df[
                ~symbols_df['source'].str.lower().isin(['bkr', 'finra', 'silverblatt', 'occ', 'usda'])
            ]
            
            logger.info(f"📊 Loaded {len(symbols_df)} total symbols")
            logger.info(f"📊 {len(symbols_df_filtered)} for symbol-based collection")
            
            # Collect symbol-based data
            logger.info("\n🔄 Starting symbol-based data collection...")
            symbol_results = self.collect_symbol_based_data(symbols_df_filtered)
            
            # Collect direct source data
            logger.info("\n🔄 Starting direct source data collection...")
            direct_results = self.collect_direct_source_data()
            
            # Combine results
            all_results = {**symbol_results, **direct_results}
            
            # Save raw data for each source
            logger.info("\n💾 Saving raw data files...")
            saved_files = []
            for source, data in all_results.items():
                if not data.empty:
                    filepath = self.collector.save_raw_data(source, data)
                    if filepath:
                        saved_files.append(filepath)
            
            # Summary
            logger.info("\n" + "="*60)
            logger.info("RAW DATA COLLECTION SUMMARY")
            logger.info("="*60)
            
            total_rows = sum(len(df) for df in all_results.values())
            sources_with_data = sum(1 for df in all_results.values() if not df.empty)
            
            logger.info(f"✅ Sources with data: {sources_with_data}/{len(all_results)}")
            logger.info(f"📊 Total rows collected: {total_rows:,}")
            logger.info(f"💾 Files saved: {len(saved_files)}")
            
            # Timing
            duration = datetime.now() - start_time
            logger.info(f"⏱️  Collection completed in {duration}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Raw collection failed: {str(e)}")
            return False