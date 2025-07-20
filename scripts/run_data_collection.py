"""
Main Data Collection Pipeline Entry Point

This script provides a simple entry point for running the complete
data collection pipeline using DuckDB and bronze layer exports
with incremental loading support.
"""

import argparse
import logging
import sys
from datetime import datetime

# Import the main pipeline class
from src_pipeline.pipelines.data_collection import DataCollectionPipeline
from src_pipeline.core.date_utils import DateUtils

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main function that orchestrates the entire data collection pipeline.
    """
    parser = argparse.ArgumentParser(
        description='Bristol Gate Data Collection Pipeline with Incremental Loading',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_data_collection.py                                 # Incremental loading (default)
    python run_data_collection.py --full-refresh                  # Full refresh (reload all data)
    python run_data_collection.py --incremental                   # Explicit incremental mode
    python run_data_collection.py --sources yahoo,fred,baker      # Only collect from specific sources
    python run_data_collection.py --sources yahoo --full-refresh  # Full refresh for Yahoo only

Available Sources:
    • yahoo     - Yahoo Finance stock/ETF data
    • fred      - Federal Reserve Economic Data (FRED)
    • eia       - Energy Information Administration
    • baker     - Baker Hughes rig count data
    • finra     - FINRA margin statistics
    • sp500     - S&P 500 earnings/estimates (Silverblatt)
    • usda      - USDA agricultural data

Features:
    • Incremental loading: Only fetches data newer than existing data
    • Source filtering: Test specific sources during development
    • Respects API rate limits (especially important for FRED)
    • Append-only bronze layer for complete audit trail
    • Fast pipeline execution with optimal DuckDB storage
        """
    )
    
    parser.add_argument('--incremental', action='store_true', default=True,
                       help='Use incremental loading (default)')
    parser.add_argument('--full-refresh', action='store_true',
                       help='Perform full refresh (reload all data)')
    parser.add_argument('--sources', type=str,
                       help='Comma-separated list of sources to collect (e.g., yahoo,fred,baker). Leave empty for all sources.')
    
    args = parser.parse_args()
    
    # Parse sources list
    allowed_sources = None
    if args.sources:
        allowed_sources = [source.strip() for source in args.sources.split(',')]
        logger.info(f"🎯 Source filtering: {', '.join(allowed_sources)}")
    
    # Determine incremental mode
    incremental_mode = not args.full_refresh
    
    logger.info("🚀 Bristol Gate Data Collection Pipeline")
    logger.info(f"⏰ Started at: {DateUtils.format_current_datetime()}")
    logger.info(f"📋 Mode: {'INCREMENTAL' if incremental_mode else 'FULL REFRESH'}")
    
    try:
        # Initialize and run the data collection pipeline
        pipeline = DataCollectionPipeline(incremental=incremental_mode, allowed_sources=allowed_sources)
        success = pipeline.run_full_pipeline()
        
        if not success:
            logger.error("❌ Data collection pipeline failed")
            sys.exit(1)
        else:
            logger.info("🎉 Data collection pipeline completed successfully!")
            logger.info("📊 Data is now available in:")
            logger.info("   • DuckDB staging tables (bristol_gate.duckdb)")
            logger.info("   • Bronze layer parquet files (data/bronze/)")
            
            if incremental_mode:
                logger.info("💡 Tip: Use --full-refresh to reload all data if needed")
            
    except KeyboardInterrupt:
        logger.info("⏹️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Fatal error in data collection pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
