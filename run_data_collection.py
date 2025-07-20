"""
Main Data Collection Pipeline Entry Point

This script provides a simple entry point for running the complete
data collection pipeline with simplified parquet storage.
"""

import argparse
import logging
import sys
from datetime import datetime

# Import the main pipeline class
from src.data_collection import DataCollectionPipeline

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
        description='Euclid Macro Data Collection Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_data_collection.py                                 # Collect from all sources
    python run_data_collection.py --sources yahoo,fred,baker      # Only collect from specific sources

Available Sources:
    • yahoo     - Yahoo Finance stock/ETF data
    • fred      - Federal Reserve Economic Data (FRED)
    • eia       - Energy Information Administration
    • baker     - Baker Hughes rig count data
    • finra     - FINRA margin statistics
    • sp500     - S&P 500 earnings/estimates (Silverblatt)
    • usda      - USDA agricultural data
    • occ       - OCC options and futures volume data

Features:
    • Simple parquet storage: One file per source + combined file
    • Source filtering: Test specific sources during development
    • Respects API rate limits (especially important for FRED)
    • Fast pipeline execution with optimal parquet storage
        """
    )
    
    parser.add_argument('--sources', type=str,
                       help='Comma-separated list of sources to collect (e.g., yahoo,fred,baker). Leave empty for all sources.')
    
    args = parser.parse_args()
    
    # Parse sources list
    allowed_sources = None
    if args.sources:
        allowed_sources = [source.strip() for source in args.sources.split(',')]
        logger.info(f"🎯 Source filtering: {', '.join(allowed_sources)}")
    
    logger.info("🚀 Euclid Macro Data Collection Pipeline")
    logger.info(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Initialize and run the data collection pipeline
        pipeline = DataCollectionPipeline(allowed_sources=allowed_sources)
        success = pipeline.run_full_pipeline()
        
        if not success:
            logger.error("❌ Data collection pipeline failed")
            sys.exit(1)
        else:
            logger.info("🎉 Data collection pipeline completed successfully!")
            logger.info("📊 Data is now available in:")
            logger.info("   • Individual source files: data/raw/{source}_{timestamp}.parquet")
            logger.info("   • Combined file: data/raw/all_sources_combined_latest.parquet")
            
    except KeyboardInterrupt:
        logger.info("⏹️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Fatal error in data collection pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
