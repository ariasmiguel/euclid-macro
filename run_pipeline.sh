#!/bin/bash

# Euclid Macro Data Collection Pipeline Runner
# This script runs the data collection pipeline with comprehensive logging

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
LOGS_DIR="$PROJECT_ROOT/data/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOGS_DIR/pipeline_${TIMESTAMP}.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOGS_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] ✅${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] ⚠️${NC} $1"
}

print_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ❌${NC} $1"
}

# Function to log to file
log_to_file() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -s, --sources SOURCES    Comma-separated list of sources to collect"
    echo "                           (e.g., yahoo,fred,baker,sp500,finra,occ,usda,eia)"
    echo "  -h, --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                      # Collect from all sources"
    echo "  $0 -s sp500,baker       # Collect only S&P 500 and Baker Hughes data"
    echo "  $0 -s occ               # Collect only OCC data (long running)"
    echo ""
    echo "Available Sources:"
    echo "  • yahoo     - Yahoo Finance stock/ETF data"
    echo "  • fred      - Federal Reserve Economic Data (FRED)"
    echo "  • eia       - Energy Information Administration"
    echo "  • baker     - Baker Hughes rig count data"
    echo "  • finra     - FINRA margin statistics"
    echo "  • sp500     - S&P 500 earnings/estimates (Silverblatt)"
    echo "  • usda      - USDA agricultural data"
    echo "  • occ       - OCC options and futures volume data (long running)"
}

# Parse command line arguments
SOURCES=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--sources)
            SOURCES="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_status "🚀 Starting Euclid Macro Data Collection Pipeline"
    print_status "📁 Project root: $PROJECT_ROOT"
    print_status "📝 Log file: $LOG_FILE"
    
    # Log startup
    log_to_file "Pipeline started"
    log_to_file "Project root: $PROJECT_ROOT"
    log_to_file "Log file: $LOG_FILE"
    
    # Change to project directory
    cd "$PROJECT_ROOT"
    
    # Check if Python environment is available
    if ! command -v python &> /dev/null; then
        print_error "Python is not installed or not in PATH"
        log_to_file "ERROR: Python not found"
        exit 1
    fi
    
    # Check if required files exist
    if [[ ! -f "scripts/run_data_collection.py" ]]; then
        print_error "scripts/run_data_collection.py not found in project root"
        log_to_file "ERROR: scripts/run_data_collection.py not found"
        exit 1
    fi
    
    if [[ ! -f "data/symbols.csv" ]]; then
        print_warning "data/symbols.csv not found - some sources may fail"
        log_to_file "WARNING: data/symbols.csv not found"
    fi
    
    # Build command
    CMD="uv run python scripts/run_data_collection.py"
    if [[ -n "$SOURCES" ]]; then
        CMD="$CMD --sources $SOURCES"
        print_status "🎯 Source filtering enabled: $SOURCES"
        log_to_file "Source filtering: $SOURCES"
    else
        print_status "📊 All sources enabled"
        log_to_file "All sources enabled"
    fi
    
    # Run the pipeline
    print_status "⏳ Executing: $CMD"
    log_to_file "Executing: $CMD"
    
    START_TIME=$(date +%s)
    
    # Run the command and capture output
    if $CMD 2>&1 | tee -a "$LOG_FILE"; then
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        
        print_success "Pipeline completed successfully!"
        print_success "Duration: ${DURATION} seconds"
        log_to_file "Pipeline completed successfully in ${DURATION} seconds"
        
        # Show summary
        print_status "📊 Data files created:"
        if [[ -d "data/raw" ]]; then
            for source_dir in data/raw/*/; do
                if [[ -d "$source_dir" ]]; then
                    source_name=$(basename "$source_dir")
                    file_count=$(find "$source_dir" -name "*.parquet" | wc -l)
                    print_status "   • $source_name: $file_count files"
                fi
            done
        fi
        
        print_status "📝 Full log available at: $LOG_FILE"
        
    else
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        
        print_error "Pipeline failed after ${DURATION} seconds"
        log_to_file "Pipeline failed after ${DURATION} seconds"
        print_status "📝 Check log file for details: $LOG_FILE"
        exit 1
    fi
}

# Handle interrupts gracefully
trap 'print_warning "Pipeline interrupted by user"; log_to_file "Pipeline interrupted by user"; exit 1' INT TERM

# Run main function
main "$@" 