# 🎯 Data Collection Update Plan - Simplified Storage

## **Goal**
Update the data collection pipeline to:
- Remove all DuckDB dependencies
- Implement parallel fetching from all sources
- Save one parquet file per source
- Create a combined long-format parquet file
- Prepare for ClickHouse Cloud upload

## **Phase 1: Core Infrastructure Updates**

### **1.1 Update Project Dependencies**
Remove DuckDB and add ClickHouse dependencies:

```toml
# pyproject.toml - Data fetching dependencies only
[project]
dependencies = [
    # Core data handling
    "pandas>=2.0.0",
    "numpy>=1.26.0",
    "pyarrow>=14.0.0",
    
    # Data sources
    "yfinance>=0.2.18",
    "fredapi>=0.5.1",
    "myeia>=0.4.8",
    "requests>=2.31.0",
    "selenium>=4.15.0",
    "selenium-stealth>=1.0.6",
    "webdriver-manager>=4.0.0",
    "openpyxl>=3.1.2",
    "beautifulsoup4>=4.12.0",
    "lxml>=4.9.0",
    
    # Database
    "clickhouse-connect>=0.7.0",
    
    # Utilities
    "python-dotenv>=1.0.0",
    "rich>=13.0.0",
    "typer>=0.9.0",
    "loguru>=0.7.0",
    
    # Parallel processing
    "concurrent-futures-backport>=3.1.0",
    "tqdm>=4.65.0",
]
```

### **1.2 Simplified Storage Manager**
One parquet per source + combined file:

```python
# src/core/storage_manager.py
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger
import clickhouse_connect
from dotenv import load_dotenv
import os

load_dotenv()

class StorageManager:
    def __init__(self):
        self.raw_path = Path("data/raw")
        self.symbols_path = Path("data/symbols.csv")
        self.logs_path = Path("data/logs")
        
        # Create directories
        for path in [self.raw_path, self.logs_path]:
            path.mkdir(parents=True, exist_ok=True)
            
        # ClickHouse client
        self._ch_client = None
    
    @property
    def ch_client(self):
        """Lazy load ClickHouse client"""
        if self._ch_client is None:
            self._ch_client = clickhouse_connect.get_client(
                host=os.getenv('CLICKHOUSE_HOST'),
                password=os.getenv('CLICKHOUSE_PASSWORD'),
                port=8443,
                secure=True
            )
        return self._ch_client
    
    def save_source_data(self, source: str, data: pd.DataFrame) -> Path:
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
    
    def save_combined_data(self, all_data: Dict[str, pd.DataFrame]) -> Path:
        """Combine all source data into single long-format parquet"""
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
                combined_dfs.append(df)
        
        if not combined_dfs:
            return None
            
        # Create long format dataframe
        long_df = pd.concat(combined_dfs, ignore_index=True)
        
        # Ensure consistent column order
        required_cols = ['date', 'symbol', 'value', 'source']
        extra_cols = [col for col in long_df.columns if col not in required_cols]
        column_order = required_cols + extra_cols
        long_df = long_df[[col for col in column_order if col in long_df.columns]]
        
        # Save combined file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"all_sources_combined_{timestamp}.parquet"
        file_path = self.raw_path / filename
        long_df.to_parquet(file_path, index=False, compression='snappy')
        
        # Also save as 'latest' for easy access
        latest_path = self.raw_path / "all_sources_combined_latest.parquet"
        long_df.to_parquet(latest_path, index=False, compression='snappy')
        
        logger.info(f"Saved combined data: {len(long_df):,} total rows to {file_path}")
        return file_path
    
    def upload_to_clickhouse(self, data: pd.DataFrame, table_name: str):
        """Upload dataframe to ClickHouse Cloud"""
        try:
            # Create table if not exists
            if table_name == "macro_raw_data":
                self.ch_client.command("""
                    CREATE TABLE IF NOT EXISTS macro_raw_data (
                        date Date,
                        symbol String,
                        value Float64,
                        source String,
                        volume Nullable(Float64),
                        open Nullable(Float64),
                        high Nullable(Float64),
                        low Nullable(Float64)
                    ) ENGINE = MergeTree()
                    ORDER BY (date, symbol, source)
                """)
            elif table_name == "macro_symbols":
                self.ch_client.command("""
                    CREATE TABLE IF NOT EXISTS macro_symbols (
                        symbol String,
                        source String,
                        description String,
                        unit String
                    ) ENGINE = MergeTree()
                    ORDER BY (source, symbol)
                """)
            
            # Upload data
            self.ch_client.insert_df(table_name, data)
            logger.info(f"Uploaded {len(data):,} rows to ClickHouse table: {table_name}")
            
        except Exception as e:
            logger.error(f"ClickHouse upload failed: {str(e)}")
            raise
    
    def load_symbols(self) -> pd.DataFrame:
        """Load symbols from CSV"""
        if self.symbols_path.exists():
            return pd.read_csv(self.symbols_path)
        else:
            # Create template
            df = pd.DataFrame(columns=['symbol', 'source', 'description', 'unit'])
            df.to_csv(self.symbols_path, index=False)
            logger.warning("Created empty symbols.csv - please populate it")
            return df
```

## **Phase 2: Parallel Data Collection**

### **2.1 Enhanced Base Fetcher**
Return all data as single dataframe per source:

```python
# src/core/base_fetcher.py
from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from loguru import logger
import time

class BaseDataFetcher(ABC):
    """Base class with parallel fetching support"""
    
    def __init__(self, source_name: str, max_workers: int = 10):
        self.source_name = source_name
        self.max_workers = max_workers
        self.logger = logger.bind(source=source_name)
        
    @abstractmethod
    def fetch_single_series(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch a single data series - must be implemented by subclasses"""
        pass
    
    def fetch_batch_parallel(self, symbols_df: pd.DataFrame, 
                           start_date: Optional[datetime] = None,
                           end_date: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch multiple series in parallel and return combined dataframe"""
        
        # Default date range
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = datetime(1950, 1, 1)
        
        all_data = []
        failed_symbols = []
        
        # Filter symbols for this source
        source_symbols = symbols_df[symbols_df['source'].str.lower() == self.source_name.lower()]
        
        if source_symbols.empty:
            self.logger.warning(f"No symbols found for {self.source_name}")
            return pd.DataFrame()
        
        self.logger.info(f"Starting parallel fetch for {len(source_symbols)} symbols with {self.max_workers} workers")
        
        # Create progress bar
        with tqdm(total=len(source_symbols), desc=f"{self.source_name} Progress") as pbar:
            # Use ThreadPoolExecutor for parallel fetching
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_symbol = {
                    executor.submit(
                        self._fetch_with_retry, 
                        row['symbol'], 
                        start_date, 
                        end_date
                    ): row['symbol'] 
                    for _, row in source_symbols.iterrows()
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        data = future.result()
                        if not data.empty:
                            all_data.append(data)
                            pbar.set_postfix({"collected": len(all_data), "failed": len(failed_symbols)})
                        else:
                            failed_symbols.append(symbol)
                    except Exception as e:
                        self.logger.error(f"Failed to fetch {symbol}: {str(e)}")
                        failed_symbols.append(symbol)
                    finally:
                        pbar.update(1)
        
        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Log summary
            self.logger.info(
                f"✅ {self.source_name} complete: "
                f"{len(all_data)}/{len(source_symbols)} symbols successful, "
                f"{len(combined_df):,} total rows"
            )
            
            if failed_symbols:
                self.logger.warning(f"Failed symbols: {', '.join(failed_symbols[:10])}")
            
            return combined_df
        else:
            self.logger.warning(f"No data collected for {self.source_name}")
            return pd.DataFrame()
    
    def _fetch_with_retry(self, symbol: str, start_date: datetime, 
                         end_date: datetime, max_retries: int = 3) -> pd.DataFrame:
        """Fetch with retry logic"""
        for attempt in range(max_retries):
            try:
                data = self.fetch_single_series(symbol, start_date, end_date)
                if not data.empty:
                    return data
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                wait_time = (attempt + 1) * 2  # Exponential backoff
                self.logger.debug(f"Retry {attempt + 1} for {symbol} after {wait_time}s")
                time.sleep(wait_time)
        
        return pd.DataFrame()
```

### **2.2 Main Collection Script**
Collect all sources and save combined file:

```python
# scripts/collect_data.py
import typer
from rich.console import Console
from rich.table import Table
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import Dict
import time

from src.core.storage_manager import StorageManager
from src.core.logging_setup import setup_logging, console

app = typer.Typer(help="Raw Data Collection Pipeline")

# Map of available sources
FETCHER_MAP = {
    'yahoo': ('src.fetchers.yahoo_fetcher', 'YahooFetcher'),
    'fred': ('src.fetchers.fred_fetcher', 'FredFetcher'),
    'eia': ('src.fetchers.eia_fetcher', 'EIAFetcher'),
    'baker': ('src.fetchers.baker_fetcher', 'BakerFetcher'),
    'finra': ('src.fetchers.finra_fetcher', 'FINRAFetcher'),
    'sp500': ('src.fetchers.sp500_fetcher', 'SP500Fetcher'),
    'usda': ('src.fetchers.usda_fetcher', 'USDAFetcher'),
}

def collect_source(source: str, symbols_df: pd.DataFrame, 
                  start_date: datetime, end_date: datetime) -> tuple[str, pd.DataFrame]:
    """Collect data for a single source and return the data"""
    logger = setup_logging()
    
    try:
        # Dynamic import
        module_name, class_name = FETCHER_MAP[source]
        module = __import__(module_name, fromlist=[class_name])
        fetcher_class = getattr(module, class_name)
        fetcher = fetcher_class()
        
        # Fetch data in parallel (returns combined dataframe)
        data = fetcher.fetch_batch_parallel(symbols_df, start_date, end_date)
        
        return source, data
        
    except Exception as e:
        logger.error(f"Error collecting {source}: {str(e)}")
        return source, pd.DataFrame()

@app.command()
def collect(
    sources: str = typer.Option("all", help="Comma-separated sources or 'all'"),
    start_date: str = typer.Option("1950-01-01", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, help="End date (YYYY-MM-DD), defaults to today"),
    max_workers: int = typer.Option(4, help="Number of parallel processes"),
    upload: bool = typer.Option(False, help="Upload to ClickHouse after collection"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging")
):
    """🚀 Collect raw financial data in parallel"""
    
    # Setup
    logger = setup_logging(verbose)
    storage = StorageManager()
    
    # Parse dates
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date) if end_date else datetime.now()
    
    console.print(f"📊 [bold blue]Starting parallel data collection[/bold blue]")
    console.print(f"📅 Date range: {start_dt.date()} to {end_dt.date()}")
    
    # Load symbols
    symbols_df = storage.load_symbols()
    if symbols_df.empty:
        console.print("❌ [red]No symbols found in data/symbols.csv![/red]")
        return
    
    # Determine sources to collect
    if sources.lower() == "all":
        sources_list = list(FETCHER_MAP.keys())
    else:
        sources_list = [s.strip() for s in sources.split(',')]
        # Validate sources
        invalid = [s for s in sources_list if s not in FETCHER_MAP]
        if invalid:
            console.print(f"❌ Invalid sources: {', '.join(invalid)}")
            return
    
    console.print(f"📦 Sources to collect: {', '.join(sources_list)}")
    console.print(f"🔄 Using {max_workers} parallel processes")
    
    start_time = time.time()
    
    # Collect data from all sources in parallel
    all_source_data = {}
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_source = {
            executor.submit(
                collect_source, 
                source, 
                symbols_df, 
                start_dt, 
                end_dt
            ): source 
            for source in sources_list
        }
        
        # Collect results as they complete
        with console.status("[bold green]Collecting data from sources...") as status:
            for future in as_completed(future_to_source):
                try:
                    source, data = future.result()
                    
                    if not data.empty:
                        # Save source data
                        storage.save_source_data(source, data)
                        all_source_data[source] = data
                        status.update(f"[bold green]Collected {source}: {len(data):,} rows")
                    else:
                        logger.warning(f"No data collected for {source}")
                        
                except Exception as e:
                    logger.error(f"Process failed: {str(e)}")
    
    # Save combined data
    if all_source_data:
        console.print("\n📊 [bold]Combining all sources...[/bold]")
        combined_path = storage.save_combined_data(all_source_data)
        
        # Summary statistics
        elapsed_time = time.time() - start_time
        
        # Create summary table
        table = Table(title="Collection Summary", show_header=True)
        table.add_column("Source", style="cyan")
        table.add_column("Symbols", justify="right")
        table.add_column("Rows", justify="right", style="green")
        
        total_rows = 0
        for source, data in all_source_data.items():
            symbols_count = data['symbol'].nunique() if 'symbol' in data.columns else 0
            table.add_row(source, str(symbols_count), f"{len(data):,}")
            total_rows += len(data)
        
        table.add_row("─" * 10, "─" * 10, "─" * 10)
        table.add_row("[bold]TOTAL", "[bold]All", f"[bold]{total_rows:,}")
        
        console.print(table)
        console.print(f"\n⏱️  Total time: {elapsed_time:.1f} seconds")
        console.print(f"💾 Combined file: {combined_path}")
        
        # Upload to ClickHouse if requested
        if upload:
            console.print("\n☁️  [bold]Uploading to ClickHouse...[/bold]")
            
            # Load combined data
            combined_df = pd.read_parquet(combined_path)
            
            # Upload raw data
            storage.upload_to_clickhouse(combined_df, "macro_raw_data")
            
            # Upload symbols
            storage.upload_to_clickhouse(symbols_df, "macro_symbols")
            
            console.print("✅ [bold green]Upload complete![/bold green]")
    else:
        console.print("❌ No data collected from any source")

@app.command()
def upload(
    file: str = typer.Option("all_sources_combined_latest.parquet", help="File to upload"),
    table: str = typer.Option("macro_raw_data", help="Target table name")
):
    """☁️ Upload existing parquet file to ClickHouse"""
    logger = setup_logging()
    storage = StorageManager()
    
    file_path = storage.raw_path / file
    
    if not file_path.exists():
        console.print(f"❌ File not found: {file_path}")
        return
    
    console.print(f"📂 Loading {file_path}...")
    data = pd.read_parquet(file_path)
    console.print(f"📊 Loaded {len(data):,} rows")
    
    console.print(f"☁️  Uploading to ClickHouse table: {table}...")
    storage.upload_to_clickhouse(data, table)
    
    console.print("✅ Upload complete!")

@app.command()
def test(
    source: str = typer.Argument(..., help="Source to test (e.g., yahoo)"),
    symbol: str = typer.Argument(..., help="Symbol to test (e.g., AAPL)"),
    days: int = typer.Option(30, help="Number of days to fetch")
):
    """🧪 Test single symbol fetch"""
    logger = setup_logging(True)  # Always verbose for testing
    
    # Import fetcher
    module_name, class_name = FETCHER_MAP[source]
    module = __import__(module_name, fromlist=[class_name])
    fetcher_class = getattr(module, class_name)
    fetcher = fetcher_class()
    
    # Test fetch
    end_date = datetime.now()
    start_date = end_date - pd.Timedelta(days=days)
    
    console.print(f"🧪 Testing {source} fetcher with {symbol}...")
    
    try:
        data = fetcher.fetch_single_series(symbol, start_date, end_date)
        
        if not data.empty:
            console.print(f"✅ Success! Retrieved {len(data)} rows")
            console.print(f"📊 Date range: {data['date'].min()} to {data['date'].max()}")
            console.print(f"📈 Columns: {list(data.columns)}")
            console.print("\n🔍 Sample data:")
            console.print(data.head())
        else:
            console.print("❌ No data retrieved")
            
    except Exception as e:
        console.print(f"❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    app()
```

## **Phase 3: ClickHouse Setup**

### **3.1 Environment Configuration**
Create `.env` file:
```bash
# ClickHouse Cloud
CLICKHOUSE_HOST=your-cluster.clickhouse.cloud
CLICKHOUSE_PASSWORD=your_password

# API Keys
FRED_API_KEY=your_fred_key
EIA_TOKEN=your_eia_token
```

### **3.2 ClickHouse Tables**
The script automatically creates these tables:

```sql
-- Raw data table (long format)
CREATE TABLE macro_raw_data (
    date Date,
    symbol String,
    value Float64,
    source String,
    volume Nullable(Float64),
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY (date, symbol, source);

-- Symbols metadata
CREATE TABLE macro_symbols (
    symbol String,
    source String,
    description String,
    unit String
) ENGINE = MergeTree()
ORDER BY (source, symbol);
```

## **Phase 4: Implementation Steps**

### **Step 1: Quick Setup (30 minutes)**
```bash
# Create project
mkdir -p euclid-macro/data/raw
cd euclid-macro

# Initialize with uv
uv init
uv venv
source .venv/bin/activate

# Add all dependencies
uv add pandas numpy pyarrow yfinance fredapi myeia requests selenium
uv add selenium-stealth webdriver-manager openpyxl beautifulsoup4 lxml
uv add python-dotenv rich typer loguru tqdm clickhouse-connect

# Create .env file
echo "CLICKHOUSE_HOST=your-cluster.clickhouse.cloud" > .env
echo "CLICKHOUSE_PASSWORD=your_password" >> .env

# Create symbols.csv
echo "symbol,source,description,unit" > data/symbols.csv
```

### **Step 2: Test Collection**
```bash
# Add test symbols
echo "AAPL,yahoo,Apple Inc,USD" >> data/symbols.csv
echo "GDP,fred,Gross Domestic Product,USD" >> data/symbols.csv

# Test single source
python scripts/collect_data.py collect --sources yahoo

# Check output
ls -la data/raw/
# Should see: yahoo_20250720_143022.parquet
```

### **Step 3: Full Collection with Upload**
```bash
# Populate data/symbols.csv with all symbols

# Collect all sources and upload to ClickHouse
python scripts/collect_data.py collect --sources all --upload

# Or collect without upload, then upload separately
python scripts/collect_data.py collect --sources all
python scripts/collect_data.py upload
```

## **Expected Output Structure**
```
data/raw/
├── yahoo_20250720_143022.parquet          # All Yahoo data
├── fred_20250720_143122.parquet           # All FRED data
├── eia_20250720_143222.parquet            # All EIA data
├── all_sources_combined_20250720_144000.parquet  # Combined long format
└── all_sources_combined_latest.parquet    # Symlink to latest combined
```

## **Benefits of This Approach**

1. **📦 Simple Storage**: One parquet per source + one combined file
2. **⚡ Efficient**: Less file I/O, faster processing
3. **☁️ Cloud Ready**: Direct upload to ClickHouse
4. **📊 Long Format**: Ready for analysis and ML
5. **🔄 Easy Updates**: Just re-run and upload new data
6. **💾 Space Efficient**: Compressed parquet files

## **Query Examples in ClickHouse**

Once uploaded, you can query:

```sql
-- Get date range for any symbol
SELECT 
    symbol,
    min(date) as start_date,
    max(date) as end_date,
    count(*) as data_points
FROM macro_raw_data
WHERE symbol = 'AAPL'
GROUP BY symbol;

-- Get all available symbols by source
SELECT 
    source,
    count(DISTINCT symbol) as symbol_count,
    count(*) as total_rows
FROM macro_raw_data
GROUP BY source
ORDER BY source;

-- Get latest data for all symbols
SELECT 
    symbol,
    max(date) as latest_date,
    argMax(value, date) as latest_value
FROM macro_raw_data
GROUP BY symbol
ORDER BY symbol;
```

This simplified approach should make the data collection much cleaner and more efficient!