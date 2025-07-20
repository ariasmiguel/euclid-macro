# src/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# ClickHouse settings
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'macro_data')

# API Keys
FRED_API_KEY = os.getenv('FRED_API_KEY')
EIA_TOKEN = os.getenv('EIA_TOKEN')

# Data paths
RAW_DATA_PATH = os.getenv('RAW_DATA_PATH')
BAKER_DOWNLOAD_PATH = os.getenv('BAKER_DOWNLOAD_PATH')
FINRA_DOWNLOAD_PATH = os.getenv('FINRA_DOWNLOAD_PATH')
SP500_DOWNLOAD_PATH = os.getenv('SP500_DOWNLOAD_PATH')
USDA_DOWNLOAD_PATH = os.getenv('USDA_DOWNLOAD_PATH')