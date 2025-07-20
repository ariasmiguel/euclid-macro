"""
Configuration Manager

This module provides centralized configuration and environment variable
management for the data pipeline, eliminating duplicate configuration code.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import os
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
from dotenv import load_dotenv


class ConfigurationManager:
    """
    Centralized configuration management for the data pipeline.
    
    Handles:
    - Environment variable loading and validation
    - API credential management
    - Default configuration values
    - Configuration validation
    """
    
    def __init__(self, env_file: Optional[str] = None, logger_name: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            env_file: Optional path to .env file
            logger_name: Optional custom logger name
        """
        self.logger = logging.getLogger(logger_name or "config_manager")
        
        # Load environment variables
        self._load_environment(env_file)
        
        # Store loaded configuration
        self._config_cache = {}
    
    def _load_environment(self, env_file: Optional[str] = None) -> None:
        """
        Load environment variables from .env file.
        
        Args:
            env_file: Optional path to .env file
        """
        try:
            if env_file and Path(env_file).exists():
                load_dotenv(env_file)
                self.logger.debug(f"Loaded environment from: {env_file}")
            else:
                load_dotenv()  # Load from default locations
                self.logger.debug("Loaded environment variables")
        except Exception as e:
            self.logger.warning(f"Failed to load environment file: {e}")
    
    def get_api_credential(self, service_name: str, required: bool = True) -> Optional[str]:
        """
        Get API credential for a service.
        
        Args:
            service_name: Name of the service (e.g., 'fred', 'eia')
            required: Whether the credential is required
            
        Returns:
            API credential or None if not found
            
        Raises:
            ValueError: If required credential is missing
        """
        # Standard API key patterns
        key_patterns = [
            f"{service_name.upper()}_API_KEY",
            f"{service_name.upper()}_TOKEN", 
            f"{service_name.upper()}_KEY",
            f"API_KEY_{service_name.upper()}"
        ]
        
        credential = None
        found_key = None
        
        for pattern in key_patterns:
            credential = os.getenv(pattern)
            if credential:
                found_key = pattern
                break
        
        if credential:
            self.logger.debug(f"Found {service_name} credential: {found_key}")
            # Cache for future use
            self._config_cache[f"{service_name}_credential"] = credential
        else:
            message = f"No API credential found for {service_name}. Tried: {key_patterns}"
            if required:
                self.logger.error(message)
                raise ValueError(message)
            else:
                self.logger.warning(message)
        
        return credential
    
    def get_default_date_ranges(self) -> Dict[str, str]:
        """
        Get default date ranges for different data sources.
        
        Returns:
            Dictionary of default date ranges
        """
        default_ranges = {
            'fred': os.getenv('FRED_DEFAULT_START_DATE', '1900-01-01'),
            'eia': os.getenv('EIA_DEFAULT_START_DATE', '1900-01-01'),
            'yahoo': os.getenv('YAHOO_DEFAULT_START_DATE', '1990-01-01'),
            'baker': os.getenv('BAKER_DEFAULT_START_DATE', '2000-01-01'),
            'finra': os.getenv('FINRA_DEFAULT_START_DATE', '2010-01-01'),
            'sp500': os.getenv('SP500_DEFAULT_START_DATE', '2000-01-01'),
            'usda': os.getenv('USDA_DEFAULT_START_DATE', '2000-01-01'),
            'default': os.getenv('DEFAULT_START_DATE', '1900-01-01')
        }
        
        self.logger.debug("Retrieved default date ranges")
        return default_ranges
    
    def get_data_directories(self) -> Dict[str, str]:
        """
        Get configured data directories.
        
        Returns:
            Dictionary of data directory paths
        """
        base_data_dir = os.getenv('DATA_DIR', 'data')
        
        # directories are relative to base_data_dir
        directories = {
            'base': base_data_dir,
            'raw': os.getenv('RAW_DATA_DIR', f'{base_data_dir}/raw'),
            'downloads': os.getenv('DOWNLOAD_DIR', f'{base_data_dir}/downloads'),
            'baker': os.getenv('BAKER_DOWNLOAD_DIR', f'{base_data_dir}/baker_hughes'),
            'finra': os.getenv('FINRA_DOWNLOAD_DIR', f'{base_data_dir}/finra'),
            'sp500': os.getenv('SP500_DOWNLOAD_DIR', f'{base_data_dir}/sp500'),
            'usda': os.getenv('USDA_DOWNLOAD_DIR', f'{base_data_dir}/usda')
        }
        
        return directories
    
    def get_database_config(self) -> Dict[str, Any]:
        """
        Get database configuration.
        
        Returns:
            Dictionary with database configuration
        """
        return {
            'path': os.getenv('DUCKDB_PATH', 'bristol_gate.duckdb'),
            'memory_limit': os.getenv('DUCKDB_MEMORY_LIMIT', '2GB'),
            'threads': int(os.getenv('DUCKDB_THREADS', '4')),
            'read_only': os.getenv('DUCKDB_READ_ONLY', 'False').lower() == 'true'
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration.
        
        Returns:
            Dictionary with logging configuration
        """
        return {
            'level': os.getenv('LOG_LEVEL', 'INFO').upper(),
            'format': os.getenv('LOG_FORMAT', 
                              '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            'file': os.getenv('LOG_FILE'),  # None if not set
            'max_bytes': int(os.getenv('LOG_MAX_BYTES', '10485760')),  # 10MB
            'backup_count': int(os.getenv('LOG_BACKUP_COUNT', '5'))
        }
    
    def setup_logging_config(self) -> None:
        """
        Apply the logging configuration.
        """
        config = self.get_logging_config()
        
        # Configure basic logging
        log_level = getattr(logging, config['level'], logging.INFO)
        
        if config['file']:
            # File logging with rotation
            from logging.handlers import RotatingFileHandler
            
            handler = RotatingFileHandler(
                config['file'],
                maxBytes=config['max_bytes'],
                backupCount=config['backup_count']
            )
            
            logging.basicConfig(
                level=log_level,
                format=config['format'],
                handlers=[handler, logging.StreamHandler()]
            )
        else:
            # Console logging only
            logging.basicConfig(
                level=log_level,
                format=config['format']
            )
        
        self.logger.info(f"Logging configured: level={config['level']}")
    
    def validate_environment(self, required_services: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        Validate that required environment variables are set.
        
        Args:
            required_services: List of services that need API credentials
            
        Returns:
            Dictionary showing validation results
        """
        results = {}
        
        if required_services:
            for service in required_services:
                try:
                    credential = self.get_api_credential(service, required=True)
                    results[service] = credential is not None
                except ValueError:
                    results[service] = False
        
        # Check other important configs
        results['data_dir'] = Path(self.get_data_directories()['base']).exists()
        
        # Log results
        failed = [k for k, v in results.items() if not v]
        if failed:
            self.logger.warning(f"Environment validation failed for: {failed}")
        else:
            self.logger.info("Environment validation passed")
        
        return results
    
    def get_retry_config(self) -> Dict[str, int]:
        """
        Get retry configuration for API calls.
        
        Returns:
            Dictionary with retry settings
        """
        return {
            'max_retries': int(os.getenv('API_MAX_RETRIES', '3')),
            'base_wait_time': int(os.getenv('API_BASE_WAIT_TIME', '30')),
            'timeout': int(os.getenv('API_TIMEOUT', '60'))
        }
    
    def get_rate_limit_config(self) -> Dict[str, Dict[str, int]]:
        """
        Get rate limiting configuration by service.
        
        Returns:
            Dictionary with rate limiting settings
        """
        return {
            'fred': {
                'max_requests': int(os.getenv('FRED_MAX_REQUESTS', '120')),
                'time_window': int(os.getenv('FRED_TIME_WINDOW', '60'))
            },
            'eia': {
                'max_requests': int(os.getenv('EIA_MAX_REQUESTS', '5000')),
                'time_window': int(os.getenv('EIA_TIME_WINDOW', '3600'))  # per hour
            },
            'default': {
                'max_requests': int(os.getenv('DEFAULT_MAX_REQUESTS', '60')),
                'time_window': int(os.getenv('DEFAULT_TIME_WINDOW', '60'))
            }
        }
    
    @classmethod
    def create_default(cls) -> 'ConfigurationManager':
        """
        Create configuration manager with default settings.
        
        Returns:
            Configured ConfigurationManager instance
        """
        config_manager = cls()
        config_manager.setup_logging_config()
        return config_manager 