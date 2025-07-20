"""
Centralized Logging Setup

This module provides standardized logging configuration for the entire
src_pipeline package, eliminating duplicate logging setup code.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import logging
import sys
from typing import Optional
from pathlib import Path

from .config_manager import ConfigurationManager


class PipelineLogger:
    """
    Centralized logging setup for the data pipeline.
    
    Provides consistent logging configuration across all modules
    and eliminates duplicate logging.basicConfig() calls.
    """
    
    _configured = False
    _config_manager = None
    
    @classmethod
    def setup(cls, config_manager: Optional[ConfigurationManager] = None) -> None:
        """
        Set up logging configuration for the entire pipeline.
        
        Args:
            config_manager: Optional ConfigurationManager instance
        """
        if cls._configured:
            return
        
        if config_manager is None:
            config_manager = ConfigurationManager()
        
        cls._config_manager = config_manager
        
        try:
            # Use ConfigurationManager's logging setup
            config_manager.setup_logging_config()
            cls._configured = True
            
            # Get a logger to confirm setup
            logger = logging.getLogger("pipeline_logger")
            logger.info("🚀 Bristol Gate Pipeline logging configured")
            
        except Exception as e:
            # Fallback to basic configuration
            cls._setup_fallback_logging()
            logger = logging.getLogger("pipeline_logger")
            logger.warning(f"Used fallback logging configuration: {e}")
    
    @classmethod
    def _setup_fallback_logging(cls) -> None:
        """Set up fallback logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        cls._configured = True
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get a logger with standardized configuration.
        
        Args:
            name: Logger name (typically __name__ or module-specific name)
            
        Returns:
            Configured logger instance
        """
        # Ensure logging is configured
        if not cls._configured:
            cls.setup()
        
        return logging.getLogger(name)
    
    @classmethod
    def reset(cls) -> None:
        """Reset logging configuration (mainly for testing)."""
        cls._configured = False
        cls._config_manager = None


# Convenience function for quick logger creation
def get_logger(name: str) -> logging.Logger:
    """
    Get a standardized logger for pipeline modules.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return PipelineLogger.get_logger(name)


# Auto-setup logging when module is imported
def _auto_setup():
    """Automatically set up logging when module is first imported."""
    try:
        PipelineLogger.setup()
    except Exception:
        # If auto-setup fails, it will be retried when get_logger is called
        pass


# Perform auto-setup
_auto_setup() 