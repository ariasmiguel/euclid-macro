"""
Web Scraping Utils

This module provides utilities for web scraping operations using Selenium,
centralizing common patterns for Chrome driver setup, stealth configuration,
and download management used across fetch modules.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import os
import time
import random
import logging
from typing import Optional, Dict, Any
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth


class WebScrapingUtils:
    """
    Utility class for web scraping operations using Selenium.
    
    Centralizes:
    - Chrome driver setup and configuration
    - Stealth settings to avoid detection
    - Download directory management
    - Access control handling
    - Driver lifecycle management
    """
    
    def __init__(self, logger_name: Optional[str] = None):
        """
        Initialize web scraping utils.
        
        Args:
            logger_name: Optional custom logger name
        """
        self.logger = logging.getLogger(logger_name or "web_scraping_utils")
        self.driver: Optional[webdriver.Chrome] = None
        
    def setup_chrome_driver(self, 
                           download_dir: str,
                           headless: bool = True,
                           window_size: str = "1920,1080",
                           user_agent: Optional[str] = None) -> webdriver.Chrome:
        """
        Set up Chrome driver with standard options.
        
        Args:
            download_dir: Directory for downloads
            headless: Whether to run in headless mode
            window_size: Browser window size
            user_agent: Custom user agent string
            
        Returns:
            Configured Chrome driver instance
        """
        self.logger.info("Setting up Chrome driver...")
        
        # Ensure download directory exists
        self.ensure_download_directory(download_dir)
        
        # Setup Chrome options
        options = Options()
        
        # Standard arguments for stability and stealth
        standard_args = [
            "--disable-extensions",
            "--disable-gpu", 
            "--disable-dev-shm-usage",
            "--no-sandbox",
            f"--window-size={window_size}",
            "--disable-blink-features=AutomationControlled",
            "--disable-web-security",
            "--allow-running-insecure-content",
            "--disable-features=VizDisplayCompositor"
        ]
        
        for arg in standard_args:
            options.add_argument(arg)
        
        # Add headless mode if requested
        if headless:
            options.add_argument("--headless=new")
        
        # Set user agent
        if user_agent is None:
            user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/112.0.0.0 Safari/537.36")
        
        options.add_argument(f"--user-agent={user_agent}")
        
        # Setup download preferences
        prefs = self.get_download_preferences(download_dir)
        options.add_experimental_option("prefs", prefs)
        
        # Exclude automation switches
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Initialize driver
        self.logger.info("Initializing Chrome driver...")
        try:
            self.driver = webdriver.Chrome(options=options)
            
            # Execute script to remove webdriver property
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            
            return self.driver
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            raise
    
    def apply_stealth_settings(self, 
                              languages: list = None,
                              vendor: str = "Google Inc.",
                              platform: str = "Win32",
                              webgl_vendor: str = "Intel Inc.",
                              renderer: str = "Intel Iris OpenGL Engine") -> None:
        """
        Apply stealth settings to avoid detection.
        
        Args:
            languages: List of languages for stealth
            vendor: WebGL vendor string
            platform: Platform string
            webgl_vendor: WebGL vendor string 
            renderer: WebGL renderer string
        """
        if not self.driver:
            raise ValueError("Driver must be initialized before applying stealth settings")
        
        if languages is None:
            languages = ["en-US", "en"]
        
        self.logger.debug("Applying stealth settings...")
        
        stealth(
            self.driver,
            languages=languages,
            vendor=vendor,
            platform=platform,
            webgl_vendor=webgl_vendor,
            renderer=renderer,
            fix_hairline=True,
        )
    
    def get_download_preferences(self, download_dir: str) -> Dict[str, Any]:
        """
        Get download preferences for Chrome.
        
        Args:
            download_dir: Directory for downloads
            
        Returns:
            Dictionary of Chrome download preferences
        """
        return {
            "download.default_directory": os.path.abspath(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "plugins.always_open_pdf_externally": True,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
    
    def ensure_download_directory(self, download_dir: str) -> str:
        """
        Ensure download directory exists.
        
        Args:
            download_dir: Directory path to create
            
        Returns:
            Absolute path to the directory
        """
        abs_path = os.path.abspath(download_dir)
        
        if not os.path.exists(abs_path):
            os.makedirs(abs_path, exist_ok=True)
            self.logger.info(f"Created download directory: {abs_path}")
        else:
            self.logger.debug(f"Download directory exists: {abs_path}")
            
        return abs_path
    
    def navigate_with_delay(self, url: str, 
                           min_delay: float = 2.0, 
                           max_delay: float = 4.0) -> None:
        """
        Navigate to URL with random delay to appear human-like.
        
        Args:
            url: URL to navigate to
            min_delay: Minimum delay in seconds
            max_delay: Maximum delay in seconds
        """
        if not self.driver:
            raise ValueError("Driver must be initialized before navigation")
        
        self.logger.info(f"Navigating to: {url}")
        self.driver.get(url)
        
        # Random delay to appear human-like
        delay = random.uniform(min_delay, max_delay)
        self.logger.debug(f"Waiting {delay:.1f} seconds...")
        time.sleep(delay)
    
    def check_access_denied(self) -> bool:
        """
        Check if current page shows access denied or forbidden.
        
        Returns:
            True if access is denied, False otherwise
        """
        if not self.driver:
            return False
            
        page_source = self.driver.page_source.lower()
        access_denied_patterns = [
            "access denied",
            "forbidden", 
            "403 forbidden",
            "access to this resource on the server is denied",
            "you don't have permission to access"
        ]
        
        for pattern in access_denied_patterns:
            if pattern in page_source:
                self.logger.warning(f"Access denied detected: {pattern}")
                return True
                
        return False
    
    def wait_for_page_load(self, driver=None, timeout: int = 10) -> bool:
        """
        Wait for page to load completely.
        
        Args:
            driver: Optional driver instance (uses self.driver if None)
            timeout: Timeout in seconds
            
        Returns:
            True if page loaded successfully, False if timeout
        """
        if driver is None:
            driver = self.driver
            
        if not driver:
            self.logger.error("No driver available for page load wait")
            return False
        
        try:
            import time
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                try:
                    # Check if page is ready
                    ready_state = driver.execute_script("return document.readyState")
                    if ready_state == "complete":
                        self.logger.debug("Page loaded successfully")
                        return True
                except:
                    pass
                
                time.sleep(0.5)
            
            self.logger.warning(f"Page load timeout after {timeout} seconds")
            return False
            
        except Exception as e:
            self.logger.error(f"Error waiting for page load: {e}")
            return False
    
    def find_links_by_text(self, search_text: str, 
                          case_sensitive: bool = False) -> list:
        """
        Find links containing specific text.
        
        Args:
            search_text: Text to search for in links
            case_sensitive: Whether search should be case sensitive
            
        Returns:
            List of matching link elements
        """
        if not self.driver:
            raise ValueError("Driver must be initialized before finding links")
        
        all_links = self.driver.find_elements(By.TAG_NAME, "a")
        matching_links = []
        
        search_text_processed = search_text if case_sensitive else search_text.upper()
        
        for link in all_links:
            try:
                link_text = link.text.strip()
                link_text_processed = link_text if case_sensitive else link_text.upper()
                
                if search_text_processed in link_text_processed:
                    matching_links.append({
                        'element': link,
                        'text': link_text,
                        'href': link.get_attribute('href'),
                        'classes': link.get_attribute('class')
                    })
            except Exception as e:
                self.logger.debug(f"Error processing link: {e}")
                continue
        
        self.logger.info(f"Found {len(matching_links)} links containing '{search_text}'")
        return matching_links
    
    def safe_click(self, element, max_retries: int = 3) -> bool:
        """
        Safely click an element with retries.
        
        Args:
            element: Selenium WebElement to click
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if click was successful, False otherwise
        """
        for attempt in range(max_retries):
            try:
                # Scroll element into view
                self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(0.5)
                
                # Try to click
                element.click()
                self.logger.debug("Element clicked successfully")
                return True
                
            except Exception as e:
                self.logger.warning(f"Click attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    self.logger.error("All click attempts failed")
                    
        return False
    
    def cleanup_driver(self) -> None:
        """
        Clean up the driver and close browser.
        """
        if self.driver:
            try:
                self.logger.info("Closing browser...")
                self.driver.quit()
                self.driver = None
            except Exception as e:
                self.logger.warning(f"Error during driver cleanup: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup driver."""
        self.cleanup_driver()
    
    @classmethod
    def create_driver_with_defaults(cls, download_dir: str, 
                                   headless: bool = True,
                                   apply_stealth: bool = True) -> 'WebScrapingUtils':
        """
        Create a driver with commonly used default settings.
        
        Args:
            download_dir: Directory for downloads
            headless: Whether to run in headless mode
            apply_stealth: Whether to apply stealth settings
            
        Returns:
            WebScrapingUtils instance with configured driver
        """
        scraper = cls()
        scraper.setup_chrome_driver(download_dir, headless=headless)
        
        if apply_stealth:
            scraper.apply_stealth_settings()
            
        return scraper 