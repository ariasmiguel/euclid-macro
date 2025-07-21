import requests
import pandas as pd
from pathlib import Path
import time
from datetime import datetime
import logging

class SimpleSP500Fetcher:
    """
    Simplified S&P 500 fetcher that uses cloudscraper to bypass Cloudflare/Akamai protection
    """
    
    def __init__(self, download_dir="data/raw/sp500"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        # URLs
        self.excel_url = "https://www.spglobal.com/spdji/en/documents/additional-material/sp-500-eps-est.xlsx"
        self.referer_url = "https://www.spglobal.com/spdji/en/indices/equity/sp-500/"
        
    def download_file_with_cloudscraper(self):
        """
        Use cloudscraper to bypass anti-bot protection
        """
        try:
            # Import cloudscraper (install with: pip install cloudscraper)
            import cloudscraper
            
            # Create a cloudscraper instance
            scraper = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'desktop': True
                }
            )
            
            # Add headers
            headers = {
                'Referer': self.referer_url,
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
            }
            
            self.logger.info("Attempting download with cloudscraper...")
            
            # First visit the main page
            self.logger.info(f"Visiting referer page: {self.referer_url}")
            main_page = scraper.get(self.referer_url, headers=headers)
            
            if main_page.status_code != 200:
                self.logger.warning(f"Referer page returned status: {main_page.status_code}")
            
            # Small delay to mimic human behavior
            time.sleep(2)
            
            # Now download the Excel file
            self.logger.info(f"Downloading Excel file from: {self.excel_url}")
            response = scraper.get(self.excel_url, headers=headers, stream=True)
            
            if response.status_code == 200:
                file_path = self.download_dir / "sp-500-eps-est.xlsx"
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                self.logger.info(f"✅ File downloaded successfully: {file_path}")
                self.logger.info(f"File size: {file_path.stat().st_size} bytes")
                return str(file_path)
            else:
                self.logger.error(f"Download failed with status: {response.status_code}")
                return None
                
        except ImportError:
            self.logger.error("cloudscraper not installed. Install with: pip install cloudscraper")
            return None
        except Exception as e:
            self.logger.error(f"Download failed: {str(e)}")
            return None
    
    def download_file_with_requests_fallback(self):
        """
        Fallback method using regular requests with enhanced headers
        """
        try:
            session = requests.Session()
            
            # Comprehensive headers to appear more like a real browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
            }
            
            session.headers.update(headers)
            
            # Visit main page first
            self.logger.info("Trying fallback method with enhanced headers...")
            main_response = session.get(self.referer_url, timeout=30)
            
            # Update referer for Excel download
            session.headers.update({'Referer': self.referer_url})
            
            # Try to download Excel
            response = session.get(self.excel_url, timeout=30, stream=True)
            
            if response.status_code == 200:
                file_path = self.download_dir / "sp-500-eps-est.xlsx"
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                self.logger.info(f"✅ Fallback download successful: {file_path}")
                return str(file_path)
            else:
                self.logger.error(f"Fallback download failed with status: {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"Fallback download failed: {str(e)}")
            return None
    
    def fetch_data(self):
        """
        Main method to fetch S&P 500 data
        """
        # Try cloudscraper first
        file_path = self.download_file_with_cloudscraper()
        
        # If cloudscraper fails, try regular requests
        if not file_path:
            self.logger.info("Cloudscraper failed, trying fallback method...")
            file_path = self.download_file_with_requests_fallback()
        
        if not file_path:
            self.logger.error("All download methods failed")
            return pd.DataFrame()
        
        # Process the Excel file (you can use your existing processing logic)
        try:
            # Read the Excel file
            df = pd.read_excel(file_path, sheet_name='ESTIMATES & PEs', skiprows=6)
            self.logger.info(f"Successfully read Excel file with {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"Failed to process Excel file: {str(e)}")
            return pd.DataFrame()


# Alternative: Manual download instruction
def manual_download_instruction():
    """
    If automated methods fail, provide manual download instructions
    """
    print("""
    ⚠️  MANUAL DOWNLOAD REQUIRED ⚠️
    
    The S&P Global website is blocking automated downloads.
    Please follow these steps:
    
    1. Open your web browser
    2. Go to: https://www.spglobal.com/spdji/en/indices/equity/sp-500/
    3. Scroll down to find "S&P 500 Earnings and Estimate Report"
    4. Click on the Excel download link
    5. Save the file as: data/raw/sp500/sp-500-eps-est.xlsx
    
    Once downloaded, the pipeline can process the file normally.
    
    To automate this in the future, consider:
    - Using a paid API service for S&P 500 data
    - Setting up a scheduled manual download process
    - Using a proxy service that can bypass anti-bot protection
    """)


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Try automated download
    fetcher = SimpleSP500Fetcher()
    data = fetcher.fetch_data()
    
    if data.empty:
        # If automated download fails, show manual instructions
        manual_download_instruction()
    else:
        print(f"✅ Successfully fetched {len(data)} rows of S&P 500 data")
        print(data.head())