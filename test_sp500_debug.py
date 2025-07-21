"""
Enhanced debug script for S&P 500 fetcher with VPN-like approaches
"""

import logging
import requests
import time
import random
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.fetchers.fetch_sp500 import SP500Fetcher
from src.utils.web_scraping_utils import WebScrapingUtils

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_random_user_agent():
    """Get a random realistic user agent"""
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    return random.choice(user_agents)

def test_session_based_download():
    """Test session-based download with better session management"""
    print("🔍 Testing session-based download with enhanced session management...")
    
    url = "https://www.spglobal.com/spdji/en/documents/additional-material/sp-500-eps-est.xlsx"
    
    # Create a session to maintain cookies
    session = requests.Session()
    
    # Set realistic headers
    session.headers.update({
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    })
    
    try:
        # First, visit the referer page to establish session
        referer_url = "https://www.spglobal.com/spdji/en/indices/equity/sp-500/"
        print(f"🌐 Visiting referer page: {referer_url}")
        
        referer_response = session.get(referer_url, timeout=30)
        print(f"📊 Referer response status: {referer_response.status_code}")
        
        if referer_response.status_code == 200:
            print("✅ Referer page accessed successfully")
            time.sleep(2)  # Small delay to mimic human behavior
        else:
            print(f"⚠️ Referer page returned status: {referer_response.status_code}")
        
        # Now try to download the file
        print(f"📥 Attempting download from: {url}")
        
        # Update headers for file download
        session.headers.update({
            'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
            'Referer': referer_url,
        })
        
        response = session.get(url, timeout=30, stream=True)
        
        print(f"📊 Download response status: {response.status_code}")
        print(f"📊 Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            # Save the file
            download_dir = Path("data/raw/sp500")
            download_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = download_dir / "sp-500-eps-est.xlsx"
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"✅ File downloaded successfully: {file_path}")
            print(f"📊 File size: {file_path.stat().st_size} bytes")
            return str(file_path)
        else:
            print(f"❌ Download failed with status code: {response.status_code}")
            print(f"📄 Response content: {response.text[:500]}...")
            return None
            
    except Exception as e:
        print(f"❌ Session-based download failed: {str(e)}")
        return None

def test_enhanced_selenium_with_stealth():
    """Test Selenium with enhanced stealth and VPN-like approaches"""
    print("🔍 Testing enhanced Selenium with stealth settings...")
    
    download_dir = Path("data/raw/sp500")
    download_dir.mkdir(parents=True, exist_ok=True)
    
    # Clear any existing files
    for file in download_dir.glob("*.xlsx"):
        file.unlink()
    
    scraper = None
    try:
        # Use the existing WebScrapingUtils with stealth
        scraper = WebScrapingUtils.create_driver_with_defaults(
            download_dir=str(download_dir),
            headless=True,
            apply_stealth=True
        )
        
        driver = scraper.driver
        
        # Test referer page with longer wait
        referer_url = "https://www.spglobal.com/spdji/en/indices/equity/sp-500/"
        print(f"🌐 Visiting referer page: {referer_url}")
        
        # Use the utility method for navigation with delay
        scraper.navigate_with_delay(referer_url, min_delay=3.0, max_delay=6.0)
        
        print(f"📄 Page title: {driver.title}")
        print(f"📄 Current URL: {driver.current_url}")
        
        # Check if we got an error page
        if "error" in driver.title.lower():
            print("🚫 Error page detected on referer")
            print(f"📄 Page source preview: {driver.page_source[:500]}...")
            return None
        
        # Try to find any download links on the page
        try:
            links = driver.find_elements(By.TAG_NAME, "a")
            excel_links = [link for link in links if link.get_attribute("href") and ".xlsx" in link.get_attribute("href")]
            print(f"🔗 Found {len(excel_links)} Excel links on the page")
            
            for i, link in enumerate(excel_links[:3]):  # Show first 3
                href = link.get_attribute("href")
                text = link.text.strip()
                print(f"   {i+1}. {text} -> {href}")
        except Exception as e:
            print(f"⚠️ Could not search for links: {e}")
        
        # Now try the direct download URL
        download_url = "https://www.spglobal.com/spdji/en/documents/additional-material/sp-500-eps-est.xlsx"
        print(f"📥 Attempting to download from: {download_url}")
        
        # Navigate to download URL with delay
        scraper.navigate_with_delay(download_url, min_delay=5.0, max_delay=8.0)
        
        print(f"📄 Final URL: {driver.current_url}")
        print(f"📄 Page title: {driver.title}")
        
        # Check for downloaded files
        excel_files = list(download_dir.glob("*.xlsx"))
        print(f"📁 Files in download directory: {[f.name for f in excel_files]}")
        
        if excel_files:
            latest_file = max(excel_files, key=lambda x: x.stat().st_mtime)
            print(f"✅ File downloaded: {latest_file}")
            print(f"📊 File size: {latest_file.stat().st_size} bytes")
            return str(latest_file)
        else:
            print("❌ No files downloaded")
            
            # Check if we got redirected to an error page
            if "error" in driver.current_url.lower() or "access denied" in driver.page_source.lower():
                print("🚫 Access denied or error page detected")
                print(f"📄 Page source preview: {driver.page_source[:500]}...")
            
            return None
            
    except Exception as e:
        print(f"❌ Enhanced Selenium download failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        if scraper:
            scraper.cleanup_driver()

def test_alternative_urls():
    """Test alternative URLs that might work"""
    print("🔍 Testing alternative URLs...")
    
    alternative_urls = [
        "https://www.spglobal.com/spdji/en/documents/additional-material/sp-500-eps-est.xlsx",
        "https://www.spglobal.com/spdji/en/indices/equity/sp-500/#overview",
        "https://www.spglobal.com/spdji/en/indices/equity/sp-500/",
    ]
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    })
    
    for url in alternative_urls:
        try:
            print(f"🔗 Testing URL: {url}")
            response = session.get(url, timeout=10)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                print(f"   ✅ URL accessible")
                if "xlsx" in url:
                    return url
            else:
                print(f"   ❌ URL not accessible")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None

def test_sp500_fetcher():
    """Test the S&P 500 fetcher with detailed debugging"""
    print("🧪 Testing S&P 500 fetcher with detailed debugging...")
    
    try:
        # Create fetcher with explicit download directory
        fetcher = SP500Fetcher(download_dir="data/raw/sp500")
        
        # Test the fetcher
        data = fetcher.fetch_batch()
        
        if not data.empty:
            print(f"✅ S&P 500 fetcher successful!")
            print(f"📊 Data shape: {data.shape}")
            print(f"📅 Date range: {data['date'].min()} to {data['date'].max()}")
            print(f"📈 Metrics: {data['metric'].unique().tolist()}")
            print(f"🔢 Sample data:")
            print(data.head())
        else:
            print("❌ S&P 500 fetcher returned empty data")
            
    except Exception as e:
        print(f"❌ S&P 500 fetcher failed: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    """Run all tests"""
    print("🚀 Starting enhanced S&P 500 fetcher debugging...")
    print("=" * 60)
    
    # Test 1: Session-based download
    print("\n1️⃣ Testing session-based download...")
    session_result = test_session_based_download()
    
    # Test 2: Enhanced Selenium with stealth
    print("\n2️⃣ Testing enhanced Selenium with stealth...")
    selenium_result = test_enhanced_selenium_with_stealth()
    
    # Test 3: Alternative URLs
    print("\n3️⃣ Testing alternative URLs...")
    alt_url = test_alternative_urls()
    
    # Test 4: Original fetcher
    print("\n4️⃣ Testing original fetcher...")
    test_sp500_fetcher()
    
    print("\n" + "=" * 60)
    print("📋 Summary:")
    print(f"   Session-based download: {'✅' if session_result else '❌'}")
    print(f"   Enhanced Selenium: {'✅' if selenium_result else '❌'}")
    print(f"   Alternative URL found: {'✅' if alt_url else '❌'}")
    
    if session_result or selenium_result:
        print("🎉 At least one download method worked!")
    else:
        print("💡 Next steps to try:")
        print("   - Wait a few hours and try again (rate limiting)")
        print("   - Use a VPN to change IP address")
        print("   - Try different user agents")
        print("   - Check if the website requires registration/login")
        print("   - Look for alternative data sources")

if __name__ == "__main__":
    main() 