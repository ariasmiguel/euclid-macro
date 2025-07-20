"""
File Download Utils

This module provides utilities for file download operations, managing
download directories, waiting for downloads, and finding downloaded files.

Part of the src_pipeline refactoring to eliminate code duplication.
"""

import os
import time
import logging
import requests
from pathlib import Path
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, unquote


class FileDownloadUtils:
    """
    Utility class for file download operations.
    
    Centralizes:
    - Directory creation and management
    - File detection and filtering
    - Download waiting logic
    - File path resolution
    - HTTP download operations
    """
    
    def __init__(self, download_dir: Optional[str] = None, logger_name: Optional[str] = None):
        """
        Initialize file download utils.
        
        Args:
            download_dir: Optional default download directory
            logger_name: Optional custom logger name
        """
        self.logger = logging.getLogger(logger_name or "file_download_utils")
        self.download_dir = download_dir
    
    def ensure_directory_exists(self, directory_path: str) -> str:
        """
        Ensure a directory exists, creating it if necessary.
        
        Args:
            directory_path: Path to the directory
            
        Returns:
            Absolute path to the directory
        """
        abs_path = os.path.abspath(directory_path)
        
        if not os.path.exists(abs_path):
            os.makedirs(abs_path, exist_ok=True)
            self.logger.info(f"Created directory: {abs_path}")
        else:
            self.logger.debug(f"Directory already exists: {abs_path}")
            
        return abs_path
    
    def find_excel_files(self, directory: str, 
                        include_xlsb: bool = True) -> List[str]:
        """
        Find Excel files in a directory.
        
        Args:
            directory: Directory to search
            include_xlsb: Whether to include .xlsb files
            
        Returns:
            List of Excel file paths
        """
        if not os.path.exists(directory):
            self.logger.warning(f"Directory does not exist: {directory}")
            return []
        
        extensions = ['.xlsx', '.xls']
        if include_xlsb:
            extensions.append('.xlsb')
        
        excel_files = []
        try:
            for filename in os.listdir(directory):
                if any(filename.lower().endswith(ext) for ext in extensions):
                    excel_files.append(os.path.join(directory, filename))
            
            self.logger.debug(f"Found {len(excel_files)} Excel files in {directory}")
            return excel_files
            
        except Exception as e:
            self.logger.error(f"Error finding Excel files: {e}")
            return []
    
    def get_most_recent_file(self, file_paths: List[str]) -> Optional[str]:
        """
        Get the most recently created/modified file from a list.
        
        Args:
            file_paths: List of file paths
            
        Returns:
            Path to the most recent file, or None if list is empty
        """
        if not file_paths:
            return None
        
        try:
            most_recent = max(file_paths, key=os.path.getctime)
            self.logger.debug(f"Most recent file: {most_recent}")
            return most_recent
            
        except Exception as e:
            self.logger.error(f"Error finding most recent file: {e}")
            return None
    
    def wait_for_download(self, 
                         directory: str,
                         expected_filename: Optional[str] = None,
                         timeout_seconds: int = 60,
                         check_interval: float = 2.0) -> Optional[str]:
        """
        Wait for a file to be downloaded to a directory.
        
        Args:
            directory: Directory to monitor
            expected_filename: Expected filename (if known)
            timeout_seconds: Maximum time to wait
            check_interval: How often to check (seconds)
            
        Returns:
            Path to the downloaded file, or None if timeout
        """
        self.logger.info(f"Waiting for download in {directory}...")
        
        start_time = time.time()
        initial_files = set(os.listdir(directory)) if os.path.exists(directory) else set()
        
        while time.time() - start_time < timeout_seconds:
            time.sleep(check_interval)
            
            if not os.path.exists(directory):
                continue
            
            current_files = set(os.listdir(directory))
            new_files = current_files - initial_files
            
            # Filter out temporary download files
            completed_files = [
                f for f in new_files 
                if not f.endswith('.crdownload') 
                and not f.endswith('.tmp')
                and not f.startswith('.')
            ]
            
            if expected_filename:
                # Look for specific filename
                if expected_filename in completed_files:
                    file_path = os.path.join(directory, expected_filename)
                    self.logger.info(f"Expected file downloaded: {file_path}")
                    return file_path
            else:
                # Look for any new completed file
                if completed_files:
                    # Get the most recent one
                    file_paths = [os.path.join(directory, f) for f in completed_files]
                    most_recent = self.get_most_recent_file(file_paths)
                    if most_recent:
                        self.logger.info(f"File downloaded: {most_recent}")
                        return most_recent
            
            elapsed = time.time() - start_time
            self.logger.debug(f"Still waiting for download... ({elapsed:.1f}s)")
        
        self.logger.warning(f"Download timeout after {timeout_seconds} seconds")
        return None
    
    def wait_for_excel_download(self,
                               directory: str, 
                               timeout_seconds: int = 60) -> Optional[str]:
        """
        Wait specifically for an Excel file to be downloaded.
        
        Args:
            directory: Directory to monitor
            timeout_seconds: Maximum time to wait
            
        Returns:
            Path to the downloaded Excel file, or None if timeout
        """
        self.logger.info("Waiting for Excel file download...")
        
        start_time = time.time()
        initial_excel_files = set(self.find_excel_files(directory))
        
        while time.time() - start_time < timeout_seconds:
            time.sleep(2.0)
            
            current_excel_files = set(self.find_excel_files(directory))
            new_excel_files = current_excel_files - initial_excel_files
            
            if new_excel_files:
                # Get the most recent Excel file
                most_recent = self.get_most_recent_file(list(new_excel_files))
                if most_recent:
                    self.logger.info(f"Excel file downloaded: {most_recent}")
                    return most_recent
            
            elapsed = time.time() - start_time
            if elapsed % 10 == 0:  # Log every 10 seconds
                self.logger.debug(f"Still waiting for Excel download... ({elapsed:.0f}s)")
        
        self.logger.warning(f"Excel download timeout after {timeout_seconds} seconds")
        return None
    
    def wait_for_download_completion(self, 
                                   timeout: int = 60,
                                   expected_filename: Optional[str] = None,
                                   directory: Optional[str] = None,
                                   file_extension: Optional[str] = None) -> Optional[str]:
        """
        Wait for download completion in the specified or configured download directory.
        
        Args:
            timeout: Timeout in seconds
            expected_filename: Optional expected filename
            directory: Optional directory to monitor (uses self.download_dir if not provided)
            file_extension: Optional file extension to filter for (e.g., '.xlsx')
            
        Returns:
            Path to downloaded file, or None if timeout
        """
        download_directory = directory or self.download_dir
        
        if not download_directory:
            self.logger.error("No download directory specified and no default configured")
            return None
        
        # If file_extension is provided and no expected_filename, wait for any file with that extension
        if file_extension and not expected_filename:
            return self._wait_for_file_with_extension(download_directory, file_extension, timeout)
            
        return self.wait_for_download(
            directory=download_directory,
            expected_filename=expected_filename,
            timeout_seconds=timeout
        )
    
    def _wait_for_file_with_extension(self, directory: str, extension: str, timeout: int) -> Optional[str]:
        """
        Wait for any file with the specified extension to be downloaded.
        
        Args:
            directory: Directory to monitor
            extension: File extension to look for (e.g., '.xlsx')
            timeout: Timeout in seconds
            
        Returns:
            Path to downloaded file, or None if timeout
        """
        import time
        
        self.logger.info(f"Waiting for file with extension {extension} in {directory}...")
        
        start_time = time.time()
        initial_files = set()
        
        if os.path.exists(directory):
            initial_files = set([
                f for f in os.listdir(directory) 
                if f.lower().endswith(extension.lower())
            ])
        
        while time.time() - start_time < timeout:
            time.sleep(2.0)
            
            if not os.path.exists(directory):
                continue
            
            current_files = set([
                f for f in os.listdir(directory) 
                if f.lower().endswith(extension.lower())
                and not f.endswith('.crdownload')
                and not f.endswith('.tmp')
                and not f.startswith('.')
            ])
            
            new_files = current_files - initial_files
            
            if new_files:
                # Get the most recent file
                file_paths = [os.path.join(directory, f) for f in new_files]
                most_recent = self.get_most_recent_file(file_paths)
                if most_recent:
                    self.logger.info(f"File with extension {extension} downloaded: {most_recent}")
                    return most_recent
            
            elapsed = time.time() - start_time
            if elapsed % 10 == 0:  # Log every 10 seconds
                self.logger.debug(f"Still waiting for {extension} file... ({elapsed:.0f}s)")
        
        self.logger.warning(f"File download timeout after {timeout} seconds")
        return None
    
    def download_file_from_url(self,
                              url: str,
                              download_dir: str,
                              filename: Optional[str] = None,
                              headers: Optional[Dict[str, str]] = None,
                              timeout: int = 60) -> Optional[str]:
        """
        Download a file from a URL.
        
        Args:
            url: URL to download from
            download_dir: Directory to save file
            filename: Optional custom filename
            headers: Optional HTTP headers
            timeout: Request timeout in seconds
            
        Returns:
            Path to downloaded file, or None if failed
        """
        try:
            # Ensure download directory exists
            self.ensure_directory_exists(download_dir)
            
            # Determine filename
            if not filename:
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path)
                filename = unquote(filename)  # Decode URL encoding
                
                if not filename or '.' not in filename:
                    filename = "downloaded_file"
            
            file_path = os.path.join(download_dir, filename)
            
            # Set default headers
            if headers is None:
                headers = {
                    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                 "AppleWebKit/537.36 (KHTML, like Gecko) "
                                 "Chrome/112.0.0.0 Safari/537.36")
                }
            
            self.logger.info(f"Downloading file from: {url}")
            self.logger.debug(f"Saving to: {file_path}")
            
            # Download the file
            response = requests.get(url, headers=headers, stream=True, timeout=timeout)
            response.raise_for_status()
            
            # Save the file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            self.logger.info(f"File downloaded successfully: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error downloading file from {url}: {e}")
            return None
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Get information about a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file information
        """
        try:
            stat = os.stat(file_path)
            return {
                'path': file_path,
                'filename': os.path.basename(file_path),
                'size_bytes': stat.st_size,
                'size_mb': stat.st_size / (1024 * 1024),
                'created_time': stat.st_ctime,
                'modified_time': stat.st_mtime,
                'extension': Path(file_path).suffix.lower()
            }
        except Exception as e:
            self.logger.error(f"Error getting file info for {file_path}: {e}")
            return {}
    
    def cleanup_old_files(self,
                         directory: str,
                         max_age_days: int = 7,
                         file_extensions: Optional[List[str]] = None) -> int:
        """
        Clean up old files in a directory.
        
        Args:
            directory: Directory to clean
            max_age_days: Maximum age of files to keep
            file_extensions: Optional list of extensions to target
            
        Returns:
            Number of files deleted
        """
        if not os.path.exists(directory):
            return 0
        
        current_time = time.time()
        max_age_seconds = max_age_days * 24 * 60 * 60
        deleted_count = 0
        
        try:
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                
                if not os.path.isfile(file_path):
                    continue
                
                # Check extension filter
                if file_extensions:
                    file_ext = Path(file_path).suffix.lower()
                    if file_ext not in file_extensions:
                        continue
                
                # Check age
                file_age = current_time - os.path.getctime(file_path)
                if file_age > max_age_seconds:
                    os.remove(file_path)
                    deleted_count += 1
                    self.logger.debug(f"Deleted old file: {file_path}")
            
            if deleted_count > 0:
                self.logger.info(f"Cleaned up {deleted_count} old files from {directory}")
            
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up directory {directory}: {e}")
            return 0 