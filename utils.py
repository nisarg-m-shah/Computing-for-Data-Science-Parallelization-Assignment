"""
Utility functions for the distributed sales analysis system
"""

import logging
import os
import time
import pickle
import socket
from datetime import datetime
import config

def setup_logging(name, log_file=None, level=None):
    """Setup logging configuration"""
    level = level or config.LOG_LEVEL
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level))
    
    # File handler if specified
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level))
        
        formatter = logging.Formatter(config.LOG_FORMAT)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    formatter = logging.Formatter(config.LOG_FORMAT)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def send_data(socket_obj, data):
    """Send serialized data over socket"""
    serialized = pickle.dumps(data)
    data_size = len(serialized)
    
    # Send size header (16 bytes)
    size_header = f"{data_size}".encode().ljust(16)
    socket_obj.send(size_header)
    
    # Send actual data
    socket_obj.sendall(serialized)

def receive_data(socket_obj):
    """Receive serialized data from socket"""
    # Receive size header
    size_data = socket_obj.recv(16)
    data_size = int(size_data.decode().strip())
    
    # Receive actual data
    data = b""
    while len(data) < data_size:
        chunk = socket_obj.recv(min(data_size - len(data), 8192))
        if not chunk:
            break
        data += chunk
    
    return pickle.loads(data)

def generate_analysis_id():
    """Generate unique analysis ID"""
    timestamp = int(time.time())
    return f"analysis_{timestamp}"

def format_bytes(bytes_value):
    """Format bytes into human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_value < 1024:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024
    return f"{bytes_value:.2f} TB"

def validate_csv_file(csv_file):
    """Validate CSV file has required columns"""
    try:
        import pandas as pd
        df = pd.read_csv(csv_file, nrows=1)  # Read just first row
        required_cols = ['Price', 'Quantity']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"
        return True, "Valid CSV file"
    except Exception as e:
        return False, f"Error reading CSV: {e}"