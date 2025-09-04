"""
Configuration settings for the distributed sales analysis system
"""

import os

# Server Configuration
SERVER_HOST = 'localhost'
SERVER_PORT = 9999
MAX_WORKERS = 4
CHUNK_SIZE = 100000  # Rows per chunk

# Database Configuration
DATABASE_PATH = os.path.join('data', 'sales_analysis.db')
DATABASE_TIMEOUT = 30.0

# Data Configuration
DATA_FOLDER = 'data'
LOGS_FOLDER = 'logs'
DEFAULT_CSV_FILE = os.path.join(DATA_FOLDER, 'sample_sales_data.csv')

# Sample Data Configuration
SAMPLE_DATA_ROWS = 500000
SAMPLE_DATA_SEED = 42

# Logging Configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Create folders if they don't exist
for folder in [DATA_FOLDER, LOGS_FOLDER]:
    os.makedirs(folder, exist_ok=True)
