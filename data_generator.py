"""
Generate sample sales data for testing the distributed system
"""

import pandas as pd
import numpy as np
import os
import logging
import config

logger = logging.getLogger(__name__)

class SalesDataGenerator:
    """Generate realistic sample sales data"""
    
    def __init__(self, seed=None):
        self.seed = seed or config.SAMPLE_DATA_SEED
        np.random.seed(self.seed)
    
    def generate_sample_data(self, num_rows=None, filename=None):
        """Generate sample sales dataset"""
        num_rows = num_rows or config.SAMPLE_DATA_ROWS
        filename = filename or config.DEFAULT_CSV_FILE
        
        logger.info(f"Generating {num_rows:,} rows of sample data...")
        
        # Generate realistic data
        data = {
            'TransactionID': [f'TXN{i:08d}' for i in range(num_rows)],
            'ProductID': [f'PROD{np.random.randint(1000, 9999)}' for _ in range(num_rows)],
            'CustomerID': [f'CUST{np.random.randint(100000, 999999)}' for _ in range(num_rows)],
            'Price': self._generate_prices(num_rows),
            'Quantity': np.random.choice([1, 2, 3, 4, 5], size=num_rows, p=[0.5, 0.25, 0.15, 0.07, 0.03]),
            'Category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports'], num_rows),
            'Date': pd.date_range('2023-01-01', '2024-12-31', periods=num_rows).strftime('%Y-%m-%d'),
            'SalesRep': [f'REP{np.random.randint(1, 100):03d}' for _ in range(num_rows)]
        }
        
        df = pd.DataFrame(data)
        
        # Save to file
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, index=False)
        
        total_sales = (df['Price'] * df['Quantity']).sum()
        
        logger.info(f"Sample data created: {filename}")
        logger.info(f"  Rows: {len(df):,}")
        logger.info(f"  Price range: ${df['Price'].min():.2f} - ${df['Price'].max():.2f}")
        logger.info(f"  Total sales: ${total_sales:,.2f}")
        
        return filename
    
    def _generate_prices(self, num_rows):
        """Generate realistic price distribution"""
        # Log-normal distribution for realistic pricing
        prices = np.random.lognormal(mean=3.0, sigma=1.0, size=num_rows)
        # Cap prices at reasonable limits
        prices = np.clip(prices, 5.0, 999.99)
        return np.round(prices, 2)
