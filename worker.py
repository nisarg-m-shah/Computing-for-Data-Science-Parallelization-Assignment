"""
Worker node for distributed sales analysis system
"""

import socket
import time
import pandas as pd
import logging
import os

import config
from utils import setup_logging, send_data, receive_data

class DistributedWorker:
    """Worker node that processes data chunks"""
    
    def __init__(self, server_host=None, server_port=None):
        self.server_host = server_host or config.SERVER_HOST
        self.server_port = server_port or config.SERVER_PORT
        
        # Setup logging
        log_file = os.path.join(config.LOGS_FOLDER, 'worker.log')
        self.logger = setup_logging('DistributedWorker', log_file)
    
    def process_chunk(self, chunk_df, chunk_info):
        """Process data chunk and calculate statistics"""
        start_time = time.time()
        worker_id = chunk_info['worker_id']
        
        self.logger.info(f"Processing {len(chunk_df):,} rows...")
        
        try:
            # Calculate sales statistics
            sales_amounts = chunk_df['Price'] * chunk_df['Quantity']
            
            results = {
                'worker_id': worker_id,
                'analysis_id': chunk_info['analysis_id'],
                'rows_processed': len(chunk_df),
                'total_sales_amount': float(sales_amounts.sum()),
                'min_price': float(chunk_df['Price'].min()),
                'max_price': float(chunk_df['Price'].max()),
                'avg_price': float(chunk_df['Price'].mean()),
                'processing_time': time.time() - start_time,
                'chunk_start_index': chunk_info.get('start_index', 0),
                'chunk_end_index': chunk_info.get('end_index', len(chunk_df) - 1)
            }
            
            self.logger.info(f"Processed {len(chunk_df):,} rows in {results['processing_time']:.2f}s")
            self.logger.info(f"Sales: ${results['total_sales_amount']:,.2f}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error processing chunk: {e}")
            raise
    
    def start_worker(self):
        """Connect to server and process assigned work"""
        self.logger.info("Starting worker...")
        
        try:
            # Connect to server
            worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            worker_socket.connect((self.server_host, self.server_port))
            
            print(f"🔌 Connected to server at {self.server_host}:{self.server_port}")
            
            # Receive chunk info and data
            chunk_info = receive_data(worker_socket)
            chunk_df = receive_data(worker_socket)
            
            print(f"📥 Received {len(chunk_df):,} rows to process")
            print(f"👷 Worker ID: {chunk_info['worker_id']}")
            
            # Process the chunk
            results = self.process_chunk(chunk_df, chunk_info)
            
            # Send results back
            send_data(worker_socket, results)
            
            print(f"📤 Results sent back to server")
            print(f"✅ Worker completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Worker error: {e}")
            print(f"❌ Worker error: {e}")
            raise
        finally:
            worker_socket.close()
