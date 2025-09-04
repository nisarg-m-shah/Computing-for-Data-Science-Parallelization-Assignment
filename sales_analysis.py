# Distributed Sales Data Analysis System
# Simple approach: Dynamic chunk size based on number of workers

import socket
import pickle
import threading
import time
import pandas as pd
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Tuple, Any
import logging
import sys
import numpy as np
import math

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class DatabaseManager:
    """Manages SQL database operations for storing partial and final results"""
    
    def __init__(self, db_path: str = "sales_analysis.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database schema"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS worker_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        worker_id TEXT NOT NULL,
                        rows_processed INTEGER NOT NULL,
                        total_sales_amount REAL NOT NULL,
                        min_price REAL NOT NULL,
                        max_price REAL NOT NULL,
                        avg_price REAL NOT NULL,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
                logging.info("Database initialized successfully")
        except Exception as e:
            logging.error(f"Database initialization failed: {e}")
            raise
    
    def insert_partial_result(self, worker_id: str, results: Dict[str, float]):
        """Insert partial results from a worker"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO worker_results 
                    (worker_id, rows_processed, total_sales_amount, min_price, max_price, avg_price)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    worker_id,
                    results['rows_processed'],
                    results['total_sales_amount'],
                    results['min_price'],
                    results['max_price'],
                    results['avg_price']
                ))
                conn.commit()
                logging.info(f"Stored results for worker {worker_id}")
        except Exception as e:
            logging.error(f"Failed to insert results for worker {worker_id}: {e}")
            raise
    
    def get_aggregated_results(self) -> Dict[str, float]:
        """Aggregate all partial results using SQL queries"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get aggregated statistics
                cursor = conn.execute("""
                    SELECT 
                        SUM(rows_processed) as total_rows,
                        SUM(total_sales_amount) as total_sales,
                        MIN(min_price) as global_min_price,
                        MAX(max_price) as global_max_price,
                        SUM(avg_price * rows_processed) / SUM(rows_processed) as weighted_avg_price,
                        COUNT(*) as num_workers
                    FROM worker_results
                """)
                
                row = cursor.fetchone()
                if row and row[0] is not None:
                    return {
                        'total_rows': int(row[0]),
                        'total_sales_amount': float(row[1]),
                        'min_price': float(row[2]),
                        'max_price': float(row[3]),
                        'avg_price': float(row[4]),
                        'num_workers': int(row[5])
                    }
                else:
                    return {}
        except Exception as e:
            logging.error(f"Failed to aggregate results: {e}")
            raise
    
    def clear_results(self):
        """Clear all previous results"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM worker_results")
                conn.commit()
                logging.info("Previous results cleared")
        except Exception as e:
            logging.error(f"Failed to clear results: {e}")


class SalesAnalysisServer:
    """Server coordinator that manages dataset distribution and result aggregation"""
    
    def __init__(self, host: str = 'localhost', port: int = 9999):
        self.host = host
        self.port = port
        self.db_manager = DatabaseManager()
        self.workers_completed = 0
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def load_and_split_dataset(self, csv_path: str, num_workers: int) -> List[pd.DataFrame]:
        """Load dataset and split into chunks based on number of workers"""
        try:
            self.logger.info(f"Loading dataset from {csv_path}")
            # Load the dataset
            df = pd.read_csv(csv_path)
            self.logger.info(f"Dataset loaded: {len(df)} rows, {len(df.columns)} columns")
            
            # Map your actual column names to expected names
            df['Price'] = df['Unit Price']
            df['Quantity'] = df['Units Sold']
            self.logger.info("Using dataset columns: 'Unit Price' -> 'Price', 'Units Sold' -> 'Quantity'")
            
            # Calculate chunk size based on number of workers
            total_rows = len(df)
            chunk_size = math.ceil(total_rows / num_workers)
            
            self.logger.info(f"Splitting {total_rows} rows into {num_workers} chunks of ~{chunk_size} rows each")
            
            # Split into chunks
            chunks = []
            for i in range(num_workers):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, total_rows)
                
                if start_idx < total_rows:
                    chunk = df.iloc[start_idx:end_idx].copy()
                    chunks.append(chunk)
                    self.logger.info(f"Chunk {i+1}: rows {start_idx} to {end_idx-1} ({len(chunk)} rows)")
            
            return chunks
            
        except Exception as e:
            self.logger.error(f"Failed to load dataset: {e}")
            raise
    
    def handle_worker(self, client_socket: socket.socket, chunks: List[pd.DataFrame]):
        """Handle a single worker connection"""
        worker_id = f"worker_{client_socket.getpeername()[1]}"
        self.logger.info(f"Connected to {worker_id}")
        
        try:
            # Send worker ID
            client_socket.send(worker_id.encode())
            
            # Get a chunk for this worker
            with self.lock:
                if chunks:
                    chunk_to_send = chunks.pop(0)
                else:
                    chunk_to_send = None
            
            if chunk_to_send is not None:
                # Serialize and send the chunk
                serialized_chunk = pickle.dumps(chunk_to_send)
                chunk_size = len(serialized_chunk)
                
                # Send chunk size first
                client_socket.send(str(chunk_size).encode().ljust(16))
                # Send the actual chunk
                client_socket.sendall(serialized_chunk)
                
                self.logger.info(f"Sent chunk of {len(chunk_to_send)} rows to {worker_id}")
                
                # Receive results
                result_size = int(client_socket.recv(16).decode().strip())
                result_data = b""
                while len(result_data) < result_size:
                    result_data += client_socket.recv(result_size - len(result_data))
                
                results = pickle.loads(result_data)
                
                # Store results in database
                self.db_manager.insert_partial_result(worker_id, results)
                
                with self.lock:
                    self.workers_completed += 1
                
                self.logger.info(f"Received and stored results from {worker_id}")
            else:
                self.logger.info(f"No chunks available for {worker_id}")
                
        except Exception as e:
            self.logger.error(f"Error handling {worker_id}: {e}")
        finally:
            client_socket.close()
            self.logger.info(f"Connection with {worker_id} closed")
    
    def run_server(self, csv_path: str, max_workers: int = 4):
        """Run the server to coordinate analysis"""
        try:
            # Clear previous results
            self.db_manager.clear_results()
            
            # Load and split dataset based on number of workers
            chunks = self.load_and_split_dataset(csv_path, max_workers)
            actual_workers = len(chunks)
            
            # Start server
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen(max_workers)
            
            self.logger.info(f"Server listening on {self.host}:{self.port}")
            self.logger.info(f"Ready to accept {actual_workers} workers")
            
            # Accept worker connections
            threads = []
            
            for i in range(actual_workers):
                client_socket, addr = server_socket.accept()
                thread = threading.Thread(
                    target=self.handle_worker,
                    args=(client_socket, chunks)
                )
                thread.start()
                threads.append(thread)
            
            # Wait for all workers to complete
            for thread in threads:
                thread.join()
            
            server_socket.close()
            
            # Generate final aggregated results
            self.generate_final_report()
            
        except Exception as e:
            self.logger.error(f"Server error: {e}")
            raise
    
    def generate_final_report(self):
        """Generate and display final aggregated results"""
        try:
            results = self.db_manager.get_aggregated_results()
            
            if results:
                print("\n" + "="*60)
                print("DISTRIBUTED SALES ANALYSIS - FINAL RESULTS")
                print("="*60)
                print(f"Total Rows Processed: {results['total_rows']:,}")
                print(f"Total Sales Amount: ${results['total_sales_amount']:,.2f}")
                print(f"Minimum Price: ${results['min_price']:.2f}")
                print(f"Maximum Price: ${results['max_price']:.2f}")
                print(f"Average Price: ${results['avg_price']:.2f}")
                print(f"Number of Workers: {results['num_workers']}")
                print("="*60)
            else:
                print("No results found in database")
                
        except Exception as e:
            self.logger.error(f"Failed to generate report: {e}")


class SalesAnalysisWorker:
    """Worker node that processes dataset chunks"""
    
    def __init__(self, server_host: str = 'localhost', server_port: int = 9999):
        self.server_host = server_host
        self.server_port = server_port
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def process_chunk(self, chunk: pd.DataFrame) -> Dict[str, float]:
        """Process a data chunk and compute statistics"""
        try:
            # Ensure Price and Quantity columns exist
            if 'Price' not in chunk.columns or 'Quantity' not in chunk.columns:
                raise ValueError("Required columns 'Price' and 'Quantity' not found")
            
            # Calculate statistics
            total_sales = (chunk['Price'] * chunk['Quantity']).sum()
            min_price = chunk['Price'].min()
            max_price = chunk['Price'].max()
            avg_price = chunk['Price'].mean()
            rows_processed = len(chunk)
            
            results = {
                'rows_processed': rows_processed,
                'total_sales_amount': float(total_sales),
                'min_price': float(min_price),
                'max_price': float(max_price),
                'avg_price': float(avg_price)
            }
            
            self.logger.info(f"Processed {rows_processed} rows")
            return results
            
        except Exception as e:
            self.logger.error(f"Error processing chunk: {e}")
            raise
    
    def run_worker(self):
        """Connect to server and process assigned chunk"""
        try:
            # Connect to server
            worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            worker_socket.connect((self.server_host, self.server_port))
            
            # Receive worker ID
            worker_id = worker_socket.recv(1024).decode()
            self.logger.info(f"Connected as {worker_id}")
            
            # Receive chunk size
            chunk_size = int(worker_socket.recv(16).decode().strip())
            
            # Receive chunk data
            chunk_data = b""
            while len(chunk_data) < chunk_size:
                chunk_data += worker_socket.recv(chunk_size - len(chunk_data))
            
            # Deserialize chunk
            chunk = pickle.loads(chunk_data)
            self.logger.info(f"Received chunk with {len(chunk)} rows")
            
            # Process chunk
            results = self.process_chunk(chunk)
            
            # Send results back
            serialized_results = pickle.dumps(results)
            result_size = len(serialized_results)
            
            worker_socket.send(str(result_size).encode().ljust(16))
            worker_socket.sendall(serialized_results)
            
            self.logger.info("Results sent to server")
            worker_socket.close()
            
        except Exception as e:
            self.logger.error(f"Worker error: {e}")
            raise

def run_server_mode(max_workers: int = 3):
    """Run in server mode"""
    csv_path = 'sample_sales_data.csv'
    
    server = SalesAnalysisServer()
    server.run_server(csv_path, max_workers)


def run_worker_mode():
    """Run in worker mode"""
    worker = SalesAnalysisWorker()
    worker.run_worker()


if __name__ == "__main__":
    print("Distributed Sales Data Analysis System")
    print("=====================================")
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "server":
            max_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 3
            run_server_mode(max_workers)
        elif sys.argv[1] == "worker":
            run_worker_mode()
        else:
            print("Usage: python script.py [server|worker] [max_workers]")
    else:
        print("Usage:")
        print("  Server mode: python script.py server [max_workers]")
        print("  Worker mode: python script.py worker")
        print("\nRunning demo with sample data...")
        
        # Demo mode
        run_server_mode()