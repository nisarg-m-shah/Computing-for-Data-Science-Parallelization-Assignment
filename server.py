"""
Distributed server (coordinator) for sales analysis system
"""

import socket
import threading
import time
import pandas as pd
import logging
from datetime import datetime

import config
from database_manager import DatabaseManager
from data_generator import SalesDataGenerator
from utils import setup_logging, send_data, receive_data, generate_analysis_id, format_bytes

class DistributedServer:
    """Server that coordinates distributed sales analysis"""
    
    def __init__(self, host=None, port=None, chunk_size=None):
        self.host = host or config.SERVER_HOST
        self.port = port or config.SERVER_PORT
        self.chunk_size = chunk_size or config.CHUNK_SIZE
        
        # Setup logging
        log_file = os.path.join(config.LOGS_FOLDER, 'server.log')
        self.logger = setup_logging('DistributedServer', log_file)
        
        # Initialize components
        self.db = DatabaseManager()
        self.data_generator = SalesDataGenerator()
        
        # Analysis tracking
        self.analysis_id = generate_analysis_id()
        self.start_time = None
        self.workers_completed = 0
        self.total_chunks = 0
        self.lock = threading.Lock()
        
        self.logger.info(f"Server initialized - Analysis ID: {self.analysis_id}")
    
    def load_data(self, csv_file=None):
        """Load and validate CSV data"""
        if csv_file is None or not os.path.exists(csv_file):
            self.logger.info("Creating sample data...")
            csv_file = self.data_generator.generate_sample_data()
        
        self.logger.info(f"Loading data from: {csv_file}")
        
        try:
            df = pd.read_csv(csv_file)
            self.logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
            
            # Validate required columns
            required_cols = ['Price', 'Quantity']
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                raise ValueError(f"Missing required columns: {missing}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            raise
    
    def split_data_into_chunks(self, df):
        """Split DataFrame into chunks for distribution"""
        chunks = []
        
        for i in range(0, len(df), self.chunk_size):
            end_idx = min(i + self.chunk_size, len(df))
            chunk = df.iloc[i:end_idx].copy()
            
            # Add metadata
            chunk.attrs = {
                'chunk_id': len(chunks),
                'start_index': i,
                'end_index': end_idx - 1
            }
            
            chunks.append(chunk)
        
        self.total_chunks = len(chunks)
        self.logger.info(f"Split data into {self.total_chunks} chunks")
        return chunks
    
    def handle_worker(self, client_socket, client_address, chunk):
        """Handle individual worker connection"""
        worker_id = f"{self.analysis_id}_worker_{client_address[1]}"
        self.logger.info(f"Worker connected: {worker_id}")
        
        try:
            # Prepare chunk info
            chunk_info = {
                'analysis_id': self.analysis_id,
                'worker_id': worker_id,
                'chunk_id': chunk.attrs['chunk_id'],
                'start_index': chunk.attrs['start_index'],
                'end_index': chunk.attrs['end_index'],
                'chunk_size': len(chunk)
            }
            
            # Send chunk info and data
            send_data(client_socket, chunk_info)
            send_data(client_socket, chunk)
            
            self.logger.info(f"Sent {len(chunk):,} rows to {worker_id}")
            
            # Receive results
            results = receive_data(client_socket)
            self.logger.info(f"Received results from {worker_id}")
            
            # Store in database
            self.db.insert_worker_result(results)
            
            # Update progress
            with self.lock:
                self.workers_completed += 1
                progress = (self.workers_completed / self.total_chunks) * 100
                self.logger.info(f"Progress: {self.workers_completed}/{self.total_chunks} ({progress:.1f}%)")
        
        except Exception as e:
            self.logger.error(f"Error handling worker {worker_id}: {e}")
        finally:
            client_socket.close()
    
    def start_server(self, csv_file=None, max_workers=None):
        """Start the distributed analysis server"""
        max_workers = max_workers or config.MAX_WORKERS
        self.start_time = datetime.now()
        
        self.logger.info("Starting distributed sales analysis server")
        self.logger.info(f"Server: {self.host}:{self.port}")
        self.logger.info(f"Max workers: {max_workers}")
        self.logger.info(f"Chunk size: {self.chunk_size:,} rows")
        
        # Clear previous results and load data
        self.db.clear_analysis_results(self.analysis_id)
        df = self.load_data(csv_file)
        chunks = self.split_data_into_chunks(df)
        
        # Limit workers to available chunks
        actual_workers = min(max_workers, len(chunks))
        
        # Start TCP server
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(actual_workers)
        
        print(f"\n🚀 Server listening on {self.host}:{self.port}")
        print(f"📊 Ready to process {len(chunks)} chunks with {actual_workers} workers")
        print(f"💾 Results stored in: {self.db.db_path}")
        print(f"\n🔥 Start workers: python run_system.py worker\n")
        
        # Handle worker connections
        threads = []
        
        try:
            for i in range(actual_workers):
                print(f"⏳ Waiting for worker {i+1}/{actual_workers}...")
                client_socket, client_address = server_socket.accept()
                
                chunk = chunks[i] if i < len(chunks) else chunks[-1]
                
                thread = threading.Thread(
                    target=self.handle_worker,
                    args=(client_socket, client_address, chunk),
                    daemon=True
                )
                thread.start()
                threads.append(thread)
            
            # Wait for completion
            print(f"⏳ Processing... waiting for {len(threads)} workers to complete")
            for thread in threads:
                thread.join()
            
            print("✅ All workers completed!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Server interrupted")
        finally:
            server_socket.close()
        
        self.generate_final_report()
    
    def generate_final_report(self):
        """Generate and display final analysis report"""
        end_time = datetime.now()
        processing_time = (end_time - self.start_time).total_seconds()
        
        # Get aggregated results
        results = self.db.get_aggregated_results(self.analysis_id)
        
        if not results:
            print("❌ No results found!")
            return
        
        # Save summary
        self.db.save_analysis_summary(self.analysis_id, results)
        
        # Display results
        print("\n" + "="*70)
        print("🎉 DISTRIBUTED ANALYSIS COMPLETE!")
        print("="*70)
        print(f"🆔 Analysis ID:           {self.analysis_id}")
        print(f"👥 Workers:               {results['total_workers']}")
        print(f"📊 Rows Processed:        {results['total_rows']:,}")
        print(f"💰 Total Sales:          ${results['total_sales_amount']:,.2f}")
        print(f"🔻 Min Price:            ${results['global_min_price']:.2f}")
        print(f"🔺 Max Price:            ${results['global_max_price']:.2f}")
        print(f"📈 Avg Price:            ${results['weighted_avg_price']:.2f}")
        print(f"⏱️  Processing Time:      {processing_time:.2f} seconds")
        print(f"🚀 Processing Rate:       {results['total_rows']/processing_time:,.0f} rows/sec")
        print("="*70)