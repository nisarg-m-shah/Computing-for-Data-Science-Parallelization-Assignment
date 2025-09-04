"""
SQLite database manager for the distributed sales analysis system
"""

import sqlite3
import threading
import logging
from contextlib import contextmanager
from datetime import datetime
import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles all SQLite database operations"""
    
    def __init__(self, db_path=None):
        self.db_path = db_path or config.DATABASE_PATH
        self.lock = threading.Lock()
        self.init_database()
    
    def init_database(self):
        """Initialize database with required tables"""
        logger.info(f"Initializing database: {self.db_path}")
        
        with self.get_connection() as conn:
            # Worker results table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS worker_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    worker_id TEXT NOT NULL,
                    analysis_id TEXT NOT NULL,
                    rows_processed INTEGER NOT NULL,
                    total_sales_amount REAL NOT NULL,
                    min_price REAL NOT NULL,
                    max_price REAL NOT NULL,
                    avg_price REAL NOT NULL,
                    processing_time_seconds REAL,
                    chunk_start_index INTEGER,
                    chunk_end_index INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Analysis summary table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS analysis_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    analysis_id TEXT UNIQUE NOT NULL,
                    total_workers INTEGER NOT NULL,
                    total_rows_processed INTEGER NOT NULL,
                    total_sales_amount REAL NOT NULL,
                    global_min_price REAL NOT NULL,
                    global_max_price REAL NOT NULL,
                    weighted_avg_price REAL NOT NULL,
                    total_processing_time_seconds REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_worker_results_analysis_id 
                ON worker_results(analysis_id)
            """)
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper error handling"""
        conn = None
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=config.DATABASE_TIMEOUT
            )
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def clear_analysis_results(self, analysis_id):
        """Clear results for specific analysis"""
        with self.lock:
            with self.get_connection() as conn:
                conn.execute("DELETE FROM worker_results WHERE analysis_id = ?", (analysis_id,))
                conn.execute("DELETE FROM analysis_summary WHERE analysis_id = ?", (analysis_id,))
                conn.commit()
                logger.info(f"Cleared results for analysis: {analysis_id}")
    
    def insert_worker_result(self, result_data):
        """Insert worker results"""
        with self.lock:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO worker_results (
                        worker_id, analysis_id, rows_processed, total_sales_amount,
                        min_price, max_price, avg_price, processing_time_seconds,
                        chunk_start_index, chunk_end_index
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    result_data['worker_id'],
                    result_data['analysis_id'],
                    result_data['rows_processed'],
                    result_data['total_sales_amount'],
                    result_data['min_price'],
                    result_data['max_price'],
                    result_data['avg_price'],
                    result_data.get('processing_time', 0),
                    result_data.get('chunk_start_index', 0),
                    result_data.get('chunk_end_index', 0)
                ))
                conn.commit()
                logger.info(f"Stored results for worker: {result_data['worker_id']}")
    
    def get_aggregated_results(self, analysis_id):
        """Get aggregated results for analysis"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_workers,
                    SUM(rows_processed) as total_rows,
                    SUM(total_sales_amount) as total_sales,
                    MIN(min_price) as global_min_price,
                    MAX(max_price) as global_max_price,
                    SUM(avg_price * rows_processed) / SUM(rows_processed) as weighted_avg_price,
                    SUM(processing_time_seconds) as total_processing_time
                FROM worker_results
                WHERE analysis_id = ?
            """, (analysis_id,))
            
            row = cursor.fetchone()
            if row and row[0] > 0:
                return {
                    'total_workers': row[0],
                    'total_rows': row[1],
                    'total_sales_amount': row[2],
                    'global_min_price': row[3],
                    'global_max_price': row[4],
                    'weighted_avg_price': row[5],
                    'total_processing_time': row[6] or 0
                }
            return None
    
    def save_analysis_summary(self, analysis_id, summary_data):
        """Save final analysis summary"""
        with self.lock:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO analysis_summary (
                        analysis_id, total_workers, total_rows_processed,
                        total_sales_amount, global_min_price, global_max_price,
                        weighted_avg_price, total_processing_time_seconds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    analysis_id,
                    summary_data['total_workers'],
                    summary_data['total_rows'],
                    summary_data['total_sales_amount'],
                    summary_data['global_min_price'],
                    summary_data['global_max_price'],
                    summary_data['weighted_avg_price'],
                    summary_data['total_processing_time']
                ))
                conn.commit()
                logger.info(f"Saved analysis summary: {analysis_id}")