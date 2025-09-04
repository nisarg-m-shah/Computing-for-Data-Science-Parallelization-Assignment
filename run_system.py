"""
Main entry point for the distributed sales analysis system
"""

import sys
import os
import argparse

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import DistributedServer
from worker import DistributedWorker
from database_manager import DatabaseManager
from data_generator import SalesDataGenerator
import config

def run_server(args):
    """Run the distributed server"""
    server = DistributedServer()
    server.start_server(
        csv_file=args.csv_file,
        max_workers=args.max_workers
    )

def run_worker(args):
    """Run a worker node"""
    worker = DistributedWorker()
    worker.start_worker()

def generate_data(args):
    """Generate sample data"""
    generator = SalesDataGenerator()
    filename = generator.generate_sample_data(
        num_rows=args.rows,
        filename=args.output
    )
    print(f"✅ Sample data generated: {filename}")

def show_stats(args):
    """Show database statistics"""
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        # Get analysis summaries
        cursor = conn.execute("""
            SELECT analysis_id, total_workers, total_rows_processed, 
                   total_sales_amount, created_at
            FROM analysis_summary 
            ORDER BY created_at DESC 
            LIMIT 10
        """)
        
        analyses = cursor.fetchall()
        
        if analyses:
            print("\n📊 RECENT ANALYSES")
            print("="*60)
            for analysis in analyses:
                analysis_id, workers, rows, sales, created = analysis
                print(f"🆔 {analysis_id}")
                print(f"   👥 Workers: {workers} | 📊 Rows: {rows:,}")
                print(f"   💰 Sales: ${sales:,.2f} | 🕒 {created}")
                print()
        else:
            print("📁 No analyses found. Run a server first!")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Distributed Sales Analysis System')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Server command
    server_parser = subparsers.add_parser('server', help='Run the distributed server')
    server_parser.add_argument('--csv-file', help='Path to CSV file')
    server_parser.add_argument('--max-workers', type=int, default=config.MAX_WORKERS,
                             help='Maximum number of workers')
    server_parser.set_defaults(func=run_server)
    
    # Worker command
    worker_parser = subparsers.add_parser('worker', help='Run a worker node')
    worker_parser.set_defaults(func=run_worker)
    
    # Generate data command
    data_parser = subparsers.add_parser('generate-data', help='Generate sample data')
    data_parser.add_argument('--rows', type=int, default=config.SAMPLE_DATA_ROWS,
                           help='Number of rows to generate')
    data_parser.add_argument('--output', default=config.DEFAULT_CSV_FILE,
                           help='Output CSV file path')
    data_parser.set_defaults(func=generate_data)
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show database statistics')
    stats_parser.set_defaults(func=show_stats)
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        print("\n🎯 Quick Start:")
        print("1. Generate data:  python run_system.py generate-data")
        print("2. Start server:   python run_system.py server")
        print("3. Start worker:   python run_system.py worker  (in another terminal)")
        print("4. View stats:     python run_system.py stats")
        return
    
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
