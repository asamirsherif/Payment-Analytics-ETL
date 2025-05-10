#!/usr/bin/env python3
"""
run.py - Main entry point for the ETL processing application.

This script orchestrates the execution of the ETL pipeline:
1. Checks if the database has existing tables with data
2. Asks the user whether to start with new data or use existing data
3. If starting with new data, runs the complete ETL pipeline:
   - Generates configuration (generate_config.py)
   - Cleans and processes data (data_cleaner.py)
   - Loads data to PostgreSQL (load_to_postgres.py)
4. Executes a predefined SQL query to analyze the data
5. Exports results to an Excel file with multiple sheets

Usage:
    python run.py
"""

import os
import sys
import subprocess
import pandas as pd
import time
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import re
import threading

# Import the optimized sql_executor module
from sql_executor import run_sql_query

# Load environment variables for database connection
load_dotenv()

# Configuration
QUERY_FILE = "payment_analysis.sql"
PROCESSED_DATA_DIR = "./processed_data"
OUTPUT_DIR = "./output"

def get_db_engine():
    """
    Create and return a SQLAlchemy engine for database connections.
    
    Returns:
        engine: SQLAlchemy engine object or None if connection failed
    """
    try:
        # Create SQLAlchemy engine from connection parameters
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print(f"Connected to database '{os.getenv('DB_NAME')}' on {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}")
        return engine
    
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def check_database_tables(engine):
    """
    Check if database has tables with data.
    
    Args:
        engine: SQLAlchemy engine object
        
    Returns:
        list: List of table names that contain data
    """
    tables_with_data = []
    
    try:
        # Get list of tables in public schema
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT tablename FROM pg_catalog.pg_tables
                WHERE schemaname = 'public'
            """))
            tables = [row[0] for row in result]
            
            # Check if tables contain data
            for table in tables:
                result = conn.execute(text(f"SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1)"))
                has_data = result.scalar()
                if has_data:
                    tables_with_data.append(table)
        
        return tables_with_data
    
    except Exception as e:
        print(f"Error checking database tables: {e}")
        return []

def status_monitor(start_time, description, stop_event):
    """
    Monitor and display elapsed time for a running process.
    
    Args:
        start_time: Time when the process started
        description: Description of the process
        stop_event: Threading event to signal when to stop monitoring
    """
    elapsed = 0
    while not stop_event.is_set():
        elapsed = time.time() - start_time
        mins, secs = divmod(int(elapsed), 60)
        hours, mins = divmod(mins, 60)
        time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
        print(f"\r{description} - Running... Elapsed time: {time_str}", end="", flush=True)
        time.sleep(1)
    
    # Print final elapsed time
    elapsed = time.time() - start_time
    mins, secs = divmod(int(elapsed), 60)
    hours, mins = divmod(mins, 60)
    time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
    print(f"\r{description} - Completed. Total time: {time_str}                ")

def run_command(command, description):
    """
    Execute a shell command and handle the result.
    
    Args:
        command: The command to execute
        description: Description of what the command does
        
    Returns:
        bool: True if the command succeeded, False otherwise
    """
    print(f"\n{'=' * 50}")
    print(f"Executing: {description}")
    print(f"{'=' * 50}")
    
    start_time = time.time()
    start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Started at: {start_datetime}")
    
    # Create a stop event for the monitor thread
    stop_event = threading.Event()
    
    # Start the monitor thread
    monitor = threading.Thread(target=status_monitor, args=(start_time, description, stop_event))
    monitor.daemon = True
    monitor.start()
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        
        # Signal the monitor to stop
        stop_event.set()
        monitor.join()
        
        end_time = time.time()
        elapsed = end_time - start_time
        mins, secs = divmod(int(elapsed), 60)
        hours, mins = divmod(mins, 60)
        time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
        
        print(f"\nOutput:")
        print(result.stdout)
        if result.stderr:
            print(f"Warnings/Info: {result.stderr}")
        print(f"{description} completed successfully in {time_str}")
        return True
    except subprocess.CalledProcessError as e:
        # Signal the monitor to stop
        stop_event.set()
        monitor.join()
        
        end_time = time.time()
        elapsed = end_time - start_time
        mins, secs = divmod(int(elapsed), 60)
        hours, mins = divmod(mins, 60)
        time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
        
        print(f"\nERROR: {description} failed after {time_str}")
        print(f"Exit code: {e.returncode}")
        print(f"Error output:")
        print(e.stderr)
        return False

def run_etl_pipeline():
    """
    Run the complete ETL pipeline: generate config, clean data, and load to database.
    
    Returns:
        bool: True if the pipeline ran successfully, False otherwise
    """
    # Create processed data directory if it doesn't exist
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    pipeline_start = time.time()
    print(f"ETL Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Generate configuration
    if not run_command("python generate_config.py", "Generate configuration"):
        return False
    
    # 2. Clean and process data
    if not run_command("python data_cleaner.py --output_dir ./processed_data", "Clean and process data"):
        return False
    
    # 3. Load data to PostgreSQL
    if not run_command("python load_to_postgres.py --parquet-dir ./processed_data --yes", "Load data to PostgreSQL"):
        return False
    
    pipeline_end = time.time()
    elapsed = pipeline_end - pipeline_start
    mins, secs = divmod(int(elapsed), 60)
    hours, mins = divmod(mins, 60)
    time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
    
    print(f"\nComplete ETL Pipeline finished in {time_str}")
    return True

def main():
    """
    Main function that orchestrates the ETL workflow.
    """
    main_start_time = time.time()
    print(f"Process started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get database engine
    engine = get_db_engine()
    
    # Exit if database connection failed
    if engine is None:
        print("ERROR: Could not connect to the database. Please check your .env file and database server.")
        sys.exit(1)
    
    try:
        # Check for tables with data
        tables_with_data = check_database_tables(engine)
        
        # Provide information about existing data
        if tables_with_data:
            print("\nThe following tables already contain data:")
            for table in tables_with_data:
                print(f"  - {table}")
        else:
            print("\nNo tables with data found in the database.")
        
        # Ask user what they want to do
        print("\nChoose an option:")
        print("1. Process new data (will overwrite existing data)")
        print("2. Use existing data (skip processing)")
        
        while True:
            choice = input("\nEnter your choice (1 or 2): ").strip()
            if choice in ['1', '2']:
                break
            print("Invalid choice. Please enter 1 or 2.")
        
        # Execute based on user choice
        if choice == '1':
            # Process new data
            print("\nProcessing new data...")
            if not run_etl_pipeline():
                print("ERROR: ETL pipeline failed. Please check the logs for details.")
                sys.exit(1)
        else:
            # Use existing data
            if not tables_with_data:
                print("WARNING: No existing data found but you chose to skip processing.")
                print("The analysis query may not return any results.")
        
        # Run the analysis query using the optimized sql_executor module
        output_file = f"{OUTPUT_DIR}/{Path(QUERY_FILE).stem}_results.xlsx"
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Set a reasonable timeout for the queries (6 minutes)
        query_timeout = 360
        
        print(f"\nExecuting SQL queries with {query_timeout} second timeout limit...")
        
        sql_start_time = time.time()
        sql_start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"SQL execution started at: {sql_start_datetime}")
        
        # Create a stop event for the monitor thread
        stop_event = threading.Event()
        
        # Start the monitor thread
        monitor = threading.Thread(target=status_monitor, args=(sql_start_time, "SQL execution", stop_event))
        monitor.daemon = True
        monitor.start()
        
        try:
            # Execute SQL query using env file instead of passing engine
            run_sql_query(query_file=QUERY_FILE, output_file=output_file, timeout=query_timeout)
            # Signal the monitor to stop
            stop_event.set()
            monitor.join()
            
            sql_end_time = time.time()
            elapsed = sql_end_time - sql_start_time
            mins, secs = divmod(int(elapsed), 60)
            hours, mins = divmod(mins, 60)
            time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
            print(f"\nSQL execution completed in {time_str}")
        except Exception as e:
            # Signal the monitor to stop
            stop_event.set()
            monitor.join()
            print(f"\nERROR during SQL execution: {str(e)}")
    
    finally:
        # Calculate and display total elapsed time
        main_end_time = time.time()
        elapsed = main_end_time - main_start_time
        mins, secs = divmod(int(elapsed), 60)
        hours, mins = divmod(mins, 60)
        time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"
        
        print(f"\nAnalysis complete. Total execution time: {time_str}")
        print(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 