#!/usr/bin/env python3
"""
sql_executor.py - Optimized SQL query execution module.

This module provides functions for parsing, optimizing, and executing SQL queries:
- Parse SQL files and extract individual queries
- Execute queries with proper transaction management and timeout controls
- Handle common SQL execution problems (CTEs, multi-statements)
- Export results to Excel with progress tracking

Features timeouts and cancellation support to prevent hanging on long-running queries.
"""

import os
import re
import time
import signal
import sys
import logging
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Dict, List, Tuple, Optional, Any, Union, Callable

# Third-party imports
import pandas as pd
import sqlparse
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError, OperationalError, TimeoutError as SQLATimeoutError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s')
logger = logging.getLogger(__name__)

# Default timeout for query execution (seconds)
DEFAULT_QUERY_TIMEOUT = 120

# Define timeout overrides for specific query patterns
# Format: (regex_pattern, timeout_in_seconds)
TIMEOUT_OVERRIDES = [
    (r'checkout.*reconciliation', 60),  # Shorter timeout for checkout reconciliation queries
    (r'.*large.*data.*', 300),          # Longer timeout for queries mentioning "large data"
    (r'.*historical.*', 240),           # Longer timeout for historical queries
]

class QueryTimeoutException(Exception):
    """Exception raised when a query execution times out."""
    pass

def strip_sql_comments(sql_statement: str) -> str:
    # Remove block comments /* ... */
    # Ensure it handles nested comments if any (though rare in SQL)
    # Simpler regex for non-nested:
    sql_statement = re.sub(r"/\\*.*?\\*/", "", sql_statement, flags=re.DOTALL)
    # Remove line comments -- ...
    sql_statement = re.sub(r"--.*?\\n", "\\n", sql_statement) # Keep newline to help split
    sql_statement = re.sub(r"--.*$", "", sql_statement) # For last line comment
    return sql_statement.strip()

def parse_sql_script(script_content):
    """
    Parse a SQL script into individual statements with careful handling of comments
    and transactions. Returns a list of SQL statements.
    """
    logger.info(f"Starting to parse SQL script content ({len(script_content)} chars).")
    
    # Normalize line endings to Unix style
    script_content = script_content.replace('\r\n', '\n').replace('\r', '\n')
    
    # Remove comments correctly without affecting the query structure
    script_clean = sqlparse.format(
        script_content, 
        strip_comments=True,
        reindent=False,
        keyword_case='upper'
    )
    
    # Use sqlparse to split statements
    statements = sqlparse.split(script_clean)
    
    # Process each statement to further clean and categorize
    cleaned_statements = []
    
    for i, stmt in enumerate(statements):
        # Trim whitespace
        stmt = stmt.strip()
        if not stmt:
            continue  # Skip empty statements
            
        # Determine statement type for logging
        stmt_type = "Unknown"
        if stmt.upper().startswith("SELECT"):
            stmt_type = "SELECT"
        elif "CREATE VIEW" in stmt.upper() or "CREATE OR REPLACE VIEW" in stmt.upper():
            stmt_type = "CREATE VIEW"
            # Try to extract view name for better logs
            view_match = re.search(r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+([^\s(]+)', stmt, re.IGNORECASE)
            if view_match:
                stmt_type = f"CREATE VIEW {view_match.group(2)}"
        elif "CREATE TABLE" in stmt.upper():
            stmt_type = "CREATE TABLE"
        elif "CREATE INDEX" in stmt.upper():
            stmt_type = "CREATE INDEX"
        elif "DROP" in stmt.upper():
            stmt_type = "DROP"
        elif "DO" in stmt.upper() and "$$" in stmt:
            stmt_type = "DO Block"
        elif "INSERT" in stmt.upper():
            stmt_type = "INSERT"
        elif "UPDATE" in stmt.upper():
            stmt_type = "UPDATE"
        
        # Add statements as tuples: (statement_type, statement_text)
        cleaned_statements.append((stmt_type, stmt))
        
    logger.info(f"Successfully parsed SQL script into {len(cleaned_statements)} statements.")
    return cleaned_statements

def execute_query_with_timeout(conn, query, timeout=DEFAULT_QUERY_TIMEOUT, engine=None, execution_options=None, output_format="excel", output_file_path=None):
    """
    Execute a SQL query with a timeout to prevent hanging.
    
    Args:
        conn: SQLAlchemy connection
        query: SQL query string
        timeout: Maximum execution time in seconds
        engine: SQLAlchemy engine (for cancellation operations)
        execution_options: Additional execution options for the query
        output_format: Output format for the query results
        output_file_path: Path to the output file for native CSV export
        
    Returns:
        Tuple of (success, result_df, error_message)
    """
    execution_options = execution_options or {}
    
    # Check if this is a DDL statement, which should be handled differently
    is_ddl = any(ddl_keyword in query.upper() for ddl_keyword in [
        "CREATE INDEX", "CREATE TABLE", "CREATE VIEW", "DROP VIEW", 
        "ALTER TABLE", "DROP TABLE", "DROP INDEX"
    ])
    
    # Special case for DO blocks which need special handling
    is_do_block = query.strip().upper().startswith("DO $$")
    
    # Define a function to execute in a separate thread
    def _execute_query():
        try:
            # For DDL statements, don't use server-side cursors or execution options
            if is_ddl or is_do_block:
                logger.debug(f"Using special DDL execution mode for query type: {'DO block' if is_do_block else 'DDL statement'}")
                
                # DO blocks need special handling with raw cursor
                if is_do_block:
                    # Get raw DBAPI connection
                    raw_conn = conn.connection.driver_connection
                    original_autocommit = raw_conn.autocommit
                    
                    try:
                        # Set autocommit mode for DO blocks
                        raw_conn.autocommit = True
                        with raw_conn.cursor() as cursor:
                            logger.debug(f"Executing complete DO block via raw DBAPI cursor")
                            cursor.execute(query)
                        return True, None, None
                    except Exception as do_err:
                        error_message = str(do_err)
                        logger.error(f"Error executing DO block: {error_message}")
                        # Try to roll back any failed transaction
                        try:
                            if not raw_conn.autocommit:
                                raw_conn.rollback()
                                logger.info("Rolled back transaction after DO block error")
                        except Exception as rollback_err:
                            logger.error(f"Error rolling back after DO block error: {rollback_err}")
                        return False, None, error_message
                    finally:
                        # Reset autocommit mode
                        raw_conn.autocommit = original_autocommit
                else:
                    # Regular DDL with autocommit
                    conn_with_options = conn.execution_options(
                        isolation_level="AUTOCOMMIT",
                        stream_results=False
                    )
                    conn_with_options.execute(text(query))
                    return True, None, None
            
            # For SELECT queries with large result sets, use more direct approaches
            is_select = query.strip().upper().startswith(('SELECT', 'WITH')) and 'SELECT' in query.upper()
            
            if is_select:
                if output_format == "csv_native" and output_file_path:
                    logger.debug(f"Streaming results directly to CSV: {output_file_path}")
                    # Construct COPY command
                    copy_sql = f"COPY ({query.rstrip(';')}) TO STDOUT WITH CSV HEADER"
                    
                    # Get the raw psycopg2 connection for copy_expert
                    raw_conn = conn.connection.driver_connection
                    
                    with open(output_file_path, 'wb') as f_out:
                        with raw_conn.cursor() as cursor:
                            # SQL for copy_expert must not have a trailing semicolon for COPY TO STDOUT
                            cursor.copy_expert(sql=copy_sql, file=f_out)
                    return True, None, None
                
                # Try direct psycopg2 approach first - this is more reliable for large datasets
                logger.info("Using direct psycopg2 cursor for SELECT query")
                try:
                    # Get raw connection
                    raw_conn = conn.connection.driver_connection
                    cursor = raw_conn.cursor()
                    
                    # Execute the query directly
                    logger.info(f"Executing query using raw cursor: {query[:100]}...")
                    cursor.execute(query)
                    
                    # Check if we got results
                    logger.info(f"Raw cursor description: {cursor.description is not None}")
                    if cursor.description is None:
                        logger.warning("Cursor has no description (no result columns) - query may not be a SELECT or returned no columns")
                        return True, pd.DataFrame(), None
                    
                    # Get column names 
                    column_names = [desc[0] for desc in cursor.description]
                    logger.info(f"Column names from cursor: {column_names}")
                    
                    # Fetch rows
                    logger.info("Fetching rows from cursor...")
                    psycopg2_rows = cursor.fetchall()
                    row_count = len(psycopg2_rows) if psycopg2_rows else 0
                    logger.info(f"Fetched {row_count} rows using direct cursor")
                    
                    # Create DataFrame
                    if row_count > 0:
                        df = pd.DataFrame(psycopg2_rows, columns=column_names)
                        logger.info(f"Created DataFrame with shape: {df.shape}")
                        
                        # Write CSV directly if that's the output format
                        if output_format == "csv" and output_file_path:
                            logger.info(f"Writing {row_count} rows directly to CSV: {output_file_path}")
                            df.to_csv(output_file_path, index=False)
                            logger.info(f"Successfully wrote CSV file: {output_file_path}")
                        
                        return True, df, None
                    else:
                        # Check if this is a SELECT from a view that was just created
                        # Look for view name in the query
                        view_match = re.search(r'FROM\s+([a-zA-Z0-9_]+)(?:\s|;|$)', query, re.IGNORECASE)
                        if view_match and 'payment_analysis_view' in view_match.group(1).lower():
                            logger.warning(f"SELECT from '{view_match.group(1)}' returned no rows despite view existing. Trying diagnostic query...")
                            
                            # Try a diagnostic query to check if the view has data
                            try:
                                cursor.execute(f"SELECT COUNT(*) FROM {view_match.group(1)}")
                                count_result = cursor.fetchone()
                                if count_result and count_result[0] > 0:
                                    logger.warning(f"View has {count_result[0]} rows but main query returned none. Trying to fetch all rows directly.")
                                    
                                    # Try direct fetch with LIMIT to get actual data
                                    cursor.execute(f"SELECT * FROM {view_match.group(1)}")
                                    direct_rows = cursor.fetchall()
                                    if direct_rows:
                                        direct_df = pd.DataFrame(direct_rows, columns=[desc[0] for desc in cursor.description])
                                        logger.info(f"Retrieved {len(direct_df)} rows directly from view")
                                        
                                        # Write CSV directly
                                        if output_format == "csv" and output_file_path:
                                            logger.info(f"Writing {len(direct_df)} rows directly to CSV: {output_file_path}")
                                            direct_df.to_csv(output_file_path, index=False)
                                            logger.info(f"Successfully wrote CSV file: {output_file_path}")
                                        
                                        return True, direct_df, None
                            except Exception as diag_err:
                                logger.error(f"Diagnostic query failed: {str(diag_err)}")
                                # Rollback after query failure
                                try:
                                    raw_conn.rollback()
                                    logger.info("Rolled back transaction after diagnostic query failure")
                                except Exception as rb_err:
                                    logger.error(f"Error rolling back after diagnostic failure: {rb_err}")
                        
                        logger.warning("No rows returned from query, creating empty DataFrame")
                        # Create an empty DataFrame with the right columns
                        empty_df = pd.DataFrame(columns=column_names)
                        return True, empty_df, None
                    
                except Exception as psycopg2_err:
                    # Log the error but continue to try SQLAlchemy approach
                    logger.error(f"Error with direct psycopg2 approach: {str(psycopg2_err)}")
                    # Try to roll back any failed transaction
                    try:
                        raw_conn.rollback()
                        logger.info("Rolled back transaction after psycopg2 query error")
                    except Exception as rb_err:
                        logger.error(f"Error rolling back after psycopg2 error: {rb_err}")
                    logger.info("Falling back to SQLAlchemy approach")
                
                # Fallback to SQLAlchemy approach
                # Use modern SQLAlchemy patterns with with_options for execution options
                logger.info("Using SQLAlchemy for SELECT query")
                conn_with_options = conn.execution_options(**execution_options)
                result = conn_with_options.execute(text(query))
                
                logger.info(f"SQLAlchemy result object created, has keys: {hasattr(result, 'keys')}")
                if hasattr(result, 'keys'):
                    column_names = result.keys()
                    logger.info(f"SQLAlchemy result column names: {column_names}")
                
                # For large results, fetch in chunks to avoid memory issues
                if 'stream_results' in execution_options and execution_options['stream_results']:
                    chunk_size = execution_options.get('max_row_buffer', 10000)
                    
                    # Use partitioning for more efficient memory usage
                    df = pd.DataFrame()
                    columns = result.keys()
                    
                    # Stream rows in chunks for memory efficiency
                    row_count = 0
                    for partition in result.partitions(chunk_size):
                        chunk_df = pd.DataFrame(partition, columns=columns)
                        row_count += len(chunk_df)
                        df = pd.concat([df, chunk_df], ignore_index=True) if not df.empty else chunk_df
                    
                    logger.info(f"Query returned {row_count} rows in streaming mode")
                else:
                    # For smaller results, fetch all at once
                    logger.info("About to fetchall() from SQLAlchemy result")
                    rows = result.fetchall()
                    if rows:
                        logger.info(f"Query returned {len(rows)} rows from database")
                        df = pd.DataFrame(rows, columns=result.keys())
                        logger.info(f"Created DataFrame with shape: {df.shape}")
                    else:
                        logger.warning("Query returned zero rows (empty result set)")
                        
                        # Check if this is a SELECT from a view that was just created
                        view_match = re.search(r'FROM\s+([a-zA-Z0-9_]+)(?:\s|;|$)', query, re.IGNORECASE)
                        if view_match and 'payment_analysis_view' in view_match.group(1).lower():
                            try:
                                # Try a direct count query to check if view has data
                                check_result = conn_with_options.execute(text(f"SELECT COUNT(*) FROM {view_match.group(1)}"))
                                count_val = check_result.scalar()
                                
                                if count_val and count_val > 0:
                                    logger.warning(f"View has {count_val} rows but main query returned none. Trying a direct query.")
                                    
                                    # Try direct query to get data
                                    direct_result = conn_with_options.execute(text(f"SELECT * FROM {view_match.group(1)}"))
                                    direct_rows = direct_result.fetchall()
                                    
                                    if direct_rows:
                                        direct_df = pd.DataFrame(direct_rows, columns=direct_result.keys())
                                        logger.info(f"Retrieved {len(direct_df)} rows directly from view")
                                        
                                        if output_format == "csv" and output_file_path:
                                            logger.info(f"Writing {len(direct_df)} rows to CSV: {output_file_path}")
                                            direct_df.to_csv(output_file_path, index=False)
                                            logger.info(f"Successfully wrote CSV from direct query: {output_file_path}")
                                        
                                        return True, direct_df, None
                            except Exception as direct_err:
                                logger.error(f"Direct query failed: {str(direct_err)}")
                                # Handle transaction error
                                try:
                                    if hasattr(conn, 'rollback'):
                                        conn.rollback()
                                    logger.info("Rolled back transaction after direct query failure")
                                except Exception as rb_err:
                                    logger.error(f"Error rolling back after direct query failure: {rb_err}")
                        
                        df = pd.DataFrame()  # Empty DataFrame
                
                # If requested output format is CSV and not using native export, write directly
                if df is not None and len(df) > 0 and output_format == "csv" and output_file_path:
                    try:
                        logger.info(f"Writing {len(df)} rows to CSV: {output_file_path}")
                        df.to_csv(output_file_path, index=False)
                        logger.info(f"Successfully wrote {len(df)} rows to {output_file_path}")
                    except Exception as csv_err:
                        logger.error(f"Error writing CSV: {str(csv_err)}")
                
                return True, df, None
            else:
                # For non-SELECT queries
                conn_with_options = conn.execution_options(timeout=timeout*1000)
                conn_with_options.execute(text(query))
                return True, None, None
        except Exception as e:
            error_msg = str(e)
            # Only log the first 200 characters of the query to avoid huge logs
            query_preview = query[:200].replace('\n', ' ') + "..." if len(query) > 200 else query.replace('\n', ' ')
            logger.error(f"Query execution error: {error_msg} | Failing Query Preview: {query_preview}")
            
            # Try to roll back the transaction to avoid "transaction is aborted" errors
            try:
                # Check if we have a raw connection
                if hasattr(conn, 'connection') and hasattr(conn.connection, 'driver_connection'):
                    # This is a SQLAlchemy connection with raw connection access
                    raw_conn = conn.connection.driver_connection
                    if hasattr(raw_conn, 'rollback'):
                        raw_conn.rollback()
                        logger.info("Rolled back raw connection after query error")
                
                # Also try SQLAlchemy rollback if available
                if hasattr(conn, 'rollback'):
                    conn.rollback()
                    logger.info("Rolled back SQLAlchemy connection after query error")
            except Exception as rollback_err:
                logger.error(f"Failed to rollback transaction: {rollback_err}")
            
            return False, None, error_msg
    
    # Use ThreadPoolExecutor to run with timeout
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_execute_query)
        try:
            success, df, error = future.result(timeout=timeout)
            return success, df, error
        except TimeoutError:
            # Query execution timed out
            logger.warning(f"Query execution timed out after {timeout} seconds")
            
            # Try to cancel the query
            if engine:
                try:
                    # Create a new connection to issue the cancellation
                    with engine.connect() as cancel_conn:
                        # First try pg_cancel_backend (soft cancel)
                        # Get the process ID of active queries
                        pid_query = text(
                            "SELECT pid FROM pg_stat_activity WHERE state = 'active' "
                            "AND query NOT ILIKE :pattern "
                            "ORDER BY query_start DESC LIMIT 5;"
                        )
                        pid_result = cancel_conn.execute(pid_query, {"pattern": '%pg_stat_activity%'})
                        pids = [row[0] for row in pid_result]
                        
                        if pids:
                            # Try cancellation
                            for pid in pids:
                                logger.warning(f"Attempting soft cancellation of query with PID {pid}")
                                cancel_conn.execute(
                                    text("SELECT pg_cancel_backend(:pid);"), 
                                    {"pid": pid}
                                )
                            
                            # Wait to see if it worked
                            time.sleep(2)
                            
                            # Check if queries are still running
                            if pids:
                                still_running_query = text(
                                    "SELECT pid FROM pg_stat_activity WHERE pid = ANY(:pids) AND state = 'active';"
                                )
                                still_running = cancel_conn.execute(still_running_query, {"pids": pids})
                                still_running_pids = [row[0] for row in still_running]
                                
                                # If queries are still running, try terminating them
                                if still_running_pids:
                                    for pid in still_running_pids:
                                        logger.warning(f"Soft cancellation failed, terminating query with PID {pid}")
                                        cancel_conn.execute(
                                            text("SELECT pg_terminate_backend(:pid);"),
                                            {"pid": pid}
                                        )
                except Exception as cancel_error:
                    logger.error(f"Failed to cancel query: {cancel_error}")
            
            # Return timeout error
            return False, None, f"Query execution timed out after {timeout} seconds"

def get_query_timeout(query_name, default_timeout=DEFAULT_QUERY_TIMEOUT):
    """
    Determine the appropriate timeout for a query based on its name.
    
    Args:
        query_name: The name of the query
        default_timeout: Default timeout to use if no override matches
        
    Returns:
        Timeout value in seconds
    """
    query_name_lower = query_name.lower()
    
    # Check for timeout overrides based on the query name
    for pattern, timeout in TIMEOUT_OVERRIDES:
        if re.search(pattern, query_name_lower, re.IGNORECASE):
            logger.info(f"Using custom timeout of {timeout}s for query matching pattern: {pattern}")
            return timeout
    
    return default_timeout

def run_sql_query(query_file, output_file=None, timeout=DEFAULT_QUERY_TIMEOUT, 
                max_workers=4, batch_size=250000, progress_callback=None, date_filter=None, 
                env_path=Path(".env"), output_format="csv"):
    """
    Execute SQL queries from a file and export results to CSV or Excel.
    Relies on parse_sql_script to break down the SQL file content.
    All individual statements are then passed to execute_query_with_timeout.
    """
    try:
        optimized_engine = get_db_engine_from_env(env_path, max_workers, timeout)
        if not optimized_engine:
            return {"success": False, "message": "Failed to connect to database"}
        
        logger.info(f"Using database connection with optimization settings for file: {query_file}")
        
        # Determine how to handle the query (file or string)
        if query_file and isinstance(query_file, str):
            if os.path.exists(query_file):
                logger.info(f"Reading SQL from file: {query_file}")
                with open(query_file, 'r', encoding='utf-8') as f:
                    query_content = f.read()
            else:
                # Assume query_file is actually a SQL string
                logger.info("Using provided string as SQL query")
                query_content = query_file
        else:
            return {"success": False, "message": "Invalid query input: must be a file path or SQL string"}
            
        # Parse the query content
        stmt_tuples = parse_sql_script(query_content)
        if not stmt_tuples:
            return {"success": False, "message": "No valid SQL statements found"}
        
        # Extract just the statement text for processing (keep the type for logging)
        stmt_types = [s[0] for s in stmt_tuples]
        statements = [s[1] for s in stmt_tuples]
        
        logger.info(f"Parsed {len(statements)} SQL statements from query")
        
        # Process date filter if provided
        if date_filter:
            # Check if date filter should be applied (respect the 'enabled' flag)
            if date_filter.get('enabled', False) and date_filter.get('start_date') and date_filter.get('end_date'):
                logger.info(f"Applying date filter: {date_filter['start_date']} to {date_filter['end_date']}")
                # Replace date parameters in statements if they exist
                for i, stmt in enumerate(statements):
                    statements[i] = stmt.replace("{{start_date}}", date_filter['start_date'])
                    statements[i] = stmt.replace("{{end_date}}", date_filter['end_date'])
            else:
                logger.info("Date filter provided but not enabled or missing dates - not applying filter")
        
        # Ensure output directory exists
        if output_file:
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"Created output directory: {output_dir}")
        
        # Execute statements
        with optimized_engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            
            # Track all errors for reporting
            all_errors = []
            
            # Determine if the statements include creating a view
            creates_view = any("CREATE VIEW" in stmt_types[i] for i in range(len(stmt_types)))
            
            # Track the last df for statements that produce results
            last_result_df = None
            processed_count = 0
            
            # Keep track of DDL execution results for views and tables
            view_created = False
            view_name = None
            
            # Track execution status for commits
            had_ddl = False
            
            # Execute each statement
            for i, stmt in enumerate(statements):
                # Get the statement type for this statement
                stmt_type = stmt_types[i] if i < len(stmt_types) else "Unknown"
                
                # Update progress if callback provided
                if progress_callback:
                    progress = int((i / len(statements)) * 100)
                    progress_callback(progress, f"Executing statement {i+1} of {len(statements)}: {stmt_type}")
                
                # Skip empty statements
                if not stmt.strip():
                    continue
                    
                # Determine statement type
                is_select = stmt.strip().upper().startswith('SELECT') or (
                    stmt.strip().upper().startswith('WITH') and 'SELECT' in stmt.upper())
                is_ddl = any(ddl_keyword in stmt.upper() for ddl_keyword in [
                    "CREATE INDEX", "CREATE TABLE", "CREATE VIEW", "DROP VIEW", 
                    "ALTER TABLE", "DROP TABLE", "DROP INDEX"
                ])
                is_view_creation = "CREATE VIEW" in stmt.upper() or "CREATE OR REPLACE VIEW" in stmt.upper()
                
                # Extract view name for verification if this creates a view
                if is_view_creation:
                    view_match = re.search(r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+([^\s(]+)', stmt, re.IGNORECASE)
                    if view_match:
                        view_name = view_match.group(2).strip()
                        logger.info(f"Detected view creation for: {view_name}")
                    had_ddl = True
                
                # Execute the statement with appropriate timeout 
                logger.info(f"Executing statement {i+1}/{len(statements)} ({stmt_type}): {stmt[:100]}...")
                
                execution_options = {
                    "isolation_level": "AUTOCOMMIT" if is_ddl else None,
                    "stream_results": True if is_select else False
                }
                
                # Try a direct COPY approach for SELECTs from views that have just been created
                if (is_select and view_created and view_name and 
                    re.search(rf'FROM\s+{view_name}(?:\s|;|$)', stmt, re.IGNORECASE)):
                    try:
                        logger.info(f"Using direct COPY approach for view {view_name}")
                        
                        # Prepare the output file
                        out_file = output_file if output_file else f"query_result_{int(time.time())}.csv"
                        
                        # Create a direct COPY statement
                        copy_stmt = f"COPY ({stmt}) TO STDOUT WITH CSV HEADER"
                        
                        # Get raw connection
                        raw_conn = conn.connection.driver_connection
                        
                        # Open the output file 
                        with open(out_file, 'w', encoding='utf-8') as f:
                            # Use the native PostgreSQL COPY command
                            with raw_conn.cursor() as cursor:
                                cursor.copy_expert(copy_stmt, f)
                                
                        logger.info(f"Direct COPY successful, results saved to {out_file}")
                        
                        # Try to read the first few rows for preview
                        try:
                            df_preview = pd.read_csv(out_file, nrows=5)
                            row_count = len(pd.read_csv(out_file, usecols=[0]))
                            logger.info(f"COPY produced {row_count} rows, preview: {df_preview.shape}")
                            last_result_df = df_preview  # Save the preview
                        except Exception as preview_err:
                            logger.warning(f"Could not read preview from COPY output: {preview_err}")
                        
                        # Indicate success and continue to next statement
                        processed_count += 1
                        continue
                    except Exception as copy_err:
                        logger.warning(f"Direct COPY approach failed, falling back to regular execution: {copy_err}")
                
                # Regular execution through SQLAlchemy
                success, df, error = execute_query_with_timeout(
                    conn, stmt, timeout, optimized_engine, 
                    execution_options, output_format, output_file if is_select and i == len(statements) - 1 else None
                )
                
                if not success:
                    all_errors.append(f"Error in statement {i+1} ({stmt_type}): {error}")
                    logger.error(f"Statement {i+1} ({stmt_type}) failed: {error}")
                    
                    # For the last statement (usually the SELECT query), we may want to abort
                    if i == len(statements) - 1 and is_select:
                        return {
                            "success": False,
                            "message": f"Final SELECT statement failed: {error}",
                            "details": all_errors
                        }
                else:
                    processed_count += 1
                    
                    # If DDL statement was executed, we need to commit and verify
                    if is_ddl:
                        had_ddl = True
                        if is_view_creation and view_name:
                            # Verify the view was created successfully
                            verify_stmt = f"SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_name = '{view_name.lower()}');"
                            v_success, v_df, v_error = execute_query_with_timeout(conn, verify_stmt, 5)
                            
                            if v_success and v_df is not None and len(v_df) > 0 and v_df.iloc[0, 0]:
                                logger.info(f"Verified view '{view_name}' was created successfully")
                                view_created = True
                                
                                # Execute an explicit COMMIT and wait to ensure transaction is complete
                                try:
                                    logger.info("Executing explicit COMMIT after view creation")
                                    conn.execute(text("COMMIT;"))
                                    time.sleep(0.2)  # Small delay to ensure transaction completes
                                except Exception as commit_err:
                                    logger.warning(f"Error during explicit COMMIT: {commit_err}")
                            else:
                                logger.warning(f"View '{view_name}' may not have been created successfully")
                    
                    # Keep track of the DataFrame from SELECT statements
                    if is_select and df is not None:
                        # For the final SELECT statement, save the result
                        if i == len(statements) - 1:
                            last_result_df = df
                            
                            # Save DataFrame to output file if requested and not already done
                            if output_file and not os.path.exists(output_file):
                                try:
                                    if output_format.lower() == "csv":
                                        logger.info(f"Saving {len(df)} rows to CSV: {output_file}")
                                        df.to_csv(output_file, index=False)
                                    elif output_format.lower() == "csv_native":
                                        # This would have been handled by the direct COPY approach
                                        logger.info(f"Using CSV native output format (already handled if possible)")
                                        # If not handled, fall back to normal CSV
                                        if not os.path.exists(output_file):
                                            df.to_csv(output_file, index=False)
                                    elif output_format.lower() == "excel":
                                        logger.info(f"Saving {len(df)} rows to Excel: {output_file}")
                                        df.to_excel(output_file, index=False)
                                    elif output_format.lower() == "parquet":
                                        logger.info(f"Saving {len(df)} rows to Parquet: {output_file}")
                                        df.to_parquet(output_file, index=False)
                                    else:
                                        logger.warning(f"Unknown output format: {output_format}")
                                except Exception as save_err:
                                    all_errors.append(f"Error saving output: {save_err}")
                                    logger.error(f"Failed to save output: {save_err}")
            
            # Get final diagnostic information to return
            row_count = len(last_result_df) if last_result_df is not None else 0
            col_count = len(last_result_df.columns) if last_result_df is not None and hasattr(last_result_df, 'columns') else 0
            
            # Create a diagnostic sample file for troubleshooting if row count is 0
            if row_count == 0 and creates_view and output_file:
                try:
                    # Try to directly query the view with minimal SELECT and save diagnostics
                    if view_name:
                        diag_output = output_file.replace('.csv', '_diagnostic_sample.csv')
                        if not diag_output.endswith('.csv'):
                            diag_output += '_diagnostic_sample.csv'
                            
                        # Execute a simple SELECT * with LIMIT to check what's in the view
                        logger.info(f"Creating diagnostic sample query on view: {view_name}")
                        diag_query = f"SELECT * FROM {view_name} LIMIT 10;"
                        
                        d_success, d_df, d_error = execute_query_with_timeout(
                            conn, diag_query, 10, optimized_engine, 
                            {"isolation_level": "AUTOCOMMIT"}, "csv", diag_output
                        )
                        
                        if d_success and d_df is not None and len(d_df) > 0:
                            logger.info(f"Diagnostic sample created with {len(d_df)} rows, saved to {diag_output}")
                        else:
                            logger.warning(f"Diagnostic query returned no data: {d_error if d_error else 'No error'}")
                            
                            # Try one more time with a bare-bones query that shouldn't filter anything
                            final_diag = diag_output.replace('.csv', '_minimal.csv')
                            minimal_query = f"SELECT * FROM {view_name};"
                            m_success, m_df, m_error = execute_query_with_timeout(
                                conn, minimal_query, 10, optimized_engine, 
                                {"isolation_level": "AUTOCOMMIT"}, "csv", final_diag
                            )
                            
                            if m_success and m_df is not None and len(m_df) > 0:
                                logger.info(f"Minimal diagnostic sample created with {len(m_df)} rows")
                except Exception as diag_err:
                    logger.error(f"Error creating diagnostic sample: {diag_err}")
            
            # If we executed successfully, but there were previous errors, include them in the result
            if all_errors:
                message = f"Completed with {len(all_errors)} errors. Processed {processed_count}/{len(statements)} statements."
                if row_count > 0:
                    message += f" Result has {row_count} rows, {col_count} columns."
                elif last_result_df is not None:
                    message += f" Result is an empty DataFrame with {col_count} columns."
                else:
                    message += " No result data returned."
                    
                return {
                    "success": True,  # Still consider it a success if we got through all statements
                    "message": message,
                    "details": all_errors,
                    "row_count": row_count,
                    "column_count": col_count,
                    "result_file": output_file if output_file else None
                }
            
            # Normal success case
            message = f"Successfully executed {processed_count}/{len(statements)} SQL statements."
            if row_count > 0:
                message += f" Result has {row_count} rows, {col_count} columns."
            elif last_result_df is not None:
                message += f" Result is an empty DataFrame with {col_count} columns."
            else:
                message += " No result data returned."
        
        return {
                "success": True,
                "message": message,
                "row_count": row_count,
                "column_count": col_count,
                "result_file": output_file if output_file else None
        }

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in run_sql_query: {error_msg}")
        logger.error(traceback.format_exc())
        return {"success": False, "message": error_msg}

def get_db_engine_from_env(env_path=Path(".env"), max_workers=4, timeout=120):
    """
    Create and return a SQLAlchemy engine using environment variables.
    
    Args:
        env_path: Path to .env file (default: .env in current directory)
        max_workers: Maximum number of worker threads for parallel operations
        timeout: Default statement timeout in seconds
        
    Returns:
        SQLAlchemy engine or None if connection failed
    """
    try:
        # Check if .env file exists
        if not env_path.exists():
            logger.warning(f".env file not found at: {env_path.resolve()}")
            logger.info("Falling back to environment variables")
        else:
            # Load environment variables from .env file
            logger.info(f"Reading database credentials from .env file: {env_path}")
            load_dotenv(dotenv_path=env_path)
        
        # Get database connection parameters
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        # Validate required parameters
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"Missing required database parameters: {', '.join(missing_vars)}")
            return None
        
        # Create connection string using URL object for safer handling
        # This is a more modern way to handle connection URLs in SQLAlchemy
        from sqlalchemy.engine.url import URL
        connection_url = URL.create(
            drivername="postgresql",
            username=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name
        )
        
        # Set platform-appropriate options
        is_windows = sys.platform.startswith('win')
        
        # Base options that work on all platforms
        options = [
            f"statement_timeout={timeout * 1000}",  # Convert to milliseconds
            "work_mem=64MB",
            "maintenance_work_mem=256MB",
            f"max_parallel_workers_per_gather={max_workers}",
            f"max_parallel_workers={max_workers*2}",
            "effective_cache_size=4GB"
        ]
        
        # Add platform-specific options
        if not is_windows:
            # posix_fadvise is only available on Linux/Unix
            options.append("effective_io_concurrency=8")
        else:
            # On Windows, effective_io_concurrency must be 0
            options.append("effective_io_concurrency=0")
        
        # Join options with spaces
        options_str = " ".join([f"-c {opt}" for opt in options])
        
        # Create engine with optimized settings for PostgreSQL
        # Using best practices for connection pooling
        engine = create_engine(
            connection_url,
            connect_args={
                "connect_timeout": 30,
                "options": options_str,
                "application_name": "SQLExecutor"  # Best practice: identify your application
            },
            pool_size=max(5, max_workers),  # Ensure enough connections for parallel work
            max_overflow=max_workers*2,
            pool_timeout=30,
            pool_recycle=1800,  # Recycle connections after 30 minutes
            pool_pre_ping=True,  # Check connection validity before use
            execution_options={
                "timeout": timeout * 1000  # Set timeout in milliseconds
            }
        )
        
        # Test connection using a context manager for proper resource cleanup
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info(f"Connected to database '{db_name}' on {db_host}:{db_port}")
        return engine
    
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

if __name__ == "__main__":
    # Example usage when run directly
    import argparse
    from pathlib import Path
    import sys
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Execute SQL queries with timeout protection")
    parser.add_argument("query_file", help="Path to SQL query file")
    parser.add_argument("--output", help="Path for output Excel/CSV/Parquet file")
    parser.add_argument("--timeout", type=int, default=DEFAULT_QUERY_TIMEOUT,
                        help=f"Query timeout in seconds (default: {DEFAULT_QUERY_TIMEOUT})")
    parser.add_argument("--env-file", type=str, default=".env",
                       help="Path to .env file (default: .env in current directory)")
    parser.add_argument("--max-workers", type=int, default=4,
                       help="Maximum number of worker threads (default: 4)")
    parser.add_argument("--batch-size", type=int, default=250000,
                        help="Batch size for processing large result sets (default: 250000)")
    parser.add_argument("--output-format", type=str, default="excel", 
                        choices=["excel", "csv", "parquet", "csv_native"], 
                        help="Output file format (default: excel)")
    
    args = parser.parse_args()
    
    # Run the query with named parameters for clarity
    try:
        success_info = run_sql_query(
            query_file=args.query_file, 
            output_file=args.output, 
            timeout=args.timeout,
            max_workers=args.max_workers,
            batch_size=args.batch_size,
            env_path=Path(args.env_file),
            output_format=args.output_format
        )
        
        # Exit with appropriate code
        sys.exit(0 if success_info.get('success', False) else 1)
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        sys.exit(1) 