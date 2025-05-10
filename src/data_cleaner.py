#!/usr/bin/env python3
"""
data_cleaner.py - Process CSV and Excel files based on generated_config and output to Parquet format.

Usage:
    python data_cleaner.py --output_dir processed_data [--batch_size 50000]
"""

import argparse
import os
import sys
import traceback
import logging
import warnings
import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Callable, Type
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.parser import parse as date_parse
import psutil
from functools import lru_cache

# Import openpyxl and xlrd which are needed for Excel file support
# These will only be used if Excel files are processed
try:
    import openpyxl
    import xlrd
    HAS_EXCEL_SUPPORT = True
except ImportError:
    HAS_EXCEL_SUPPORT = False
    warnings.warn("Excel support libraries (openpyxl, xlrd) not found. Excel files will not be processed correctly.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_cleaner.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Suppress specific FutureWarning from pandas replace
warnings.filterwarnings(
    "ignore", 
    category=FutureWarning, 
    message=".*Downcasting behavior in `replace` is deprecated.*"
)

# Import the generated configuration
try:
    from generated_config import CONFIG
except ImportError:
    logger.critical("Could not find generated_config.py. Run generate_config.py first.")
    sys.exit(1)

# For Windows, ensure proper process spawning
if sys.platform == 'win32':
    import multiprocessing.popen_spawn_win32
    # Set the start method to 'spawn' to avoid issues with process forking on Windows
    multiprocessing.set_start_method('spawn', force=True)

# --- DATA CLEANER FRAMEWORK ---
class DataCleaner:
    """Base class for all data cleaners with common utility methods."""
    
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """Override this method in subclasses to implement specific cleaning logic."""
        raise NotImplementedError("Subclasses must implement clean method")
    
    @staticmethod
    def is_null_string(value) -> bool:
        """Helper method to check if a value is a null representation in string form."""
        return isinstance(value, str) and (
            value.lower() == "none" or 
            value.lower() == "nan" or 
            value == "<NA>"
        )
    
    @staticmethod
    def handle_nulls(series: pd.Series) -> pd.Series:
        """Standardized method for handling null values in a consistent way."""
        # Handle NaN/None values
        is_null = series.isna()
        # Convert to string, but preserve nulls
        result = series.astype(str)
        result[is_null] = pd.NA
        return result

# Registry of data cleaners for different Python types
DATA_CLEANERS: Dict[str, Type[DataCleaner]] = {}

def register_cleaner(data_type: str) -> Callable:
    """Decorator to register data cleaning classes by type."""
    def decorator(cleaner_class: Type[DataCleaner]) -> Type[DataCleaner]:
        DATA_CLEANERS[data_type] = cleaner_class
        return cleaner_class
    return decorator

@register_cleaner("integer")
class IntegerCleaner(DataCleaner):
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """Clean and convert to integer type using nullable Int64."""
        # Convert to numeric, coercing errors to pd.NA
        numeric_series = pd.to_numeric(series, errors='coerce')
        
        # Convert to nullable integer type Int64
        # This handles pd.NA correctly and allows integers alongside missing values
        result = numeric_series.astype(pd.Int64Dtype())
        return result

@register_cleaner("float")
class FloatCleaner(DataCleaner):
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """Clean and convert to float type using nullable Float64."""
        # Convert series to string for consistent processing
        str_series = series.astype(str)
        
        # Handle obvious NULL values first
        null_mask = (
            series.isna() | 
            str_series.str.strip().isin(['', 'None', 'NULL', 'null', 'NA', 'N/A', 'na', 'n/a', '<NA>'])
        )
        
        # Remove currency symbols, commas, spaces, and other non-numeric characters
        # But keep decimal points and minus signs
        cleaned = str_series.str.replace(r'[^0-9.-]', '', regex=True)
        
        # Handle empty strings which should be null
        cleaned = cleaned.replace('', pd.NA)
        
        # Set identified NULLs to pd.NA
        cleaned[null_mask] = pd.NA
        
        # Convert to numeric, coercing errors to pd.NA
        numeric_series = pd.to_numeric(cleaned, errors='coerce')
        
        # Convert to nullable float type Float64
        result = numeric_series.astype(pd.Float64Dtype())
        
        # Log basic stats for debugging if requested
        source = options.get('source', '')
        col_name = options.get('column_name', 'unknown')
        if source == 'portal' and col_name == 'Order Total':
            logger.info(f"Portal transaction_amount: {len(result)} values, {result.isna().sum()} nulls")
            if not result.empty:
                non_null = result[~result.isna()]
                if not non_null.empty:
                    logger.info(f"Sample values: {non_null.head(5).tolist()}")
                    
        return result

@register_cleaner("date")
class DateCleaner(DataCleaner):
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """
        Clean and convert to date type, handling multiple formats including Excel dates.
        Excel stores dates as days since 1899-12-30 (or 1904-01-01 in some cases).
        """
        # First check for actual NULL values or "None" strings and preserve them as NaT
        null_mask = series.isna() | series.astype(str).apply(cls.is_null_string)
        
        # Get source-specific information if available
        source = options.get('source', '')
    
        # Source-specific date formats to try first
        source_formats = {
            'tamara': ['%m/%d/%Y %H:%M', '%m/%d/%Y'],  # Added m/d/Y format for Tamara
            'payfort': ['%Y-%m-%d'],
            'metabase': ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y'],
            'checkout_v1': ['%m/%d/%Y', '%Y-%m-%d %H:%M:%S'],  # Added m/d/Y format for checkout
            'checkout_v2': ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y'],
            'bank': ['%d-%m-%Y'],
            'portal': ['%d/%m/%Y']
    }
    
        # Create a new series to store cleaned dates
        result = pd.Series(index=series.index, dtype='datetime64[ns]')
    
        # Set null values to NaT immediately
        result[null_mask] = pd.NaT
        
        # Only process non-null values
        valid_series = series[~null_mask]
        
        if valid_series.empty:
            return result  # All values were null, return early
        
        # First, try to handle Excel numeric dates (numbers like 43891 or 45376 which represent days since 1899-12-30)
        # This needs to happen BEFORE the string format parsing attempts
        numeric_mask = valid_series.astype(str).str.match(r'^\d+(\.\d*)?$').fillna(False)
        if numeric_mask.any():
            # Process Excel numeric dates
            numeric_dates = valid_series[numeric_mask].astype(str).astype(float)
            from pandas import Timestamp
            excel_base = Timestamp('1899-12-30')
            
            for idx in numeric_dates.index:
                try:
                    days = float(numeric_dates[idx])
                    # Excel has a leap year bug where it treats 1900 as a leap year
                    if days > 59:  # If after February 28, 1900
                        days -= 1  # Adjust for Excel's leap year bug
                    
                    # Create a timestamp by adding days to the Excel epoch
                    result[idx] = excel_base + pd.Timedelta(days=days)
                except Exception as e:
                    logger.debug(f"Failed to convert Excel date '{numeric_dates[idx]}': {str(e)}")
        
        # Try source-specific formats if available
        if source and source in source_formats:
            formats_to_try = source_formats[source]
            
            # Try each format in the source-specific list
            for date_format in formats_to_try:
                # Only process values that still have NaT
                remaining_mask = ~null_mask & result.isna()
                if not remaining_mask.any():
                    break  # All dates processed, exit the loop
                
                try:
            # Try this format
                    date_result = pd.to_datetime(
                        valid_series[remaining_mask], 
                        format=date_format, 
                        errors='coerce'
                    )
                    
                    # Update only successfully parsed values
                    valid_dates = date_result[~date_result.isna()]
                    if not valid_dates.empty:
                        for idx in valid_dates.index:
                            result[idx] = valid_dates[idx]
                            
                except Exception as e:
                    logger.debug(f"Error with format {date_format}: {str(e)}")
        
        # If we still have unparsed dates, try general approach with more formats
        remaining_mask = ~null_mask & result.isna()
        if remaining_mask.any():
            
            # Generic date formats to try, including both day-first and month-first variants
            date_formats = [
                '%d/%m/%Y',      # Day first (UK/Europe)
                '%m/%d/%Y',      # Month first (US)
                '%Y-%m-%d',      # ISO
                '%d-%m-%Y',      # Day-month-year with dashes
                '%Y/%m/%d',      # ISO with slashes
                '%d.%m.%Y',      # European with dots
                '%m.%d.%Y',      # US with dots
                '%d %b %Y',      # Day month-name year
                '%b %d %Y',      # Month-name day year
                '%Y%m%d',        # Compact
                '%d/%m/%Y %H:%M:%S',  # With time
                '%m/%d/%Y %H:%M:%S',  # US with time
                '%Y-%m-%d %H:%M:%S',  # ISO with time
            ]
            
            remaining = valid_series[remaining_mask]
            
            # Try each format in order for remaining values
            for date_format in date_formats:
                # Again, only process values that still have NaT
                current_remaining = ~null_mask & result.isna()
                if not current_remaining.any():
                    break  # All dates processed
                
                try:
                    date_result = pd.to_datetime(
                        valid_series[current_remaining], 
                        format=date_format, 
                        errors='coerce'
                    )
                    
                    # Update only successfully parsed values
                    valid_dates = date_result[~date_result.isna()]
                    if not valid_dates.empty:
                        for idx in valid_dates.index:
                            result[idx] = valid_dates[idx]
                            
                except Exception:
                    continue  # Try next format
        
        # As a last resort, try pandas general date parsing for remaining values
        final_remaining = ~null_mask & result.isna()
        if final_remaining.any():
            try:
                # Let pandas infer the format
                inferred_dates = pd.to_datetime(
                    valid_series[final_remaining], 
                    errors='coerce',
                    dayfirst=False,  # Try US-style parsing first (month first)
                    yearfirst=False
                )
                
                # Update successfully parsed values
                valid_inferred = inferred_dates[~inferred_dates.isna()]
                if not valid_inferred.empty:
                    for idx in valid_inferred.index:
                        result[idx] = valid_inferred[idx]
                    
            except Exception as e:
                logger.debug(f"Pandas inference error: {str(e)}")
    
        # Check if we have too many nulls after parsing
        null_percentage = result.isna().mean() * 100
        if null_percentage > 50:  # If more than 50% are null, this is suspicious
                parsed = (~result.isna()).sum()
                total = len(result)
                logger.warning(f"High null rate ({null_percentage:.1f}%) in date column after parsing. Parsed {parsed}/{total} dates.")
                
                # Log some sample unparsed values to help diagnose the issue
                unparsed_mask = ~null_mask & result.isna()
                if unparsed_mask.any():
                    unparsed_samples = valid_series[unparsed_mask].iloc[:5].tolist()
                    logger.warning(f"Sample unparsed date values: {unparsed_samples}")
        
        return result

@register_cleaner("time")
class TimeCleaner(DataCleaner):
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """Clean and convert to time, handling Excel time formats."""
        # First check for actual NULL values or "None" strings
        null_mask = series.isna() | series.astype(str).apply(cls.is_null_string)
        
        # First, try automatic conversion for standard time strings
        result = pd.Series(index=series.index, dtype='object')
    
        # Set null values to None immediately
        result[null_mask] = None
        
        # Only process non-null values
        valid_series = series[~null_mask]
        
        if valid_series.empty:
            return result  # All values were null, return early
    
    # Try pandas datetime with format
        try:
            times = pd.to_datetime(valid_series, errors='coerce', format='%H:%M:%S').dt.time
            non_null_times = pd.Series(times)
            # Update non-null positions
            result[~null_mask] = result[~null_mask].fillna(non_null_times)
        except Exception as e:
            logger.debug(f"Failed to parse times with standard format: {str(e)}")
    
    # For values that couldn't be parsed, try additional formats
        # We only process values that are not originally null but still have None after the first pass
        still_null_mask = ~null_mask & result.isna()
        if still_null_mask.any():
            problematic = series[still_null_mask]
        
        # Try to handle Excel numeric times (fractions of a day)
        numeric_mask = problematic.str.match(r'^\d*\.\d+$').fillna(False)
        if numeric_mask.any():
            numeric_times = problematic[numeric_mask].astype(float)
            
            for idx in numeric_times.index:
                try:
                    # Excel times are stored as fraction of day
                    fraction = float(numeric_times[idx])
                    # Ensure we only have the fractional part (time component)
                    fraction = fraction % 1
                    
                    # Convert to seconds, then to time
                    seconds = int(fraction * 86400)  # 86400 seconds in a day
                    hours, remainder = divmod(seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    
                    result[idx] = datetime.time(hours, minutes, seconds)
                except Exception as e:
                    logger.debug(f"Failed to convert Excel time '{numeric_times[idx]}': {str(e)}")
        
        # Try various time formats
        time_formats = ['%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p']
        
        for time_format in time_formats:
            # Only process remaining NaT values that weren't originally null
            remaining_mask = ~null_mask & result.isna()
            if not remaining_mask.any():
                break
            
            # Try this format
            remaining = series[remaining_mask]
            for idx in remaining.index:
                try:
                    time_obj = datetime.strptime(str(remaining[idx]).strip(), time_format).time()
                    result[idx] = time_obj
                except Exception:
                    pass
    
        # Check if we have too many nulls after parsing
        null_percentage = result.isna().mean() * 100
        if null_percentage > 50:  # If more than 50% are null, this is suspicious
            logger.warning(f"High null rate ({null_percentage:.1f}%) in time column after parsing")
        
        return result

@register_cleaner("string")
class StringCleaner(DataCleaner):
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """Clean and standardize strings using vectorized operations."""
        # Handle NaN values efficiently
        is_null = series.isna()
        
        # Convert non-null values to string efficiently
        str_series = series.astype(str)
        str_series[is_null] = pd.NA
        
        # Efficiently handle Excel formula remnants
        excel_formula_pattern = r'^="(.*)"$'
        has_excel_format = str_series.str.match(excel_formula_pattern).fillna(False)
        
        if has_excel_format.any():
            str_series = str_series.str.replace(excel_formula_pattern, r'\1', regex=True)
        
        # Use vectorized operations for non-null values
        non_null_mask = ~str_series.isna()
        if non_null_mask.any():
            str_series.loc[non_null_mask] = (
                str_series.loc[non_null_mask]
                .str.strip()
                .str.replace(r'\r\n|\n\r|\r|\n', ' ', regex=True)
                .str.replace(r'[\x00-\x1F\x7F]', '', regex=True)
            )
        
        return str_series

@register_cleaner("boolean")
class BooleanCleaner(DataCleaner):
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """Standardize boolean values from various formats."""
        # Map various boolean representations to standard True/False
        true_values = ['yes', 'y', 'true', 't', '1', 'on', 'completed', 'success']
        false_values = ['no', 'n', 'false', 'f', '0', 'off', 'failed', 'failure']
        
        # Convert to lowercase for case-insensitive matching
        str_series = series.astype(str).str.lower()
        
        # Create result series
        result = pd.Series(index=series.index, dtype='boolean')
        
        # Apply mapping
        result = result.mask(str_series.isin(true_values), True)
        result = result.mask(str_series.isin(false_values), False)
        
        # Check for values that couldn't be mapped
        unmapped = ~(str_series.isin(true_values) | str_series.isin(false_values))
        if unmapped.any():
            unique_unmapped = str_series[unmapped].unique()
            if len(unique_unmapped) > 0:
                logger.warning(f"Found {len(unique_unmapped)} unique values that couldn't be mapped to boolean: {unique_unmapped[:5]}")
        
        return result

@register_cleaner("uuid")
class UuidCleaner(DataCleaner):
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """Validate and standardize UUID/GUID values."""
        import uuid
        
        def is_valid_uuid(val):
            try:
                uuid_obj = uuid.UUID(str(val).strip())
                return str(uuid_obj)
            except (ValueError, AttributeError):
                return None
        
        result = series.apply(is_valid_uuid)
        
        # Check if any values failed validation
        if result.isna().any():
            invalid_count = result.isna().sum()
            logger.warning(f"Found {invalid_count} invalid UUIDs in column")
            
        return result

@register_cleaner("text")
class TextCleaner(DataCleaner):
    @classmethod
    def clean(cls, series: pd.Series, options: Dict[str, Any]) -> pd.Series:
        """Clean text fields with special handling for encoding issues."""
        # Apply basic string cleaning first (which now includes null byte removal)
        str_series = StringCleaner.clean(series, options)
        
        # Then handle common encoding problems (specific to this project's data)
        common_replacements = {
            'Ã˜': 'ا',  # Arabic alef
            'Ù†': 'ن',  # Arabic noon
            'Ø¹': 'ع',  # Arabic ain
            'Ø§': 'ا',  # Arabic alef
            'Ù…': 'م',  # Arabic meem
        }
        
        # Apply common replacements if they appear in the text
        for orig, repl in common_replacements.items():
            if str_series.str.contains(orig).any():
                logger.info(f"Fixing encoding issues by replacing '{orig}' with '{repl}'")
                str_series = str_series.str.replace(orig, repl, regex=False)
        
        # Handle HTML entities
        str_series = str_series.str.replace('&amp;', '&', regex=False)
        str_series = str_series.str.replace('&lt;', '<', regex=False)
        str_series = str_series.str.replace('&gt;', '>', regex=False)
        str_series = str_series.str.replace('&quot;', '"', regex=False)
        
        # Replace multiple spaces with single space (already done in clean_string, but safe to repeat)
        str_series = str_series.str.replace(r'\s+', ' ', regex=True)
        
        return str_series

def get_csv_reader_options(source: str, file_path: str) -> Dict[str, Any]:
    """Get source-specific CSV reader options, with Excel CSV compatibility."""
    # Default options - use QUOTE_MINIMAL for better handling of delimiters within fields
    # Remove low_memory as it's not compatible with the python engine used later
    options = {
        'encoding': 'utf-8',
        'sep': ',',
        # 'low_memory': False, # Removed - not supported by python engine
        'dtype': str,
        'na_filter': False,
        'on_bad_lines': 'warn',
        'quoting': 0,  # Use QUOTE_MINIMAL by default
        'dayfirst': True,  # Assume day-first date format (dd/mm/yyyy)
    }
    
    # Try to detect encoding
    detected_encoding = detect_encoding(file_path, source)
    logger.info(f"Detected encoding for {file_path}: {detected_encoding}")
    options['encoding'] = detected_encoding
    
    # Source-specific overrides
    if source == 'payfort':
        options['thousands'] = ','
    elif source == 'tamara':
        options['decimal'] = ','
    elif source == 'checkout_v1' or source == 'checkout_v2':
        # These might contain mixed formats
        options['quoting'] = 0  # QUOTE_MINIMAL - handle quotes properly
    
    return options

class DataFrameTransformer:
    """Handles cleaning and transformation of DataFrames based on configuration."""
    
    def __init__(self, column_config: Dict, source: str):
        """
        Initialize the transformer.
        
        Args:
            column_config: Dictionary with column configuration
            source: Source identifier
        """
        self.column_config = column_config
        self.source = source
        self.failed_columns = []
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform a DataFrame based on configuration.
        
        Args:
            df: Raw DataFrame from CSV
            
        Returns:
            Cleaned and transformed DataFrame
        """
        # Reset failed columns tracking
        self.failed_columns = []
        
        # Filter DataFrame to only include columns defined in the config
        filtered_df = self._filter_columns(df)
        
        # Remove null bytes from all kept columns
        cleaned_df = self._remove_null_bytes(filtered_df)
        
        # Transform each column according to configuration
        result_df = self._transform_columns(cleaned_df)
        
        # Add metadata columns
        result_df = self._add_metadata(result_df)
        
        return result_df
    
    def _filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter DataFrame to only include columns defined in the config."""
        # Extract the set of EXPECTED ORIGINAL column names from the config
        config_original_columns = set(specs["original"] for specs in self.column_config.values())
        actual_columns_in_df = set(df.columns)

        # Find exact matches
        columns_to_keep = list(config_original_columns.intersection(actual_columns_in_df))
        
        # --- Logging ---
        missing_from_df = list(config_original_columns - actual_columns_in_df)
        if missing_from_df:
            logger.warning(f"[{self.source}] Configured columns NOT FOUND in CSV chunk: {missing_from_df}")
        extra_in_df = list(actual_columns_in_df - config_original_columns)
        if extra_in_df:
            logger.warning(f"[{self.source}] Columns in CSV chunk NOT IN config (DROPPED): {extra_in_df}")
        # --- End Logging ---

        if not columns_to_keep:
            logger.error(f"[{self.source}] No columns left to keep after filtering! Check header matching in config vs CSV.")
            # Return an empty DataFrame matching index type if possible
            return pd.DataFrame(index=df.index) 
            
        try:
            # Attempt to select the columns we intend to keep
            filtered_df = df[columns_to_keep].copy() # Use copy to avoid SettingWithCopyWarning later
            logger.debug(f"[{self.source}] Successfully filtered to {len(columns_to_keep)} columns: {columns_to_keep}")
            if filtered_df.empty and not df.empty:
                 logger.error(f"[{self.source}] Filtering resulted in an EMPTY DataFrame even though input was not empty and columns were selected ({columns_to_keep}). Check CSV integrity or slicing issue.")
                 return pd.DataFrame(index=df.index) # Return empty frame
            return filtered_df
        except Exception as e:
            logger.error(f"[{self.source}] CRITICAL ERROR during column filtering/selection: {e}")
            logger.error(f"[{self.source}] Attempted to keep: {columns_to_keep}")
            logger.error(f"[{self.source}] Available columns were: {list(actual_columns_in_df)}")
            logger.error(traceback.format_exc())
            # Return empty frame on critical error
            return pd.DataFrame(index=df.index)

    def _remove_null_bytes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggressively remove null bytes from all kept columns."""
        if df.empty: # Add check for empty input
             return df

        # Create a copy to avoid modifying the input DataFrame
        result_df = df.copy()
        
        for col in result_df.columns: # Iterate original names of kept columns
            if result_df[col].dtype == 'object': # Only apply to object/string types
                try:
                    # Use .loc to avoid SettingWithCopyWarning
                        result_df.loc[:, col] = result_df[col].astype(str).str.replace(r'[\x00-\x1F\x7F]', '', regex=True)
                except Exception as e:
                        logger.warning(f"Could not apply null byte removal to column '{col}': {e}")
        
        return result_df
    
    def _transform_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform each column according to configuration."""
        if df.empty: # Add check for empty input
             logger.warning(f"[{self.source}] _transform_columns received an empty DataFrame. Skipping transformation.")
             return df # Return the empty DataFrame

        # Create new DataFrame for the result
        result_df = pd.DataFrame(index=df.index)
        
        # Reset failed columns tracking
        self.failed_columns = []
        success_columns = []
        skipped_columns = []
        
        # Map the columns from original names to target names and apply data cleaning
        # NOTE: df here should ONLY contain columns listed in 'columns_to_keep' from _filter_columns
        for mapped_col, specs in self.column_config.items():
            original_name = specs["original"]
            mapped_name = specs["map_to"]
            py_type = specs["py_type"]
        
            # Skip if original column not in the filtered DataFrame (should only happen if filtering failed)
            if original_name not in df.columns:
                # This condition implies the column was in the config but didn't survive filtering.
                skipped_columns.append(original_name)
                continue
            
            try:
                # Get the original column series
                original_series = df[original_name]
                
                # Get appropriate data cleaner function
                cleaner_class = DATA_CLEANERS.get(py_type)
                if cleaner_class is None:
                    logger.warning(f"No cleaner found for type '{py_type}', using string cleaner for '{original_name}'")
                    cleaner_class = StringCleaner
                
                # Create options dictionary with source information and column details
                cleaning_options = {
                    'source': self.source,
                    'column_name': original_name,
                    'mapped_name': mapped_name
                }
                    
                # Apply the appropriate cleaner function to the column
                cleaned_series = cleaner_class.clean(original_series, cleaning_options)
                
                # Add to result DataFrame with the mapped name
                result_df[mapped_name] = cleaned_series
                success_columns.append(original_name)
                
            except Exception as e:
                logger.error(f"Error processing column '{original_name}' (mapped to '{mapped_name}'): {str(e)}")
                logger.error(f"Error details: {traceback.format_exc()}")
                self.failed_columns.append(original_name)
                
                # Even if transformation fails, preserve the column by copying directly
                try:
                    result_df[mapped_name] = df[original_name].copy()
                    logger.info(f"Preserved column '{original_name}' -> '{mapped_name}' despite error")
                except Exception as copy_error:
                    # If direct copy fails, add as NA to maintain schema
                    result_df[mapped_name] = pd.NA
                    logger.info(f"Added column '{mapped_name}' as NA due to error: {str(copy_error)}")
        
        # Log information about processing results
        if self.failed_columns:
            logger.warning(f"Failed to transform {len(self.failed_columns)} columns: {self.failed_columns}")
        if skipped_columns:
            logger.warning(f"Skipped {len(skipped_columns)} columns not in input data: {skipped_columns}")
        logger.info(f"Successfully processed {len(success_columns)}/{len(self.column_config)} columns")

        return result_df
    
    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata columns to the DataFrame."""
        # Create a copy to avoid modifying the input DataFrame
        result_df = df.copy()
        
        # Add source information
        result_df['data_source'] = self.source
        
        # Add processing timestamp
        result_df['processed_at'] = pd.Timestamp.now()
        
        return result_df

def clean_dataframe(df: pd.DataFrame, column_config: Dict, source: str) -> pd.DataFrame:
    """
    Clean and transform a DataFrame based on configuration.
    
    This is a compatibility wrapper around the DataFrameTransformer class.
    
    Args:
        df: Raw DataFrame from CSV
        column_config: Dictionary with column configuration
        source: Source identifier
        
    Returns:
        Cleaned and transformed DataFrame
    """
    transformer = DataFrameTransformer(column_config, source)
    return transformer.transform(df)

def read_csv_with_excel_handling(file_path: str, options: Dict[str, Any]) -> pd.DataFrame:
    """
    Read a CSV file with special handling for Excel-exported CSV quirks.
    Returns a pandas DataFrame.
    """
    # Check if this is actually a native Excel file (.xlsx, .xls, .xlsm)
    if file_path.lower().endswith(('.xlsx', '.xls', '.xlsm')):
        return read_excel_file(file_path, options)
        
    try:
        # First try with the provided options
        logger.info(f"Attempting to read {file_path} with standard options")
        return pd.read_csv(file_path, **options)
    except pd.errors.ParserError as e:
        error_msg = f"Parser error for {file_path}: {str(e)}"
        logger.warning(error_msg)
        
        # Try to detect the delimiter
        try:
            # Try to open the file with the specified encoding
            encoding = options.get('encoding', 'utf-8')
            try:
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    first_line = f.readline().strip()
            except Exception as e:
                logger.warning(f"Error reading first line with encoding {encoding}: {str(e)}")
                # Fallback to binary read
                with open(file_path, 'rb') as f:
                    first_line_bytes = f.readline()
                    first_line = first_line_bytes.decode(encoding, errors='replace').strip()
            
            # Check if this might be semicolon-delimited (common in European Excel)
            if ';' in first_line and ',' not in first_line:
                options['sep'] = ';'
                logger.info(f"Detected semicolon delimiter in {file_path}")
            elif '\t' in first_line and ',' not in first_line:
                options['sep'] = '\t'
                logger.info(f"Detected tab delimiter in {file_path}")
            
            # Try with more robust Excel handling
            excel_options = options.copy()
            excel_options['engine'] = 'python'  # More flexible but slower engine
            excel_options['quoting'] = 0  # QUOTE_MINIMAL
            excel_options['escapechar'] = '\\'  # Handle escape characters
            
            # Increase error tolerance
            excel_options['on_bad_lines'] = 'skip'  # Skip lines that can't be parsed
            
            logger.info(f"Retrying with Excel-specific options for {file_path}")
            return pd.read_csv(file_path, **excel_options)
            
        except Exception as e2:
            error_msg = f"Excel handling failed for {file_path}: {str(e2)}"
            logger.error(error_msg)
            
            # Last resort: try with lowest common denominator options
            try:
                fallback_options = {
                    'encoding': 'utf-8',
                    'sep': ',',
                    'engine': 'python',
                    'quoting': 0,
                    'on_bad_lines': 'skip',
                    'dtype': str  # Read everything as strings
                }
                
                if 'nrows' in options:
                    fallback_options['nrows'] = options['nrows']
                if 'chunksize' in options:
                    fallback_options['chunksize'] = options['chunksize']
                
                logger.info(f"Last resort: trying with minimal options for {file_path}")
                return pd.read_csv(file_path, **fallback_options)
            except Exception as e3:
                error_msg = f"All read attempts failed for {file_path}: {str(e3)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
    
    except UnicodeDecodeError as e:
        error_msg = f"Unicode decode error for {file_path}: {str(e)}"
        logger.warning(error_msg)
        
        # Try different encoding
        try:
            # Try with latin1 (should never fail as it can read any bytes)
            excel_options = options.copy()
            excel_options['encoding'] = 'latin1'
            excel_options['engine'] = 'python'
            
            logger.info(f"Retrying with latin1 encoding for {file_path}")
            return pd.read_csv(file_path, **excel_options)
        except Exception as e2:
            error_msg = f"Latin1 fallback failed for {file_path}: {str(e2)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
    except Exception as e:
        error_msg = f"Initial read failed for {file_path}: {str(e)}"
        logger.warning(error_msg)
        
        # Try with more robust engine
        try:
            # Try with python engine (more tolerant)
            python_options = options.copy()
            python_options['engine'] = 'python'
            python_options['on_bad_lines'] = 'skip'
            
            logger.info(f"Retrying with python engine for {file_path}")
            return pd.read_csv(file_path, **python_options)
        except Exception as e2:
            error_msg = f"Python engine fallback failed for {file_path}: {str(e2)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

def read_excel_file(file_path: str, options: Dict[str, Any]) -> pd.DataFrame:
    """
    Read a native Excel file (.xlsx, .xls) and return as a DataFrame.
    Handles chunking to simulate CSV-like reading behavior.
    """
    logger.info(f"Reading native Excel file: {file_path}")
    
    # Check if Excel support is available
    if not HAS_EXCEL_SUPPORT:
        error_msg = f"Cannot read Excel file {file_path}: Excel support libraries (openpyxl, xlrd) not installed"
        logger.error(error_msg)
        raise ImportError(error_msg + ". Please install with: pip install openpyxl xlrd")
    
    # Extract relevant options for Excel reading
    excel_options = {
        'dtype': str  # Read everything as strings for consistency with CSV handling
    }
    
    # Add sheet_name if specified (default to first sheet)
    excel_options['sheet_name'] = options.get('sheet_name', 0)
    
    # Add nrows if chunking is requested
    if 'nrows' in options:
        excel_options['nrows'] = options['nrows']
    
    # Add header option (default to 0)
    excel_options['header'] = options.get('header', 0)
    
    # Read the Excel file
    try:
        logger.info(f"Attempting to read Excel file with options: {excel_options}")
        df = pd.read_excel(file_path, **excel_options)
        
        # If chunksize was requested, we need to handle it differently since read_excel doesn't support it
        # Here we're reading the whole file but can return a subset based on chunksize
        if 'chunksize' in options and 'nrows' not in options:
            chunksize = options['chunksize']
            logger.info(f"Excel file read with {len(df)} rows. Processing first chunk of {chunksize} rows.")
            # Only return the first chunk - this is a compromise since Excel doesn't support native chunking
            # For large Excel files, memory could be an issue
            return df.iloc[:chunksize]
            
        logger.info(f"Excel file read successfully with {len(df)} rows")
        return df
    
    except Exception as e:
        error_msg = f"Error reading Excel file {file_path}: {str(e)}"
        logger.error(error_msg)
        
        # Try alternative approaches for problematic Excel files
        try:
            # Try with xlrd engine for older .xls files
            logger.info(f"Retrying with xlrd engine for {file_path}")
            alt_options = excel_options.copy()
            alt_options['engine'] = 'xlrd'
            return pd.read_excel(file_path, **alt_options)
        except Exception as e_xlrd:
            logger.warning(f"xlrd engine failed: {str(e_xlrd)}")
            
            try:
                # Try with openpyxl engine for newer .xlsx files
                logger.info(f"Retrying with openpyxl engine for {file_path}")
                alt_options = excel_options.copy()
                alt_options['engine'] = 'openpyxl'
                return pd.read_excel(file_path, **alt_options)
            except Exception as e_openpyxl:
                logger.error(f"All Excel reading attempts failed for {file_path}")
                logger.error(f"Original error: {str(e)}")
                logger.error(f"xlrd error: {str(e_xlrd)}")
                logger.error(f"openpyxl error: {str(e_openpyxl)}")
                raise ValueError(f"Could not read Excel file {file_path}")

def validate_file(file_path: str) -> bool:
    """
    Validate that the file exists and is readable.
    Raises an exception if any validation fails.
    """
    path = Path(file_path)
    
    # Check if file exists
    if not path.exists():
        error_msg = f"File does not exist: {file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Check if it's actually a file
    if not path.is_file():
        error_msg = f"Path is not a file: {file_path}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Check if file is empty
    if path.stat().st_size == 0:
        error_msg = f"File is empty: {file_path}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Try to open the file
    try:
        with open(file_path, 'rb') as f:
            # Try to read first few bytes
            f.read(10)
        logger.info(f"File validated successfully: {file_path}")
        return True
    except Exception as e:
        error_msg = f"Unable to read file {file_path}: {str(e)}"
        logger.error(error_msg)
        raise IOError(error_msg)

# --- FILE PROCESSING FRAMEWORK ---
class SchemaManager:
    """Manages PyArrow schema creation and validation based on column configuration."""
    
    @staticmethod
    def create_target_schema(column_config: Dict[str, Dict]) -> Tuple[pa.Schema, List[str]]:
        """
        Create PyArrow schema from column configuration.
        
        Returns:
            Tuple of (schema, column_names)
        """
        target_pa_schema_fields = []
        
        # Add data columns from config
        for mapped_col, specs in column_config.items():
            mapped_name = specs["map_to"]
            py_type = specs["py_type"]
            arrow_type = None
            
            # Map Python types to Arrow types
            if py_type == "integer": arrow_type = pa.int64()
            elif py_type == "float": arrow_type = pa.float64()
            elif py_type == "date": arrow_type = pa.date32()
            elif py_type == "time": arrow_type = pa.time32('s')
            elif py_type == "boolean": arrow_type = pa.bool_()
            else: arrow_type = pa.string()  # Use string() for all text types
            
            target_pa_schema_fields.append((mapped_name, arrow_type))
            
        # Add metadata columns
        target_pa_schema_fields.extend([
            ('data_source', pa.string()),
            ('processed_at', pa.timestamp('ns'))
        ])
        
        # Create schema and extract column names
        target_pa_schema = pa.schema(target_pa_schema_fields)
        target_column_names = [name for name, _ in target_pa_schema_fields]
        
        return target_pa_schema, target_column_names
    
    # Removed clean_null_values_for_schema method

class FileProcessor:
    """
    Handles file operations with proper error handling and validation.
    Implements batch processing for large files.
    """
    
    def __init__(self, source: str, output_dir: str, batch_size: int = 250000):
        """
        Initialize the file processor.
        
        Args:
            source: Source identifier (e.g., 'tamara', 'payfort')
            output_dir: Directory to save processed files
            batch_size: Number of rows to process at once
        """
        self.source = source
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.schema_manager = SchemaManager()
        self.total_rows = 0
        self.error_count = 0
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
    
    def validate_file(self, file_path: str) -> bool:
        """Validate that the file exists and is readable."""
        path = Path(file_path)
        
        # Check if file exists
        if not path.exists():
            error_msg = f"File does not exist: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Check if it's actually a file
        if not path.is_file():
            error_msg = f"Path is not a file: {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Check if file is empty
        if path.stat().st_size == 0:
            error_msg = f"File is empty: {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Try to open the file
        try:
            with open(file_path, 'rb') as f:
                # Try to read first few bytes
                f.read(10)
            logger.info(f"File validated successfully: {file_path}")
            return True
        except Exception as e:
            error_msg = f"Unable to read file {file_path}: {str(e)}"
            logger.error(error_msg)
            raise IOError(error_msg)
    
    @lru_cache(maxsize=128)
    def _get_column_cleaners(self, column_config):
        """Cache the column cleaner configuration to reduce setup time"""
        return {specs["original"]: DATA_CLEANERS.get(specs["py_type"], StringCleaner) 
                for mapped_col, specs in column_config.items()}

    def get_csv_reader(self, file_path: str, column_config: Dict) -> pd.io.parsers.TextFileReader:
        """Get CSV reader with appropriate options for the source."""
        csv_options = get_csv_reader_options(self.source, file_path)
        csv_options['chunksize'] = self.batch_size
        csv_options['engine'] = 'python'  # For more robust handling
        
        # Performance optimization - memory mapping is compatible with python engine
        csv_options['memory_map'] = True  
        
        # Remove incompatible options with python engine
        keys_to_remove = ['low_memory']
        for key in keys_to_remove:
            if key in csv_options:
                del csv_options[key]
        
        # Try to read sample for validation
        self._validate_sample(file_path, csv_options, column_config)
        
        return pd.read_csv(file_path, **csv_options)
    
    def get_file_reader(self, file_path: str, column_config: Dict):
        """
        Get appropriate reader for the file type (CSV or Excel)
        Returns either a TextFileReader for CSV or a generator for Excel files
        """
        # Check if this is an Excel file
        if file_path.lower().endswith(('.xlsx', '.xls', '.xlsm')):
            return self._get_excel_reader(file_path, column_config)
        else:
            return self.get_csv_reader(file_path, column_config)
    
    def _get_excel_reader(self, file_path: str, column_config: Dict):
        """
        Create a generator that simulates chunked reading for Excel files.
        Since Excel doesn't natively support chunking, we read the whole file
        and then yield chunks to match the CSV chunking behavior.
        """
        # Get CSV reader options and adapt for Excel
        excel_options = get_csv_reader_options(self.source, file_path)
        
        # Remove CSV-specific options
        for key in ['sep', 'encoding', 'quoting', 'escapechar', 'on_bad_lines', 'memory_map']:
            if key in excel_options:
                del excel_options[key]
        
        # Validate Excel file sample
        self._validate_sample(file_path, excel_options, column_config)
        
        # Read the entire Excel file (this could be memory-intensive for large files)
        logger.info(f"Reading entire Excel file {file_path} into memory for chunk processing")
        try:
            df = read_excel_file(file_path, excel_options)
            
            # Create a generator to yield chunks
            total_rows = len(df)
            logger.info(f"Excel file {file_path} loaded with {total_rows} rows, will process in chunks of {self.batch_size}")
            
            # Process in chunks
            for start in range(0, total_rows, self.batch_size):
                end = min(start + self.batch_size, total_rows)
                logger.info(f"Yielding Excel chunk rows {start} to {end}")
                yield df.iloc[start:end]
        
        except Exception as e:
            logger.error(f"Error creating Excel reader for {file_path}: {str(e)}")
            raise
    
    def _validate_sample(self, file_path: str, csv_options: Dict, column_config: Dict) -> None:
        """Read a sample of the file to validate columns."""
        try:
            sample_options = csv_options.copy()
            sample_options['nrows'] = 5
            
            # Remove chunksize if present to ensure we get a DataFrame, not a TextFileReader
            if 'chunksize' in sample_options:
                del sample_options['chunksize']
            
            # Check if this is an Excel file
            if file_path.lower().endswith(('.xlsx', '.xls', '.xlsm')):
                sample_df = read_excel_file(file_path, sample_options)
            else:
                # For CSV files
                sample_df = read_csv_with_excel_handling(file_path, sample_options)
            
            # Now sample_df should be a DataFrame, not a TextFileReader
            if sample_df.empty:
                 logger.warning(f"Sample data read was empty for {file_path}")
                 return
                
            # Validate required columns
            expected_original_columns = {specs["original"]: specs.get("required", True) 
                                       for specs in column_config.values()}
                
            # Separate required and optional columns
            required_original_columns = {col for col, req in expected_original_columns.items() if req}
            optional_original_columns = {col for col, req in expected_original_columns.items() if not req}
                
            actual_columns_in_sample = set(sample_df.columns)
                
            # Check for missing required columns
            missing_required_columns = list(required_original_columns - actual_columns_in_sample)
            if missing_required_columns:
                 logger.warning(f"Missing REQUIRED original columns in sample of {file_path}: {missing_required_columns}")
                
            # Check for missing optional columns
            missing_optional_columns = list(optional_original_columns - actual_columns_in_sample)
            if missing_optional_columns:
                 logger.info(f"Missing OPTIONAL original columns in sample of {file_path}: {missing_optional_columns}")
                
        except Exception as sample_e:
            logger.warning(f"Could not read sample from {file_path}: {str(sample_e)}")
    
    def process_file(self, file_path: str, column_config: Dict) -> str:
        """
        Process a CSV or Excel file in batches and save to Parquet format.
        
        Args:
            file_path: Path to CSV or Excel file
            column_config: Dictionary with column configuration
            
        Returns:
            Path to output Parquet file
        """
        # Reset counters
        self.total_rows = 0
        self.error_count = 0
        
        # Validate the file first
        self.validate_file(file_path)
        
        # Generate output file name
        file_name = Path(file_path).stem
        output_file = os.path.join(self.output_dir, f"{self.source}_{file_name}.parquet")
        
        # Create schema
        target_schema, target_column_names = SchemaManager.create_target_schema(column_config)
        
        # Process the file
        self._process_file_in_batches(file_path, output_file, column_config, target_schema, target_column_names)
        
        # Post-process if needed (duplicate detection)
        if self.total_rows > 0 and self.error_count == 0:
            self._post_process_file(output_file)
        elif self.error_count > 0:
            logger.warning(f"Skipping post-processing for {output_file} due to {self.error_count} batch error(s).")
        elif self.total_rows == 0:
            logger.warning(f"Skipping post-processing for {output_file} as no rows were processed.")
        
        return output_file
    
    def _process_file_in_batches(self, file_path: str, output_file: str, 
                               column_config: Dict, target_schema: pa.Schema, 
                               target_column_names: List[str]) -> None:
        """Process a file in batches and write to Parquet."""
        writer = None
        file_reader = None
        
        try:
            # Get appropriate file reader based on file type (CSV or Excel)
            file_reader = self.get_file_reader(file_path, column_config)
            
            # Open Parquet writer with optimized settings
            writer = pq.ParquetWriter(
                output_file,
                target_schema,
                compression='snappy',  # Stick with snappy for better compatibility
                version='2.6',
                use_dictionary=True,
                write_statistics=True
            )
            logger.info(f"[{self.source}] Starting batch processing for {file_path}")
            
            # Process batches
            for i, chunk in enumerate(file_reader):
                try:
                    logger.info(f"[{self.source}] Processing batch {i+1} ({len(chunk)} rows)")
                    raw_columns = chunk.columns.tolist() # Added Log
                    logger.debug(f"[{self.source}] Batch {i+1}: Columns read from file: {raw_columns}") # Added Log
                
                    # Clean and transform the chunk
                    cleaned_chunk = clean_dataframe(chunk, column_config, self.source)
                    cleaned_columns = cleaned_chunk.columns.tolist() # Added Log
                    logger.debug(f"[{self.source}] Batch {i+1}: Columns after cleaning/transformation: {cleaned_columns}") # Added Log

                    # Ensure schema conformance and correct dtypes right before conversion
                    conformed_chunk = self._ensure_schema_conformance(cleaned_chunk, target_schema, target_column_names) 
                    conformed_columns = conformed_chunk.columns.tolist() # Added Log
                    logger.debug(f"[{self.source}] Batch {i+1}: Columns after schema conformance: {conformed_columns}") # Added Log

                    # Check if columns were lost during cleaning/conformance
                    lost_during_clean = set(raw_columns) - set(cleaned_columns) # Heuristic - needs refinement based on config
                    gained_during_conform = set(conformed_columns) - set(cleaned_columns) # Columns added by conformance (likely nulls)
                    
                    # This comparison is tricky because names change (_filter_columns -> _transform_columns)
                    # A more robust check would compare expected mapped columns vs cleaned_columns
                    expected_mapped_cols = {specs["map_to"] for specs in column_config.values() 
                                            if specs["original"] in raw_columns}
                    missing_after_clean = expected_mapped_cols - set(cleaned_columns)
                    if missing_after_clean:
                         logger.warning(f"[{self.source}] Batch {i+1}: Columns expected after mapping but MISSING after cleaning: {list(missing_after_clean)}")

                    if gained_during_conform:
                        logger.warning(f"[{self.source}] Batch {i+1}: Columns ADDED during schema conformance (likely all null): {list(gained_during_conform)}")


                    # Update row count
                    self.total_rows += len(conformed_chunk)
                
                    # Convert to PyArrow and write
                    try:
                        # No need for clean_null_values_for_schema anymore
                        # Convert to PyArrow with explicit schema preservation
                        pa_table = pa.Table.from_pandas(conformed_chunk, schema=target_schema, preserve_index=False)
                        
                        # Write batch to Parquet
                        writer.write_table(pa_table)
                        # logger.debug(f"Successfully wrote batch {i+1} to Parquet.") # Removed Log
                    
                    except Exception as e:
                        self._handle_conversion_error(e, conformed_chunk) # Pass conformed chunk
                        raise

                except Exception as batch_e:
                    self.error_count += 1
                    error_msg = f"Error processing batch {i+1} from {file_path}: {str(batch_e)}"
                    logger.error(error_msg)
                    logger.error(f"Stack trace for batch {i+1}: {traceback.format_exc()}")
                    
                    # Fail fast on first batch error
                    if self.error_count == 1:
                        raise RuntimeError(f"Failing fast due to error in first problematic batch ({i+1})")

            logger.info(f"[{self.source}] Finished processing {self.total_rows} rows for {file_path}.")

        except Exception as e:
            error_msg = f"Failed processing file {file_path}: {str(e)}"
            logger.error(error_msg)
            
            # Cleanup if error
            if os.path.exists(output_file):
                try:
                    # Close writer before removing
                    if writer is not None:
                        try:
                           writer.close()
                        except:
                           pass
                        writer = None

                    os.remove(output_file)
                    logger.info(f"Removed partial output file {output_file} due to processing error.")
                except Exception as e_remove:
                    logger.warning(f"Failed to remove partial file {output_file}: {e_remove}")
            
            raise RuntimeError(error_msg)

        finally:
            # Ensure writer is closed
            if writer is not None:
                 try:
                      writer.close()
                      logger.info(f"Parquet writer closed successfully for {output_file}")
                 except Exception as e_close:
                        logger.error(f"Error closing Parquet writer for {output_file}: {e_close}")

    def _ensure_schema_conformance(self, df: pd.DataFrame, schema: pa.Schema, 
                                 column_names: List[str]) -> pd.DataFrame:
        """
        Ensure the DataFrame conforms to the schema and has appropriate nullable dtypes.
        Handles missing columns and corrects dtypes before PyArrow conversion.
        """
        # Create a copy to avoid modifying the original chunk
        result_df = df.copy()
        current_columns = set(result_df.columns)
        
        # Process each column based on its type in the schema
        for field in schema:
            col_name = field.name
            arrow_type = field.type
            
            # Handle metadata columns separately if needed, or ensure they exist
            if col_name in ('data_source', 'processed_at'):
                if col_name not in current_columns:
                     # Add if missing (should have been added in _add_metadata)
                     logger.warning(f"[{self.source}] _ensure_schema: Metadata column '{col_name}' missing, adding.")
                     if col_name == 'data_source':
                         result_df[col_name] = self.source 
                     elif col_name == 'processed_at':
                         result_df[col_name] = pd.Timestamp.now()
                continue # Skip further processing for metadata columns

            # Add missing data columns with appropriate null values
            if col_name not in current_columns:
                logger.warning(f"[{self.source}] _ensure_schema: Column '{col_name}' (expected by schema) missing from cleaned data, adding as null.")
                if pa.types.is_integer(arrow_type):
                    result_df[col_name] = pd.Series(pd.NA, index=result_df.index, dtype=pd.Int64Dtype())
                elif pa.types.is_floating(arrow_type):
                    result_df[col_name] = pd.Series(pd.NA, index=result_df.index, dtype=pd.Float64Dtype())
                elif pa.types.is_boolean(arrow_type):
                    result_df[col_name] = pd.Series(pd.NA, index=result_df.index, dtype=pd.BooleanDtype())
                elif pa.types.is_temporal(arrow_type): # Includes date, time, timestamp
                    result_df[col_name] = pd.Series(pd.NaT, index=result_df.index, dtype='datetime64[ns]')
                else: # String or other types
                    result_df[col_name] = pd.Series(pd.NA, index=result_df.index, dtype=pd.StringDtype()) # Use nullable string
                continue

            # Ensure correct nullable dtype for existing columns before PyArrow conversion
            try:
                current_dtype = result_df[col_name].dtype
                target_dtype = None
                if pa.types.is_integer(arrow_type) and not isinstance(current_dtype, pd.Int64Dtype):
                    target_dtype = pd.Int64Dtype()
                elif pa.types.is_floating(arrow_type) and not isinstance(current_dtype, pd.Float64Dtype):
                    target_dtype = pd.Float64Dtype()
                elif pa.types.is_boolean(arrow_type) and not isinstance(current_dtype, pd.BooleanDtype):
                     target_dtype = pd.BooleanDtype()
                elif pa.types.is_date(arrow_type) and not pd.api.types.is_datetime64_any_dtype(current_dtype):
                     # Special handling for date conversion
                     result_df[col_name] = pd.to_datetime(result_df[col_name], errors='coerce')
                     continue # Skip generic conversion below
                elif pa.types.is_time(arrow_type):
                     pass 
                elif (pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)) and not isinstance(current_dtype, pd.StringDtype):
                     target_dtype = pd.StringDtype()

                # Apply generic conversion if needed
                if target_dtype:
                    if isinstance(target_dtype, (pd.Int64Dtype, pd.Float64Dtype)):
                         result_df[col_name] = pd.to_numeric(result_df[col_name], errors='coerce').astype(target_dtype)
                    else:
                         result_df[col_name] = result_df[col_name].astype(target_dtype)


            except Exception as e:
                 logger.warning(f"Could not ensure dtype for column '{col_name}' (Arrow type: {arrow_type}): {e}. Proceeding with existing dtype.")
        
        # Reorder columns to match schema, dropping extras
        final_columns = [name for name in column_names if name in result_df.columns]
        dropped_columns = set(result_df.columns) - set(final_columns)
        if dropped_columns:
            logger.debug(f"[{self.source}] _ensure_schema: Dropping extra columns not in schema: {list(dropped_columns)}")
        result_df = result_df[final_columns]
        
        return result_df
    
    def _handle_conversion_error(self, error: Exception, df: pd.DataFrame) -> None:
        """Handle errors during conversion to PyArrow."""
        logger.error(f"Error creating PyArrow table: {str(error)}")
        
        # Try to identify problematic columns
        for col in df.columns:
            try:
                # Check column data for problematic values
                col_data = df[col]
                logger.info(f"Column {col} type: {col_data.dtype}, non-null count: {col_data.count()}")
                if col_data.count() > 0:
                    # Print first non-null value if any
                    first_non_null = col_data[col_data.notna()].iloc[0] if any(col_data.notna()) else "All null"
                    logger.info(f"First non-null in {col}: {first_non_null}")
            except Exception as col_e:
                logger.error(f"Error checking column {col}: {str(col_e)}")
    
    def _post_process_file(self, output_file: str) -> None:
        """
        Post-process the Parquet file (duplicate detection).
        
        Args:
            output_file: Path to the Parquet file
        """
        try:
            logger.info(f"Starting post-processing (duplicate detection) for {output_file}")
            
            # Read the Parquet file
            full_df = pd.read_parquet(output_file)
            
            # Add duplicate detection column
            full_df = detect_duplicates(full_df, self.source)
            
            # Rewrite the file with duplicate information
            self._rewrite_with_duplicates(full_df, output_file)
            
            logger.info(f"Finished post-processing for {output_file}")
            
        except Exception as post_e:
            error_msg = f"Failed during post-processing for {output_file}: {str(post_e)}"
            logger.error(error_msg)
            logger.error(f"Stack trace: {traceback.format_exc()}")
            logger.warning(f"Post-processing failed. The file {output_file} exists but may lack duplicate flags.")
    
    def _rewrite_with_duplicates(self, df: pd.DataFrame, output_file: str) -> None:
        """Rewrite Parquet file with duplicate information."""
        try:
            # Clean string columns before schema detection
            for col in df.columns:
                if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col].dtype):
                    # Replace NaN strings with None before conversion to Arrow
                    df[col] = df[col].astype(str).replace('nan', None)
                
            # Create the schema with explicit string handling - This might be less reliable now
            # Let's try inferring from the DataFrame with nullable dtypes first
            try:
                final_schema = pa.Schema.from_pandas(df, preserve_index=False)
                logger.info(f"Final schema inferred successfully with {len(final_schema)} fields")
            except Exception as e_infer:
                 logger.warning(f"Schema inference failed in rewrite: {e_infer}. Attempting manual creation.")
                 # Fallback to manual creation if inference fails
                 try:
                     final_schema = pa.Schema.from_pandas(df, preserve_index=False) # Re-attempting basic inference first
                 except Exception:
                     # If still failing, maybe log error and proceed without strict schema? Risky.
                     logger.error(f"Could not create final schema for {output_file}. Writing without strict schema.")
                     final_schema = None # Signal to write without schema


            # Special handling for date and time columns removed (handled by _ensure_schema_conformance now)
            
            # Overwriting requires writing the table inside the try block after schema creation
            logger.info(f"Overwriting {output_file} with duplicate information.")
            # Write with potentially inferred schema or no schema if creation failed
            table = pa.Table.from_pandas(df, schema=final_schema, preserve_index=False) 
            pq.write_table(table, output_file, compression='snappy', version='2.6') 

        except pa.ArrowTypeError as e_schema: # This specific catch might be less relevant now
             logger.error(f"Error during PyArrow conversion/writing for {output_file}: {e_schema}")
             # Log additional details if possible
             problematic_cols = []
             for col_name in df.columns:
                 try:
                     pa.array(df[col_name])
                 except Exception as col_e:
                     problematic_cols.append(f"{col_name} ({df[col_name].dtype}): {col_e}")
             if problematic_cols:
                 logger.error(f"Problematic columns during conversion check: {problematic_cols}")
             # Decide if you want to re-raise or just log
             # raise # Optionally re-raise the error

        except Exception as e_rewrite: # Generic catch for other potential errors
            logger.error(f"Failed during rewrite operation for {output_file}: {e_rewrite}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            # raise # Optionally re-raise

def process_csv_to_parquet(file_path: str, column_config: Dict, source: str, 
                          output_dir: str, batch_size: int) -> str:
    """
    Process a single CSV or Excel file in batches and save to a Parquet file.
    
    This function is a high-level wrapper around the FileProcessor class.
    
    Args:
        file_path: Path to the CSV or Excel file
        column_config: Dictionary with column configuration
        source: Source identifier (e.g., 'tamara', 'payfort')
        output_dir: Directory to save processed files
        batch_size: Number of rows to process at once
    
    Returns:
        Path to the output Parquet file
    """
    processor = FileProcessor(source, output_dir, batch_size)
    return processor.process_file(file_path, column_config)

def detect_duplicates(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Detect potential duplicate records within a DataFrame.
    Adds a 'is_potential_duplicate' column to flag suspicious records.
    """
    # Define key columns to use for duplicate detection based on source
    key_columns = {
        'portal': ['order_id', 'transaction_date', 'payment_method'],
        'metabase': ['order_id', 'payment_online_transaction_id', 'payment_online_transaction_id'],
        'checkout_v1': ['payment_unique_number', 'action_id', 'order_id', 'rrn'],
        'checkout_v2': ['action_id', 'rrn', 'order_id'],
        'tamara': ['merchant_order_reference_id', 'payment_online_transaction_id', 'tamara_txn_reference'],
        'payfort': ['order_id', 'authorization_code', 'transaction_amount'],
        'bank': ['masked_card', 'transaction_amount', 'authorization_code']
    }
    
    # Get the appropriate key columns for this source
    cols = key_columns.get(source, [])
    
    # Skip if no key columns defined or they don't exist in the DataFrame
    if not cols or not all(col in df.columns for col in cols):
        missing = [col for col in cols if col not in df.columns]
        if missing:
            logger.warning(f"Missing duplicate detection columns for {source}: {missing}")
            # Try fallback to a smaller set of columns if possible
            available_cols = [col for col in cols if col in df.columns]
            if len(available_cols) >= 2:  # Need at least 2 columns for reasonable duplicate detection
                logger.info(f"Using fallback columns for duplicate detection: {available_cols}")
                cols = available_cols
            else:
                # Just add the column with all False values
                df['is_potential_duplicate'] = False
                return df
    
    # Find duplicated rows based on key columns
    df['is_potential_duplicate'] = df.duplicated(subset=cols, keep=False)
    
    # Log duplicate information (only if duplicates are found)
    if df['is_potential_duplicate'].any():
        dupe_count = df['is_potential_duplicate'].sum()
        total_rows = len(df)
        dupe_percentage = (dupe_count / total_rows) * 100
        
        # Calculate unique groups of duplicates
        dupe_groups = df[df['is_potential_duplicate']].groupby(cols).ngroups
        
        logger.warning(f"Found {dupe_count} potential duplicate records ({dupe_percentage:.1f}%) across {dupe_groups} unique groups in source '{source}' based on columns: {cols}")
        
        # If extremely high duplication rate (over 80%), log additional warning
        if dupe_percentage > 80:
            logger.warning(f"ATTENTION: Extremely high duplication rate ({dupe_percentage:.1f}%) detected in {source}. Check key columns and data quality.")
    
    return df

def detect_encoding(file_path: str, source: str = None) -> str:
    """
    Detect the encoding of a file with better handling for Excel CSVs.
    Returns the encoding name as a string.
    """
    logger.info(f"Detecting encoding for {file_path}")
    
    # Known encodings for specific sources (cached from previous successful runs)
    KNOWN_ENCODINGS = {
        'tamara': 'utf-8-sig',  
        'payfort': 'utf-8-sig',
        'checkout_v1': 'utf-8-sig',
        'checkout_v2': 'utf-8-sig',
        'portal': 'utf-8-sig'  # Portal data is typically exported from Excel
    }
    
    # Check if we have a cached encoding for this source
    if source and source in KNOWN_ENCODINGS:
        logger.info(f"Using known encoding {KNOWN_ENCODINGS[source]} for source '{source}'")
        return KNOWN_ENCODINGS[source]
    
    # First, try to detect if this is an Excel-generated CSV (BOM marker check)
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(4)  # Read first 4 bytes to check for BOM
            
        # Check for Excel's UTF-8 with BOM (common in Excel-exported CSVs)
        if raw_data.startswith(b'\xef\xbb\xbf'):
            logger.info(f"Detected Excel UTF-8 with BOM (UTF-8-SIG) for {file_path}")
            return 'utf-8-sig'
        
        # Check for UTF-16 LE BOM (less common in Excel but possible)
        if raw_data.startswith(b'\xff\xfe') or raw_data.startswith(b'\xfe\xff'):
            logger.info(f"Detected UTF-16 BOM for {file_path}")
            return 'utf-16'
    except Exception as e:
        logger.warning(f"Error checking for BOM markers: {str(e)}")
    
    # Try with chardet for more general encoding detection
    try:
        import chardet
        
        # Read a reasonable chunk of the file for detection
        max_size = min(1024*1024, os.path.getsize(file_path))  # Max 1MB or file size
        
        raw_data = None
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(max_size)
        except Exception as e:
            logger.warning(f"Error reading file for encoding detection: {str(e)}")
            return 'utf-8'  # Default to utf-8 if we can't read the file
            
        # Detect encoding
        if raw_data:
            result = chardet.detect(raw_data)
            confidence = result.get('confidence', 0)
            encoding = result.get('encoding', 'utf-8')
            
            if encoding is None:
                logger.warning(f"Chardet returned None encoding with confidence {confidence}, using utf-8")
                return 'utf-8'
            
            logger.info(f"Detected encoding {encoding} with confidence {confidence} for {file_path}")
            
            # If high confidence, use the detected encoding
            if confidence > 0.9:
                return encoding
            
            # Excel CSV files are often detected as ascii or iso-8859-1 with high confidence
            # But they're actually utf-8 or utf-8-sig
            if confidence > 0.7 and encoding.lower() in ('ascii', 'iso-8859-1', 'windows-1252'):
                # Try to determine if this is an Excel file by checking for Excel-specific patterns
                try:
                    with open(file_path, 'rb') as f:
                        first_line = f.readline().decode(encoding, errors='replace')
                    
                    # Excel CSVs often use semicolons as separators in European settings
                    if ';' in first_line and ',' not in first_line:
                        logger.info(f"File appears to be Excel-generated CSV with semicolons, using utf-8-sig for {file_path}")
                        return 'utf-8-sig'
                    
                    # Excel typically uses CR+LF line endings
                    if '\r\n' in first_line:
                        logger.info(f"File appears to be Excel-generated with CRLF, using utf-8 for {file_path}")
                        return 'utf-8'
                except Exception as e:
                    logger.warning(f"Error checking Excel patterns: {str(e)}")
        
        # If still here with low confidence, default to utf-8
        if confidence < 0.7:
            logger.info(f"Low confidence detection ({confidence}), defaulting to utf-8 for {file_path}")
            return 'utf-8'
        
        return encoding
    except ImportError:
        logger.warning("Chardet library not available, defaulting to utf-8")
        return 'utf-8'
    except Exception as e:
        logger.warning(f"Chardet encoding detection failed for {file_path}: {str(e)}")
        
        # Fallback to common encodings in order of likelihood
        for enc in ['utf-8-sig', 'utf-8', 'windows-1256', 'cp1256', 'utf-16']:
            try:
                with open(file_path, 'r', encoding=enc) as f:
                    f.read(100)  # Try reading a small part of the file
                logger.info(f"Fallback encoding detection succeeded with {enc} for {file_path}")
                return enc
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.warning(f"Error testing encoding {enc}: {str(e)}")
        
        # If all else fails, use a safe default
        logger.warning(f"All encoding detection methods failed for {file_path}, using utf-8")
        return 'utf-8'

class DataCleanerApp:
    """Main application class that orchestrates the data cleaning process."""
    
    def __init__(self, output_dir: str, batch_size: int = 250000, max_workers: int = None, 
                 process_based: bool = True, cpu_percent: int = 90):
        """
        Initialize the application.
        
        Args:
            output_dir: Directory for output files
            batch_size: Rows to process at once (default: 250000)
            max_workers: Maximum number of worker processes (defaults to cores-1)
            process_based: Whether to use process-based parallelism (True) or thread-based (False)
            cpu_percent: Maximum CPU percentage to use (0-100)
        """
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.process_based = process_based
        self.cpu_percent = min(100, max(10, cpu_percent))  # Keep between 10-100%
        
        # Auto-detect cores if not specified
        if max_workers is None:
            try:
                import psutil
                available_cores = psutil.cpu_count(logical=True)
                self.max_workers = max(1, available_cores - 1)
            except ImportError:
                # Fallback if psutil not available
                import multiprocessing
                self.max_workers = max(1, multiprocessing.cpu_count() - 1)
        else:
            self.max_workers = max_workers
        
        self.parquet_files = {}
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
    def clean_output_directory(self) -> None:
        """Clean the output directory by removing existing Parquet files."""
        logger.info(f"Checking and cleaning output directory: {os.path.abspath(self.output_dir)}")
        if os.path.exists(self.output_dir):
            cleaned_files = 0
            for item in os.listdir(self.output_dir):
                item_path = os.path.join(self.output_dir, item)
                try:
                    if item.endswith('.parquet') and os.path.isfile(item_path):
                        os.remove(item_path)
                        logger.debug(f"Removed existing file: {item_path}")
                        cleaned_files += 1
                except Exception as e:
                    logger.warning(f"Failed to remove existing item {item_path}: {e}")
            
            if cleaned_files > 0:
                 logger.info(f"Cleaned {cleaned_files} .parquet file(s) from output directory.")
            else:
                 logger.info("Output directory already clean or contains no managed items.")
        else:
            logger.info("Output directory does not exist, will be created.")
            
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)
    
    def run(self, config: Dict) -> Dict:
        """
        Run the data cleaning process for all sources in the config.
        """
        # Initialize tracking dictionary
        self.parquet_files = {source: [] for source in config.keys()}
        
        if self.process_based:
            import multiprocessing as mp
            
            # Create a flat list of ALL work to be done across ALL sources
            all_tasks = []
            for source, source_config in config.items():
                for file_path in source_config["files"]:
                    # Each task is: (source, file_path, column_config, output_dir, batch_size)
                    all_tasks.append((source, file_path, source_config["columns"], 
                                      self.output_dir, self.batch_size))
            
            # Now run ALL tasks in parallel across all sources
            logger.info(f"Processing a total of {len(all_tasks)} files from all sources in parallel")
            
            # Initialize multiprocessing with spawn method on Windows
            if hasattr(mp, 'set_start_method') and sys.platform == 'win32':
                try:
                    mp.set_start_method('spawn', force=True)
                except RuntimeError:
                    pass  # Already set
                
            # Use context manager for clean process management
            with mp.Pool(processes=self.max_workers) as pool:
                try:
                    # Use method that requires no serialization of instance methods
                    results = pool.map(mp_worker_wrapper, all_tasks)
                    
                    # Process results to update tracking dictionary
                    for task, output_file in zip(all_tasks, results):
                        source, file_path = task[0], task[1]
                        if output_file:  # Only add successful results
                            self.parquet_files[source].append(output_file)
                            logger.info(f"Successfully processed {file_path} to {output_file}")
                except Exception as e:
                    logger.error(f"Error in parallel processing: {str(e)}")
                    raise
        else:
            # Original sequential version
            for source, source_config in config.items():
                logger.info(f"Processing source: {source}")
                self._process_source_parallel(source, source_config)
        
        # Report results
        processed_count = sum(len(files) for files in self.parquet_files.values())
        logger.info(f"Processing complete: {processed_count} files processed.")
        logger.info(f"Output directory: {os.path.abspath(self.output_dir)}")
        
        return self.parquet_files
    
    def _process_source_parallel(self, source: str, source_config: Dict) -> None:
        """Process all files for a specific source in parallel."""
        file_paths = source_config["files"]
        total_files = len(file_paths)
        
        if total_files == 0:
            logger.info(f"No files to process for source: {source}")
            return
            
        logger.info(f"Processing {total_files} files in parallel for source: {source}")
        
        if self.process_based and total_files > 1:  # Only use multiprocessing if multiple files
            # Use multiprocessing with clear parameter passing
            import multiprocessing as mp
            
            # Create a list of tasks with parameters
            tasks = []
            for file_path in file_paths:
                task = (source, file_path, source_config["columns"], self.output_dir, self.batch_size)
                tasks.append(task)
            
            # Use context manager for clean process management
            with mp.Pool(processes=min(self.max_workers, total_files)) as pool:
                # Start the pool with a simple interface to avoid pickle issues
                try:
                    results = pool.starmap(mp_worker_wrapper, tasks)
                    
                    # Store results
                    for file_path, output_file in zip(file_paths, results):
                        self.parquet_files[source].append(output_file)
                        logger.info(f"Successfully processed {file_path} to {output_file}")
                except Exception as e:
                    logger.error(f"Error in parallel processing for source {source}: {str(e)}")
                    raise
        else:
            # Thread-based processing or single file processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {
                    executor.submit(self._process_single_file, source, file_path, source_config["columns"]): file_path
                    for file_path in file_paths
                }
                
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        output_file = future.result()
                        self.parquet_files[source].append(output_file)
                        logger.info(f"Successfully processed to {output_file}")
                    except Exception as e:
                        error_msg = f"Error processing {file_path}: {str(e)}"
                        logger.error(error_msg)
                        raise RuntimeError(error_msg)
    
    def _process_single_file(self, source: str, file_path: str, column_config: Dict) -> str:
        """Process a single file in a separate process."""
        # When using ProcessPoolExecutor, this runs in a completely separate process
        try:
            # Reconfigure logging for this process to avoid duplication
            if self.process_based:
                # Reset handlers to avoid duplicate logging
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
                # Add a single handler for this process
                process_handler = logging.StreamHandler(sys.stdout)
                process_handler.setFormatter(logging.Formatter('%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s'))
                logger.addHandler(process_handler)
            
            logger.info(f"Process {os.getpid()} starting on file: {file_path}")
            file_processor = FileProcessor(source, self.output_dir, self.batch_size)
            output_file = file_processor.process_file(file_path, column_config)
            logger.info(f"Process {os.getpid()} completed file: {file_path}")
            return output_file
        except Exception as e:
            logger.error(f"Process {os.getpid()} failed on file {file_path}: {str(e)}")
            # Re-raise to be caught by the executor
            raise

# Create a clean multiprocessing wrapper function that doesn't depend on class instance
def mp_worker_wrapper(args):
    """Completely isolated worker function for multiprocessing"""
    source, file_path, column_config, output_dir, batch_size = args
    

    # Set process priority to prevent system overload
    try:
        import psutil
        process = psutil.Process(os.getpid())
        if sys.platform == 'win32':
            process.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        else:
            os.nice(10)  # Lower priority on Unix systems
    except (ImportError, AttributeError):
        pass  # Continue if psutil not available or operation not supported
    
    # Create a unique process identifier
    pid = os.getpid()
    
    try:
        # Configure logging for this process
        import logging
        logging.root.handlers = []  # Remove all handlers
        
        # Create a unique logger for this process
        process_logger = logging.getLogger(f"process_{pid}")
        while process_logger.handlers:
            process_logger.removeHandler(process_logger.handlers[0])
            
        # Configure process-specific logging
        formatter = logging.Formatter(f'%(asctime)s - PID:{pid} - %(levelname)s - %(message)s')
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        process_logger.addHandler(console)
        process_logger.setLevel(logging.INFO)
        process_logger.propagate = False
        
        process_logger.info(f"Process {pid} starting on file: {file_path}")
        
        # Process the file with optimized batch size
        processor = FileProcessor(source, output_dir, batch_size)
        output_file = processor.process_file(file_path, column_config)
        
        process_logger.info(f"Process {pid} completed processing {file_path}")
        return output_file
        
    except Exception as e:
        error_msg = f"Process {pid} failed processing {file_path}: {str(e)}"
        try:
            if 'process_logger' in locals():
                process_logger.error(error_msg)
            else:
                print(error_msg)
        except:
            print(error_msg)
        return None

def main():
    """Main entry point for the data cleaner application."""
    parser = argparse.ArgumentParser(description='Clean data and output to Parquet format')
    
    # Set default output to a generally accessible location
    default_output_dir = os.path.join(os.getenv('ProgramData', 'C:/ProgramData'), 'ETL_Pipeline_Output')
    parser.add_argument('--output_dir', default=default_output_dir, 
                      help=f'Directory for output files (default: {default_output_dir})')
    parser.add_argument('--batch_size', type=int, default=250000, 
                      help='Rows to process at once')
    parser.add_argument('--log_level', default='INFO', 
                      help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--parallel', action='store_true', default=True,
                      help='Enable parallel processing of files')
    parser.add_argument('--thread_based', action='store_true',
                      help='Use thread-based parallelism instead of process-based')
    parser.add_argument('--max_workers', type=int, default=None,
                      help='Maximum number of worker processes (defaults to cores-1)')
    parser.add_argument('--cpu_percent', type=int, default=90,
                      help='Maximum CPU percentage to use (0-100)')
    
    args = parser.parse_args()
    
    # Auto-detect optimal number of workers if not specified
    if args.max_workers is None:
        try:
            import psutil
            available_cores = psutil.cpu_count(logical=True)
            args.max_workers = max(1, available_cores - 1)
            logger.info(f"Auto-detected {available_cores} cores, using {args.max_workers} workers")
        except ImportError:
            import multiprocessing
            available_cores = multiprocessing.cpu_count()
            args.max_workers = max(1, available_cores - 1)
            logger.info(f"Auto-detected {available_cores} cores, using {args.max_workers} workers")
    
    # Set logging level
    logger.setLevel(getattr(logging, args.log_level.upper()))
    
    logger.info("Starting data cleaning process")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Batch size: {args.batch_size}")
    
    if args.parallel:
        parallel_type = "thread-based" if args.thread_based else "process-based"
        logger.info(f"Parallel processing enabled ({parallel_type}) with {args.max_workers} workers")
    
    try:
        # Create and run the application
        app = DataCleanerApp(
            args.output_dir,
            args.batch_size,
            args.max_workers,
            process_based=not args.thread_based,
            cpu_percent=args.cpu_percent
        )
        app.clean_output_directory()
        app.run(CONFIG)
        
        logger.info("Use load_to_postgres.py to load the generated Parquet files into PostgreSQL.")
        return 0
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())