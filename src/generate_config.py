#!/usr/bin/env python3
"""
build_config.py  –  emit generated_config.py ready for SQL import.

Quick start:
    pip install pandas python-dateutil
    python build_config.py --paths paths.json --sample 20
"""

import argparse, json, re, sys
from pathlib import Path

import pandas as pd
from dateutil.parser import parse as date_parse
from pandas.api.types import is_integer_dtype, is_float_dtype, is_datetime64_any_dtype

# ---------------------------------------------------------------------------
# 1.  How each inferred type maps to an SQL type (tune as you wish)
# ---------------------------------------------------------------------------
SQL_TYPE_MAP = {
    "integer": "INTEGER",         # PostgreSQL: INTEGER for standard integers
    "float":   "NUMERIC(18,2)",   # PostgreSQL: NUMERIC for precise decimals
    "date":    "DATE",            # PostgreSQL: DATE type
    "time":    "TIME",            # PostgreSQL: TIME type
    "string":  "VARCHAR(255)",    # PostgreSQL: VARCHAR with length
    "text":    "TEXT",            # PostgreSQL: TEXT for unlimited length strings
}

# Known columns that should use TEXT type instead of VARCHAR
TEXT_COLUMNS = {
    'portal': ['note', 'order_items', 'customer_name', 'chef_name', 'commission_percentage', 
              'promotion_template_id', 'promotion_tier_id', 'gift_endurance', 'customer_address',
              'external_client_id', 'external_order_id', 'gift_from', 'gift_message',
              'order_content', 'order_description', 'phone_number', 'order_address',
              'customer_email', 'driver_id', 'additional_info'],
    'metabase': ['order_status', 'reservation_status', 'order_details', 'customer_info', 
                'payment_details', 'comments', 'address', 'notes', 'description', 'message'],
    'tamara': ['comment', 'customer_name', 'address', 'order_reference_id', 'consumer_email', 
              'consumer_info', 'description', 'details'],
    'checkout_v1': ['customer_address', 'customer_email', 'customer_name', 'payment_description', 
                  'order_details', 'notes', 'comments', 'message', 'id_token', 'description'],
    'checkout_v2': ['customer_address', 'customer_email', 'customer_name', 'payment_description', 
                  'order_details', 'notes', 'comments', 'message', 'id_token', 'description'],
    'bank': ['customer_info', 'transaction_details', 'notes', 'description', 
           'payment_description', 'reference', 'comments'],
    'payfort': ['customer_email', 'customer_name', 'card_description', 'payment_description', 
              'notes', 'comments', 'message', 'status_description']
}

# Date columns that should always be treated as DATE type
DATE_COLUMNS = {
    'transaction_date', 'action_date_utc_1', 'txn_created_at_gmt_03_00', 'posting_date',
    'order_created_at_gmt_03_00', 'delivery_date', 'settlement_date', 'created_at',
    'updated_at', 'payment_date', 'processing_date', 'authorization_date', 'capture_date'
}

# Optional explicit source→table mapping, else table == source key
TABLE_MAP = {
    # "portal": "stg_orders",
    # "metabase": "stg_metabase",
}

# ---------------------------------------------------------------------------
# 2.  Regex patterns that map CSV headings → canonical column names
#     (built from the PDF's terminology — extend as necessary)
# ---------------------------------------------------------------------------
STD_FIELD_PATTERNS = {
    r"order.*id":                                  "order_id",
    r"payment.*online.*transaction.*id":           "payment_online_transaction_id",
    r"reference.*id":                              "reference_id",
    r"\brnn\b|retrieval.*reference":               "rnn",
    r"auth(orization)?_?(response_)?code":         "authorization_code",
    r"(gross_?)?amount|txn?_?amount":              "transaction_amount",
    r"(txn|transaction)_?type|payment_?type":      "transaction_type",
    r"(txn|transaction)_?date|posting_?date":      "transaction_date",
    r"response.*code":                             "response_code",
    r"settle(d|ment).*status|captured":            "settlement_status",
}

# ---------------------------------------------------------------------------
# Source-specific column mappings (from unified config)
# ---------------------------------------------------------------------------
SOURCE_MAPPINGS = {
    "portal": {
        "Order Id": "order_id",
        "Order Number": "transaction_id",
        "Order Total": "transaction_amount",
        "Delivery Date": "transaction_date",
        "Order Status": "status",
        "Payment Method": "payment_method",
        "RRN": "rrn",
        "Auth Code": "authorization_code",
        "Wallet Paid Amount": "wallet_paid_amount",
        "Main Wallet Paid Amount": "main_wallet_paid_amount",
        "Store Wallet Paid Amount": "store_wallet_paid_amount",
        "Collected Cash": "collected_cash",
        "Total Transfer Fees": "total_transfer_fees",
        "Chef Total": "chef_total",
        "Commission Amount": "commission_amount",
        "Integration": "gateway",
        "Delivery Type": "delivery_type",
        "Service Fees": "service_fees"
    },
    "bank": {
        "RRN": "rrn",
        "Authorization Code": "authorization_code",
        "Transaction Amount": "transaction_amount",
        "Transaction Date": "transaction_date",
        "Payment Status": "status",
        "Payment ID": "payment_online_transaction_id",
        "Card Type": "card_type",
        "Card Number": "masked_card",
        "Transaction Type": "transaction_type",
        "Posting Date": "posting_date"
    },
    "metabase": {
        "RRN": "rrn",
        "Auth Code": "authorization_code",
        "Transaction Amount": "transaction_amount",
        "Transaction Date": "transaction_date",
        "Payment Status": "status",
        "payment_online_transaction_id": "gateway_order_id",
        "Response Code": "response_code",
        "Order ID": "portal_order_id"
    },
    "checkout_v1": {
        "Acquirer Reference ID": "rrn",
        "Auth Code": "authorization_code",
        "Amount": "transaction_amount",
        "Action Date UTC": "transaction_date",
        "Action Type": "status",
        "Payment ID": "payment_online_transaction_id",
        "Response Code": "response_code",
        "Reference": "order_id"
    },
    "checkout_v2": {
        "Acquirer Reference Number": "rrn",
        "Auth Code": "authorization_code",
        "Amount": "transaction_amount",
        "Action Date UTC": "action_date_utc_1",
        "Action Type": "status",
        "Payment ID": "payment_online_transaction_id",
        "Response Code": "response_code",
        "Reference": "order_id"
    },
    "tamara": {
        "Merchant Order Number": "order_id",
        "Tamara Order Id": "payment_online_transaction_id",
        "Order Amount": "transaction_amount",
        "Transaction Date DD/MM/YYYY": "txn_created_at_gmt_03_00",
        "Order Status": "status",
        "RRN": "rrn",
        "Auth Code": "authorization_code",
        "Payment Type": "payment_method",
        "Event": "transaction_type",
        "Currency": "currency"
    },
    "payfort": {
        "Merchant Reference": "order_id",
        "FORT ID": "payment_online_transaction_id",
        "Amount": "transaction_amount",
        "Date & Time": "transaction_date",
        "time": "time",
        "Operation": "status",
        "Response Code": "response_code",
        "Reconciliation Reference (RRN)": "rrn",
        "Authorization Code": "authorization_code",
        "Payment Option": "payment_method",
        "Payment Method": "payment_method_type",
        "Channel": "channel"
    }
}

# Column type overrides by field name
FIELD_TYPE_OVERRIDES = {
    "order_id": "integer",
    "portal_order_id": "integer",
    "gateway_order_id": "integer",
    "rrn": "string",
    "amount": "float",
    "transaction_amount": "float",
    "total": "float",
    "fees": "float",
    "cash": "float",
    "wallet_paid_amount": "float",
    "chef_total": "float",
    "commission_amount": "float",
    "transaction_date": "date",
    "action_date_utc_1": "date",
    "txn_created_at_gmt_03_00": "date",
    "posting_date": "date",
    "time": "time"
}

# Unified set of target column names
UNIFIED_COLUMNS = {
    "order_id",
    "transaction_id",
    "transaction_amount",
    "transaction_date",
    "status",
    "payment_method",
    "rrn",
    "authorization_code",
    "wallet_paid_amount",
    "main_wallet_paid_amount",
    "store_wallet_paid_amount",
    "collected_cash",
    "total_transfer_fees",
    "chef_total",
    "commission_amount",
    "gateway",
    "delivery_type",
    "service_fees",
    "payment_online_transaction_id",
    "response_code",
    "card_type",
    "masked_card",
    "transaction_type",
    "posting_date",
    "currency",
    "channel",
    "payment_method_type",
    "portal_order_id",
    "gateway_order_id"
}

def args():
    p = argparse.ArgumentParser()
    p.add_argument("--paths", default="paths.json", help="path to paths.json")
    p.add_argument("--sample", type=int, default=20, help="rows to sample per file")
    return p.parse_args()

def snake(name: str) -> str:
    """Convert any string to snake_case."""
    return re.sub(r"__+", "_",
           re.sub(r"[^\w]+", "_", name.strip().lower())
           ).strip("_")

def map_standard_name(col_name: str, source: str) -> str:
    """Return a canonical field name based on source-specific mapping."""
    # Try source-specific mapping first
    if source in SOURCE_MAPPINGS:
        if col_name in SOURCE_MAPPINGS[source]:
            return SOURCE_MAPPINGS[source][col_name]
    
    # For unmapped columns, convert to snake_case
    mapped_name = snake(col_name)
    
    # Avoid conflict with potential auto-generated PK 'id'
    if mapped_name == "id":
        return "source_id"
        
    return mapped_name

def guess_py_type(series: pd.Series, mapped_name: str) -> str:
    """Determine the Python type for a series, with special handling for known fields."""
    # First check field type overrides
    if mapped_name in FIELD_TYPE_OVERRIDES:
        return FIELD_TYPE_OVERRIDES[mapped_name]
    
    # Check if this is a date field by name
    if any(date_term in mapped_name for date_term in DATE_COLUMNS) or 'date' in mapped_name:
        return "date"
    
    # Drop NA values before checking
    s = series.dropna()
    if s.empty: 
        return "string"

    # Check for credit card and similar fields
    if any(card_term in str(series.name).lower() for card_term in ["card", "cc", "bin", "pan", "masked"]):
        return "string"

    # Check for masked card pattern in the data
    if len(s) > 0:
        first_val = str(s.iloc[0])
        if "*" in first_val and any(c.isdigit() for c in first_val):
            return "string"  # Contains asterisks and digits - likely a masked card

    # Check for amount fields
    if any(amt_term in str(series.name).lower() for amt_term in ["amount", "total", "fees", "cash"]):
        return "float"
    
    # Standard type inference
    if is_integer_dtype(s):            return "integer"
    if is_float_dtype(s):              return "float"
    if is_datetime64_any_dtype(s):     return "date"
    
    # Fuzzy date test for string fields that might be dates
    samples = s.sample(min(len(s), 6), random_state=0)
    parsed = sum(1 for v in samples if _looks_like_date(v))
    return "date" if parsed >= len(samples)//2 else "string"

def _looks_like_date(val):
    try:
        date_parse(str(val), fuzzy=False); return True
    except Exception:
        return False

def analyse_csv(path: Path, sample: int, source: str):
    """Analyze CSV with source-specific column mapping, handling duplicate mapped names."""
    try:
        # Force UTF-8 and comma separator
        df = pd.read_csv(
            path, 
            nrows=sample if sample > 0 else None,
            encoding='utf-8',
            sep=',',
            dtype=str,  # Read all columns as string initially
            na_filter=False  # Don't interpret "NA" as missing
        )
    except Exception as e:
        raise ValueError(f"Failed to read {path}: {str(e)}")

    out = {}
    seen_mapped_names = set() # Track mapped names already processed for this file

    # First, process columns from SOURCE_MAPPINGS for this source to ensure they're included
    if source in SOURCE_MAPPINGS:
        for original_col, mapped_name in SOURCE_MAPPINGS[source].items():
            # Check if the original column exists in the dataframe
            matching_cols = [col for col in df.columns if col.replace('\ufeff', '').strip() == original_col]
            
            if matching_cols:
                col = matching_cols[0]  # Use the first matching column
                
                # Check if this mapped name has already been processed
                if mapped_name in seen_mapped_names:
                    print(f"WARNING: Duplicate mapping for '{mapped_name}' from original column '{original_col}' in file {path}.", file=sys.stderr)
                    continue
                
                # Mark this mapped name as seen
                seen_mapped_names.add(mapped_name)
                
                # Store specification using mapped_name as key with the exact original column name
                out[mapped_name] = {
                    "original": original_col, # Store the exact original column name from SOURCE_MAPPINGS
                    "map_to": mapped_name,
                    "py_type": guess_py_type(df[col], mapped_name),
                    "sql_type": "VARCHAR(255)"  # Default type, will be refined later
                }

    # Then process any remaining columns in the dataframe
    for col in df.columns:
        # Clean column name - remove BOM and whitespace
        clean_col = col.replace('\ufeff', '').strip()
        
        # Skip if this column was already processed from SOURCE_MAPPINGS
        if source in SOURCE_MAPPINGS and clean_col in SOURCE_MAPPINGS[source]:
            continue
            
        mapped_name = map_standard_name(clean_col, source)

        # Check if this mapped name has already been processed
        if mapped_name in seen_mapped_names:
            print(f"WARNING: Duplicate mapping for '{mapped_name}' from original column '{clean_col}' in file {path}. Ignoring.", file=sys.stderr)
            continue # Skip this column

        # Mark this mapped name as seen
        seen_mapped_names.add(mapped_name)

        # Store specification using mapped_name as key
        out[mapped_name] = {
            "original": clean_col, # Store the original column name
            "map_to": mapped_name,
            "py_type": guess_py_type(df[col], mapped_name),
            "sql_type": "VARCHAR(255)"  # Default type, will be refined later
        }

    return out

def build(paths_file: Path, sample: int):
    """Build configuration with source-specific handling."""
    spec = json.loads(Path(paths_file).read_text(encoding="utf-8"))
    if "sources" not in spec:
        raise KeyError("'sources' missing")
    
    out = {}
    seen_tables = set()
    
    for src, info in spec["sources"].items():
        if src not in SOURCE_MAPPINGS:
            raise ValueError(f"No mapping defined for source: {src}")
        
        target_table = TABLE_MAP.get(src, src)
        if target_table in seen_tables:
            raise ValueError(f"Duplicate target table name detected: '{target_table}'")
        
        seen_tables.add(target_table)
        out[src] = {
            "target_table": target_table,
            "files": [str(Path(p).expanduser().resolve()) for p in info.get("files", [])],
            "columns": {} # Now keyed by mapped_name
        }
        
        for f in out[src]["files"]:
            file_columns = analyse_csv(Path(f), sample, src)
            # Merge column definitions (keyed by mapped_name)
            for mapped_name, spec in file_columns.items():
                if mapped_name not in out[src]["columns"]:
                    # If this mapped_name hasn't been seen from other files for this source, add it
                    out[src]["columns"][mapped_name] = spec
                # else: Optional: Add logic here if you want to compare/merge types across files for the same mapped_name
        
        # Update SQL types based on Python types and known TEXT columns
        # Iterate through the merged columns (keyed by mapped_name)
        for mapped_name, col_spec in out[src]["columns"].items(): 
            py_type = col_spec["py_type"]
            
            # Enforce type consistency based on mapped_name
            # Special handling for transaction_date field
            if mapped_name in DATE_COLUMNS or 'date' in mapped_name:
                py_type = "date"
                col_spec["py_type"] = "date"
            # Handle ID fields
            elif mapped_name in ["order_id", "portal_order_id", "gateway_order_id"]:
                py_type = "integer"
                col_spec["py_type"] = "integer"
            # Handle RRN field
            elif mapped_name == "rrn":
                py_type = "string"
                col_spec["py_type"] = "string"
            # Handle amount fields consistently
            elif any(term in mapped_name for term in ["amount", "total", "fee", "cash"]):
                py_type = "float"
                col_spec["py_type"] = "float"
            
            # Check if this column should be TEXT
            if src in TEXT_COLUMNS and mapped_name in TEXT_COLUMNS.get(src, []):
                col_spec["sql_type"] = SQL_TYPE_MAP["text"]
            # Special case for payfort time column
            elif src == 'payfort' and mapped_name == 'time':
                col_spec["sql_type"] = SQL_TYPE_MAP["time"]
            # Otherwise use the SQL_TYPE_MAP
            elif py_type in SQL_TYPE_MAP:
                col_spec["sql_type"] = SQL_TYPE_MAP[py_type]
            else:
                # Default to string type
                col_spec["sql_type"] = SQL_TYPE_MAP["string"]
    
    return out

def write_py(cfg, outfile="generated_config.py"):
    Path(outfile).write_text(
        "# AUTO‑GENERATED — Edit build_config.py, not this file.\nCONFIG = "
        + json.dumps(cfg, indent=4, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )

def main():
    a = args()
    cfg = build(a.paths, a.sample)
    write_py(cfg)

if __name__ == "__main__":
    main()
