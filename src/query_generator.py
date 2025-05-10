#!/usr/bin/env python3
"""
query_generator.py - Generate customized SQL queries for payment analysis.

This module allows dynamically creating SQL queries with selected fields and options
for the payment analysis process. It provides functionality to:
1. Select specific fields from different tables
2. Enable/disable reconciliation with bank data
3. Maintain required join keys and relationships
"""

import re
import os
from typing import Dict, List, Set, Optional, Tuple
import json

# Add this import to load generated_config
try:
    from generated_config import CONFIG
except ImportError:
    # Fallback to loading from file if module import fails
    CONFIG = {}
    config_path = os.path.join(os.path.dirname(__file__), 'generated_config.py')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            content = f.read()
            # Extract the CONFIG dictionary from the file content
            match = re.search(r'CONFIG\s*=\s*(\{.*\})', content, re.DOTALL)
            if match:
                # Use safer eval with limited globals
                CONFIG = eval(match.group(1), {"__builtins__": {}})

# Define essential join keys that must be included for the query to work properly
ESSENTIAL_JOIN_KEYS = {
    "order_id", "payment_online_transaction_id", "gateway_order_id", "gateway_transaction_id",
    "portal_order_id", "rrn", "authorization_code"
}

# Add new function to get SQL type from CONFIG
def get_sql_type(table: str, field: str) -> str:
    """
    Get the SQL type for a field in a specific table from the CONFIG.
    Returns a default VARCHAR type if the field or table is not found.
    """
    if table in CONFIG and "columns" in CONFIG[table] and field in CONFIG[table]["columns"]:
        return CONFIG[table]["columns"][field].get("sql_type", "VARCHAR(255)")
    return "VARCHAR(255)"

# Update type resolution function to avoid hardcoding date type
def get_consistent_field_type(field_name: str) -> str:
    """
    Get a consistent SQL type for a field that will be used in UNION queries.
    This ensures that all tables use the same type for the same field.
    """
    # Check if any table has this field and get its type
    for table in CONFIG:
        if "columns" in CONFIG[table] and field_name in CONFIG[table]["columns"]:
            return CONFIG[table]["columns"][field_name].get("sql_type", "VARCHAR(255)")
    
    # For fields not directly in CONFIG, use these reasonable defaults
    if field_name.endswith("_amount") or "amount" in field_name or "total" in field_name:
        return "NUMERIC(18,2)"
    if field_name in ["order_id", "portal_order_id", "gateway_order_id"]:
        return "INTEGER"
    
    # Default to VARCHAR if no type found
    return "VARCHAR(255)"

# Define source table name mappings
SOURCE_TABLES = {
    "portal": "portal",
    "metabase": "metabase",
    "checkout_v1": ("checkout_v1", "c1"),
    "checkout_v2": ("checkout_v2", "c2"),
    "payfort": ("payfort", "pf"),
    "tamara": ("tamara", "t"),
    "bank": "bank",
    "analysis": None  # Special case - not an actual table but derived fields
}

# Update FIELD_MAPPINGS for transaction_date to use dynamic type
FIELD_MAPPINGS = {
    # Portal fields
    "portal_order_id": "p.order_id::INTEGER",
    "portal_transaction_amount": f"p.transaction_amount::{get_consistent_field_type('transaction_amount')} AS portal_amount",
    "portal_gateway": f"p.gateway::{get_consistent_field_type('gateway')} AS portal_gateway",
    "portal_status": f"p.status::{get_consistent_field_type('status')} AS portal_status",
    "portal_payment_method": f"p.payment_method::{get_consistent_field_type('payment_method')} AS portal_payment_method",
    "portal_transaction_date": f"p.transaction_date::{get_consistent_field_type('transaction_date')} AS portal_transaction_date",
    "portal_customer_name": f"p.customer_name::{get_consistent_field_type('customer_name')} AS portal_customer_name",
    "portal_chef_name": f"p.chef_name::{get_consistent_field_type('chef_name')} AS portal_chef_name",
    "portal_chef_total": f"p.chef_total::{get_consistent_field_type('chef_total')} AS portal_chef_total",
    "portal_commission_amount": f"p.commission_amount::{get_consistent_field_type('commission_amount')} AS portal_commission_amount",
    "portal_delivery_type": f"p.delivery_type::{get_consistent_field_type('delivery_type')} AS portal_delivery_type",
    "portal_discount_type": f"p.discount_type::{get_consistent_field_type('discount_type')} AS portal_discount_type",
    "portal_ispaid": f"p.ispaid::{get_consistent_field_type('ispaid')} AS portal_ispaid",
    "portal_transaction_id": f"p.transaction_id::{get_consistent_field_type('transaction_id')} AS portal_transaction_id",
    "portal_wallet_paid_amount": f"p.wallet_paid_amount::{get_consistent_field_type('wallet_paid_amount')} AS portal_wallet_paid_amount",
    "portal_promo_code_total_discount": f"p.promo_code_total_discount::{get_consistent_field_type('promo_code_total_discount')} AS portal_promo_code_total_discount",
    
    # Metabase fields
    "metabase_transaction_amount": "m.transaction_amount::NUMERIC(18,2) AS metabase_amount",
    "metabase_status": "m.status::VARCHAR AS metabase_status",
    "metabase_gateway": "m.gateway::VARCHAR AS metabase_gateway",
    "metabase_gateway_order_id": "m.gateway_order_id AS metabase_src_gateway_order_id",
    "metabase_gateway_transaction_id": "m.gateway_transaction_id AS metabase_src_gateway_transaction_id",
    "metabase_portal_order_id": "m.portal_order_id::INTEGER AS metabase_portal_order_id",
    "metabase_app_version": "m.app_version::VARCHAR AS metabase_app_version",
    "metabase_platform": "m.platform::VARCHAR AS metabase_platform",
    "metabase_order_status": "m.order_status::VARCHAR AS metabase_order_status",
    "metabase_order_total": "m.order_total::NUMERIC(18,2) AS metabase_order_total",
    "metabase_hyperpay_transaction_id": "m.hyperpay_transaction_id::VARCHAR AS metabase_hyperpay_transaction_id",
    "metabase_success": "m.success::VARCHAR AS metabase_success",
    "metabase_transactiontype": "m.transactiontype::VARCHAR AS metabase_transactiontype",
    
    # Analysis fields (derived in gateway_analysis CTE)
    "analysis_gateway_source": "'{gateway_code}'::VARCHAR AS gateway_source",
    "analysis_gateway_order_id": "{alias}.order_id::INTEGER AS gateway_order_id", 
    "analysis_gateway_transaction_id": "{alias}.payment_online_transaction_id::VARCHAR AS gateway_transaction_id", 
    "analysis_amount": "{alias}.transaction_amount::NUMERIC(18,2) AS amount", 
    "analysis_status": "{alias}.status::VARCHAR AS status", 
    "analysis_transaction_date": "{alias}.transaction_date::" + get_consistent_field_type('transaction_date') + " AS transaction_date",
    "analysis_authorization_code": "{alias}.authorization_code::VARCHAR AS authorization_code", 
    "analysis_rrn": "{alias}.rrn::VARCHAR AS rrn", 
    "analysis_payment_method": "{alias}.payment_method::VARCHAR AS payment_method",
    "analysis_response_code": "{alias}.response_code::VARCHAR AS response_code", 
    "analysis_response_description": "{alias}.response_description::VARCHAR AS response_description", 
    "analysis_is_auth": "(UPPER({alias}.status) LIKE '%AUTHORISATION%' OR UPPER({alias}.status) LIKE '%AUTHORIZATION%')::BOOLEAN AS is_auth",
    "analysis_is_capture": "(UPPER({alias}.status) LIKE '%CAPTURE%')::BOOLEAN AS is_capture",
    "analysis_is_refund": "(UPPER({alias}.status) LIKE '%REFUND%')::BOOLEAN AS is_refund",
    "analysis_is_void": "(UPPER({alias}.status) LIKE '%VOID%' OR UPPER({alias}.status) = 'CANCEL')::BOOLEAN AS is_void",
    
    "analysis_final_rrn": "final_rrn",
    "analysis_final_authorization_code": "final_authorization_code",
    "analysis_transaction_analysis": "transaction_analysis",
    "analysis_transaction_outcome": "transaction_outcome",
    
    # Bank reconciliation fields ordered to match payment_analysis.sql
    "bank_authorization_code": "b.authorization_code::VARCHAR AS bank_auth_code",
    "bank_rrn": "b.rrn::VARCHAR AS bank_rrn",
    "bank_transaction_amount": "b.transaction_amount::NUMERIC(18,2) AS bank_amount",
    "bank_transaction_date": "b.transaction_date::DATE AS bank_transaction_date",
    "bank_transaction_type": "b.transaction_type::VARCHAR AS bank_transaction_type",
    "bank_status": "b.status::VARCHAR AS bank_status",
    "bank_bank_name": "b.bank_name::VARCHAR AS bank_bank_name",
    "bank_card_type": "b.card_type::VARCHAR AS bank_card_type",
    "bank_cashback_amount": "b.cashback_amount::NUMERIC(18,2) AS bank_cashback_amount",
    "bank_discount_amount": "b.discount_amount::NUMERIC(18,2) AS bank_discount_amount",
    "bank_masked_card": "b.masked_card::VARCHAR AS bank_masked_card",
    "bank_merchant_identifier": "b.merchant_identifier::VARCHAR AS bank_merchant_identifier",
    "bank_payment_online_transaction_id": "b.payment_online_transaction_id::VARCHAR AS bank_payment_id",
    "bank_posting_date": "b.posting_date::DATE AS bank_posting_date",
    "bank_terminal_identifier": "b.terminal_identifier::VARCHAR AS bank_terminal_identifier",
    "bank_total_payment_amount": "b.total_payment_amount::NUMERIC(18,2) AS bank_total_payment_amount",
    "bank_transaction_link_url": "b.transaction_link_url::VARCHAR AS bank_transaction_link_url",
    "bank_vat_amount": "b.vat_amount::NUMERIC(18,2) AS bank_vat_amount",
    "bank_match_type": "bank_match_type",
}

# Define analysis fields that can be selected - preserved in payment_analysis.sql order
ANALYSIS_FIELDS_FOR_SELECTION = [
    'gateway_source', 'gateway_order_id', 'gateway_transaction_id', 
    'amount', 'status', 'transaction_date', 'authorization_code', 'rrn', 
    'payment_method', 'response_code', 'response_description', 
    'is_auth', 'is_capture', 'is_refund', 'is_void',
    'portal_order_id_val', 'portal_amount', 'portal_gateway', 'portal_status', 
    'portal_payment_method', 'portal_transaction_date', 'portal_customer_name', 
    'portal_chef_name', 'portal_chef_total', 'portal_commission_amount',
    'portal_delivery_type', 'portal_discount_type', 'portal_ispaid',
    'portal_transaction_id', 'portal_wallet_paid_amount', 'portal_promo_code_total_discount',
    'metabase_amount', 'metabase_status', 'metabase_gateway', 'metabase_portal_order_id',
    'metabase_app_version', 'metabase_platform', 'metabase_order_status',
    'metabase_order_total', 'metabase_hyperpay_transaction_id', 'metabase_success',
    'metabase_transactiontype',
    'final_rrn', 'final_authorization_code', 
    'transaction_analysis', 'transaction_outcome', 'bank_match_type'
]

# Define GATEWAY_FIELDS in the same order as used in payment_analysis.sql for each gateway
GATEWAY_FIELDS = {
    "checkout_v1": {
        "amount": "c1.transaction_amount::NUMERIC(18,2) AS checkout_v1_amount",
        "status": "c1.status::VARCHAR AS checkout_v1_status",
        "transaction_date": f"c1.transaction_date::{get_consistent_field_type('transaction_date')} AS checkout_v1_transaction_date",
        "payment_id": "c1.payment_online_transaction_id::VARCHAR AS checkout_v1_payment_id",
        "order_id": "c1.order_id::INTEGER AS checkout_v1_order_id",
        "authorization_code": "c1.authorization_code::VARCHAR AS checkout_v1_authorization_code",
        "rrn": "c1.rrn::VARCHAR AS checkout_v1_rrn",
        "payment_method": "c1.payment_method::VARCHAR AS checkout_v1_payment_method",
        "issuing_bank": "c1.issuing_bank::VARCHAR AS checkout_v1_issuing_bank",
        "response_code": "c1.response_code::VARCHAR AS checkout_v1_response_code",
        "response_description": "c1.response_description::VARCHAR AS checkout_v1_response_description",
        "processor": "c1.processor::VARCHAR AS checkout_v1_processor",
        "card_wallet_type": "c1.card_wallet_type::VARCHAR AS checkout_v1_card_wallet_type",
        "currency": "c1.currency::VARCHAR AS checkout_v1_currency",
    },
    "checkout_v2": {
        "amount": "c2.transaction_amount::NUMERIC(18,2) AS checkout_v2_amount",
        "status": "c2.status::VARCHAR AS checkout_v2_status",
        "transaction_date": f"c2.action_date_utc_1::{get_consistent_field_type('transaction_date')} AS checkout_v2_transaction_date",
        "payment_id": "c2.payment_online_transaction_id::VARCHAR AS checkout_v2_payment_id",
        "order_id": "c2.order_id::INTEGER AS checkout_v2_order_id",
        "authorization_code": "c2.authorization_code::VARCHAR AS checkout_v2_authorization_code",
        "rrn": "c2.rrn::VARCHAR AS checkout_v2_rrn",
        "payment_method": "c2.payment_method_name::VARCHAR AS checkout_v2_payment_method",
        "response_code": "c2.response_code::VARCHAR AS checkout_v2_response_code",
        "response_description": "c2.response_description::VARCHAR AS checkout_v2_response_description",
        "wallet": "c2.wallet::VARCHAR AS checkout_v2_wallet",
        "currency": "c2.currency_symbol::VARCHAR AS checkout_v2_currency",
        "co_badged_card": "c2.co_badged_card::VARCHAR AS checkout_v2_co_badged_card",
    },
    "payfort": {
        "amount": "pf.transaction_amount::NUMERIC(18,2) AS payfort_amount",
        "status": "pf.status::VARCHAR AS payfort_status",
        "transaction_date": f"pf.transaction_date::{get_consistent_field_type('transaction_date')} AS payfort_transaction_date",
        "payment_id": "pf.payment_online_transaction_id::VARCHAR AS payfort_payment_id",
        "order_id": "pf.order_id::INTEGER AS payfort_order_id",
        "authorization_code": "pf.authorization_code::VARCHAR AS payfort_authorization_code",
        "rrn": "pf.rrn::VARCHAR AS payfort_rrn",
        "payment_method": "pf.payment_method::VARCHAR AS payfort_payment_method",
        "payment_method_type": "pf.payment_method_type::VARCHAR AS payfort_payment_method_type",
        "acquirer_name": "pf.acquirer_name::VARCHAR AS payfort_acquirer_name",
        "merchant_country": "pf.merchant_country::VARCHAR AS payfort_merchant_country",
        "channel": "pf.channel::VARCHAR AS payfort_channel",
        "mid": "pf.mid::VARCHAR AS payfort_mid",
        "currency": "pf.currency::VARCHAR AS payfort_currency",
        "time": "pf.time::TIME AS payfort_time",
    },
    "tamara": {
        "amount": "t.txn_amount::NUMERIC(18,2) AS tamara_amount",
        "status": "t.status::VARCHAR AS tamara_status",
        "transaction_date": f"t.txn_created_at_gmt_03_00::{get_consistent_field_type('transaction_date')} AS tamara_transaction_date",
        "payment_id": "t.payment_online_transaction_id::VARCHAR AS tamara_payment_id",
        "portal_order_id": "t.order_id::INTEGER AS tamara_portal_order_id",
        "txn_reference": "t.tamara_txn_reference::VARCHAR AS tamara_txn_reference",
        "payment_method": "t.payment_method::VARCHAR AS tamara_payment_method",
        "customer_name": "t.customer_name::VARCHAR AS tamara_customer_name",
        "txn_type": "t.txn_type::VARCHAR AS tamara_txn_type",
        "txn_settlement_status": "t.txn_settlement_status::VARCHAR AS tamara_txn_settlement_status",
        "order_currency": "t.order_currency::VARCHAR AS tamara_order_currency",
        "country_code": "t.country_code::VARCHAR AS tamara_country_code",
        "store_name": "t.store_name::VARCHAR AS tamara_store_name",
        "total_amount": "t.total_amount::NUMERIC(18,2) AS tamara_total_amount",
        "order_created_at": "t.order_created_at_gmt_03_00::DATE AS tamara_order_created_at",
    }
}

# Define field lists for each gateway (used in the template)
checkout_v1_fields = [
    "c1.transaction_amount::NUMERIC(18,2) AS checkout_v1_amount",
    "c1.status::VARCHAR AS checkout_v1_status",
    "c1.transaction_date::DATE AS checkout_v1_transaction_date",
    "c1.payment_online_transaction_id::VARCHAR AS checkout_v1_payment_id",
    "c1.order_id::INTEGER AS checkout_v1_order_id",
    "c1.authorization_code::VARCHAR AS checkout_v1_authorization_code",
    "c1.rrn::VARCHAR AS checkout_v1_rrn",
    "c1.payment_method::VARCHAR AS checkout_v1_payment_method",
    "c1.issuing_bank::VARCHAR AS checkout_v1_issuing_bank",
    "c1.response_code::VARCHAR AS checkout_v1_response_code",
    "c1.response_description::VARCHAR AS checkout_v1_response_description",
    "c1.processor::VARCHAR AS checkout_v1_processor",
    "c1.card_wallet_type::VARCHAR AS checkout_v1_card_wallet_type",
    "c1.currency::VARCHAR AS checkout_v1_currency"
]

checkout_v2_fields = [
    "c2.transaction_amount::NUMERIC(18,2) AS checkout_v2_amount",
    "c2.status::VARCHAR AS checkout_v2_status",
    "c2.action_date_utc_1::DATE AS checkout_v2_transaction_date",
    "c2.payment_online_transaction_id::VARCHAR AS checkout_v2_payment_id",
    "c2.order_id::INTEGER AS checkout_v2_order_id",
    "c2.authorization_code::VARCHAR AS checkout_v2_authorization_code",
    "c2.rrn::VARCHAR AS checkout_v2_rrn",
    "c2.payment_method_name::VARCHAR AS checkout_v2_payment_method",
    "c2.response_code::VARCHAR AS checkout_v2_response_code",
    "c2.response_description::VARCHAR AS checkout_v2_response_description",
    "c2.wallet::VARCHAR AS checkout_v2_wallet",
    "c2.currency_symbol::VARCHAR AS checkout_v2_currency",
    "c2.co_badged_card::VARCHAR AS checkout_v2_co_badged_card"
]

payfort_fields = [
    "pf.transaction_amount::NUMERIC(18,2) AS payfort_amount",
    "pf.status::VARCHAR AS payfort_status",
    "pf.transaction_date::DATE AS payfort_transaction_date",
    "pf.payment_online_transaction_id::VARCHAR AS payfort_payment_id",
    "pf.order_id::INTEGER AS payfort_order_id",
    "pf.authorization_code::VARCHAR AS payfort_authorization_code",
    "pf.rrn::VARCHAR AS payfort_rrn",
    "pf.payment_method::VARCHAR AS payfort_payment_method",
    "pf.payment_method_type::VARCHAR AS payfort_payment_method_type",
    "pf.acquirer_name::VARCHAR AS payfort_acquirer_name",
    "pf.merchant_country::VARCHAR AS payfort_merchant_country",
    "pf.channel::VARCHAR AS payfort_channel",
    "pf.mid::VARCHAR AS payfort_mid",
    "pf.currency::VARCHAR AS payfort_currency",
    "pf.time::TIME AS payfort_time"
]

tamara_fields = [
    "t.txn_amount::NUMERIC(18,2) AS tamara_amount",
    "t.status::VARCHAR AS tamara_status",
    "t.txn_created_at_gmt_03_00::DATE AS tamara_transaction_date",
    "t.payment_online_transaction_id::VARCHAR AS tamara_payment_id",
    "t.order_id::INTEGER AS tamara_portal_order_id", # Note: tamara.order_id is portal_order_id
    "t.tamara_txn_reference::VARCHAR AS tamara_txn_reference",
    "t.payment_method::VARCHAR AS tamara_payment_method",
    "t.customer_name::VARCHAR AS tamara_customer_name",
    "t.txn_type::VARCHAR AS tamara_txn_type",
    "t.txn_settlement_status::VARCHAR AS tamara_txn_settlement_status",
    "t.order_currency::VARCHAR AS tamara_order_currency",
    "t.country_code::VARCHAR AS tamara_country_code",
    "t.store_name::VARCHAR AS tamara_store_name",
    "t.total_amount::NUMERIC(18,2) AS tamara_total_amount",
    "t.order_created_at_gmt_03_00::DATE AS tamara_order_created_at"
]

def is_essential_key(field_name: str) -> bool:
    """Check if a field is an essential join key."""
    return any(key in field_name for key in ESSENTIAL_JOIN_KEYS)

def get_cte_fields_snippet(source_name: str, selected_fields: Dict[str, List[str]]) -> str:
    """Generate the fields part of a CTE section for a specific source."""
    # This function is simplified for the example
    return "-- Fields would be generated for " + source_name

# Define a list of essential fields that must always be included in the final SELECT
# These fields are crucial for the query to work correctly
ESSENTIAL_FIELDS = [
    'gateway_source', 'gateway_order_id', 'gateway_transaction_id', 
    'amount', 'status', 'transaction_date', 'authorization_code', 'rrn',
    'payment_method', 'response_code', 'response_description',
    'is_auth', 'is_capture', 'is_refund', 'is_void',
    'final_rrn', 'final_authorization_code', 
    'transaction_analysis', 'transaction_outcome'
]

# Define a mapping between potential field references and their actual aliases in the view
def get_field_mapping():
    """Generate a mapping between field names that might be requested and their actual aliases in the view"""
    field_aliases = {
        # Bank fields mapping
        "bank_authorization_code": "bank_auth_code",
        "bank_payment_online_transaction_id": "bank_payment_id",
        "bank_transaction_amount": "bank_amount",
        
        # Checkout fields mapping 
        "checkout_v1_payment_id": "checkout_v1_payment_online_transaction_id",
        "checkout_v2_payment_id": "checkout_v2_payment_online_transaction_id",
        "checkout_v2_payment_method": "checkout_v2_payment_method_name",
        "checkout_v2_currency": "checkout_v2_currency_symbol",
        
        # Payfort fields mapping
        "payfort_payment_id": "payfort_payment_online_transaction_id",
        
        # Tamara fields mapping
        "tamara_payment_id": "tamara_payment_online_transaction_id",
        "tamara_transaction_amount": "tamara_txn_amount",
        "tamara_transaction_date": "tamara_txn_created_at_gmt_03_00",
        "tamara_order_created_at": "tamara_order_created_at_gmt_03_00",
        
        # Portal fields mapping
        "portal_order_id": "portal_order_id_val",
        
        # Any other mappings that might be needed
    }
    return field_aliases

def extract_view_field_aliases(query_parts: List[str]) -> Set[str]:
    """
    Extract all field aliases defined in the view to ensure we only select fields that exist.
    This helps prevent "column does not exist" errors.
    
    Args:
        query_parts: List of SQL query parts that make up the view definition
    
    Returns:
        Set of field aliases that are defined in the view
    """
    aliases = set()
    
    # Look for AS statements in the SQL to extract field aliases
    for part in query_parts:
        matches = re.findall(r'AS\s+([a-zA-Z0-9_]+)', part)
        for match in matches:
            aliases.add(match.strip())
    
    return aliases

def log_field_validation_info(requested_fields: List[str], view_aliases: Set[str], field_mapping: Dict[str, str]) -> None:
    """
    Log information about field validation for debugging purposes.
    
    Args:
        requested_fields: List of fields requested in the final SELECT
        view_aliases: Set of field aliases defined in the view
        field_mapping: Mapping between requested field names and view aliases
    """
    missing_fields = []
    mapped_fields = []
    
    for field in requested_fields:
        if field not in view_aliases:
            if field in field_mapping and field_mapping[field] in view_aliases:
                mapped_fields.append(f"{field} -> {field_mapping[field]}")
            else:
                missing_fields.append(field)
    
    # Log this information (can be written to a file or printed to console)
    debug_info = {
        "missing_fields": missing_fields,
        "mapped_fields": mapped_fields,
        "total_requested": len(requested_fields),
        "total_available": len(view_aliases),
        "total_missing": len(missing_fields)
    }
    
    # Write to a debug log file
    try:
        with open("query_generator_debug.log", "w") as f:
            f.write(json.dumps(debug_info, indent=2))
    except Exception:
        pass  # Silently fail if we can't write debug info

def generate_payment_analysis_query(selected_fields: Dict[str, List[str]], include_reconciliation: bool = True, date_filter: Optional[Dict[str, str]] = None, limit: Optional[int] = None, return_distinct_orders: bool = False) -> str:
    """
    Generate a customized SQL query for payment analysis based on selected fields.
    Args:
        selected_fields: Dictionary mapping data sources to lists of selected fields (currently not used to reduce columns in CTEs for simplicity)
        include_reconciliation: Whether to include bank reconciliation logic
        date_filter: Dictionary with 'start_date' and 'end_date' for filtering portal transaction dates
        limit: Optional integer limit for the final SELECT statement
        return_distinct_orders: Whether to return only unique gateway_order_id records
    Returns:
        Generated SQL query string
    """
    query_parts = [
        "-- Recommended Indices for Performance",
        "DO $$",
        "BEGIN",
        # Index creation statements
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_portal_order_id') THEN CREATE INDEX idx_portal_order_id ON portal (order_id); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_metabase_portal_order_id') THEN CREATE INDEX idx_metabase_portal_order_id ON metabase (portal_order_id); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_metabase_gateway_ids') THEN CREATE INDEX idx_metabase_gateway_ids ON metabase (gateway_order_id, gateway_transaction_id); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_checkout_v1_join_keys') THEN CREATE INDEX idx_checkout_v1_join_keys ON checkout_v1 (order_id, payment_online_transaction_id); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_checkout_v1_status_code') THEN CREATE INDEX idx_checkout_v1_status_code ON checkout_v1 (status, response_code); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_checkout_v2_join_keys') THEN CREATE INDEX idx_checkout_v2_join_keys ON checkout_v2 (order_id, payment_online_transaction_id); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_checkout_v2_status_code') THEN CREATE INDEX idx_checkout_v2_status_code ON checkout_v2 (status, response_code); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_payfort_join_keys') THEN CREATE INDEX idx_payfort_join_keys ON payfort (order_id, payment_online_transaction_id); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_payfort_status') THEN CREATE INDEX idx_payfort_status ON payfort (status); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_tamara_payment_id') THEN CREATE INDEX idx_tamara_payment_id ON tamara (payment_online_transaction_id); END IF;",
        "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_tamara_status_type') THEN CREATE INDEX idx_tamara_status_type ON tamara (status, txn_type); END IF;",
    ]
    if include_reconciliation:
        query_parts.extend([
            "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_bank_auth_rrn') THEN CREATE INDEX idx_bank_auth_rrn ON bank (authorization_code, rrn); END IF;",
            "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_bank_rrn') THEN CREATE INDEX idx_bank_rrn ON bank (rrn); END IF;",
            "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_bank_auth_code') THEN CREATE INDEX idx_bank_auth_code ON bank (authorization_code); END IF;",
        ])
    query_parts.extend([
        "END$$;",
        "",
        "-- Drop the view first to allow changing column data types",
        "DROP VIEW IF EXISTS payment_analysis_view;",
        "",
        "-- Create or replace a view that encapsulates our payment analysis query",
        "CREATE OR REPLACE VIEW payment_analysis_view AS",
        "WITH",
        "gateway_analysis AS (",
    ])
    
    date_filter_clause = ""
    if date_filter and date_filter.get('enabled', False) and date_filter.get('start_date') and date_filter.get('end_date'):
        date_type = "DATE" 
        date_filter_clause = f"WHERE p.transaction_date::{date_type} BETWEEN '{date_filter['start_date']}'::DATE AND '{date_filter['end_date']}'::DATE"

    # Define the full template for a gateway SELECT statement
    # This template includes placeholders for all common fields and specific gateway fields
    gateway_select_template = """
    SELECT
        '{gateway_source_val}'::VARCHAR AS gateway_source,
        {alias}.order_id::{gateway_order_id_type} AS gateway_order_id,
        {alias}.payment_online_transaction_id::VARCHAR AS gateway_transaction_id,
        {amount_col}::{amount_type} AS amount,
        {alias}.status::VARCHAR AS status,
        {transaction_date_col}::{transaction_date_type} AS transaction_date,
        {auth_code_col}::{auth_code_type} AS authorization_code,
        {rrn_col}::{rrn_type} AS rrn,
        {payment_method_col}::{payment_method_type} AS payment_method,
        {response_code_col}::{response_code_type} AS response_code,
        {response_description_col}::{response_description_type} AS response_description,
        {is_auth_expr}::BOOLEAN AS is_auth,
        {is_capture_expr}::BOOLEAN AS is_capture,
        {is_refund_expr}::BOOLEAN AS is_refund,
        {is_void_expr}::BOOLEAN AS is_void,
        
        -- Portal data
        p.order_id::INTEGER AS portal_order_id_val, -- Renamed to avoid conflict with p.order_id for join
        p.transaction_amount::NUMERIC(18,2) AS portal_amount,
        p.gateway::VARCHAR AS portal_gateway,
        p.status::VARCHAR AS portal_status,
        p.payment_method::VARCHAR AS portal_payment_method,
        p.transaction_date::DATE AS portal_transaction_date,
        p.customer_name::VARCHAR AS portal_customer_name,
        p.chef_name::VARCHAR AS portal_chef_name,
        p.chef_total::NUMERIC(18,2) AS portal_chef_total,
        p.commission_amount::NUMERIC(18,2) AS portal_commission_amount,
        p.delivery_type::VARCHAR AS portal_delivery_type,
        p.discount_type::VARCHAR AS portal_discount_type,
        p.ispaid::VARCHAR AS portal_ispaid,
        p.transaction_id::VARCHAR AS portal_transaction_id,
        p.wallet_paid_amount::NUMERIC(18,2) AS portal_wallet_paid_amount,
        p.promo_code_total_discount::NUMERIC(18,2) AS portal_promo_code_total_discount,
        
        -- Metabase data
        m.transaction_amount::NUMERIC(18,2) AS metabase_amount,
        m.status::VARCHAR AS metabase_status,
        m.gateway::VARCHAR AS metabase_gateway,
        m.portal_order_id::INTEGER AS metabase_portal_order_id,
        m.app_version::VARCHAR AS metabase_app_version,
        m.platform::VARCHAR AS metabase_platform,
        m.order_status::VARCHAR AS metabase_order_status,
        m.order_total::NUMERIC(18,2) AS metabase_order_total,
        m.hyperpay_transaction_id::VARCHAR AS metabase_hyperpay_transaction_id,
        m.success::VARCHAR AS metabase_success,
        m.transactiontype::VARCHAR AS metabase_transactiontype,
        
        -- Gateway-specific columns (actual for current, NULL for others)
        {checkout_v1_cols_str},
        {checkout_v2_cols_str},
        {payfort_cols_str},
        {tamara_cols_str}
    FROM {table_name} {alias}
    JOIN metabase m ON {join_m_condition}
    JOIN portal p ON m.portal_order_id = p.order_id -- Original p.order_id used for join
    {date_filter_here}
    """

    gateways_config = {
        "checkout_v1": {
            "alias": "c1", "table_name": "checkout_v1", "gateway_order_id_type": "INTEGER",
            "amount_col": "c1.transaction_amount", "amount_type": "NUMERIC(18,2)",
            "transaction_date_col": "c1.transaction_date", "transaction_date_type": "DATE",
            "auth_code_col": "c1.authorization_code", "auth_code_type": "VARCHAR",
            "rrn_col": "c1.rrn", "rrn_type": "VARCHAR",
            "payment_method_col": "c1.payment_method", "payment_method_type": "VARCHAR",
            "response_code_col": "c1.response_code", "response_code_type": "VARCHAR",
            "response_description_col": "c1.response_description", "response_description_type": "VARCHAR",
            "is_auth_expr": "(UPPER(c1.status) LIKE '%AUTHORISATION%' OR UPPER(c1.status) LIKE '%AUTHORIZATION%')",
            "is_capture_expr": "(UPPER(c1.status) LIKE '%CAPTURE%')",
            "is_refund_expr": "(UPPER(c1.status) LIKE '%REFUND%')",
            "is_void_expr": "(UPPER(c1.status) LIKE '%VOID%' OR UPPER(c1.status) = 'CANCEL')",
            "join_m_condition": "c1.order_id = m.gateway_order_id AND c1.payment_online_transaction_id = m.gateway_transaction_id",
        },
        "checkout_v2": {
            "alias": "c2", "table_name": "checkout_v2", "gateway_order_id_type": "INTEGER",
            "amount_col": "c2.transaction_amount", "amount_type": "NUMERIC(18,2)",
            "transaction_date_col": "c2.action_date_utc_1", "transaction_date_type": "DATE",
            "auth_code_col": "c2.authorization_code", "auth_code_type": "VARCHAR",
            "rrn_col": "c2.rrn", "rrn_type": "VARCHAR",
            "payment_method_col": "c2.payment_method_name", "payment_method_type": "VARCHAR",
            "response_code_col": "c2.response_code", "response_code_type": "VARCHAR",
            "response_description_col": "c2.response_description", "response_description_type": "VARCHAR",
            "is_auth_expr": "(UPPER(c2.status) LIKE '%AUTHORISATION%' OR UPPER(c2.status) LIKE '%AUTHORIZATION%')",
            "is_capture_expr": "(UPPER(c2.status) LIKE '%CAPTURE%')",
            "is_refund_expr": "(UPPER(c2.status) LIKE '%REFUND%')",
            "is_void_expr": "(UPPER(c2.status) LIKE '%VOID%' OR UPPER(c2.status) = 'CANCEL')",
            "join_m_condition": "c2.order_id = m.gateway_order_id AND c2.payment_online_transaction_id = m.gateway_transaction_id",
        },
        "payfort": {
            "alias": "pf", "table_name": "payfort", "gateway_order_id_type": "INTEGER",
            "amount_col": "pf.transaction_amount", "amount_type": "NUMERIC(18,2)",
            "transaction_date_col": "pf.transaction_date", "transaction_date_type": "DATE",
            "auth_code_col": "pf.authorization_code", "auth_code_type": "VARCHAR",
            "rrn_col": "pf.rrn", "rrn_type": "VARCHAR",
            "payment_method_col": "pf.payment_method", "payment_method_type": "VARCHAR",
            "response_code_col": "''", "response_code_type": "VARCHAR", # Payfort specific
            "response_description_col": "''", "response_description_type": "VARCHAR", # Payfort specific
            "is_auth_expr": "(UPPER(pf.status) LIKE '%AUTHORIZATION%')", # Payfort specific variation
            "is_capture_expr": "(UPPER(pf.status) LIKE '%CAPTURE%')",
            "is_refund_expr": "(UPPER(pf.status) LIKE '%REFUND%')",
            "is_void_expr": "(UPPER(pf.status) LIKE '%VOID%' OR UPPER(pf.status) = 'CANCEL')",
            "join_m_condition": "pf.order_id = m.gateway_order_id AND pf.payment_online_transaction_id = m.gateway_transaction_id",
        },
        "tamara": {
            "alias": "t", "table_name": "tamara", "gateway_order_id_type": "INTEGER",
            "amount_col": "t.txn_amount", "amount_type": "NUMERIC(18,2)",
            "transaction_date_col": "t.txn_created_at_gmt_03_00", "transaction_date_type": "DATE",
            "auth_code_col": "''", "auth_code_type": "VARCHAR", # Tamara specific
            "rrn_col": "t.tamara_txn_reference", "rrn_type": "VARCHAR", # Tamara specific
            "payment_method_col": "t.payment_method", "payment_method_type": "VARCHAR",
            "response_code_col": "''", "response_code_type": "VARCHAR", # Tamara specific
            "response_description_col": "''", "response_description_type": "VARCHAR", # Tamara specific
            "is_auth_expr": "(t.txn_type = 'AUTHORIZE')", # Tamara specific
            "is_capture_expr": "FALSE", # Tamara specific
            "is_refund_expr": "(t.txn_type = 'REFUND')", # Tamara specific
            "is_void_expr": "(t.txn_type = 'CANCEL')", # Tamara specific
            "join_m_condition": "t.payment_online_transaction_id = m.gateway_transaction_id", # Tamara specific join
        }
    }

    gw_specific_field_lists = {
        "checkout_v1": checkout_v1_fields,
        "checkout_v2": checkout_v2_fields,
        "payfort": payfort_fields,
        "tamara": tamara_fields
    }

    gw_sections_sql = []
    gateway_names_ordered = ["checkout_v1", "checkout_v2", "payfort", "tamara"]

    for gw_name in gateway_names_ordered:
        config = gateways_config[gw_name]
        
        cols_str = {}
        for gw_key_for_cols in gateway_names_ordered:
            field_list_for_this_gw = gw_specific_field_lists[gw_key_for_cols]
            if gw_key_for_cols == gw_name: # Actual fields for the current gateway
                cols_str[f'{gw_key_for_cols}_cols_str'] = ",\n        ".join(field_list_for_this_gw)
            else: # NULL placeholders for other gateways
                null_placeholders = []
                for field_def_str in field_list_for_this_gw:
                    field_alias = field_def_str.split(" AS ")[-1].strip()
                    # Determine type for NULL casting from the original definition
                    type_part = field_def_str.split(" AS ")[0].split("::")[-1].strip()
                    null_placeholders.append(f"NULL::{type_part} AS {field_alias}")
                cols_str[f'{gw_key_for_cols}_cols_str'] = ",\n        ".join(null_placeholders)

        section_sql = gateway_select_template.format(
            gateway_source_val=gw_name,
            alias=config["alias"],
            gateway_order_id_type=config["gateway_order_id_type"],
            amount_col=config["amount_col"], amount_type=config["amount_type"],
            transaction_date_col=config["transaction_date_col"], transaction_date_type=config["transaction_date_type"],
            auth_code_col=config["auth_code_col"], auth_code_type=config["auth_code_type"],
            rrn_col=config["rrn_col"], rrn_type=config["rrn_type"],
            payment_method_col=config["payment_method_col"], payment_method_type=config["payment_method_type"],
            response_code_col=config["response_code_col"], response_code_type=config["response_code_type"],
            response_description_col=config["response_description_col"], response_description_type=config["response_description_type"],
            is_auth_expr=config["is_auth_expr"],
            is_capture_expr=config["is_capture_expr"],
            is_refund_expr=config["is_refund_expr"],
            is_void_expr=config["is_void_expr"],
            checkout_v1_cols_str=cols_str["checkout_v1_cols_str"],
            checkout_v2_cols_str=cols_str["checkout_v2_cols_str"],
            payfort_cols_str=cols_str["payfort_cols_str"],
            tamara_cols_str=cols_str["tamara_cols_str"],
            table_name=config["table_name"],
            join_m_condition=config["join_m_condition"],
            date_filter_here=date_filter_clause
        )
        gw_sections_sql.append(section_sql)

    query_parts.append("\n    UNION ALL\n".join(gw_sections_sql))
    query_parts.append("),") # Close gateway_analysis CTE

    # Add ranked_transactions CTE
    query_parts.append("ranked_transactions AS (")
    query_parts.append("    SELECT")
    query_parts.append("        ga.*,")
    query_parts.append("        (MAX(CASE WHEN ga.is_auth THEN 1 ELSE 0 END) OVER (PARTITION BY ga.gateway_order_id, ga.gateway_transaction_id))::INTEGER AS has_auth_in_group,")
    query_parts.append("        (MAX(CASE WHEN ga.is_capture THEN 1 ELSE 0 END) OVER (PARTITION BY ga.gateway_order_id, ga.gateway_transaction_id))::INTEGER AS has_capture_in_group,")
    query_parts.append("        (MAX(CASE WHEN ga.is_auth THEN ga.rrn END) OVER (PARTITION BY ga.gateway_order_id, ga.gateway_transaction_id))::VARCHAR AS auth_rrn_in_group,")
    query_parts.append("        (MAX(CASE WHEN ga.is_auth THEN ga.authorization_code END) OVER (PARTITION BY ga.gateway_order_id, ga.gateway_transaction_id))::VARCHAR AS auth_authorization_code_in_group")
    query_parts.append("    FROM gateway_analysis ga")
    query_parts.append("),")

    # Add transaction_with_analysis CTE
    query_parts.append("transaction_with_analysis AS (")
    query_parts.append("SELECT")
    query_parts.append("        rt.*,")
    query_parts.append("        CASE WHEN rt.is_capture AND rt.has_auth_in_group = 1 AND rt.has_capture_in_group = 1 THEN rt.auth_rrn_in_group ELSE rt.rrn END::VARCHAR AS final_rrn,")
    query_parts.append("        CASE WHEN rt.is_capture AND rt.has_auth_in_group = 1 AND rt.has_capture_in_group = 1 THEN rt.auth_authorization_code_in_group ELSE rt.authorization_code END::VARCHAR AS final_authorization_code,")
    query_parts.append("        CASE")
    query_parts.append("            WHEN rt.is_refund THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) IN ('FULLY_REFUNDED', 'PARTIALLY_REFUNDED') THEN 'Refund Successful' ELSE 'Refund Failed/Pending' END")
    query_parts.append("            WHEN rt.is_void OR UPPER(rt.status) = 'CANCEL' THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'CANCELED' THEN 'Void Successful' ELSE 'Void Failed/Pending' END")
    query_parts.append("            WHEN rt.is_capture THEN")
    query_parts.append("                CASE")
    query_parts.append("                    WHEN rt.has_auth_in_group = 0 THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Capture Successful (auth_less)' ELSE 'Capture Failed/Pending (auth_less)' END")
    query_parts.append("                    WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN CASE WHEN rt.has_auth_in_group = 1 THEN 'Capture Successful (Authorisation Merged)' ELSE 'Capture Successful' END")
    query_parts.append("                    ELSE 'Capture Failed/Pending (Check Response: ' || rt.response_code || ')'")
    query_parts.append("                END")
    query_parts.append("            WHEN rt.is_auth THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Authorisation Successful' ELSE 'Authorisation Failed (Check Response: ' || rt.response_code || ')' END")
    query_parts.append("            WHEN rt.response_code = '10000' THEN 'General Success (Code 10000)'")
    query_parts.append("            WHEN UPPER(rt.status) = 'APPROVED' THEN 'General Success (Approved)'")
    query_parts.append("            WHEN rt.response_code != '10000' AND rt.response_code IS NOT NULL AND rt.response_code != '' THEN 'General Failure (Code: ' || rt.response_code || ')'")
    query_parts.append("            WHEN UPPER(rt.status) IN ('DECLINED', 'FAILED') THEN 'General Failure (Declined/Failed)'")
    query_parts.append("            ELSE 'Unknown/Incomplete Status'")
    query_parts.append("        END::VARCHAR AS transaction_analysis,")
    query_parts.append("        CASE")
    query_parts.append("            WHEN (CASE")
    query_parts.append("                WHEN rt.is_refund THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) IN ('FULLY_REFUNDED', 'PARTIALLY_REFUNDED') THEN 'Success' ELSE 'Failure' END")
    query_parts.append("                WHEN rt.is_void OR UPPER(rt.status) = 'CANCEL' THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'CANCELED' THEN 'Success' ELSE 'Failure' END")
    query_parts.append("                WHEN rt.is_capture THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END")
    query_parts.append("                WHEN rt.is_auth THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END")
    query_parts.append("                WHEN rt.response_code = '10000' THEN 'Success'")
    query_parts.append("                WHEN UPPER(rt.status) = 'APPROVED' THEN 'Success'")
    query_parts.append("                ELSE 'Failure'")
    query_parts.append("            END) = 'Success' THEN 'Success' ELSE 'Failure'")
    query_parts.append("        END::VARCHAR AS transaction_outcome")
    query_parts.append("    FROM ranked_transactions rt")
    query_parts.append("    WHERE NOT (rt.is_auth AND rt.has_auth_in_group = 1 AND rt.has_capture_in_group = 1)")
    query_parts.append("      AND (CASE WHEN (CASE WHEN rt.is_refund THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) IN ('FULLY_REFUNDED', 'PARTIALLY_REFUNDED') THEN 'Success' ELSE 'Failure' END WHEN rt.is_void OR UPPER(rt.status) = 'CANCEL' THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'CANCELED' THEN 'Success' ELSE 'Failure' END WHEN rt.is_capture THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END WHEN rt.is_auth THEN CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END WHEN rt.response_code = '10000' THEN 'Success' WHEN UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END) = 'Success' THEN 'Success' ELSE 'Failure' END) = 'Success'")
    query_parts.append(")") # Close transaction_with_analysis CTE

    # Final SELECT statement for the VIEW definition
    if include_reconciliation:
        query_parts.append("SELECT ")
        query_parts.append("    ta.*,")
        query_parts.append("    b.authorization_code::VARCHAR AS bank_auth_code,")
        query_parts.append("    b.rrn::VARCHAR AS bank_rrn,")
        query_parts.append("    b.transaction_amount::NUMERIC(18,2) AS bank_amount,")
        query_parts.append("    b.transaction_date::DATE AS bank_transaction_date,")
        query_parts.append("    b.transaction_type::VARCHAR AS bank_transaction_type,")
        query_parts.append("    b.status::VARCHAR AS bank_status,")
        query_parts.append("    b.bank_name::VARCHAR AS bank_bank_name,")
        query_parts.append("    b.card_type::VARCHAR AS bank_card_type,")
        query_parts.append("    b.cashback_amount::NUMERIC(18,2) AS bank_cashback_amount,")
        query_parts.append("    b.discount_amount::NUMERIC(18,2) AS bank_discount_amount,")
        query_parts.append("    b.masked_card::VARCHAR AS bank_masked_card,")
        query_parts.append("    b.merchant_identifier::VARCHAR AS bank_merchant_identifier,")
        query_parts.append("    b.payment_online_transaction_id::VARCHAR AS bank_payment_id,")
        query_parts.append("    b.posting_date::DATE AS bank_posting_date,")
        query_parts.append("    b.terminal_identifier::VARCHAR AS bank_terminal_identifier, ")
        query_parts.append("    b.total_payment_amount::NUMERIC(18,2) AS bank_total_payment_amount,")
        query_parts.append("    b.transaction_link_url::VARCHAR AS bank_transaction_link_url,")
        query_parts.append("    b.vat_amount::NUMERIC(18,2) AS bank_vat_amount,")
        query_parts.append("    CASE ")
        query_parts.append("        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code AND ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01 THEN 'MATCH: Authorization Code + RRN + Amount'")
        query_parts.append("        WHEN b.rrn IS NOT NULL AND ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01 THEN 'MATCH: RRN + Amount'")
        query_parts.append("        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01 THEN 'MATCH: Authorization Code + Amount'")
        query_parts.append("        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code AND (b.transaction_date::DATE = ta.transaction_date::DATE) AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01 THEN 'FALLBACK MATCH: Authorization Code + Same Day + Amount'")
        query_parts.append("        WHEN b.rrn IS NOT NULL AND ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0 THEN 'PARTIAL MATCH: RRN + Approx Amount'")
        query_parts.append("        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0 THEN 'PARTIAL MATCH: Authorization Code + Approx Amount'")
        query_parts.append("        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code AND (b.transaction_date::DATE = ta.transaction_date::DATE) AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0 THEN 'PARTIAL MATCH: Authorization Code + Same Day + Approx Amount'")
        query_parts.append("        ELSE NULL")
        query_parts.append("    END::VARCHAR AS bank_match_type")
        query_parts.append("FROM transaction_with_analysis ta")
        query_parts.append("LEFT JOIN bank b ON")
        query_parts.append("    (")
        query_parts.append("        (ta.final_authorization_code = b.authorization_code AND ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01)")
        query_parts.append("        OR (ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01)")
        query_parts.append("        OR (ta.final_authorization_code = b.authorization_code AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01)")
        query_parts.append("        OR (ta.final_authorization_code = b.authorization_code AND (b.transaction_date::DATE = ta.transaction_date::DATE) AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01)")
        query_parts.append("        OR (ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0)")
        query_parts.append("        OR (ta.final_authorization_code = b.authorization_code AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0)")
        query_parts.append("        OR (ta.final_authorization_code = b.authorization_code AND (b.transaction_date::DATE = ta.transaction_date::DATE) AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0)")
        query_parts.append("    )")
    else: # If no reconciliation, select directly from transaction_with_analysis
        query_parts.append("SELECT * FROM transaction_with_analysis")
    
    # Close the CREATE VIEW statement with a semicolon
    view_definition_sql = "\n".join(query_parts) + ";"
    
    # After defining the view, extract all field aliases from the view definition
    view_aliases = extract_view_field_aliases(query_parts)
    
    # Get the field mapping for correcting any discrepancies
    field_alias_mapping = get_field_mapping()
    
    # Process selected fields for the final SELECT statement
    final_select_aliases = []
    processed_selected_fields = set()

    # Process user-selected fields - respect user selection
    for source_key, field_list in selected_fields.items():
        for field_name in field_list:
            # Get the target alias for this field based on source and field name
            alias_to_add = None
            
            if source_key == 'portal':
                # Portal fields are prefixed with portal_
                if field_name.startswith('portal_'):
                    alias_to_add = field_name
                else:
                    alias_to_add = f"portal_{field_name}"
                
                # Handle special case for order_id which is portal_order_id_val in the view
                if alias_to_add == "portal_order_id":
                    alias_to_add = "portal_order_id_val"
            
            elif source_key == 'metabase':
                # Metabase fields are prefixed with metabase_
                if field_name.startswith('metabase_'):
                    alias_to_add = field_name
                else:
                    alias_to_add = f"metabase_{field_name}"
            
            elif source_key == 'bank':
                # Bank fields are prefixed with bank_
                if field_name.startswith('bank_'):
                    alias_to_add = field_name
                else:
                    alias_to_add = f"bank_{field_name}"
                
                # Only include bank fields if reconciliation is enabled
                if not include_reconciliation:
                    alias_to_add = None
            
            elif source_key == 'analysis':
                # Analysis fields with no prefix
                alias_to_add = field_name
            
            elif source_key in ['checkout_v1', 'checkout_v2', 'payfort', 'tamara']:
                # Gateway-specific fields are prefixed with gateway name
                if field_name.startswith(f"{source_key}_"):
                    alias_to_add = field_name
                else:
                    alias_to_add = f"{source_key}_{field_name}"
            
            # Add the field if it's valid and apply mapping if needed
            if alias_to_add:
                # Check if this field name needs to be mapped to a different alias
                if alias_to_add in field_alias_mapping:
                    alias_to_add = field_alias_mapping[alias_to_add]
                processed_selected_fields.add(alias_to_add)

    # Ensure essential fields are always included - these are needed for joins and core functionality
    for field in ESSENTIAL_FIELDS:
        processed_selected_fields.add(field)
    
    # If no fields were selected at all, add some reasonable defaults
    default_fields = ['gateway_order_id', 'transaction_date', 'amount', 'status', 'gateway_source']
    
    if not processed_selected_fields:
        for field in default_fields:
            processed_selected_fields.add(field)
    
    # Remove duplicates and sort for consistency
    final_select_aliases = sorted(list(set(processed_selected_fields)))
    
    # Log debug information about field validation
    log_field_validation_info(final_select_aliases, view_aliases, field_alias_mapping)
    
    # Before constructing the final SELECT, validate that all fields exist in the view
    validated_select_aliases = []
    for alias in final_select_aliases:
        # Check if the field exists in the view or needs to be mapped
        if alias in view_aliases:
            validated_select_aliases.append(alias)
        elif alias in field_alias_mapping and field_alias_mapping[alias] in view_aliases:
            # Use the mapped alias if it exists in the view
            validated_select_aliases.append(field_alias_mapping[alias])
        # Skip fields that don't exist in the view to prevent errors
    
    # Generate the final SELECT statement with validated fields
    if return_distinct_orders:
        # Ensure gateway_order_id is the first column in validated_select_aliases
        if 'gateway_order_id' in validated_select_aliases:
            validated_select_aliases.remove('gateway_order_id')
            validated_select_aliases.insert(0, 'gateway_order_id')
            final_select_statement = f"SELECT DISTINCT ON (gateway_order_id)\n    {',\n    '.join(validated_select_aliases)}\nFROM payment_analysis_view"
        else:
            # If gateway_order_id is not in validated_select_aliases, we need to add it
            validated_select_aliases.insert(0, 'gateway_order_id')
            final_select_statement = f"SELECT DISTINCT ON (gateway_order_id)\n    {',\n    '.join(validated_select_aliases)}\nFROM payment_analysis_view"
    else:
        # Regular SELECT without DISTINCT ON
        final_select_statement = f"SELECT\n    {',\n    '.join(validated_select_aliases)}\nFROM payment_analysis_view"
    
    if limit:
        final_select_statement += f"\nLIMIT {limit}"
    
    final_select_statement += ";"
    
    # Return the complete SQL with view definition and final SELECT properly separated
    return view_definition_sql + "\n\n" + final_select_statement