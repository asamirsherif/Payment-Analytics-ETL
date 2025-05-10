# Payment Analysis SQL Query Generator

A flexible system for generating customized SQL queries for payment data analysis.

## Overview

This project provides tools to dynamically generate SQL queries for payment transaction analysis based on user-selected fields and options. It integrates with the ETL Pipeline Manager GUI to allow visual configuration of queries.

## Files

- **query_generator.py**: Core module for generating SQL queries based on selected fields
- **example_generator.py**: Example script demonstrating how to use the query generator
- **etl_gui.py**: Enhanced ETL Pipeline Manager with query customization UI

## Features

- **Field Selection**: Choose which fields to include from each data source
- **Optional Reconciliation**: Enable/disable bank reconciliation logic
- **Essential Field Protection**: Essential join keys are always included for proper query functioning
- **Query Execution**: Generate, save, and execute queries directly from the UI

## Usage

### GUI Usage

1. Launch the ETL Pipeline Manager: `python etl_gui.py`
2. Navigate to the "Query Generator" tab
3. Select desired fields from each data source
4. Toggle the bank reconciliation option if needed
5. Click "Generate Query" to create the SQL
6. Use the action buttons to save, copy, or execute the query

### API Usage

The query generator can also be used programmatically:

```python
from query_generator import generate_payment_analysis_query

# Define the fields to include
selected_fields = {
    "analysis": ["gateway_source", "amount", "status", "transaction_date"],
    "portal": ["order_id", "transaction_amount", "portal_status"],
    "metabase": ["portal_order_id", "metabase_amount"],
}

# Generate the query (with or without reconciliation)
sql_query = generate_payment_analysis_query(selected_fields, include_reconciliation=True)

# Save or execute the query as needed
with open("my_query.sql", "w") as f:
    f.write(sql_query)
```

## Field Organization

Fields are organized by data source:

- **Portal**: Customer and order information
- **Metabase**: Integration data
- **Checkout V1/V2**: Payment gateway data
- **Payfort**: Alternative payment gateway data
- **Tamara**: Alternative payment method data
- **Bank**: Reconciliation data from bank statements
- **Analysis**: Derived fields and transaction analysis

## Customization

The `query_generator.py` module can be extended by:

1. Adding new field mappings in `FIELD_MAPPINGS`
2. Adding support for additional data sources in `SOURCE_TABLES`
3. Enhancing the query structure in `generate_payment_analysis_query()`

## Requirements

- Python 3.6+
- PyQt5 (for GUI functionality)
- SQLAlchemy (for database interaction)

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run the ETL GUI: `python etl_gui.py`

## Example

```python
# Generate a minimal query
minimal_fields = {
    "analysis": ["gateway_source", "amount", "status"],
    "portal": ["order_id", "transaction_amount"],
}

minimal_query = generate_payment_analysis_query(minimal_fields, include_reconciliation=False)
```

## License

This project is licensed under the MIT License. 