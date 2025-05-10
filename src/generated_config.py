# AUTO‑GENERATED — Edit build_config.py, not this file.
CONFIG = {
    "bank": {
        "columns": {
            "authorization_code": {
                "map_to": "authorization_code",
                "original": "Authorization Code",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "bank_name": {
                "map_to": "bank_name",
                "original": "Bank Name",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "card_type": {
                "map_to": "card_type",
                "original": "Card Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "cashback_amount": {
                "map_to": "cashback_amount",
                "original": "Cashback Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "discount_amount": {
                "map_to": "discount_amount",
                "original": "Discount Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "masked_card": {
                "map_to": "masked_card",
                "original": "Card Number",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "merchant_identifier": {
                "map_to": "merchant_identifier",
                "original": "Merchant Identifier",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "payment_online_transaction_id": {
                "map_to": "payment_online_transaction_id",
                "original": "Payment ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "posting_date": {
                "map_to": "posting_date",
                "original": "Posting Date",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "rrn": {
                "map_to": "rrn",
                "original": "RRN",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "status": {
                "map_to": "status",
                "original": "Payment Status",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "terminal_identifier": {
                "map_to": "terminal_identifier",
                "original": "Terminal Identifier",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "total_payment_amount": {
                "map_to": "total_payment_amount",
                "original": "Total Payment Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "transaction_amount": {
                "map_to": "transaction_amount",
                "original": "Transaction Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "transaction_date": {
                "map_to": "transaction_date",
                "original": "Transaction Date",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "transaction_link_url": {
                "map_to": "transaction_link_url",
                "original": "Transaction Link URL",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "transaction_type": {
                "map_to": "transaction_type",
                "original": "Transaction Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "vat_amount": {
                "map_to": "vat_amount",
                "original": "VAT Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            }
        },
        "files": [
            "D:\\sql_approach_v\\input\\test\\unified_bank_data_with_nameFEB.csv"
        ],
        "target_table": "bank"
    },
    "checkout_v1": {
        "columns": {
            "action_id": {
                "map_to": "action_id",
                "original": "Action ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "authorization_code": {
                "map_to": "authorization_code",
                "original": "Auth Code",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "card_wallet_type": {
                "map_to": "card_wallet_type",
                "original": "Card Wallet Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "cc_bin": {
                "map_to": "cc_bin",
                "original": "CC BIN",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "currency": {
                "map_to": "currency",
                "original": "Currency",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "issuing_bank": {
                "map_to": "issuing_bank",
                "original": "Issuing Bank",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "order_id": {
                "map_to": "order_id",
                "original": "Reference",
                "py_type": "integer",
                "sql_type": "INTEGER"
            },
            "payment_method": {
                "map_to": "payment_method",
                "original": "Payment Method",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "payment_online_transaction_id": {
                "map_to": "payment_online_transaction_id",
                "original": "Payment ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "payment_unique_number": {
                "map_to": "payment_unique_number",
                "original": "Payment Unique Number",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "processor": {
                "map_to": "processor",
                "original": "Processor",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "response_code": {
                "map_to": "response_code",
                "original": "Response Code",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "response_description": {
                "map_to": "response_description",
                "original": "Response Description",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "rrn": {
                "map_to": "rrn",
                "original": "Acquirer Reference ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "status": {
                "map_to": "status",
                "original": "Action Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "transaction_amount": {
                "map_to": "transaction_amount",
                "original": "Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "transaction_date": {
                "map_to": "transaction_date",
                "original": "Action Date UTC",
                "py_type": "date",
                "sql_type": "DATE"
            }
        },
        "files": [
            "D:\\sql_approach_v\\input\\test\\checkout v1 FEB  10 days.csv"
        ],
        "target_table": "checkout_v1"
    },
    "checkout_v2": {
        "columns": {
            "action_date_utc_1": {
                "map_to": "action_date_utc_1",
                "original": "Action Date UTC.1",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "action_id": {
                "map_to": "action_id",
                "original": "Action ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "authorization_code": {
                "map_to": "authorization_code",
                "original": "Auth Code",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "co_badged_card": {
                "map_to": "co_badged_card",
                "original": "Co-Badged Card",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "currency_symbol": {
                "map_to": "currency_symbol",
                "original": "Currency Symbol",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "order_id": {
                "map_to": "order_id",
                "original": "Reference",
                "py_type": "integer",
                "sql_type": "INTEGER"
            },
            "payment_method_name": {
                "map_to": "payment_method_name",
                "original": "Payment Method Name",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "payment_online_transaction_id": {
                "map_to": "payment_online_transaction_id",
                "original": "Payment ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "response_code": {
                "map_to": "response_code",
                "original": "Response Code",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "response_description": {
                "map_to": "response_description",
                "original": "Response Description",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "rrn": {
                "map_to": "rrn",
                "original": "Acquirer Reference Number",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "status": {
                "map_to": "status",
                "original": "Action Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "transaction_amount": {
                "map_to": "transaction_amount",
                "original": "Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "wallet": {
                "map_to": "wallet",
                "original": "Wallet",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            }
        },
        "files": [
            "D:\\sql_approach_v\\input\\test\\2024-02-01-2024-02-29-10-days.csv"
        ],
        "target_table": "checkout_v2"
    },
    "metabase": {
        "columns": {
            "app_version": {
                "map_to": "app_version",
                "original": "app_version",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "donation_amount": {
                "map_to": "donation_amount",
                "original": "Donation Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "donation_id": {
                "map_to": "donation_id",
                "original": "Donation ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "gateway": {
                "map_to": "gateway",
                "original": "Gateway",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "gateway_order_id": {
                "map_to": "gateway_order_id",
                "original": "payment_online_transaction_id",
                "py_type": "integer",
                "sql_type": "INTEGER"
            },
            "gateway_transaction_id": {
                "map_to": "gateway_transaction_id",
                "original": "gateway_transaction_id",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "hyperpay_merchant_transaction_id": {
                "map_to": "hyperpay_merchant_transaction_id",
                "original": "hyperpay_merchant_transaction_id",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "hyperpay_transaction_id": {
                "map_to": "hyperpay_transaction_id",
                "original": "hyperpay_transaction_id",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "order_chef_total": {
                "map_to": "order_chef_total",
                "original": "order_chef_total",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "order_status": {
                "map_to": "order_status",
                "original": "order_status",
                "py_type": "date",
                "sql_type": "TEXT"
            },
            "order_total": {
                "map_to": "order_total",
                "original": "Order Total",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "platform": {
                "map_to": "platform",
                "original": "platform",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "portal_order_id": {
                "map_to": "portal_order_id",
                "original": "Order ID",
                "py_type": "integer",
                "sql_type": "INTEGER"
            },
            "reservation_id": {
                "map_to": "reservation_id",
                "original": "Reservation ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "reservation_status": {
                "map_to": "reservation_status",
                "original": "Reservation Status",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "reservation_total_price": {
                "map_to": "reservation_total_price",
                "original": "Reservation Total Price",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "source_id": {
                "map_to": "source_id",
                "original": "ID",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "status": {
                "map_to": "status",
                "original": "Payment Status",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "success": {
                "map_to": "success",
                "original": "Success",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "transaction_amount": {
                "map_to": "transaction_amount",
                "original": "Transaction Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "transactiontype": {
                "map_to": "transactiontype",
                "original": "TransactionType",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            }
        },
        "files": [
            "D:\\sql_approach_v\\input\\test\\METABASE-FEB-1-15.csv"
        ],
        "target_table": "metabase"
    },
    "payfort": {
        "columns": {
            "acquirer_name": {
                "map_to": "acquirer_name",
                "original": "Acquirer Name",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "authorization_code": {
                "map_to": "authorization_code",
                "original": "Authorization Code",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "channel": {
                "map_to": "channel",
                "original": "Channel",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "currency": {
                "map_to": "currency",
                "original": "Currency",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "merchant_country": {
                "map_to": "merchant_country",
                "original": "Merchant Country",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "mid": {
                "map_to": "mid",
                "original": "MID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "order_id": {
                "map_to": "order_id",
                "original": "Merchant Reference",
                "py_type": "integer",
                "sql_type": "INTEGER"
            },
            "payment_method": {
                "map_to": "payment_method",
                "original": "Payment Option",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "payment_method_type": {
                "map_to": "payment_method_type",
                "original": "Payment Method",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "payment_online_transaction_id": {
                "map_to": "payment_online_transaction_id",
                "original": "FORT ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "period": {
                "map_to": "period",
                "original": "Period",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "rrn": {
                "map_to": "rrn",
                "original": "Reconciliation Reference (RRN)",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "status": {
                "map_to": "status",
                "original": "Operation",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "time": {
                "map_to": "time",
                "original": "time",
                "py_type": "time",
                "sql_type": "TIME"
            },
            "transaction_amount": {
                "map_to": "transaction_amount",
                "original": "Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "transaction_date": {
                "map_to": "transaction_date",
                "original": "Date & Time",
                "py_type": "date",
                "sql_type": "DATE"
            }
        },
        "files": [
            "D:\\sql_approach_v\\input\\test\\Payfort-2024.csv"
        ],
        "target_table": "payfort"
    },
    "portal": {
        "columns": {
            "additional_delivery_fee": {
                "map_to": "additional_delivery_fee",
                "original": "Additional delivery fee",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "bonus": {
                "map_to": "bonus",
                "original": "Bonus",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "chef_name": {
                "map_to": "chef_name",
                "original": "Chef Name",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "chef_total": {
                "map_to": "chef_total",
                "original": "Chef Total",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "collected_cash": {
                "map_to": "collected_cash",
                "original": "Collected Cash",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "commission_amount": {
                "map_to": "commission_amount",
                "original": "Commission Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "commission_percentage": {
                "map_to": "commission_percentage",
                "original": "Commission Percentage",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "compensation_by_thechefz": {
                "map_to": "compensation_by_thechefz",
                "original": "Compensation By TheChefz",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "compensation_by_vendor": {
                "map_to": "compensation_by_vendor",
                "original": "Compensation By Vendor",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "customer_name": {
                "map_to": "customer_name",
                "original": "Customer Name",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "dark_store_fees": {
                "map_to": "dark_store_fees",
                "original": "Dark Store Fees",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "delivery_fees_by_customer": {
                "map_to": "delivery_fees_by_customer",
                "original": "Delivery Fees By Customer",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "delivery_type": {
                "map_to": "delivery_type",
                "original": "Delivery Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "discount_type": {
                "map_to": "discount_type",
                "original": "Discount Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "driver_delivery_fees": {
                "map_to": "driver_delivery_fees",
                "original": "Driver Delivery Fees",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "estimated_delivery_date": {
                "map_to": "estimated_delivery_date",
                "original": "Estimated Delivery Date",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "extra_delivery_fees": {
                "map_to": "extra_delivery_fees",
                "original": "Extra Delivery Fees",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "fees": {
                "map_to": "fees",
                "original": "Fees",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "gateway": {
                "map_to": "gateway",
                "original": "Integration",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "gift_endurance": {
                "map_to": "gift_endurance",
                "original": "Gift Endurance",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "is_catering": {
                "map_to": "is_catering",
                "original": "Is Catering",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "ispaid": {
                "map_to": "ispaid",
                "original": "isPaid",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "main_wallet_paid_amount": {
                "map_to": "main_wallet_paid_amount",
                "original": "Main Wallet Paid Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "note": {
                "map_to": "note",
                "original": "Note",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "order_id": {
                "map_to": "order_id",
                "original": "Order Id",
                "py_type": "integer",
                "sql_type": "INTEGER"
            },
            "order_items": {
                "map_to": "order_items",
                "original": "Order Items",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "payment_method": {
                "map_to": "payment_method",
                "original": "Payment Method",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "promo_code_discount_chef_amount": {
                "map_to": "promo_code_discount_chef_amount",
                "original": "Promo-code discount chef amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "promo_code_total_discount": {
                "map_to": "promo_code_total_discount",
                "original": "Promo-code total discount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "promocode_type": {
                "map_to": "promocode_type",
                "original": "Promocode Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "promotion_discount_chef_amount": {
                "map_to": "promotion_discount_chef_amount",
                "original": "Promotion discount chef amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "promotion_template_id": {
                "map_to": "promotion_template_id",
                "original": "Promotion Template ID",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "promotion_tier_id": {
                "map_to": "promotion_tier_id",
                "original": "Promotion Tier ID",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "promotion_total_discount": {
                "map_to": "promotion_total_discount",
                "original": "Promotion total discount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "service_fees": {
                "map_to": "service_fees",
                "original": "Service Fees",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "share_earn_order": {
                "map_to": "share_earn_order",
                "original": "Share & Earn Order",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "status": {
                "map_to": "status",
                "original": "Order Status",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "store_wallet_paid_amount": {
                "map_to": "store_wallet_paid_amount",
                "original": "Store Wallet Paid Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "thechefz_fees": {
                "map_to": "thechefz_fees",
                "original": "TheChefz Fees",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "total_transfer_fees": {
                "map_to": "total_transfer_fees",
                "original": "Total Transfer Fees",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "transaction_amount": {
                "map_to": "transaction_amount",
                "original": "Order Total",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "transaction_date": {
                "map_to": "transaction_date",
                "original": "Delivery Date",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "transaction_id": {
                "map_to": "transaction_id",
                "original": "Order Number",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "wallet_paid_amount": {
                "map_to": "wallet_paid_amount",
                "original": "Wallet Paid Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            }
        },
        "files": [
            "D:\\sql_approach_v\\input\\test\\portal.csv"
        ],
        "target_table": "portal"
    },
    "tamara": {
        "columns": {
            "comment": {
                "map_to": "comment",
                "original": "Comment",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "country_code": {
                "map_to": "country_code",
                "original": "Country Code",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "customer_name": {
                "map_to": "customer_name",
                "original": "Customer Name",
                "py_type": "string",
                "sql_type": "TEXT"
            },
            "merchant_order_reference_id": {
                "map_to": "merchant_order_reference_id",
                "original": "Merchant Order Reference ID",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "order_created_at_gmt_03_00": {
                "map_to": "order_created_at_gmt_03_00",
                "original": "Order Created At GMT+03:00",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "order_currency": {
                "map_to": "order_currency",
                "original": "Order Currency",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "order_id": {
                "map_to": "order_id",
                "original": "Merchant Order Number",
                "py_type": "integer",
                "sql_type": "INTEGER"
            },
            "payment_method": {
                "map_to": "payment_method",
                "original": "Payment Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "payment_online_transaction_id": {
                "map_to": "payment_online_transaction_id",
                "original": "Tamara Order Id",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "status": {
                "map_to": "status",
                "original": "Order Status",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "store_name": {
                "map_to": "store_name",
                "original": "Store Name",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "tamara_txn_reference": {
                "map_to": "tamara_txn_reference",
                "original": "Tamara Txn Reference",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "total_amount": {
                "map_to": "total_amount",
                "original": "Total Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "txn_amount": {
                "map_to": "txn_amount",
                "original": "Txn Amount",
                "py_type": "float",
                "sql_type": "NUMERIC(18,2)"
            },
            "txn_created_at_gmt_03_00": {
                "map_to": "txn_created_at_gmt_03_00",
                "original": "Txn Created At GMT+03:00",
                "py_type": "date",
                "sql_type": "DATE"
            },
            "txn_settlement_status": {
                "map_to": "txn_settlement_status",
                "original": "Txn Settlement Status",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            },
            "txn_type": {
                "map_to": "txn_type",
                "original": "Txn Type",
                "py_type": "string",
                "sql_type": "VARCHAR(255)"
            }
        },
        "files": [
            "D:\\sql_approach_v\\input\\test\\Tamara-FEb.csv"
        ],
        "target_table": "tamara"
    }
}
