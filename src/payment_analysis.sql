-- Recommended Indices for Performance - Only create if they don't exist:
DO $$
BEGIN
    -- Check and create indices on portal table
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_portal_order_id') THEN
        CREATE INDEX idx_portal_order_id ON portal (order_id);
    END IF;
    
    -- Check and create indices on metabase table
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_metabase_portal_order_id') THEN
        CREATE INDEX idx_metabase_portal_order_id ON metabase (portal_order_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_metabase_gateway_ids') THEN
        CREATE INDEX idx_metabase_gateway_ids ON metabase (gateway_order_id, gateway_transaction_id);
    END IF;
    
    -- Check and create indices on checkout_v1 table
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_checkout_v1_join_keys') THEN
        CREATE INDEX idx_checkout_v1_join_keys ON checkout_v1 (order_id, payment_online_transaction_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_checkout_v1_status_code') THEN
        CREATE INDEX idx_checkout_v1_status_code ON checkout_v1 (status, response_code);
    END IF;
    
    -- Check and create indices on checkout_v2 table
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_checkout_v2_join_keys') THEN
        CREATE INDEX idx_checkout_v2_join_keys ON checkout_v2 (order_id, payment_online_transaction_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_checkout_v2_status_code') THEN
        CREATE INDEX idx_checkout_v2_status_code ON checkout_v2 (status, response_code);
    END IF;
    
    -- Check and create indices on payfort table
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_payfort_join_keys') THEN
        CREATE INDEX idx_payfort_join_keys ON payfort (order_id, payment_online_transaction_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_payfort_status') THEN
        CREATE INDEX idx_payfort_status ON payfort (status);
    END IF;
    
    -- Check and create indices on tamara table
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_tamara_payment_id') THEN
        CREATE INDEX idx_tamara_payment_id ON tamara (payment_online_transaction_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_tamara_status_type') THEN
        CREATE INDEX idx_tamara_status_type ON tamara (status, txn_type);
    END IF;
    
    -- Check and create indices on bank table
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_bank_auth_rrn') THEN
        CREATE INDEX idx_bank_auth_rrn ON bank (authorization_code, rrn);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_bank_rrn') THEN
        CREATE INDEX idx_bank_rrn ON bank (rrn);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_bank_auth_code') THEN
        CREATE INDEX idx_bank_auth_code ON bank (authorization_code);
    END IF;
END$$;

-- Drop the view first to allow changing column data types
DROP VIEW IF EXISTS payment_analysis_view;

-- Create or replace a view that encapsulates our payment analysis query
CREATE OR REPLACE VIEW payment_analysis_view AS
WITH 
-- First CTE for direct gateway data analysis with portal and metabase data included
gateway_analysis AS (
    -- Checkout V1 Analysis
    SELECT
        'checkout_v1'::VARCHAR AS gateway_source,
        c1.order_id::INTEGER AS gateway_order_id,
        c1.payment_online_transaction_id::VARCHAR AS gateway_transaction_id,
        c1.transaction_amount::NUMERIC(18,2) AS amount,
        c1.status::VARCHAR AS status,
        c1.transaction_date::DATE AS transaction_date,
        c1.authorization_code::VARCHAR AS authorization_code,
        c1.rrn::VARCHAR AS rrn,
        c1.payment_method::VARCHAR AS payment_method,
        c1.response_code::VARCHAR AS response_code,
        c1.response_description::VARCHAR AS response_description,
        (UPPER(c1.status) LIKE '%AUTHORISATION%')::BOOLEAN AS is_auth,
        (UPPER(c1.status) LIKE '%CAPTURE%')::BOOLEAN AS is_capture,
        (UPPER(c1.status) LIKE '%REFUND%')::BOOLEAN AS is_refund,
        (UPPER(c1.status) LIKE '%VOID%')::BOOLEAN AS is_void,
        
        -- Portal data
        p.order_id::INTEGER,
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
        
        -- Checkout V1 specific columns 
        c1.transaction_amount::NUMERIC(18,2) AS checkout_v1_amount,
        c1.status::VARCHAR AS checkout_v1_status,
        c1.transaction_date::DATE AS checkout_v1_transaction_date,
        c1.payment_online_transaction_id::VARCHAR AS checkout_v1_payment_id,
        c1.order_id::INTEGER AS checkout_v1_order_id,
        c1.authorization_code::VARCHAR AS checkout_v1_authorization_code,
        c1.rrn::VARCHAR AS checkout_v1_rrn,
        c1.payment_method::VARCHAR AS checkout_v1_payment_method,
        c1.issuing_bank::VARCHAR AS checkout_v1_issuing_bank,
        c1.response_code::VARCHAR AS checkout_v1_response_code,
        c1.response_description::VARCHAR AS checkout_v1_response_description,
        c1.processor::VARCHAR AS checkout_v1_processor,
        c1.card_wallet_type::VARCHAR AS checkout_v1_card_wallet_type,
        c1.currency::VARCHAR AS checkout_v1_currency,
        
        -- Empty placeholders for other gateway-specific fields
        NULL::NUMERIC(18,2) AS checkout_v2_amount,
        NULL::VARCHAR AS checkout_v2_status,
        NULL::DATE AS checkout_v2_transaction_date,
        NULL::VARCHAR AS checkout_v2_payment_id,
        NULL::INTEGER AS checkout_v2_order_id,
        NULL::VARCHAR AS checkout_v2_authorization_code,
        NULL::VARCHAR AS checkout_v2_rrn,
        NULL::VARCHAR AS checkout_v2_payment_method,
        NULL::VARCHAR AS checkout_v2_response_code,
        NULL::VARCHAR AS checkout_v2_response_description,
        NULL::VARCHAR AS checkout_v2_wallet,
        NULL::VARCHAR AS checkout_v2_currency,
        NULL::VARCHAR AS checkout_v2_co_badged_card,
        
        NULL::NUMERIC(18,2) AS payfort_amount,
        NULL::VARCHAR AS payfort_status,
        NULL::DATE AS payfort_transaction_date,
        NULL::VARCHAR AS payfort_payment_id,
        NULL::INTEGER AS payfort_order_id,
        NULL::VARCHAR AS payfort_authorization_code,
        NULL::VARCHAR AS payfort_rrn,
        NULL::VARCHAR AS payfort_payment_method,
        NULL::VARCHAR AS payfort_payment_method_type,
        NULL::VARCHAR AS payfort_acquirer_name,
        NULL::VARCHAR AS payfort_merchant_country,
        NULL::VARCHAR AS payfort_channel,
        NULL::VARCHAR AS payfort_mid,
        NULL::VARCHAR AS payfort_currency,
        NULL::TIME AS payfort_time,
        
        NULL::NUMERIC(18,2) AS tamara_amount,
        NULL::VARCHAR AS tamara_status,
        NULL::DATE AS tamara_transaction_date,
        NULL::VARCHAR AS tamara_payment_id,
        NULL::INTEGER AS tamara_portal_order_id,
        NULL::VARCHAR AS tamara_txn_reference,
        NULL::VARCHAR AS tamara_payment_method,
        NULL::VARCHAR AS tamara_customer_name,
        NULL::VARCHAR AS tamara_txn_type,
        NULL::VARCHAR AS tamara_txn_settlement_status,
        NULL::VARCHAR AS tamara_order_currency,
        NULL::VARCHAR AS tamara_country_code,
        NULL::VARCHAR AS tamara_store_name,
        NULL::NUMERIC(18,2) AS tamara_total_amount,
        NULL::DATE AS tamara_order_created_at
    FROM checkout_v1 c1
    -- Join with metabase to get to portal
    JOIN metabase m ON c1.order_id = m.gateway_order_id AND c1.payment_online_transaction_id = m.gateway_transaction_id
    -- Join with portal
    JOIN portal p ON m.portal_order_id = p.order_id
    
    UNION ALL
    
    -- Checkout V2 Analysis
    SELECT
        'checkout_v2'::VARCHAR AS gateway_source,
        c2.order_id::INTEGER AS gateway_order_id,
        c2.payment_online_transaction_id::VARCHAR AS gateway_transaction_id,
        c2.transaction_amount::NUMERIC(18,2) AS amount,
        c2.status::VARCHAR AS status,
        c2.action_date_utc_1::DATE AS transaction_date,
        c2.authorization_code::VARCHAR AS authorization_code,
        c2.rrn::VARCHAR AS rrn,
        c2.payment_method_name::VARCHAR AS payment_method,
        c2.response_code::VARCHAR AS response_code,
        c2.response_description::VARCHAR AS response_description,
        (UPPER(c2.status) LIKE '%AUTHORISATION%')::BOOLEAN AS is_auth,
        (UPPER(c2.status) LIKE '%CAPTURE%')::BOOLEAN AS is_capture,
        (UPPER(c2.status) LIKE '%REFUND%')::BOOLEAN AS is_refund,
        (UPPER(c2.status) LIKE '%VOID%')::BOOLEAN AS is_void,
        
        -- Portal data
        p.order_id::INTEGER,
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
        
        -- Empty placeholders for checkout_v1 fields
        NULL::NUMERIC(18,2) AS checkout_v1_amount,
        NULL::VARCHAR AS checkout_v1_status,
        NULL::DATE AS checkout_v1_transaction_date,
        NULL::VARCHAR AS checkout_v1_payment_id,
        NULL::INTEGER AS checkout_v1_order_id,
        NULL::VARCHAR AS checkout_v1_authorization_code,
        NULL::VARCHAR AS checkout_v1_rrn,
        NULL::VARCHAR AS checkout_v1_payment_method,
        NULL::VARCHAR AS checkout_v1_issuing_bank,
        NULL::VARCHAR AS checkout_v1_response_code,
        NULL::VARCHAR AS checkout_v1_response_description,
        NULL::VARCHAR AS checkout_v1_processor,
        NULL::VARCHAR AS checkout_v1_card_wallet_type,
        NULL::VARCHAR AS checkout_v1_currency,
        
        -- Checkout V2 specific columns
        c2.transaction_amount::NUMERIC(18,2) AS checkout_v2_amount,
        c2.status::VARCHAR AS checkout_v2_status,
        c2.action_date_utc_1::DATE AS checkout_v2_transaction_date,
        c2.payment_online_transaction_id::VARCHAR AS checkout_v2_payment_id,
        c2.order_id::INTEGER AS checkout_v2_order_id,
        c2.authorization_code::VARCHAR AS checkout_v2_authorization_code,
        c2.rrn::VARCHAR AS checkout_v2_rrn,
        c2.payment_method_name::VARCHAR AS checkout_v2_payment_method,
        c2.response_code::VARCHAR AS checkout_v2_response_code,
        c2.response_description::VARCHAR AS checkout_v2_response_description,
        c2.wallet::VARCHAR AS checkout_v2_wallet,
        c2.currency_symbol::VARCHAR AS checkout_v2_currency,
        c2.co_badged_card::VARCHAR AS checkout_v2_co_badged_card,
        
        -- Empty placeholders for other gateway-specific fields
        NULL::NUMERIC(18,2) AS payfort_amount,
        NULL::VARCHAR AS payfort_status,
        NULL::DATE AS payfort_transaction_date,
        NULL::VARCHAR AS payfort_payment_id,
        NULL::INTEGER AS payfort_order_id,
        NULL::VARCHAR AS payfort_authorization_code,
        NULL::VARCHAR AS payfort_rrn,
        NULL::VARCHAR AS payfort_payment_method,
        NULL::VARCHAR AS payfort_payment_method_type,
        NULL::VARCHAR AS payfort_acquirer_name,
        NULL::VARCHAR AS payfort_merchant_country,
        NULL::VARCHAR AS payfort_channel,
        NULL::VARCHAR AS payfort_mid,
        NULL::VARCHAR AS payfort_currency,
        NULL::TIME AS payfort_time,
        
        NULL::NUMERIC(18,2) AS tamara_amount,
        NULL::VARCHAR AS tamara_status,
        NULL::DATE AS tamara_transaction_date,
        NULL::VARCHAR AS tamara_payment_id,
        NULL::INTEGER AS tamara_portal_order_id,
        NULL::VARCHAR AS tamara_txn_reference,
        NULL::VARCHAR AS tamara_payment_method,
        NULL::VARCHAR AS tamara_customer_name,
        NULL::VARCHAR AS tamara_txn_type,
        NULL::VARCHAR AS tamara_txn_settlement_status,
        NULL::VARCHAR AS tamara_order_currency,
        NULL::VARCHAR AS tamara_country_code,
        NULL::VARCHAR AS tamara_store_name,
        NULL::NUMERIC(18,2) AS tamara_total_amount,
        NULL::DATE AS tamara_order_created_at
    FROM checkout_v2 c2
    -- Join with metabase to get to portal
    JOIN metabase m ON c2.order_id = m.gateway_order_id AND c2.payment_online_transaction_id = m.gateway_transaction_id
    -- Join with portal
    JOIN portal p ON m.portal_order_id = p.order_id
    
    UNION ALL
    
    -- Payfort Analysis
    SELECT
        'payfort'::VARCHAR AS gateway_source,
        pf.order_id::INTEGER AS gateway_order_id,
        pf.payment_online_transaction_id::VARCHAR AS gateway_transaction_id,
        pf.transaction_amount::NUMERIC(18,2) AS amount,
        pf.status::VARCHAR AS status,
        pf.transaction_date::DATE AS transaction_date,
        pf.authorization_code::VARCHAR AS authorization_code,
        pf.rrn::VARCHAR AS rrn,
        pf.payment_method::VARCHAR AS payment_method,
        ''::VARCHAR AS response_code, -- Payfort doesn't have response_code in the same format
        ''::VARCHAR AS response_description,
        (UPPER(pf.status) LIKE '%AUTHORIZATION%')::BOOLEAN AS is_auth, -- Payfort might use 'AUTHORIZATION' instead of 'AUTHORISATION'
        (UPPER(pf.status) LIKE '%CAPTURE%')::BOOLEAN AS is_capture,
        (UPPER(pf.status) LIKE '%REFUND%')::BOOLEAN AS is_refund,
        (UPPER(pf.status) LIKE '%VOID%')::BOOLEAN AS is_void,
        
        -- Portal data
        p.order_id::INTEGER,
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
        
        -- Empty placeholders for checkout_v1 fields
        NULL::NUMERIC(18,2) AS checkout_v1_amount,
        NULL::VARCHAR AS checkout_v1_status,
        NULL::DATE AS checkout_v1_transaction_date,
        NULL::VARCHAR AS checkout_v1_payment_id,
        NULL::INTEGER AS checkout_v1_order_id,
        NULL::VARCHAR AS checkout_v1_authorization_code,
        NULL::VARCHAR AS checkout_v1_rrn,
        NULL::VARCHAR AS checkout_v1_payment_method,
        NULL::VARCHAR AS checkout_v1_issuing_bank,
        NULL::VARCHAR AS checkout_v1_response_code,
        NULL::VARCHAR AS checkout_v1_response_description,
        NULL::VARCHAR AS checkout_v1_processor,
        NULL::VARCHAR AS checkout_v1_card_wallet_type,
        NULL::VARCHAR AS checkout_v1_currency,
        
        -- Empty placeholders for checkout_v2 fields
        NULL::NUMERIC(18,2) AS checkout_v2_amount,
        NULL::VARCHAR AS checkout_v2_status,
        NULL::DATE AS checkout_v2_transaction_date,
        NULL::VARCHAR AS checkout_v2_payment_id,
        NULL::INTEGER AS checkout_v2_order_id,
        NULL::VARCHAR AS checkout_v2_authorization_code,
        NULL::VARCHAR AS checkout_v2_rrn,
        NULL::VARCHAR AS checkout_v2_payment_method,
        NULL::VARCHAR AS checkout_v2_response_code,
        NULL::VARCHAR AS checkout_v2_response_description,
        NULL::VARCHAR AS checkout_v2_wallet,
        NULL::VARCHAR AS checkout_v2_currency,
        NULL::VARCHAR AS checkout_v2_co_badged_card,
        
        -- Payfort specific columns 
        pf.transaction_amount::NUMERIC(18,2) AS payfort_amount,
        pf.status::VARCHAR AS payfort_status,
        pf.transaction_date::DATE AS payfort_transaction_date,
        pf.payment_online_transaction_id::VARCHAR AS payfort_payment_id,
        pf.order_id::INTEGER AS payfort_order_id,
        pf.authorization_code::VARCHAR AS payfort_authorization_code,
        pf.rrn::VARCHAR AS payfort_rrn,
        pf.payment_method::VARCHAR AS payfort_payment_method,
        pf.payment_method_type::VARCHAR AS payfort_payment_method_type,
        pf.acquirer_name::VARCHAR AS payfort_acquirer_name,
        pf.merchant_country::VARCHAR AS payfort_merchant_country,
        pf.channel::VARCHAR AS payfort_channel,
        pf.mid::VARCHAR AS payfort_mid,
        pf.currency::VARCHAR AS payfort_currency,
        pf.time::TIME AS payfort_time,
        
        -- Empty placeholders for tamara fields
        NULL::NUMERIC(18,2) AS tamara_amount,
        NULL::VARCHAR AS tamara_status,
        NULL::DATE AS tamara_transaction_date,
        NULL::VARCHAR AS tamara_payment_id,
        NULL::INTEGER AS tamara_portal_order_id,
        NULL::VARCHAR AS tamara_txn_reference,
        NULL::VARCHAR AS tamara_payment_method,
        NULL::VARCHAR AS tamara_customer_name,
        NULL::VARCHAR AS tamara_txn_type,
        NULL::VARCHAR AS tamara_txn_settlement_status,
        NULL::VARCHAR AS tamara_order_currency,
        NULL::VARCHAR AS tamara_country_code,
        NULL::VARCHAR AS tamara_store_name,
        NULL::NUMERIC(18,2) AS tamara_total_amount,
        NULL::DATE AS tamara_order_created_at
    FROM payfort pf
    -- Join with metabase to get to portal
    JOIN metabase m ON pf.order_id = m.gateway_order_id AND pf.payment_online_transaction_id = m.gateway_transaction_id
    -- Join with portal
    JOIN portal p ON m.portal_order_id = p.order_id
    
    UNION ALL
    
    -- Tamara Analysis
    SELECT
        'tamara'::VARCHAR AS gateway_source,
        t.order_id::INTEGER AS gateway_order_id,
        t.payment_online_transaction_id::VARCHAR AS gateway_transaction_id,
        t.txn_amount::NUMERIC(18,2) AS amount,
        t.status::VARCHAR AS status,
        t.txn_created_at_gmt_03_00::DATE AS transaction_date,
        ''::VARCHAR AS authorization_code, -- Tamara doesn't have authorization_code in the same format
        t.tamara_txn_reference::VARCHAR AS rrn, -- Using txn_reference as RRN equivalent
        t.payment_method::VARCHAR AS payment_method,
        ''::VARCHAR AS response_code,
        ''::VARCHAR AS response_description,
        (t.txn_type = 'AUTHORIZE')::BOOLEAN AS is_auth,
        FALSE::BOOLEAN AS is_capture, -- Tamara doesn't have explicit 'CAPTURE'
        (t.txn_type = 'REFUND')::BOOLEAN AS is_refund,
        (t.txn_type = 'CANCEL')::BOOLEAN AS is_void,
        
        -- Portal data
        p.order_id::INTEGER,
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
        
        -- Empty placeholders for checkout_v1 fields
        NULL::NUMERIC(18,2) AS checkout_v1_amount,
        NULL::VARCHAR AS checkout_v1_status,
        NULL::DATE AS checkout_v1_transaction_date,
        NULL::VARCHAR AS checkout_v1_payment_id,
        NULL::INTEGER AS checkout_v1_order_id,
        NULL::VARCHAR AS checkout_v1_authorization_code,
        NULL::VARCHAR AS checkout_v1_rrn,
        NULL::VARCHAR AS checkout_v1_payment_method,
        NULL::VARCHAR AS checkout_v1_issuing_bank,
        NULL::VARCHAR AS checkout_v1_response_code,
        NULL::VARCHAR AS checkout_v1_response_description,
        NULL::VARCHAR AS checkout_v1_processor,
        NULL::VARCHAR AS checkout_v1_card_wallet_type,
        NULL::VARCHAR AS checkout_v1_currency,
        
        -- Empty placeholders for checkout_v2 fields
        NULL::NUMERIC(18,2) AS checkout_v2_amount,
        NULL::VARCHAR AS checkout_v2_status,
        NULL::DATE AS checkout_v2_transaction_date,
        NULL::VARCHAR AS checkout_v2_payment_id,
        NULL::INTEGER AS checkout_v2_order_id,
        NULL::VARCHAR AS checkout_v2_authorization_code,
        NULL::VARCHAR AS checkout_v2_rrn,
        NULL::VARCHAR AS checkout_v2_payment_method,
        NULL::VARCHAR AS checkout_v2_response_code,
        NULL::VARCHAR AS checkout_v2_response_description,
        NULL::VARCHAR AS checkout_v2_wallet,
        NULL::VARCHAR AS checkout_v2_currency,
        NULL::VARCHAR AS checkout_v2_co_badged_card,
        
        -- Empty placeholders for payfort fields
        NULL::NUMERIC(18,2) AS payfort_amount,
        NULL::VARCHAR AS payfort_status,
        NULL::DATE AS payfort_transaction_date,
        NULL::VARCHAR AS payfort_payment_id,
        NULL::INTEGER AS payfort_order_id,
        NULL::VARCHAR AS payfort_authorization_code,
        NULL::VARCHAR AS payfort_rrn,
        NULL::VARCHAR AS payfort_payment_method,
        NULL::VARCHAR AS payfort_payment_method_type,
        NULL::VARCHAR AS payfort_acquirer_name,
        NULL::VARCHAR AS payfort_merchant_country,
        NULL::VARCHAR AS payfort_channel,
        NULL::VARCHAR AS payfort_mid,
        NULL::VARCHAR AS payfort_currency,
        NULL::TIME AS payfort_time,
        
        -- Tamara specific columns 
        t.txn_amount::NUMERIC(18,2) AS tamara_amount,
        t.status::VARCHAR AS tamara_status,
        t.txn_created_at_gmt_03_00::DATE AS tamara_transaction_date,
        t.payment_online_transaction_id::VARCHAR AS tamara_payment_id,
        t.order_id::INTEGER AS tamara_portal_order_id,
        t.tamara_txn_reference::VARCHAR AS tamara_txn_reference,
        t.payment_method::VARCHAR AS tamara_payment_method,
        t.customer_name::VARCHAR AS tamara_customer_name,
        t.txn_type::VARCHAR AS tamara_txn_type,
        t.txn_settlement_status::VARCHAR AS tamara_txn_settlement_status,
        t.order_currency::VARCHAR AS tamara_order_currency,
        t.country_code::VARCHAR AS tamara_country_code,
        t.store_name::VARCHAR AS tamara_store_name,
        t.total_amount::NUMERIC(18,2) AS tamara_total_amount,
        t.order_created_at_gmt_03_00::DATE AS tamara_order_created_at
    FROM tamara t
    -- Join with metabase to get to portal
    JOIN metabase m ON t.payment_online_transaction_id = m.gateway_transaction_id
    -- Join with portal
    JOIN portal p ON m.portal_order_id = p.order_id
),
ranked_transactions AS (
    SELECT
        ga.*,
        (MAX(CASE WHEN ga.is_auth THEN 1 ELSE 0 END) 
            OVER (PARTITION BY ga.gateway_order_id, ga.gateway_transaction_id))::INTEGER AS has_auth_in_group,
        (MAX(CASE WHEN ga.is_capture THEN 1 ELSE 0 END) 
            OVER (PARTITION BY ga.gateway_order_id, ga.gateway_transaction_id))::INTEGER AS has_capture_in_group,
        (MAX(CASE WHEN ga.is_auth THEN ga.rrn END) 
            OVER (PARTITION BY ga.gateway_order_id, ga.gateway_transaction_id))::VARCHAR AS auth_rrn_in_group,
        (MAX(CASE WHEN ga.is_auth THEN ga.authorization_code END) 
            OVER (PARTITION BY ga.gateway_order_id, ga.gateway_transaction_id))::VARCHAR AS auth_authorization_code_in_group
    FROM gateway_analysis ga
),
transaction_with_analysis AS (
SELECT
        rt.*,
        
        -- Final RRN after potential merge
        CASE 
            WHEN rt.is_capture AND rt.has_auth_in_group = 1 AND rt.has_capture_in_group = 1 THEN rt.auth_rrn_in_group 
            ELSE rt.rrn 
        END::VARCHAR AS final_rrn,
        
        -- Final authorization_code after potential merge
        CASE 
            WHEN rt.is_capture AND rt.has_auth_in_group = 1 AND rt.has_capture_in_group = 1 THEN rt.auth_authorization_code_in_group 
            ELSE rt.authorization_code 
        END::VARCHAR AS final_authorization_code,
    
        -- Transaction Analysis based on flow and response codes
        CASE
            -- REFUND Check
            WHEN rt.is_refund THEN
                CASE
                    WHEN rt.response_code = '10000' OR UPPER(rt.status) IN ('FULLY_REFUNDED', 'PARTIALLY_REFUNDED') THEN 'Refund Successful'
                    ELSE 'Refund Failed/Pending'
                END
            -- VOID/CANCEL Check
            WHEN rt.is_void OR UPPER(rt.status) = 'CANCEL' THEN
                CASE
                    WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'CANCELED' THEN 'Void Successful'
                    ELSE 'Void Failed/Pending'
                END
            -- CAPTURE Check
            WHEN rt.is_capture THEN
                CASE
                    -- Check if this is a Capture without an Authorisation
                    WHEN rt.has_auth_in_group = 0 THEN 
                        CASE
                            WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Capture Successful (auth_less)'
                            ELSE 'Capture Failed/Pending (auth_less)'
                        END
                    -- Normal Capture with Authorisation
                    WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 
                        CASE 
                            WHEN rt.has_auth_in_group = 1 THEN 'Capture Successful (Authorisation Merged)' -- Merged case
                            ELSE 'Capture Successful' -- Standard capture
                        END
                    ELSE 'Capture Failed/Pending (Check Response: ' || rt.response_code || ')'
                END
            -- AUTHORISATION Check
            WHEN rt.is_auth THEN
                CASE
                    WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Authorisation Successful'
                    ELSE 'Authorisation Failed (Check Response: ' || rt.response_code || ')'
                END
            -- Fallback checks
            WHEN rt.response_code = '10000' THEN 'General Success (Code 10000)'
            WHEN UPPER(rt.status) = 'APPROVED' THEN 'General Success (Approved)'
            WHEN rt.response_code != '10000' AND rt.response_code IS NOT NULL AND rt.response_code != '' THEN 'General Failure (Code: ' || rt.response_code || ')'
            WHEN UPPER(rt.status) IN ('DECLINED', 'FAILED') THEN 'General Failure (Declined/Failed)'
            ELSE 'Unknown/Incomplete Status'
        END::VARCHAR AS transaction_analysis,
    
        -- Transaction Outcome (Success/Failure)
        CASE 
            WHEN (CASE
                    -- REFUND Check
                    WHEN rt.is_refund THEN
                        CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) IN ('FULLY_REFUNDED', 'PARTIALLY_REFUNDED') THEN 'Success' ELSE 'Failure' END
                    -- VOID/CANCEL Check
                    WHEN rt.is_void OR UPPER(rt.status) = 'CANCEL' THEN
                        CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'CANCELED' THEN 'Success' ELSE 'Failure' END
                    -- CAPTURE Check (including auth_less captures)
                    WHEN rt.is_capture THEN
                        CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END
                    -- AUTHORISATION Check (Only relevant if it wasn't filtered out by the merge logic)
                    WHEN rt.is_auth THEN
                         CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END
                    -- Fallback checks
                    WHEN rt.response_code = '10000' THEN 'Success'
                    WHEN UPPER(rt.status) = 'APPROVED' THEN 'Success'
                    ELSE 'Failure' -- Defaults to Failure if not explicitly successful
                 END) = 'Success' THEN 'Success'
            ELSE 'Failure'
        END::VARCHAR AS transaction_outcome
    
    FROM ranked_transactions rt
    WHERE 
        -- Remove original Authorisation row if it was merged into a Capture row
        NOT (rt.is_auth AND rt.has_auth_in_group = 1 AND rt.has_capture_in_group = 1) 
        -- Filter for successful transactions only
        AND (CASE 
                WHEN (CASE
                        -- REFUND Check
                        WHEN rt.is_refund THEN
                            CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) IN ('FULLY_REFUNDED', 'PARTIALLY_REFUNDED') THEN 'Success' ELSE 'Failure' END
                        -- VOID/CANCEL Check
                        WHEN rt.is_void OR UPPER(rt.status) = 'CANCEL' THEN
                            CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'CANCELED' THEN 'Success' ELSE 'Failure' END
                        -- CAPTURE Check (including auth_less captures)
                        WHEN rt.is_capture THEN
                            CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END
                        -- AUTHORISATION Check (Only relevant if it wasn't filtered out by the merge logic)
                        WHEN rt.is_auth THEN
                             CASE WHEN rt.response_code = '10000' OR UPPER(rt.status) = 'APPROVED' THEN 'Success' ELSE 'Failure' END
                        -- Fallback checks
                        WHEN rt.response_code = '10000' THEN 'Success'
                        WHEN UPPER(rt.status) = 'APPROVED' THEN 'Success'
                        ELSE 'Failure' -- Defaults to Failure if not explicitly successful
                     END) = 'Success' THEN 'Success'
                ELSE 'Failure'
            END) = 'Success' -- Apply filter based on the derived outcome
)

-- Final query with bank table matching
SELECT 
    ta.*,
    -- Bank table columns from generated_config
    b.authorization_code::VARCHAR AS bank_auth_code,
    b.rrn::VARCHAR AS bank_rrn,
    b.transaction_amount::NUMERIC(18,2) AS bank_amount,
    b.transaction_date::DATE AS bank_transaction_date,
    b.transaction_type::VARCHAR AS bank_transaction_type,
    b.status::VARCHAR AS bank_status,
    b.bank_name::VARCHAR AS bank_bank_name,
    b.card_type::VARCHAR AS bank_card_type,
    b.cashback_amount::NUMERIC(18,2) AS bank_cashback_amount,
    b.discount_amount::NUMERIC(18,2) AS bank_discount_amount,
    b.masked_card::VARCHAR AS bank_masked_card,
    b.merchant_identifier::VARCHAR AS bank_merchant_identifier,
    b.payment_online_transaction_id::VARCHAR AS bank_payment_id,
    b.posting_date::DATE AS bank_posting_date,
    b.terminal_identifier::VARCHAR AS bank_terminal_identifier, 
    b.total_payment_amount::NUMERIC(18,2) AS bank_total_payment_amount,
    b.transaction_link_url::VARCHAR AS bank_transaction_link_url,
    b.vat_amount::NUMERIC(18,2) AS bank_vat_amount,
    
    -- Match type determination with proper fallback handling
    CASE 
        -- Primary matches (strongest)
        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code AND ta.final_rrn = b.rrn 
             AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01
            THEN 'MATCH: Authorization Code + RRN + Amount'
        
        -- Secondary matches (strong)
        WHEN b.rrn IS NOT NULL AND ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01 
            THEN 'MATCH: RRN + Amount'
        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01 
            THEN 'MATCH: Authorization Code + Amount'
        
        -- Fallback match with date and amount validation
        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code 
             AND (b.transaction_date::DATE = ta.transaction_date::DATE)
             AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01
            THEN 'FALLBACK MATCH: Authorization Code + Same Day + Amount'
        
        -- Partial matches (weaker, only when exact amount match fails)
        WHEN b.rrn IS NOT NULL AND ta.final_rrn = b.rrn 
             AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0
            THEN 'PARTIAL MATCH: RRN + Approx Amount'
        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code 
             AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0
            THEN 'PARTIAL MATCH: Authorization Code + Approx Amount'
        WHEN b.authorization_code IS NOT NULL AND ta.final_authorization_code = b.authorization_code 
             AND (b.transaction_date::DATE = ta.transaction_date::DATE)
             AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0
            THEN 'PARTIAL MATCH: Authorization Code + Same Day + Approx Amount'
        
        -- No matches
        ELSE NULL
    END::VARCHAR AS bank_match_type

FROM transaction_with_analysis ta
LEFT JOIN bank b ON
    (
        -- Primary match criteria (authorization_code + RRN + amount)
        (ta.final_authorization_code = b.authorization_code AND ta.final_rrn = b.rrn
         AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01)
        
        -- Secondary match criteria (RRN + amount)
        OR (ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01)
        
        -- Tertiary match criteria (authorization_code + amount)
        OR (ta.final_authorization_code = b.authorization_code AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01)
        
        -- Fallback criteria: Authorization code + same day + amount
        OR (ta.final_authorization_code = b.authorization_code 
            AND (b.transaction_date::DATE = ta.transaction_date::DATE)
            AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 0.01)
            
        -- Partial matches with more tolerance for amount differences (up to 1.0 unit difference)
        OR (ta.final_rrn = b.rrn AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0)
        OR (ta.final_authorization_code = b.authorization_code AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0)
        OR (ta.final_authorization_code = b.authorization_code 
            AND (b.transaction_date::DATE = ta.transaction_date::DATE)
            AND ABS(ta.amount::NUMERIC(18,2) - b.transaction_amount::NUMERIC(18,2)) <= 1.0)
    );

-- Query for getting all rows from the view
SELECT * FROM payment_analysis_view;
