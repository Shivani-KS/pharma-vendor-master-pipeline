
-- ============================================================
-- DML FILE: Vendor Master Project
-- Purpose: Insert or update records without duplication
-- ============================================================

-- 1. GOLD VENDOR MASTER

INSERT INTO gold_vendor_master (
    vendor_id,
    vendor_name,
    country,
    onboarded_date,
    lead_time_days,
    is_lead_time_imputed,
    contract_value_usd,
    source_system,
    last_modified,
    processed_at
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    vendor_name = VALUES(vendor_name),
    country = VALUES(country),
    onboarded_date = VALUES(onboarded_date),
    lead_time_days = VALUES(lead_time_days),
    is_lead_time_imputed = VALUES(is_lead_time_imputed),
    contract_value_usd = VALUES(contract_value_usd),
    source_system = VALUES(source_system),
    last_modified = VALUES(last_modified),
    processed_at = VALUES(processed_at);


-- 2. GOLD VENDOR CATEGORIES

INSERT IGNORE INTO gold_vendor_categories (
    vendor_id,
    material_category
)
VALUES (%s, %s);


-- 3. ERROR VENDOR QUARANTINE

INSERT IGNORE INTO err_vendor_quarantine (
    source_record_key,
    vendor_id,
    vendor_name,
    country,
    onboarded_date,
    lead_time_days,
    contract_value_usd,
    material_categories,
    source_system,
    last_modified,
    rejection_reason,
    processed_at
)
VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
);
