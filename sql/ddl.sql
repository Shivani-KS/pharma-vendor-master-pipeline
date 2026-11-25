
-- ============================================================
-- DDL FILE: Vendor Master Project
-- Database: pharmaceutical_procurement
-- Purpose: Create tables if they do not already exist
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_vendor_master (
    vendor_id VARCHAR(20) PRIMARY KEY,
    vendor_name VARCHAR(255) NOT NULL,
    country VARCHAR(100),
    onboarded_date DATE,
    lead_time_days INT NOT NULL,
    is_lead_time_imputed BOOLEAN NOT NULL,
    contract_value_usd DECIMAL(15,2) NOT NULL,
    source_system VARCHAR(50),
    last_modified DATETIME,
    processed_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_vendor_categories (
    vendor_id VARCHAR(20) NOT NULL,
    material_category VARCHAR(100) NOT NULL,

    PRIMARY KEY (vendor_id, material_category),

    CONSTRAINT fk_vendor_categories
        FOREIGN KEY (vendor_id)
        REFERENCES gold_vendor_master(vendor_id)
);

CREATE TABLE IF NOT EXISTS err_vendor_quarantine (
    quarantine_id INT AUTO_INCREMENT PRIMARY KEY,
    source_record_key VARCHAR(255) NOT NULL UNIQUE,
    vendor_id VARCHAR(20),
    vendor_name VARCHAR(255),
    country VARCHAR(100),
    onboarded_date DATE,
    lead_time_days INT,
    contract_value_usd VARCHAR(100),
    material_categories TEXT,
    source_system VARCHAR(50),
    last_modified DATETIME,
    rejection_reason VARCHAR(100) NOT NULL,
    processed_at DATETIME NOT NULL
);
