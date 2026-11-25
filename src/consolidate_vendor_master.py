
#####################################################################
# Script Name  : consolidate_vendor_master.py
#
# Objective    : Create a data wrangling pipeline for the given tables
#               (ERP vendor, Manual Procurement)
#
# Created by   : K S Jai Shivani
# Created date : 23-11-2026
#
# Modified date: 24-11-2026
# Modified by  :K S Jai Shivani
#
# Comments     : 
#####################################################################

"""
STEP 1: Read the external configuration from config.ini and Establish database connection
STEP 2:  Read ERP vendor CSV file, and Manual Procurement CSV file, 
        assign source system for both and combine both datasets into one dataframe.
STEP 3: Transform and validate the data and insert it into MySQL 
STEP 4: Export invalid records and create a quarantine_records csv file.
STEP 5: Verify the database records, handle errors and close the connection.
"""

#---------------------------
# IMPORT REQUIRED LIBRARIES
#---------------------------

import pandas as pd
import numpy as np
import mysql.connector
import configparser
import logging

from datetime import datetime
from pathlib import Path

import hashlib
import re

#----------------------------------
# PIPELINE CONFIGURATION
#----------------------------------

# Get the current script directory (src)
SCRIPT_DIR = Path(__file__).resolve().parent

# Get the project root directory
BASE_DIR = SCRIPT_DIR.parent

# Input CSV file paths
ERP_FILE_PATH = BASE_DIR / "data" / "raw" / "erp_vendor_master.csv"

MANUAL_FILE_PATH = BASE_DIR / "data" / "raw" / "manual_procurement_log.csv"

# External configuration file
CONFIG_FILE_PATH = BASE_DIR / "config" / "config.ini"

# SQL file directory
SQL_DIR = BASE_DIR / "sql"

# Target database name
DB_NAME = "pharmaceutical_procurement"

#---------------------------
# CONFIGURE LOGGING
#---------------------------

def setup_logging():
    """
    Create a new timestamped log file for every execution.
    """
    # Create Logs directory
    LOG_DIR = BASE_DIR / "Logs"
    LOG_DIR.mkdir(exist_ok=True)

    # Get script name
    script_name = Path(__file__).stem

    # Capture execution start time
    start_time = datetime.now()

    # Generate unique log filename
    log_file = LOG_DIR / (f"{script_name}_{start_time.strftime('%Y%m%d_%H%M%S_%f')}.log")

    # Configure logging
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filemode="w",
        force=True
    )

    # Log script start
    logging.info("=" * 80)
    logging.info(f"{script_name}.py started at "f"{start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    logging.info("=" * 80)

    print("Log file:", log_file)
    return start_time, log_file

#--------------------------------
# DEFINE REUSABLE LOG FUNCTION
#--------------------------------

def write_log(message, level="info"):
    """
    Write messages into the execution log.
    """
    log_function = getattr(
        logging,
        level.lower(),
        logging.info
    )

    log_function(message)

# Initialize logging
start_time, log_file = setup_logging()

#---------------------------------------
# STEP 1: READ EXTERNAL CONFIGURATION
#---------------------------------------

def read_configuration():
    """
    Read and validate MySQL connection parameters
    from the external config.ini file.
    """

    config = configparser.ConfigParser(interpolation=None)

    # Check configuration file
    if not config.read(CONFIG_FILE_PATH):
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_FILE_PATH}")

    # Validate DATABASE section
    if not config.has_section("DATABASE"):
        raise ValueError("DATABASE section missing in config.ini")

    # Read MySQL connection parameters
    db_config = {
        "host": config.get("DATABASE", "host"),
        "port": config.getint("DATABASE", "port"),
        "database": config.get("DATABASE", "database"),
        "user": config.get("DATABASE", "user"),
        "password": config.get("DATABASE", "password")
    }

    return db_config


#--------------------------
# STEP 2: Read file paths
#--------------------------

def read_sql_file_paths():

    config = configparser.ConfigParser(interpolation=None)
    config_file = BASE_DIR / "config"/"config.ini"

    if not config.read(config_file):
        raise FileNotFoundError("config.ini file not found.")

    if not config.has_section("FILES"):
        raise ValueError("FILES section missing in config.ini.")

    ddl_file_path = BASE_DIR / "sql" /config["FILES"]["ddl_file"]
    dml_file_path = BASE_DIR / "sql" /config["FILES"]["dml_file"]
    verification_file_path = BASE_DIR / "sql" /config["FILES"]["verification_file"]

    if not ddl_file_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_file_path}")

    if not dml_file_path.exists():
        raise FileNotFoundError(f"DML file not found: {dml_file_path}")

    if not verification_file_path.exists():
        raise FileNotFoundError(
            f"Verification SQL file not found: {verification_file_path}"
        )

    return ddl_file_path, dml_file_path, verification_file_path



#---------------------------------
# STEP 3: DATABASE CONNECTION
#---------------------------------

def create_database_connection(db_config):

    try:
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            write_log("Successfully connected to MySQL database.")

            print("Successfully connected to MySQL!")
            print("Database:", connection.database)

            return connection

    except mysql.connector.Error as error:
        write_log(f"MySQL connection failed: {error}","error")

        print("Database connection failed:", error)
        raise

# COUNTRY MAPPING

COUNTRY_MAPPING = {

    "INDIA": "India",
    "india": "India",
    "USA": "USA",
    "United States": "USA",
    "germany": "Germany",
    "uk": "UK",
    "UK": "UK"

}

#------------------------------------------------------
# STEP 4: DATA INGESTION AND SOURCE LINEAGE TAGGING
#------------------------------------------------------

def load_raw_sources(erp_path, manual_path):
    """
    Load both source CSV files, assign source system
    identifiers and combine the datasets.
    """
    # Verify ERP file
    if not erp_path.exists():
        raise FileNotFoundError(f"ERP file not found: {erp_path}")

    # Verify Manual file
    if not manual_path.exists():
        raise FileNotFoundError(f"Manual file not found: {manual_path}")

    # Read ERP CSV file
    erp_df = pd.read_csv(erp_path)

    write_log(f"ERP records loaded successfully: {len(erp_df)}")

    # Assign source system
    erp_df["source_system"] = "ERP"

    # Read Manual Procurement CSV file
    manual_df = pd.read_csv(manual_path)

    write_log(f"Manual records loaded successfully: {len(manual_df)}")

    # Assign source system
    manual_df["source_system"] = "MANUAL_LOG"

    # Combine both DataFrames
    combined_df = pd.concat(
        [erp_df, manual_df],
        ignore_index=True
    )

    write_log(f"Combined records: {len(combined_df)}")
    write_log(f"Combined columns: {len(combined_df.columns)}")

    # Display raw combined vendor master table
    print("\n" + "=" * 80)
    print("RAW COMBINED VENDOR MASTER DATA")
    print("=" * 80)

    print(combined_df.to_string(index=False))

    print("\nTotal combined records:", len(combined_df))
    print("=" * 80)

    return combined_df

#------------------------------
# STEP 5: EXECUTE DDL FILE
#------------------------------

def execute_ddl(connection, ddl_file_path):
    """
    Read and execute SQL statements from the DDL file.
    Create tables only if they do not already exist.
    """

    cursor = None

    try:
        # Read SQL file
        with open(ddl_file_path, "r", encoding="utf-8") as file:
            ddl_script = file.read()

        # Separate SQL statements
        sql_statements = ddl_script.split(";")

        # Create cursor
        cursor = connection.cursor()

        # Execute each statement
        for statement in sql_statements:
            statement = statement.strip()

            if statement:
                cursor.execute(statement)

        # Commit changes
        connection.commit()

        print("All tables created or already exist.")

    except Exception as error:
        connection.rollback()
        write_log(f"DDL execution failed: {error}", "error")
        raise

    finally:
        if cursor is not None:
            cursor.close()

#---------------------------------------------
# STEP 6: STRING AND COUNTRY STANDARDIZATION
#---------------------------------------------

def standardize_strings_and_countries(df):

    """
    Clean whitespace and standardize vendor IDs,
    vendor names and country names.
    """

    # Create a copy of the original DataFrame
    df = df.copy()

    # Identify all textual columns
    text_columns = df.select_dtypes(
        include=["object", "string"]
    ).columns

    # Remove leading and trailing whitespace
    for col in text_columns:
        df[col] = df[col].str.strip()

    write_log("Whitespace removal completed.")

    # Convert vendor_id to uppercase
    df["vendor_id"] = df["vendor_id"].str.upper()

    write_log("Vendor IDs converted to uppercase.")

    # Convert vendor_name to Title Case
    df["vendor_name"] = df["vendor_name"].str.title()

    write_log("Vendor names converted to Title Case.")

    # Standardize country names using mapping dictionary
    df["country"] = (
        df["country"]
        .map(COUNTRY_MAPPING)
        .fillna(df["country"])
    )

    write_log("Country standardization completed.")

    return df

#------------------------------------
# STEP 7: NUMERIC AND DATE PARSING
#------------------------------------

def parse_types_and_dates(df):

    """
    Clean currency values, convert numeric data types
    and standardize date columns.
    """

    # Create a copy of the DataFrame
    df = df.copy()

    # Preserve original contract values for quarantine records
    df["contract_value_usd_raw"] = (
        df["contract_value_usd"].astype("string")
    )

    # Clean contract values
    df["contract_value_usd"] = (
        df["contract_value_usd"]
        .astype("string")
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace(r"\s*USD$", "", regex=True)
        .str.strip()
    )

    write_log("Currency symbols, commas and USD removed.")

    # Convert contract values into numeric format
    df["contract_value_usd"] = pd.to_numeric(
        df["contract_value_usd"],
        errors="coerce"
    )

    write_log("Contract values converted to numeric format.")

    # Parse mixed onboarded date formats
    df["onboarded_date"] = pd.to_datetime(
        df["onboarded_date"],
        format="mixed",
        dayfirst=True,
        errors="coerce"
    )

    write_log("Onboarded dates converted to datetime format.")

    # Parse last modified timestamps
    df["last_modified"] = pd.to_datetime(
        df["last_modified"],
        errors="coerce"
    )

    write_log("Last modified timestamps converted successfully.")

    return df

#-------------------------------------
# STEP 8: QUARANTINE INVALID RECORDS
#-------------------------------------

def split_quarantine_records(df):

    df = df.copy()

    # Check 1: Missing or blank vendor name
    missing_vendor_name = (
        df["vendor_name"].isna()
        | df["vendor_name"].astype("string").str.strip().eq("").fillna(False)
    )

    # Check 2: Invalid contract value
    invalid_contract_value = df["contract_value_usd"].isna()

    # Check 3: Negative lead time
    negative_lead_time = (
        pd.to_numeric(df["lead_time_days"], errors="coerce") < 0
    )

    # Assign rejection reasons
    conditions = [
        missing_vendor_name,
        invalid_contract_value,
        negative_lead_time
    ]

    rejection_reasons = [
        "MISSING_VENDOR_NAME",
        "INVALID_CONTRACT_VALUE",
        "NEGATIVE_LEAD_TIME"
    ]

    df["rejection_reason"] = np.select(
        conditions,
        rejection_reasons,
        default=""
    )

    # Separate invalid and valid records
    df_quarantine = df[df["rejection_reason"] != ""].copy()

    df_clean = df[df["rejection_reason"] == ""].copy()

    write_log(
        f"Valid records: {len(df_clean)}, "
        f"Rejected records: {len(df_quarantine)}"
    )


     # Display clean records
    print("\n" + "=" * 80)
    print("CLEAN VENDOR MASTER DATA")
    print("=" * 80)

    print(df_clean.to_string(index=False))

    print("\nTotal clean records:", len(df_clean))
    print("=" * 80)

    return df_clean, df_quarantine

#--------------------------------------------------
# STEP 9: EXPORT QUARANTINE RECORDS TO CSV
#--------------------------------------------------

def export_quarantine_to_csv(df_quarantine):
    """
    Store only unique quarantine records.
    Existing duplicate records are also removed.
    """

    try:
        csv_file_path = BASE_DIR / "data" / "output" / "quarantine_records.csv"

        # Define columns that identify a quarantine record
        key_columns = [
            "vendor_id",
            "source_system",
            "rejection_reason"
        ]

        # Check required columns
        missing_columns = [
            col for col in key_columns
            if col not in df_quarantine.columns
        ]

        if missing_columns:
            raise ValueError(f"Missing columns for duplicate checking: {missing_columns}")

        # Read existing records if the file exists
        if csv_file_path.exists():
            existing_df = pd.read_csv(csv_file_path)

            write_log(f"Existing quarantine records: {len(existing_df)}")
        else:
            existing_df = pd.DataFrame(columns=df_quarantine.columns)

        # Combine existing and newly generated records
        combined_df = pd.concat(
            [existing_df, df_quarantine],
            ignore_index=True
        )

        # Normalize duplicate-checking columns
        for col in key_columns:
            combined_df[col] = (
                combined_df[col]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.upper()
            )

        # Remove duplicates using the selected key columns
        before_count = len(combined_df)

        combined_df = combined_df.drop_duplicates(
            subset=key_columns,
            keep="first"
        )

        duplicates_removed = before_count - len(combined_df)

        # Calculate new records compared with the existing file
        existing_keys = set(
            zip(
                existing_df["vendor_id"].fillna("").astype(str).str.strip().str.upper(),
                existing_df["source_system"].fillna("").astype(str).str.strip().str.upper(),
                existing_df["rejection_reason"].fillna("").astype(str).str.strip().str.upper()
            )
        ) if not existing_df.empty else set()

        new_keys = set(
            zip(
                combined_df["vendor_id"],
                combined_df["source_system"],
                combined_df["rejection_reason"]
            )
        )

        new_records = len(new_keys - existing_keys)

        # Save updated records
        combined_df.to_csv(
            csv_file_path,
            index=False,
            encoding="utf-8"
        )

        write_log(f"Duplicates removed: {duplicates_removed}")
        write_log(f"New unique records added: {new_records}")
        write_log(f"Total unique quarantine records: {len(combined_df)}")

        print("\nQUARANTINE CSV EXPORT")
        print("------------------------------")
        print("Duplicates removed:", duplicates_removed)
        print("New unique records added:", new_records)
        print("Total unique records:", len(combined_df))
        print("CSV file:", csv_file_path)

        return csv_file_path

    except Exception as error:
        write_log(f"Quarantine CSV export failed: {error}", "error")
        raise


#--------------------------------------------------------
# STEP 10: MISSING VALUE IMPUTATION AND DEDUPLICATION
#--------------------------------------------------------

def impute_and_deduplicate(df_clean):

    """
    Impute missing lead times using the median and
    retain the latest record for each vendor.
    """

    write_log("Missing value imputation started.")

    df = df_clean.copy()

    # Convert lead_time_days to numeric
    df["lead_time_days"] = pd.to_numeric(
        df["lead_time_days"],
        errors="coerce"
    )

    # Calculate median lead time
    median_lead_time = round(
        df["lead_time_days"].median()
    )

    write_log(f"Median lead time calculated: {median_lead_time}")

    # Identify records with missing lead times
    df["is_lead_time_imputed"] = (
        df["lead_time_days"].isna()
    )

    # Replace missing lead times with median
    df["lead_time_days"] = df["lead_time_days"].fillna(
        median_lead_time
    )

    write_log(f"Missing lead times imputed using median: {median_lead_time}")

    # Sort records by last_modified in ascending order
    df = df.sort_values(
        by="last_modified",
        ascending=True
    )

    # Retain the latest record for each vendor
    df = df.drop_duplicates(
        subset=["vendor_id"],
        keep="last"
    )

    write_log(f"Remaining vendors: {len(df)}")

    return df

#-----------------------------------
# STEP 11: CATEGORY NORMALIZATION
#-----------------------------------

def build_normalized_category_bridge(df_clean):

    """
    Split comma-separated material categories and
    generate normalized vendor-category records.
    """

    # Create a copy with required columns
    df = df_clean[[
        "vendor_id",
        "material_categories"
    ]].copy()

    # Split comma-separated categories into lists
    df["material_category"] = (
        df["material_categories"]
        .fillna("")
        .str.split(",")
    )

    write_log("Material categories split into lists.")

    # Expand lists into individual rows
    df = df.explode("material_category")

    write_log("Material categories exploded into individual rows.")

    # Remove leading and trailing whitespace
    df["material_category"] = (
        df["material_category"].str.strip()
    )

    # Remove empty or missing categories
    df = df[
        df["material_category"].notna()
        & df["material_category"].ne("")
    ]

    # Retain only bridge table columns
    df_bridge = df[[
        "vendor_id",
        "material_category"
    ]].reset_index(drop=True)

    write_log(f"Total category records: {len(df_bridge)}")

    return df_bridge


#----------------------------
# STEP 12: EXECUTE DML FILE
#----------------------------

def convert_to_mysql_value(value):

    """
    Convert Pandas and NumPy values into MySQL-compatible values.
    """

    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(value, np.generic):
        return value.item()

    return value

#------------------------------

def execute_dml(connection, dml_file_path,
                df_master, df_bridge, df_quarantine):

    """
    Read parameterized INSERT statements from the DML file
    and insert processed records into MySQL tables.
    """
    cursor = None

    try:

        # STEP 12A: Read DML file
        with open(dml_file_path, "r", encoding="utf-8") as file:
            dml_script = file.read()

        # STEP 12B: Extract all INSERT statements
        sql_statements = [
            statement.strip()
            for statement in dml_script.split(";")
            if re.search(
                r"\bINSERT\s+(?:IGNORE\s+)?INTO\b",
                statement,
                flags=re.IGNORECASE
            )
        ]

        if len(sql_statements) != 3:
            raise ValueError(
                "DML file must contain exactly 3 INSERT statements."
            )

        write_log("Three DML statements identified successfully.")

        # STEP 12C: Create database cursor
        cursor = connection.cursor()

        # Capture processing timestamp
        processed_at = datetime.now()


        # STEP 12D: PREPARE VENDOR MASTER RECORDS
        
        master_df = df_master.copy()
        master_df["processed_at"] = processed_at

        master_columns = [
            "vendor_id",
            "vendor_name",
            "country",
            "onboarded_date",
            "lead_time_days",
            "is_lead_time_imputed",
            "contract_value_usd",
            "source_system",
            "last_modified",
            "processed_at"
        ]

        master_data = [
            tuple(
                convert_to_mysql_value(value)
                for value in row
            )
            for row in master_df[master_columns].itertuples(
                index=False,
                name=None
            )
        ]

        write_log(
            f"Vendor master records prepared: {len(master_data)}"
        )

        # STEP 12E: PREPARE CATEGORY RECORDS

        category_data = [
            tuple(
                convert_to_mysql_value(value)
                for value in row
            )
            for row in df_bridge[
                ["vendor_id", "material_category"]
            ].itertuples(
                index=False,
                name=None
            )
        ]

        write_log(
            f"Category records prepared: {len(category_data)}"
        )

        # STEP 12F: PREPARE QUARANTINE RECORDS

        quarantine_data = []

        for index, row in df_quarantine.iterrows():

            # Generate a stable key using source system
            # and original combined DataFrame row index
            source_key = (
                f"{row['source_system']}|{index}"
            )

            source_record_key = hashlib.sha256(
                source_key.encode("utf-8")
            ).hexdigest()

            quarantine_values = (
                source_record_key,
                row["vendor_id"],
                row["vendor_name"],
                row["country"],
                row["onboarded_date"],
                row["lead_time_days"],
                row["contract_value_usd_raw"],
                row["material_categories"],
                row["source_system"],
                row["last_modified"],
                row["rejection_reason"],
                processed_at
            )

            quarantine_data.append(
                tuple(
                    convert_to_mysql_value(value)
                    for value in quarantine_values
                )
            )

        write_log(f"Quarantine records prepared: {len(quarantine_data)}")

        # STEP 12F: INSERT VENDOR MASTER RECORDS

        if master_data:
            cursor.executemany(
                sql_statements[0],
                master_data
            )

        write_log("Vendor master insertion completed.")

        # STEP 12G: INSERT CATEGORY RECORDS

        if category_data:
            cursor.executemany(
                sql_statements[1],
                category_data
            )

        write_log("Category insertion completed.")

        # STEP 12H: INSERT QUARANTINE RECORDS

        if quarantine_data:
            cursor.executemany(
                sql_statements[2],
                quarantine_data
            )

        write_log("Quarantine insertion completed.")


        # STEP 12I: COMMIT TRANSACTION

        connection.commit()


        print("\nDML EXECUTION RESULTS")
        print("---------------------")
        print("Vendor master records processed:", len(master_data))
        print("Category records processed:", len(category_data))
        print("Quarantine records processed:", len(quarantine_data))
        print("All DML operations completed successfully.")

    except Exception as error:

        connection.rollback()

        write_log(f"DML execution failed: {error}","error")

        print("DML execution failed:", error)
        raise

    finally:
        if cursor is not None:
            cursor.close()

#--------------------------------------------------
# STEP 13: READ VERIFICATION QUERIES FROM SQL FILE
#--------------------------------------------------

def read_verification_queries(verification_file_path):

    """
    Read verification queries from the external SQL file.
    """

    with open(verification_file_path, "r", encoding="utf-8") as file:
        verification_script = file.read()

    # Separate individual SQL queries
    sql_statements = [
        statement.strip()
        for statement in verification_script.split(";")
        if statement.strip()
    ]

    # Validate the number of queries
    if len(sql_statements) != 3:
        raise ValueError(
            "Verification SQL file must contain exactly 3 queries."
        )

    write_log(
        f"Verification queries loaded successfully: {len(sql_statements)}"
    )

    return sql_statements


#--------------------------------------------------
# STEP 13A: DATABASE RECORD VERIFICATION
#--------------------------------------------------

def verify_database_records(connection, verification_file_path):
    """
    Verify record counts, portfolio spend, median lead time,
    golden vendor master, category spend and quarantine records.
    """

    cursor = None

    try:
        # STEP 13A: Create cursor and read verification queries

        cursor = connection.cursor()
        sql_statements = read_verification_queries(verification_file_path)

        # STEP 13B: Verify vendor master count

        cursor.execute("""
            SELECT COUNT(*)
            FROM gold_vendor_master
        """)

        vendor_count = cursor.fetchone()[0]

        # STEP 13C: Verify category bridge count

        cursor.execute("""
            SELECT COUNT(*)
            FROM gold_vendor_categories
        """)

        category_count = cursor.fetchone()[0]

        # STEP 13D: Verify quarantine count

        cursor.execute("""
            SELECT COUNT(*)
            FROM err_vendor_quarantine
        """)

        quarantine_count = cursor.fetchone()[0]

        # STEP 13E: Calculate total portfolio spend

        cursor.execute("""
            SELECT SUM(contract_value_usd)
            FROM gold_vendor_master
        """)

        portfolio_spend = cursor.fetchone()[0]

        # STEP 13F: Calculate median lead time

        cursor.execute("""
            WITH ranked_lead_times AS (
                SELECT
                    lead_time_days,
                    ROW_NUMBER() OVER (
                        ORDER BY lead_time_days
                    ) AS row_num,
                    COUNT(*) OVER () AS total_rows
                FROM gold_vendor_master
            )
            SELECT AVG(lead_time_days)
            FROM ranked_lead_times
            WHERE row_num IN (
                FLOOR((total_rows + 1) / 2),
                FLOOR((total_rows + 2) / 2)
            )
        """)

        median_lead_time = cursor.fetchone()[0]

        # STEP 13G: VERIFY GOLDEN MASTER INTEGRITY

        cursor.execute(sql_statements[0])
        vendor_records = cursor.fetchall()

        vendor_columns = [
            column[0] for column in cursor.description
        ]

        vendor_df = pd.DataFrame(
            vendor_records,
            columns=vendor_columns
        )

        print("\nGOLDEN VENDOR MASTER RECORDS")
        print("=" * 80)
        print(vendor_df.to_string(index=False))

        write_log(
            f"Golden vendor master query completed. "
            f"Records retrieved: {len(vendor_records)}"
        )

        # STEP 13H: VERIFY CATEGORY-WISE SUPPLIER COUNT
        # AND TOTAL CATEGORY SPEND

        cursor.execute(sql_statements[1])
        category_records = cursor.fetchall()
        category_columns = [
            column[0] for column in cursor.description
        ]

        category_df = pd.DataFrame(
            category_records,
            columns=category_columns
        )

        print("\nCATEGORY-WISE SUPPLIER COUNT AND SPEND")
        print("=" * 80)
        print(category_df.to_string(index=False))

        write_log(
            f"Category verification query completed. "
            f"Categories retrieved: {len(category_records)}"
        )

        # STEP 13I: VERIFY QUARANTINED VENDOR RECORDS

        cursor.execute(sql_statements[2])
        quarantine_records = cursor.fetchall()

        quarantine_columns = [
            column[0] for column in cursor.description
        ]

        quarantine_df = pd.DataFrame(
            quarantine_records,
            columns=quarantine_columns
        )

        print("\nQUARANTINED VENDOR RECORDS")
        print("=" * 80)
        print(quarantine_df.to_string(index=False))

        write_log(
            f"Quarantine verification query completed. "
            f"Records retrieved: {len(quarantine_records)}"
        )


        # STEP 13J: DISPLAY VERIFICATION SUMMARY

        print("\nDATABASE VERIFICATION RESULTS")
        print("=" * 45)
        print("Vendor master records:", vendor_count)
        print("Category bridge records:", category_count)
        print("Quarantine records:", quarantine_count)
        print("Total portfolio spend:", portfolio_spend)
        print("Median lead time:", median_lead_time)


        # STEP 13K: VALIDATE EXPECTED RESULTS

        if (
            vendor_count == 5
            and category_count == 8
            and quarantine_count == 3
            and portfolio_spend is not None
            and round(float(portfolio_spend), 2) == 623650.50
            and median_lead_time is not None
            and float(median_lead_time) == 14.0
        ):

            print("\nAll database verification checks passed.")

        else:
            write_log(
                "Database verification failed. "
                "Actual results do not match expected results.",
                "error"
            )

            raise ValueError(
                "Database verification failed. "
                "Check record counts, portfolio spend and median lead time."
            )

        # STEP 13L: RETURN VERIFICATION RESULTS

        return {
            "vendor_count": vendor_count,
            "category_count": category_count,
            "quarantine_count": quarantine_count,
            "portfolio_spend": portfolio_spend,
            "median_lead_time": median_lead_time,
            "vendor_master": vendor_df,
            "category_summary": category_df,
            "quarantine_records": quarantine_df
        }

    except Exception as error:
        write_log(f"Database verification failed: {error}","error")
        raise

    finally:

        # STEP 13M: Close cursor

        if cursor is not None:
            cursor.close()

#--------------------------------------------------
# STEP 14: MAIN PIPELINE EXECUTION
#--------------------------------------------------

def main():

    """
    Execute the complete Vendor Master ETL pipeline
    in the order of function definitions.
    """

    # Initialize logging
    start_time, log_file = setup_logging()

    script_name = Path(__file__).stem

    connection = None
    pipeline_status = "FAILED"

    try:

        # STEP 1: Read external configuration
        write_log("STEP 1: External configuration - STARTED")

        db_config = read_configuration()

        write_log("STEP 1: External configuration - COMPLETED SUCCESSFULLY")


        # STEP 2: Read SQL file paths
        write_log("STEP 2: Read SQL file paths - STARTED")

        ddl_file_path, dml_file_path, verification_file_path = read_sql_file_paths()

        write_log("STEP 2: Read SQL file paths - COMPLETED SUCCESSFULLY")


        # STEP 3: Establish database connection
        write_log("STEP 3: Database connection - STARTED")

        connection = create_database_connection(db_config)

        write_log("STEP 3: Database connection - COMPLETED SUCCESSFULLY")


        # STEP 4: Load source files
        write_log("STEP 4: Data ingestion - STARTED")

        df_raw = load_raw_sources(
            ERP_FILE_PATH,
            MANUAL_FILE_PATH
        )

        write_log("STEP 4: Data ingestion - COMPLETED SUCCESSFULLY")


        # STEP 5: Execute DDL statements
        write_log("STEP 5: DDL execution - STARTED")

        execute_ddl(
            connection,
            ddl_file_path
        )

        write_log("STEP 5: DDL execution - COMPLETED SUCCESSFULLY")


        # STEP 6: Standardize strings and countries
        write_log("STEP 6: String and country standardization - STARTED")

        df_standardized = standardize_strings_and_countries(
            df_raw
        )

        write_log("STEP 6: String and country standardization - COMPLETED SUCCESSFULLY")


        # STEP 7: Parse numeric values and dates
        write_log("STEP 7: Numeric and date parsing - STARTED")

        df_parsed = parse_types_and_dates(
            df_standardized
        )

        write_log("STEP 7: Numeric and date parsing - COMPLETED SUCCESSFULLY")


        # STEP 8: Separate valid and rejected records
        write_log("STEP 8: Quarantine validation - STARTED")

        df_clean, df_quarantine = split_quarantine_records(
            df_parsed
        )

        write_log("STEP 8: Quarantine validation - COMPLETED SUCCESSFULLY")


        # STEP 9: Export quarantine records to CSV
        write_log("STEP 9: Quarantine CSV export - STARTED")

        quarantine_csv_path = export_quarantine_to_csv(
            df_quarantine
        )


        write_log("STEP 9: Quarantine CSV export - COMPLETED SUCCESSFULLY")
        

        # STEP 10: Impute missing values and remove duplicates
        write_log("STEP 10: Imputation and deduplication - STARTED")

        df_clean_dedup = impute_and_deduplicate(
            df_clean
        )

        write_log("STEP 10: Imputation and deduplication - COMPLETED SUCCESSFULLY")

        # STEP 11: Build normalized category bridge
        write_log("STEP 11: Category normalization - STARTED")

        df_bridge = build_normalized_category_bridge(
            df_clean_dedup
        )

        write_log("STEP 11: Category normalization - COMPLETED SUCCESSFULLY")


        # STEP 12: Execute DML and insert records into MySQL
        # convert_to_mysql_value() is called internally by execute_dml()

        write_log("STEP 12: MySQL data insertion - STARTED")

        execute_dml(
            connection,
            dml_file_path,
            df_clean_dedup,
            df_bridge,
            df_quarantine
        )

        write_log("STEP 12: MySQL data insertion - COMPLETED SUCCESSFULLY")


        # STEP 13: Verify database records
        write_log("STEP 13: Database verification - STARTED")

        verification_results = verify_database_records(connection,verification_file_path)
        
        write_log("STEP 13: Database verification - COMPLETED SUCCESSFULLY")


        # STEP 14: Mark pipeline as successful
        pipeline_status = "SUCCESS"

        print("\nVENDOR MASTER ETL PIPELINE COMPLETED SUCCESSFULLY.")

    except Exception as error:

        pipeline_status = "FAILED"

        write_log(
            f"Pipeline execution failed: {error}",
            "error"
        )

        print("\nPIPELINE EXECUTION FAILED:", error)

        raise

    finally:

        # STEP 15: Close database connection
        if connection is not None:

            try:

                if connection.is_connected():

                    connection.close()

                    write_log("Database connection closed.")

            except Exception as error:

                pipeline_status = "FAILED"

                write_log(
                    f"Database connection closing failed: {error}",
                    "error"
                )


        # STEP 16: Calculate execution duration
        end_time = datetime.now()

        execution_duration = end_time - start_time


        # STEP 17: Final execution logging
        write_log("=" * 80)

        write_log(f"Pipeline status: {pipeline_status}")

        write_log(f"Total execution duration: {execution_duration}")


        # Final log message
        if pipeline_status == "SUCCESS":

            write_log(
                f"{script_name}.py successfully completed at "
                f"{end_time.strftime('%d/%m/%Y %H:%M:%S')}"
            )

        else:

            write_log(
                f"{script_name}.py failed at "
                f"{end_time.strftime('%d/%m/%Y %H:%M:%S')}",
                "error"
            )


        # Display execution summary
        print("\nEXECUTION SUMMARY")
        print("------------------------------")

        print("Pipeline status:", pipeline_status)
        print("Started at:", start_time)
        print("Completed at:", end_time)
        print("Execution duration:", execution_duration)
        print("Log file:", log_file)


#--------------------------------------------------
# STEP 18: START PIPELINE
#--------------------------------------------------

if __name__ == "__main__":
    main()
