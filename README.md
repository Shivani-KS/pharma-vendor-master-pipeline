# Pharmaceutical Procurement Intelligence: Vendor Master Consolidation

## 1. Project Overview

This project implements a Python-based ETL pipeline to consolidate dirty vendor master data from multiple sources.

The source datasets include:
- ERP Vendor Master Records (`erp_vendor_master.csv`)
- Manual Procurement Logs (`manual_procurement_log.csv`)

The pipeline performs data ingestion, standardization, validation, missing value imputation, deduplication, category normalization, quarantine segregation, and data loading into a MySQL database.

## 2. Technologies Used

- Python
- Pandas
- MySQL
- MySQL Connector/Python
- SQL
- Git and GitHub

## 3. Repository Structure

```text
pharma-vendor-master-pipeline/
├── config/
│   └── config.example.ini
├── data/
│   ├── raw/
│   │   ├── erp_vendor_master.csv
│   │   └── manual_procurement_log.csv
│   └── output/
├── sql/
│   ├── ddl.sql
│   ├── dml.sql
│   └── verification_queries.sql
├── src/
│   └── consolidate_vendor_master.py
├── .gitignore
├── requirements.txt
└── README.md
```

## 4. Setup and Installation

### Step 1: Clone the repository

```bash
git clone https://github.com/<your-username>/pharma-vendor-master-pipeline.git
cd pharma-vendor-master-pipeline
```

### Step 2: Create a virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure database connection

Create a local `config.ini` file inside the `config/` directory using the provided `config.example.ini` template.

Update the MySQL host, port, database, username, and password with your local database details.

Never commit actual database credentials to GitHub.

## 5. Pipeline Execution

Execute the pipeline using:

```bash
python src/consolidate_vendor_master.py
```

The pipeline processes the source CSV files, applies data quality transformations, and loads the resulting records into MySQL.

## 6. Database Verification

The SQL verification scripts are available in:

`sql/verification_queries.sql`

These queries verify:
- Golden vendor master integrity
- Category breakdown through relational joins
- Quarantined record audit details

## 7. Output

The pipeline generates:
- Cleaned vendor master CSV
- Quarantine records CSV
- MySQL vendor master tables
- Execution logs

Generated output files and sensitive configuration files are excluded from Git tracking using `.gitignore`.

## 8. Version Control

Git is used to track project changes through meaningful commits and feature branches.

The feature branch `feature/quarantine-and-dedup` is used for isolated ETL development and is merged into `main` after testing.

## 9. Git Workflow

This project uses Git and GitHub for version control and collaborative development.

### Branches

- `main` – Contains the stable project implementation.
- `feature/quarantine-and-dedup` – Used for quarantine output path improvements.
- `feature/documentation-update` – Used for project documentation updates.

### Commit History

The project follows meaningful commit messages to track changes.

Examples:
- `feat:` – Add a new feature.
- `fix:` – Fix an existing issue.
- `docs:` – Update documentation.
- `chore:` – Perform maintenance tasks.

### Pull Request Workflow

1. Create a feature branch from `main`.
2. Make the required changes.
3. Stage and commit the changes.
4. Push the feature branch to GitHub.
5. Create a Pull Request targeting `main`.
6. Review and merge the Pull Request.