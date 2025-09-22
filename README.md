# Healix - Healthcare Data Analysis

A healthcare claims and policy analysis pipeline that processes patient data, insurance policies, and performs comprehensive exploratory data analysis.

## Quick Start

### Prerequisites
- Python 3.7+
- Jupyter Notebook

### Required Packages
```bash
pip install pandas sqlite3 matplotlib seaborn numpy jupyter
```

### Run the Analysis

1. **Exploratory Data Analysis** - Initial data exploration and insights:
   ```bash
   jupyter notebook notebooks-01/eda.ipynb
   ```

2. **Data Cleaning** - Clean and scrub PHA data:
   ```bash
   jupyter notebook notebooks-01/pha_scrubber.ipynb
   ```

3. **Data Ingestion** - Load healthcare data into SQLite database:
   ```bash
   jupyter notebook notebooks-01/db_ingest.ipynb
   ```
   Run all cells to process `healthcare_dataset.csv` into `db/claims_db.sqlite`

4. **Policy Data** - Load insurance policy data:
   ```bash
   jupyter notebook notebooks-01/db_ingest_policies.ipynb
   ```

5. **Combine Datasets** - Merge claims with policy rules:
   ```bash
   python notebooks-01/create_combined_dataset.py
   ```

## Outputs

- **Database**: `db/claims_db.sqlite` - SQLite database with claims and policy tables
- **Combined Data**: `outputs/claims_with_policy_rules.csv` - Merged dataset
- **Analysis Results**: `outputs/eda/` - Charts, statistics, and analysis reports
- **Cleaned Data**: `outputs/cleaned/` - Processed and cleaned datasets

## Key Features

- Healthcare claims data processing and analysis
- Insurance policy rule integration
- Comprehensive exploratory data analysis with visualizations
- Data quality assessment and cleaning
- SQLite database with indexed tables for efficient querying

---

*Follow the steps in order (1-5) to complete the full analysis pipeline from initial exploration to final combined dataset.*