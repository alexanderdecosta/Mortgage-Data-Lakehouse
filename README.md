# GSE Data Integration Engine (Freddie Mac & Fannie Mae)

## Project Overview
The core achievement of this pipeline is the resolution of the **"Fannie Mae Null Gap"** for the 2021–2025 period. Utilizing a custom **Mark-to-Market (MtM) enrichment engine**, the pipeline backfilled critical missing valuation data, ensuring continuity in loan-to-value analysis across the dataset. 

This high-performance data engineering platform ingests, reconciles, and standardizes massive-scale mortgage performance data from **Freddie Mac** and **Fannie Mae** using a **Medallion Architecture** to transform siloed text files into a unified, query-ready data lake..



## Architectural Features

### 1. Out-of-Core Ingestion & Enrichment
* **Time Horizon:** Processed 630M+ performance data from **2021 Q1 through 2025 Q3**.
* **Storage Efficiency:** Designed to handle 10GB+ of raw data on a standard laptop by utilizing **DuckDB**'s vectorized execution engine.
* **Vectorized HPI Joins:** Executes complex time-series joins against FHFA House Price Indices at the MSA level. This engineered a **Mark-to-Market CLTV** metric, reducing valuation null-density from **54.5% to <0.1%**.
* **Disk Management:** Optimized for high-volume processing within local storage constraints. The pipeline utilizes streaming operations to bypass RAM limitations and includes automated routines to prune intermediate artifacts.

### 2. Schema Standardization & Surgical Polish
* **Strict Type Enforcement:** Vectorized casting of `VARCHAR` strings into high-performance numeric and temporal types (e.g., `Date32`, `Decimal`).
* **Surgical Normalization:** Standardizes disparate entity naming (e.g., merging "Rocket Mortgage LLC" and "ROCKET MORTGAGE") and geographic identifiers across 60+ columns.
* **Sentinel Resolution:** Identifies institutional "placeholder" or sentinel values (e.g., `999`, `Unknown`, `NP`) within raw datasets and recasts them to true `NULL` to prevent statistical skew.

---

## Instructions: How to Reproduce

### 1. Data Acquisition
Obtain the raw source files from the following portals:
* **Fannie Mae:** [Single-Family Loan Performance Data](https://capitalmarkets.fanniemae.com/credit-risk-transfer/single-family-credit-risk-transfer/fannie-mae-single-family-loan-performance-data) (2021–2025 Acquisition/Performance).
* **Freddie Mac:** [Single-Family Loan-Level Dataset](https://sf.freddiemac.com/tools-learning/resources/sf-loan-level-dataset) (2021–2025 Historical).
* **FHFA:** [HPI Master Files](https://www.fhfa.gov/DataTools/Downloads/Pages/House-Price-Index-Datasets.aspx) (HPI_master.csv).

### 2. Directory Structure
```text
/
├── scripts/
│   ├── stream_to_parquet.py    # Initial Bronze ingestion (Raw to Silver)
│   ├── fmhpi_to_parquet.py     # HPI specific ingestion
│   ├── goldgenerator.py        # Merges parquets into gold
│   ├── 24kgoldgenerator.py     # HPI Enrichment & Silver-to-Gold join
│   ├── final_polish.py         # Final string & geo normalization
│   └── audit_24k.py            # Statistical profiling
├── data/
│   ├── raw/
│   │   ├── fmhpi/              # Raw HPI CSVs
│   │   ├── freddie/            # Raw Freddie Mac CSVs
│   │   └── fannie/             # Raw Fannie Mae CSVs
│   ├── silver/                 # Raw CSVs streamed into Parquet
│   └── gold/                   # Final merged parquets
```
### 3. Execution Sequence

Run the scripts in the following order to maintain data integrity and manage disk pressure:

stream_to_parquet.py: Streams raw Agency CSVs into partitioned Silver Parquet files.

fmhpi_to_parquet.py: Converts raw FHFA indices into indexed Parquet format.

goldgenerator.py: Aggregates processed files for enrichment.

24kgoldgenerator.py: Performs the massive time-series join between Agency data and HPI data.

final_polish.py: Executes surgical cleaning on strings and geographic codes.

audit_24k.py: Generates the final statistical profile.
