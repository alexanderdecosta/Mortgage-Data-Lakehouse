# GSE Data Integration Engine (Freddie Mac & Fannie Mae)

**Status:**  *Project In Progress* 
*Currently optimizing the unified ingestion layer and implementing Parquet-to-Parquet transformations.*

## Project Overview
This project is a high-performance data engineering platform designed to ingest, reconcile, and standardize massive-scale mortgage performance data from **Freddie Mac** and **Fannie Mae** (2015–Present). 

The primary engineering challenge is the "Variety" problem: while both agencies follow the Uniform Mortgage Data Program (UMDP), their raw data formats, delimiters, and schema naming conventions differ. This engine builds a **Medallion Architecture** to transform raw, siloed text files into a unified, query-ready data lake.



## Architectural Features

### 1. Out-of-Core Ingestion (Bronze Layer)
* **Storage Efficiency:** Designed to handle 100GB+ of raw data on a standard laptop by utilizing **DuckDB**'s vectorized execution engine.
* **Stream-to-Parquet:** Implements a streaming ingestion pipeline that converts raw pipe-delimited (Freddie) and CSV (Fannie) files directly into compressed **Apache Parquet**, bypassing memory (RAM) limitations.
* **Disk Management:** Includes automated cleanup routines to delete high-volume raw source files post-conversion, maintaining a minimal storage footprint.

### 2. Schema Standardization (Silver Layer)
* **Metadata-Driven Mapping:** Uses a unified schema dictionary to resolve naming conflicts (e.g., aligning `ORIG_DTI` vs `DTI`) and inconsistent categorical encodings.
* **Type-Safe Transformation:** Leverages **Polars LazyFrames** to perform multi-threaded type-casting, null-handling, and string-to-categorical normalization across billions of records.
* **Partitioned Storage:** Implements a columnar storage strategy partitioned by `Origination_Year` to optimize query performance for downstream analytics.

### 3. Data Integrity & Reconciliation
* **Large-Scale Deduplication:** Algorithms built to identify overlapping loan entries across GSE portfolios using composite keys (Zip Code + Original Loan Amount + Origination Date).
* **Validation Checks:** Automated schema-drift detection to ensure monthly performance updates maintain longitudinal consistency.

## 🛠️ Technical Stack
* **Engine:** [DuckDB](https://duckdb.org/) (In-process OLAP)
* **Processing:** [Polars](https://pola.rs/) (Multi-threaded DataFrames)
* **Storage:** Apache Parquet (Columnar compression / Snappy)
* **Environment:** Python 3.x / PyArrow

---

## Engineering Roadmap
- [x] Initial pipeline architecture design.
- [x] Automated ingestion script (Bronze Layer).
- [ ] Unified Schema Mapping (Silver Layer).
- [ ] Composite key deduplication logic.
- [ ] Final optimized Gold table generation.
