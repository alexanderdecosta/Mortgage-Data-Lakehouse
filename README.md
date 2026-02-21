# GSE Data Integration Engine (Freddie Mac & Fannie Mae)

## Project Overview
The core achievement of this pipeline is the resolution of the **"Fannie Mae Null Gap"** for the 2021–2025 period. Utilizing a custom **Mark-to-Market (MtM) enrichment engine**, the pipeline backfilled critical missing valuation data, ensuring continuity in loan-to-value analysis across the dataset. 

This high-performance data engineering platform ingests, reconciles, and standardizes massive-scale mortgage performance data from **Freddie Mac** and **Fannie Mae** using a **Medallion Architecture** to transform siloed text files into a unified, query-ready data lake.

---

## Architectural Features

### 1. Out-of-Core Ingestion & Enrichment
* **Time Horizon:** Processed 630M+ performance records from **2021 Q1 through 2025 Q3**.
* **Storage Efficiency:** Designed to handle 10GB+ of raw data on a standard laptop by utilizing **DuckDB**'s vectorized execution engine.
* **Vectorized HPI Joins:** Executes complex time-series joins against FHFA House Price Indices at the MSA level. This engineered a **Mark-to-Market CLTV** metric, reducing valuation null-density from **54.5% to <0.1%**.
* **Disk Management:** Optimized for high-volume processing within local storage constraints. The pipeline utilizes streaming operations to bypass RAM limitations and includes automated routines to prune intermediate artifacts.

### 2. Schema Standardization & Surgical Polish
* **Strict Type Enforcement:** Vectorized casting of `VARCHAR` strings into high-performance numeric and temporal types (e.g., `Date32`, `Decimal`).
* **Surgical Normalization:** Standardizes disparate entity naming conventions and identifiers across 60+ columns.
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

**stream_to_parquet.py:** Streams raw Agency CSVs into partitioned Silver Parquet files.

**fmhpi_to_parquet.py:** Converts raw FHFA indices into indexed Parquet format.

**goldgenerator.py:** Aggregates processed files for enrichment.

**24kgoldgenerator.py:** Performs the massive time-series join between Agency data and HPI data.

**final_polish.py:** Executes surgical cleaning on strings and geographic codes.

**audit_24k.py:** Generates the final statistical profile.

---

## Data Audit & Validation

The final gold layer was validated using `audit_24k.py`, which produces a full statistical profile across all 52 standardised columns. The results below serve as a known-good baseline — re-running the audit on a reproduced dataset should match these figures.

**Total rows: ~630.2M** (FNM: 343.5M / FRE: 286.7M)

### Numeric Distributions

| Variable | Nulls | Min | Q1 | Median | Q3 | Max | Mean |
|---|---|---|---|---|---|---|---|
| orig_int_rate | 6 | 1.5 | 2.8 | 3.1 | 4.1 | 9.8 | 3.7 |
| orig_upb | 0 | 7,000 | 176,620 | 264,225 | 382,503 | 2,326,000 | 294,129 |
| orig_term | 0 | 85 | 360 | 360 | 361 | 511 | 323.9 |
| orig_ltv | 0 | 1.0 | 58.1 | 74.3 | 80.2 | 365.0 | 70.1 |
| orig_cltv | 16 | 1.0 | 58.5 | 74.5 | 80.3 | 365.0 | 70.3 |
| borrower_count | 0 | 1 | 1 | 1 | 2 | 7 | 1.5 |
| orig_dti | 48,039 | 1.0 | 27.7 | 36.0 | 43.0 | 65.0 | 34.8 |
| credit_score | 274,122 | 300 | 725 | 765 | 791 | 850 | 754.9 |
| unit_count | 0 | 1 | 1 | 1 | 1 | 4 | 1.0 |
| mi_percent | 262,752,418 * | 0.0 | 0.0 | 0.0 | 25.0 | 55.0 | 10.6 |
| current_rate | 1,471,554 | 1.5 | 2.8 | 3.1 | 4.1 | 9.8 | 3.7 |
| current_upb | 1 | 0.0 | 161,906 | 246,793 | 361,160 | 2,326,000 | 275,474 |
| loan_age | 1,477,098 | -1 ** | 9 | 20 | 33 | 64 | 21.9 |
| months_to_mat | 1,967,716 | 1 | 306 | 332 | 348 | 537 | 299.6 |
| int_bearing_upb | 343,488,499 * | 0.0 | 164,704 | 249,317 | 364,042 | 2,298,000 | 278,362 |
| non_int_upb | 1 | 0.0 | 0.0 | 0.0 | 0.0 | 473,845 | 57.5 |
| zero_bal_upb | 627,494,605 | 0.0 | 143,876 | 229,607 | 348,198 | 2,126,462 | 261,005 |
| total_expenses | 286,744,145 | -167,459 | 0.0 | 0.0 | 0.0 | 1,358,738 | 0.1 |
| net_sales_proc | 630,229,300 | 0.0 | 102,207 | 170,864 | 281,238 | 1,080,300 | 204,354 |
| actual_loss | 286,744,141 | -1,437,647 | 0.0 | 0.0 | 0.0 | 2,074,295 | 1,109.5 |
| delinq_int | 630,232,639 | 0.0 | 4,665 | 8,416 | 15,678 | 118,573 | 11,938 |
| current_mod_cost | 628,873,086 | -345.8 | 5.1 | 17.0 | 39.1 | 1,619.8 | 33.1 |
| cumulative_mod_cost | 630,166,762 | -10,653 | 131.8 | 342.0 | 826.6 | 21,390 | 660.1 |
| credit_enhancements | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 1,091,561 | 0.3 |
| mtm_cltv | 675,849 | 0.0 | 46.4 | 60.7 | 73.8 | 370.6 | 59.5 |
| final_cltv | 675,849 | 0.0 | 45.1 | 59.3 | 73.0 | 848.0 | 58.5 |

> \* High null counts are expected by design. `mi_percent` is null for the ~41% of loans with no mortgage insurance. `int_bearing_upb` is null for all FNM rows (343.5M) as Fannie Mae does not separately disclose this field in its public dataset.
>
> \*\* ~2.4M records (0.4%) carry a negative `loan_age`, reflecting Freddie Mac reporting loans prior to their first scheduled payment date. Downstream filtering with `WHERE loan_age >= 1` is recommended for age-based analyses.

### Categorical Distributions (Key Columns)

**Agency split:**
| Agency | Rows | % |
|---|---|---|
| FNM (Fannie Mae) | 343,488,498 | 54.5% |
| FRE (Freddie Mac) | 286,746,079 | 45.5% |

**Delinquency status** — 98.8% of records are current (`00`), consistent with the low-default environment of the 2021–2025 vintage.

**Valuation method** — 67.1% appraised (`A`), 32.7% ACE appraisal waivers (`W`), reflecting the surge in waiver usage post-2020.

**Zero balance code** — 99.6% of rows are NULL (loan still active), with prepayments accounting for virtually all liquidations (0.43%), consistent with the refinance-heavy 2021–2022 period.

**`first_time_buyer`** — 2 rows contain lowercase `y` rather than `Y`. This is a known upstream data irregularity carried through intentionally; downstream queries should use `UPPER()` or `ILIKE` when filtering on this field.