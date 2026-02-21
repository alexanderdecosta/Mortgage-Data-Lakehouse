import duckdb
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "fmhpi")
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")

HPI_SOURCE = os.path.join(RAW_DIR, "fmhpi_master_file.csv")
HPI_OUTPUT = os.path.join(SILVER_DIR, "fmhpi.parquet")

os.makedirs(SILVER_DIR, exist_ok=True)

con = duckdb.connect()

print(f"Reading from: {HPI_SOURCE}")

# 2. Create Silver HPI with explicit casting
con.execute(f"""
    CREATE OR REPLACE TABLE fmhpi_silver AS
    SELECT 
        GEO_Type,
        GEO_Name,
        -- LPAD requires a VARCHAR. We cast BIGINT -> VARCHAR first.
        CASE 
            WHEN GEO_Type = 'CBSA' THEN LPAD(GEO_Code::VARCHAR, 5, '0')
            ELSE NULL 
        END AS msa_code,
        -- Building the date: cast Month to VARCHAR so LPAD can handle it
        CAST(Year || '-' || LPAD(Month::VARCHAR, 2, '0') || '-01' AS DATE) as hpi_date,
        Index_NSA::DOUBLE as index_nsa,
        Index_SA::DOUBLE as index_sa
    FROM read_csv_auto('{HPI_SOURCE}')
""")

# 3. Save to Silver
con.execute(f"COPY fmhpi_silver TO '{HPI_OUTPUT}' (FORMAT PARQUET)")

print(f"Successfully saved Silver HPI to: {HPI_OUTPUT}")
print(con.execute("SELECT GEO_Type, COUNT(*) FROM fmhpi_silver GROUP BY 1").df())