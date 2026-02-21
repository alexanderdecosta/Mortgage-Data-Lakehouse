import duckdb
import os

# Setup Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
GOLD_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "unified_performance.parquet")
HPI_PATH = os.path.join(PROJECT_ROOT, "data", "silver", "fmhpi.parquet")
ENRICHED_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "enriched_performance.parquet")

con = duckdb.connect()

# 1. OPTIMIZE DUCKDB SETTINGS FOR LOW DISK
# This tells DuckDB to be aggressive with memory and not preserve row order (faster)
con.execute("SET preserve_insertion_order=false")
con.execute("SET temp_directory='temp.tmp'") # Forces temp files to a specific spot

print("Step 1: Loading HPI lookup into memory...")
con.execute(f"CREATE OR REPLACE TABLE hpi_mem AS SELECT * FROM read_parquet('{HPI_PATH}')")

# 2. CREATE A VIEW (NO DISK SPACE USED)
print("Step 2: Creating virtual join keys (View)...")
con.execute(f"""
    CREATE OR REPLACE VIEW gold_prepared AS 
    SELECT 
        *, 
        date_trunc('month', first_pay_date)::DATE as j_orig_dt,
        date_trunc('month', report_period)::DATE as j_now_dt
    FROM read_parquet('{GOLD_PATH}')
""")

# 3. STREAM DIRECTLY TO PARQUET
# We bypass creating a local DuckDB table entirely. 
# We stream from the View -> Join -> Final Parquet file.
print("Step 3: Streaming Join directly to Final Parquet...")
con.execute(f"""
    COPY (
        WITH 
        h_state AS (SELECT * FROM hpi_mem WHERE GEO_Type = 'State'),
        h_msa AS (SELECT * FROM hpi_mem WHERE GEO_Type = 'CBSA')
        
        SELECT 
            g.* ,
            -- Synthetic Benchmark
            ROUND(
                (g.current_upb / (
                    (g.orig_upb / (NULLIF(g.orig_ltv, 0) / 100.0)) * COALESCE(
                        (hm_n.index_nsa / hm_o.index_nsa), 
                        (hs_n.index_nsa / hs_o.index_nsa)
                    )
                )) * 100, 2
            ) AS mtm_cltv,
            -- Operational Master (Dropping original eltv column here via exclusion)
            COALESCE(g.eltv, mtm_cltv) AS final_cltv
        FROM gold_prepared g
        LEFT JOIN h_state hs_o ON g.prop_state = hs_o.GEO_Name AND g.j_orig_dt = hs_o.hpi_date
        LEFT JOIN h_state hs_n ON g.prop_state = hs_n.GEO_Name AND g.j_now_dt = hs_n.hpi_date
        LEFT JOIN h_msa hm_o ON g.msa_code = hm_o.msa_code AND g.j_orig_dt = hm_o.hpi_date
        LEFT JOIN h_msa hm_n ON g.msa_code = hm_n.msa_code AND g.j_now_dt = hm_n.hpi_date
    ) TO '{ENRICHED_PATH}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
""")

# 4. FINAL VERIFICATION (Read from the new file)
print("\nStep 4: Verifying Final File...")
print(con.execute(f"SELECT agency, COUNT(*), AVG(final_cltv) FROM read_parquet('{ENRICHED_PATH}') GROUP BY agency").df())

con.close()
print(f"\nDone. Streamed join complete. File saved to {ENRICHED_PATH}")