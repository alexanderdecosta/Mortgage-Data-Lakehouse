import duckdb
import os

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENRICHED_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "enriched_performance.parquet")
FINAL_GOLD_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "purified_gold24K.parquet")

con = duckdb.connect()

print(" Executing Final Gold Polish...")
print("-> Normalizing Seller/Servicer strings")
print("-> Cleaning MSA & ZIP3 geography")
print("-> Dropping Zero-Variance features (AMORT, PPM, IO)")
print("-> Decommissioning legacy ELTV for Enriched CLTV")

con.execute(f"""
    COPY (
        SELECT 
            * EXCLUDE (
                seller_name, servicer_name, msa_code, zip_3, 
                amort_type, ppm_flag, io_indicator, eltv
            ),
            UPPER(seller_name) as seller_name,
            UPPER(servicer_name) as servicer_name,
            NULLIF(msa_code, '00000') as msa_code,
            LEFT(zip_3, 3) as zip_3
        FROM read_parquet('{ENRICHED_PATH}')
    ) TO '{FINAL_GOLD_PATH}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
""")

print(f" Polish Complete. Final dataset saved to: {FINAL_GOLD_PATH}")
con.close()