import duckdb
import os
import time

# Setup Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENRICHED_PATH = os.path.join(PROJECT_ROOT, "data", "gold", "purified_gold24k.parquet")

con = duckdb.connect()

print(" Starting Full-Spectrum Data Profile (630M Rows)...")
con.execute(f"CREATE OR REPLACE VIEW gold_view AS SELECT * FROM read_parquet('{ENRICHED_PATH}')")

# Get Schema Info
cols_info = con.execute("DESCRIBE gold_view").fetchall()
numeric_cols = [row[0] for row in cols_info if row[1] in ('DOUBLE', 'FLOAT', 'BIGINT', 'INTEGER', 'HUGEINT')]
varchar_cols = [row[0] for row in cols_info if row[1] == 'VARCHAR']
total_rows = con.execute("SELECT COUNT(*) FROM gold_view").fetchone()[0]

# --- PART 1: NUMERIC PROFILING ---
print("\n PART 1: NUMERIC DISTRIBUTIONS")
print("-" * 115)
print(f"{'Variable':<25} | {'Nulls':<10} | {'Min':<8} | {'Q1':<8} | {'Median':<8} | {'Q3':<8} | {'Max':<8} | {'Mean':<8}")
print("-" * 115)

for col in numeric_cols:
    stats = con.execute(f"""
        SELECT 
            COUNT(*) - COUNT("{col}"), MIN("{col}"), 
            APPROX_QUANTILE("{col}", 0.25), APPROX_QUANTILE("{col}", 0.50), 
            APPROX_QUANTILE("{col}", 0.75), MAX("{col}"), AVG("{col}")
        FROM gold_view
    """).fetchone()
    
    print(f"{col:<25} | {stats[0]:<10,} | {stats[1]:<8.1f} | {stats[2]:<8.1f} | {stats[3]:<8.1f} | {stats[4]:<8.1f} | {stats[5]:<8.1f} | {stats[6]:<8.1f}")

# --- PART 2: CATEGORICAL PROFILING (TOP 10) ---
print("\n\n PART 2: CATEGORICAL CONCENTRATION (Top 10 Values)")
print("-" * 115)

for col in varchar_cols:
    print(f"\n[Column: {col.upper()}]")
    
    # Get Top 10 frequencies and calculate percentage on the fly
    top_10 = con.execute(f"""
        SELECT 
            "{col}" as val, 
            COUNT(*) as freq,
            ROUND(COUNT(*) * 100.0 / {total_rows}, 2) as pct
        FROM gold_view
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """).df()
    
    # Handle the 'Null' case in the display
    top_10['val'] = top_10['val'].fillna('NULL/MISSING')
    
    print(top_10.to_string(index=False, header=True))
    print("-" * 50)

con.close()