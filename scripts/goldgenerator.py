"""
goldgenerator.py
Unified Fannie Mae / Freddie Mac performance data → gold parquet.

Pipeline:
    silver/fannie_*.parquet  ──┐
                               ├─► gold/unified_performance.parquet
    silver/freddie_*.parquet ──┘

Each agency's raw columns are normalised to a shared 52-column schema,
sentinel values are replaced with NULL, and the result is written as
a single ZSTD-compressed parquet file.
"""

import duckdb
import os

# ── Configuration ─────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(SCRIPT_DIR)
SILVER_DIR = os.path.join(BASE_PATH, "data", "silver")
GOLD_DIR   = os.path.join(BASE_PATH, "data", "gold")
OUTPUT_PATH = os.path.join(GOLD_DIR, "unified_performance.parquet")

os.makedirs(GOLD_DIR, exist_ok=True)


# ── 1. Gold schema ─────────────────────────────────────────────────────────────
# Defines the name and storage type of every output column.
# Expression dicts below must contain exactly these keys.

COLUMN_TYPES: dict[str, str] = {
    # Identifiers & dates
    "loan_id":            "VARCHAR",  "report_period":      "DATE",
    "first_pay_date":     "DATE",     "maturity_date":      "DATE",
    "zero_bal_date":      "DATE",     "ddlpi":              "DATE",
    # Origination characteristics
    "channel":            "VARCHAR",  "seller_name":        "VARCHAR",
    "servicer_name":      "VARCHAR",  "orig_int_rate":      "DOUBLE",
    "orig_upb":           "DOUBLE",   "orig_term":          "INTEGER",
    "orig_ltv":           "DOUBLE",   "orig_cltv":          "DOUBLE",
    "borrower_count":     "INTEGER",  "orig_dti":           "DOUBLE",
    "credit_score":       "INTEGER",  "first_time_buyer":   "VARCHAR",
    "loan_purpose":       "VARCHAR",  "prop_type":          "VARCHAR",
    "unit_count":         "INTEGER",  "occupancy_status":   "VARCHAR",
    "prop_state":         "VARCHAR",  "msa_code":           "VARCHAR",
    "zip_3":              "VARCHAR",  "mi_percent":         "DOUBLE",
    "amort_type":         "VARCHAR",  "ppm_flag":           "VARCHAR",
    "io_indicator":       "VARCHAR",  "special_elig_prog":  "VARCHAR",
    "valuation_method":   "VARCHAR",  "super_conf_flag":    "VARCHAR",
    # Monthly performance
    "current_rate":       "DOUBLE",   "current_upb":        "DOUBLE",
    "loan_age":           "INTEGER",  "months_to_mat":      "INTEGER",
    "delinq_status":      "VARCHAR",  "mod_flag":           "VARCHAR",
    "mi_cancel_flag":     "VARCHAR",  "borrower_assist":    "VARCHAR",
    "eltv":               "DOUBLE",   "int_bearing_upb":    "DOUBLE",
    "non_int_upb":        "DOUBLE",
    # Loss / liquidation
    "zero_bal_code":      "VARCHAR",  "zero_bal_upb":       "DOUBLE",
    "total_expenses":     "DOUBLE",   "net_sales_proc":     "DOUBLE",
    "actual_loss":        "DOUBLE",   "delinq_int":         "DOUBLE",
    "current_mod_cost":   "DOUBLE",   "cumulative_mod_cost":"DOUBLE",
    "credit_enhancements":"DOUBLE",
}


# ── 2. Sentinel configuration ──────────────────────────────────────────────────
# EXPLICIT_SENTINELS  – values documented in the Fannie / Freddie data dicts.
# IMPLICIT_SENTINELS  – values not in the docs but universally understood in
#                       GSE data (all-9s fill patterns, system -1 placeholder).

EXPLICIT_SENTINELS: dict[str, list] = {
    # Origination underwriting
    "credit_score":      [9999],   # Freddie; Fannie blank → NULL via TRY_CAST
    "orig_dti":          [999],    # Both agencies
    "orig_ltv":          [999],    # Both agencies
    "orig_cltv":         [999],    # Both agencies
    "mi_percent":        [999],    # Both agencies
    "unit_count":        [99],     # Both agencies
    "borrower_count":    [99],     # Both agencies
    "orig_term":         [999],    # No mortgage has a 999-month term
    # Monthly performance
    "eltv":              [999],    # Freddie explicit; Fannie guarded by TRY_CAST
    "loan_age":          [999],    # Seen in practice
    "months_to_mat":     [999],    # Seen in practice
    "delinq_status":     ["XX"],   # Fannie: XX = unknown
    # Flags
    "special_elig_prog": ["9", "7"],  # Both agencies: not available / not applicable
    # Geography
    "zip_3":             ["000"],  # Freddie unknown ZIP prefix
    # Freddie-specific '9' = Not Available/Not Applicable flags
    "channel":           ["9"],    # Freddie only; Fannie has no '9' for channel
    "first_time_buyer":  ["9"],    # Freddie only; Fannie uses NULL natively
    "loan_purpose":      ["9"],    # Freddie only
    "occupancy_status":  ["9", "U"],  # Freddie '9' + Fannie 'U' — both mean unknown
    "valuation_method":  ["7"],    # Freddie only: '7' = Not Available
    "mi_cancel_flag":    ["9"],    # Freddie only: '9' = Not Disclosed
}

IMPLICIT_SENTINELS: dict[str, list] = {
    # Dollar amounts: -1 is a system-generated unknown placeholder
    "orig_upb":            [-1],
    "current_upb":         [-1],
    "zero_bal_upb":        [-1],
    "total_expenses":      [-1],
    "net_sales_proc":      [-1],
    "actual_loss":         [-1],
    "current_mod_cost":    [-1],
    "cumulative_mod_cost": [-1],
    "delinq_int":          [-1],
    "int_bearing_upb":     [-1],
    "non_int_upb":         [-1],
    "credit_enhancements": [-1],
    # Rates: 0 is economically impossible; 999 used as fill in some vintages
    "orig_int_rate":       [0, 999],
    "current_rate":        [0, 999],
}


# ── 3. SQL helpers ─────────────────────────────────────────────────────────────

def _sentinel_wrap(col: str, dtype: str, expr: str) -> str:
    """
    Wrap *expr* in a CASE that maps any known sentinel value to NULL.
    DATE columns are skipped — TRY_CAST(strptime(...)) already returns NULL
    for unparseable values.
    """
    if dtype == "DATE":
        return expr

    sentinels = EXPLICIT_SENTINELS.get(col, []) + IMPLICIT_SENTINELS.get(col, [])
    if not sentinels:
        return expr

    literals = [f"'{s}'" if isinstance(s, str) else str(s) for s in sentinels]
    in_list  = ", ".join(literals)
    return f"CASE WHEN ({expr}) IN ({in_list}) THEN NULL ELSE ({expr}) END"


def _cast(col: str, dtype: str, raw: str) -> str:
    """
    Apply the correct SQL cast for *dtype*, skipping redundant wrapping when
    the raw expression already contains TRY_CAST or CASE.
    """
    already_built = raw.strip().upper().startswith(("CASE", "TRY_CAST", "("))

    if dtype == "DATE":
        if "strptime" in raw:
            return raw                              # Fannie: pre-built strptime expr
        return f"TRY_CAST(strptime(left(CAST({raw} AS VARCHAR), 6), '%Y%m') AS DATE)"
    elif dtype == "INTEGER":
        return f"TRY_CAST({raw} AS INTEGER)"
    elif dtype == "DOUBLE":
        return raw if already_built else f"TRY_CAST({raw} AS DOUBLE)"
    else:   # VARCHAR
        return raw if already_built else f"CAST({raw} AS VARCHAR)"


def build_select(exprs: dict[str, str], types: dict[str, str]) -> str:
    """
    Build a SELECT clause from *exprs* (gold_name → raw SQL fragment).
    Each column is cast to its target type then wrapped in sentinel scrubbing.
    Returns a comma-separated string ready for embedding in a SELECT statement.
    """
    parts = []
    for col, raw in exprs.items():
        dtype      = types[col]
        cast_expr  = _cast(col, dtype, raw)
        clean_expr = _sentinel_wrap(col, dtype, cast_expr)
        parts.append(f"{clean_expr} AS {col}")
    return ",\n    ".join(parts)


# ── 4. Fannie Mae column expressions ──────────────────────────────────────────
# Source: Fannie Single-Family Loan Performance Dataset glossary.
# Dates are MMYYYY in the source — rearranged to YYYYMM for strptime.
# Column positions are 1-indexed in the data dict; the parquet silver layer
# uses 0-indexed names (column01 = position 1).

def _fannie_date(col: str) -> str:
    """Rearrange Fannie MMYYYY string to YYYYMM and parse to DATE."""
    c = f"CAST({col} AS VARCHAR)"
    return f"TRY_CAST(strptime(right({c}, 4) || left({c}, 2), '%Y%m') AS DATE)"

FANNIE_EXPRS: dict[str, str] = {
    # ── Identifiers & dates ───────────────────────────────────────────────
    "loan_id":            "column001",
    "report_period":      _fannie_date("column002"),
    "first_pay_date":     _fannie_date("column014"),
    "maturity_date":      _fannie_date("column018"),
    "zero_bal_date":      _fannie_date("column044"),
    "ddlpi":              _fannie_date("column050"),

    # ── Origination characteristics ───────────────────────────────────────
    "channel":            "column003",
    "seller_name":        "column004",
    "servicer_name":      "column005",
    "orig_int_rate":      "column007",
    "orig_upb":           "column009",
    "orig_term":          "column012",
    "orig_ltv":           "column019",
    "orig_cltv":          "column020",
    "borrower_count":     "column021",
    "orig_dti":           "column022",
    "credit_score":       "column023",   # Borrower Credit Score at Origination
    "first_time_buyer":   "column025",   # Y/N/NULL — no remapping needed
    "loan_purpose":       "column026",   # C/R/P/U — pass through
    "prop_type":          "column027",
    "unit_count":         "column028",
    "occupancy_status":   "column029",   # Fannie P/S/I/U — map U (Unknown) → NULL
    "prop_state":         "column030",
    "msa_code":           "column031",
    "zip_3":              "column032",
    "mi_percent":         "column033",
    "amort_type":         "column034",
    "ppm_flag":           "column035",
    "io_indicator":       "column036",
    "special_elig_prog":  "column078",
    "valuation_method":   "column085",   # A/C/P/R/W/O — already alpha-coded
    "super_conf_flag":    "column086",   # Y/N — High Balance Loan Indicator

    # ── Monthly performance ───────────────────────────────────────────────
    "current_rate":       "column008",
    "current_upb":        "column011",
    "loan_age":           "column015",
    "months_to_mat":      "column017",
    "delinq_status":      "CAST(column039 AS VARCHAR)",   # already zero-padded 2-char
    "mod_flag":           "CAST(column041 AS VARCHAR)",   # Y/N
    "mi_cancel_flag": (
        "CASE CAST(column042 AS VARCHAR)"
        " WHEN 'M' THEN 'N'"
        " ELSE CAST(column042 AS VARCHAR) END"
    ),
    "borrower_assist": (
        "CASE CAST(column101 AS VARCHAR)"
        " WHEN 'F' THEN 'F'"
        " WHEN 'R' THEN 'R'"
        " WHEN 'T' THEN 'T'"
        " ELSE NULL END"
        # O/N/7/9 all map to NULL to match Freddie's schema
    ),
    "eltv":               "column085",   # not separately disclosed by Fannie; NULL in practice
    "int_bearing_upb":    "column109",
    "non_int_upb": (
        # Modification-Related Non-Interest Bearing UPB + Total Deferral Amount
        "COALESCE(TRY_CAST(column062 as DOUBLE), 0) + COALESCE(TRY_CAST(column107 as DOUBLE), 0)"
    ),

    # ── Loss / liquidation ────────────────────────────────────────────────
    "zero_bal_code": (
        """CASE CAST(column043 AS VARCHAR)
            WHEN '01' THEN 'Prepayment'
            WHEN '02' THEN 'Credit Event'
            WHEN '03' THEN 'Credit Event'
            WHEN '09' THEN 'Credit Event'
            WHEN '97' THEN 'Credit Event'
            WHEN '98' THEN 'Credit Event'
            WHEN '15' THEN 'Note Sale'
            WHEN '16' THEN 'Note Sale'
            WHEN '06' THEN 'Repurchase'
            WHEN '96' THEN 'Other/Removal'
            ELSE NULL
        END"""
    ),
    "zero_bal_upb":       "column045",
    # total_expenses: Foreclosure + Preservation + Asset Recovery
    #                 + Miscellaneous Holding + Taxes for Holding
    "total_expenses": (
        "COALESCE(TRY_CAST(column053 as DOUBLE), 0)"
        " + COALESCE(TRY_CAST(column054 as DOUBLE), 0)"
        " + COALESCE(TRY_CAST(column055 as DOUBLE), 0)"
        " + COALESCE(TRY_CAST(column056 as DOUBLE), 0)"
        " + COALESCE(TRY_CAST(column057 as DOUBLE), 0)"
    ),
    "net_sales_proc":     "column058",
    # actual_loss = (Zero Bal UPB - Net Sales) + Delinq Interest
    #               - (Expenses + Credit Enhancements)
    "actual_loss": (
        "COALESCE(TRY_CAST(column045 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column058 as DOUBLE), 0)"
        " + COALESCE(TRY_CAST(column083 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column053 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column054 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column055 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column056 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column057 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column059 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column060 as DOUBLE), 0)"
        " - COALESCE(TRY_CAST(column061 as DOUBLE), 0)"
    ),
    "delinq_int":         "column084",
    "current_mod_cost":   "column074",
    "cumulative_mod_cost":"column075",
    # credit_enhancements: CE Proceeds + Repurchase Make-Whole + Other Foreclosure
    "credit_enhancements": (
        "COALESCE(TRY_CAST(column059 as DOUBLE), 0)"
        " + COALESCE(TRY_CAST(column060 as DOUBLE), 0)"
        " + COALESCE(TRY_CAST(column061 as DOUBLE), 0)"
    ),
}



# ── 5. Freddie Mac column expressions ─────────────────────────────────────────
# Source: Freddie Mac Single-Family Loan-Level Dataset data dictionary.
# O-file  → column00  … column31     (origination data, suffix-less)
# P-file  → column00_1 … column31_1  (monthly performance data)
# Dates are already YYYYMM in the source.

FREDDIE_EXPRS: dict[str, str] = {
    # ── Identifiers & dates ───────────────────────────────────────────────
    "loan_id":            "column00_1",   # P: Loan Sequence Number
    "report_period":      "column01_1",   # P: Monthly Reporting Period (YYYYMM)
    "first_pay_date":     "column01",     # O: First Payment Date (YYYYMM)
    "maturity_date":      "column03",     # O: Maturity Date (YYYYMM)
    "zero_bal_date":      "column09_1",   # P: Zero Balance Effective Date (YYYYMM)
    "ddlpi":              "column12_1",   # P: Due Date of Last Paid Installment (YYYYMM)

    # ── Origination characteristics ───────────────────────────────────────
    # channel: Freddie T (TPO Not Specified) is kept; 9 (Not Available) → NULL
    "channel":            "column13",
    "seller_name":        "column23",     # O
    "servicer_name":      "column24",     # O
    "orig_int_rate":      "column12",     # O
    "orig_upb":           "column10",     # O
    # orig_term: Freddie stores (maturity - first_pay), add 1 for contractual term
    "orig_term":          "CAST(column21 AS INTEGER) + 1",
    "orig_ltv":           "column11",     # O
    "orig_cltv":          "column08",     # O
    "borrower_count":     "column22",     # O
    "orig_dti":           "column09",     # O
    "credit_score":       "column00",     # O
    # first_time_buyer: Freddie 9 → NULL
    "first_time_buyer":   "column02",
    # loan_purpose: Freddie R (Not Specified) → U to match Fannie; 9 → NULL
    "loan_purpose": (
        "CASE CAST(column20 AS VARCHAR)"
        " WHEN 'R' THEN 'U'"
        " ELSE CAST(column20 AS VARCHAR) END"
    ),
    "prop_type":          "column17",     # O
    "unit_count":         "column06",     # O
    # occupancy_status: Freddie 9 → NULL
    "occupancy_status":   "column07",
    "prop_state":         "column16",     # O
    "msa_code":           "column04",     # O
    "zip_3":              "column18",     # O: first 3 digits of postal code
    "mi_percent":         "column05",     # O
    "amort_type":         "column15",     # O
    "ppm_flag":           "column14",     # O
    "io_indicator":       "column30",     # O
    "special_elig_prog":  "column27",     # O
    # valuation_method: Freddie numeric → gold alpha standard
    # 1=ACE Waiver→W  2=Appraisal→A  3=Other→O  4=ACE+PDR→C  7=N/A→NULL
    "valuation_method": (
        "CASE CAST(column29 AS VARCHAR)"
        " WHEN '1' THEN 'W'"
        " WHEN '2' THEN 'A'"
        " WHEN '3' THEN 'O'"
        " WHEN '4' THEN 'C'"
        " ELSE CAST(column29 AS VARCHAR) END"
    ),
    "super_conf_flag": (
        # Freddie encodes "not super conforming" as a single space character.
        # Normalise to 'N' so both agencies use Y/N.
        "CASE WHEN TRIM(CAST(column25 AS VARCHAR)) = ''"
        "      OR  CAST(column25 AS VARCHAR) IS NULL"
        " THEN 'N'"
        " ELSE CAST(column25 AS VARCHAR) END"
    ),

    # ── Monthly performance ───────────────────────────────────────────────
    "current_rate":       "column10_1",   # P
    "current_upb":        "column02_1",   # P
    # loan_age: Freddie's raw value is already (reporting_period - first_pay_date),
    # add 1 to match the MBA convention used by Fannie
    "loan_age":           "CAST(column04_1 AS INTEGER) + 1",
    "months_to_mat":      "column05_1",   # P
    # delinq_status: Freddie single-digit → zero-padded 2-char to match Fannie
    "delinq_status": (
        "CASE WHEN CAST(column03_1 AS VARCHAR) = 'RA'"
        " THEN 'RA'"
        " ELSE lpad(CAST(column03_1 AS VARCHAR), 2, '0') END"
    ),
    # mod_flag: Freddie Y/P (current/prior mod) → Y;  NULL → N
    "mod_flag": (
        "CASE CAST(column07_1 AS VARCHAR)"
        " WHEN 'Y' THEN 'Y'"
        " WHEN 'P' THEN 'Y'"
        " ELSE 'N' END"
    ),
    # mi_cancel_flag: Freddie 7 (never had MI) → NA;  9 (not disclosed) → NULL
    "mi_cancel_flag": (
        "CASE CAST(column31 AS VARCHAR)"
        " WHEN '7' THEN 'NA'"
        " ELSE CAST(column31 AS VARCHAR) END"
    ),
    # borrower_assist: Freddie F/R/T pass through; NULL = no plan
    "borrower_assist":    "column29_1",   # P: already F/R/T/NULL
    # eltv: 999 → NULL handled by EXPLICIT_SENTINELS; CASE here for clarity
    "eltv":               "column25_1",
    "int_bearing_upb":    "column31_1",   # P
    "non_int_upb":        "column11_1",   # P: Non-Interest Bearing UPB

    # ── Loss / liquidation ────────────────────────────────────────────────
    "zero_bal_code": (
        """CASE CAST(column08_1 AS VARCHAR)
            WHEN '01' THEN 'Prepayment'
            WHEN '02' THEN 'Credit Event'
            WHEN '03' THEN 'Credit Event'
            WHEN '09' THEN 'Credit Event'
            WHEN '15' THEN 'Note Sale'
            WHEN '16' THEN 'Note Sale'
            WHEN '96' THEN 'Repurchase'
            ELSE NULL
        END"""
    ),
    "zero_bal_upb":       "column26_1",   # P
    "total_expenses":     "column16_1",   # P: pre-aggregated by Freddie
    "net_sales_proc":     "column14_1",   # P
    "actual_loss":        "column21_1",   # P: pre-calculated by Freddie
    "delinq_int":         "column27_1",   # P
    "current_mod_cost":   "column30_1",   # P
    "cumulative_mod_cost":"column22_1",   # P
    # credit_enhancements: MI Recoveries (P14) + Non-MI Recoveries (P16)
    "credit_enhancements": (        
        "COALESCE(TRY_CAST(column13_1 AS DOUBLE), 0)"
        " + COALESCE(TRY_CAST(column15_1 AS DOUBLE), 0)"
    ),
}


# ── 6. Execution ───────────────────────────────────────────────────────────────

con = duckdb.connect()

total = sum(os.path.getsize(os.path.join(SILVER_DIR, f)) 
            for f in os.listdir(SILVER_DIR))
print(f"{total / 1e9:.2f} GB")
print(f"{len(os.listdir(SILVER_DIR))} files")

# Verify key alignment before running views
fannie_keys = list(FANNIE_EXPRS.keys())
freddie_keys = list(FREDDIE_EXPRS.keys())
master_keys = list(COLUMN_TYPES.keys())

if fannie_keys != master_keys or freddie_keys != master_keys:
    print("CRITICAL ERROR: Dictionary keys are misaligned!")
    missing_in_freddie = set(master_keys) - set(freddie_keys)
    if missing_in_freddie: print(f"Freddie missing: {missing_in_freddie}")
    # Force stop if the order doesn't match the master schema
    raise ValueError("Dictionary key order must match COLUMN_TYPES exactly for UNION ALL to work.")

print("Step 1: Building Fannie Mae gold view...")
con.execute(f"""
    CREATE VIEW f_gold AS
    SELECT
        'FNM' AS agency,
        {build_select(FANNIE_EXPRS, COLUMN_TYPES)}
    FROM read_parquet('{SILVER_DIR}/fannie_*.parquet')
""")

print("Step 2: Building Freddie Mac gold view...")
con.execute(f"""
    CREATE VIEW r_gold AS
    SELECT
        'FRE' AS agency,
        {build_select(FREDDIE_EXPRS, COLUMN_TYPES)}
    FROM read_parquet('{SILVER_DIR}/freddie_*.parquet')
""")

print(con.execute("SELECT COUNT(*) FROM f_gold").fetchone())
print(con.execute("SELECT COUNT(*) FROM r_gold").fetchone())

print("Step 3: Exporting unified gold parquet...")

TEMP_DIR = os.path.join(BASE_PATH, "data", "temp")
os.makedirs(TEMP_DIR, exist_ok=True)

# Use dynamic variables in the PRAGMA calls
con.execute(f"PRAGMA memory_limit='4GB'")
con.execute(f"PRAGMA temp_directory='{TEMP_DIR.replace(os.sep, '/')}'")

# This prevents DuckDB from trying to keep 600M rows in a specific order
con.execute("SET preserve_insertion_order = false;")
# This reduces the number of simultaneous buffers eating RAM
con.execute("SET threads = 4;") 

# The COPY command 
con.execute(f"""
    COPY (
        SELECT * FROM f_gold
        UNION ALL
        SELECT * FROM r_gold
    )
    TO '{OUTPUT_PATH}' (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        ROW_GROUP_SIZE 100000
    )
""")

row_counts = con.execute("""
    SELECT agency, COUNT(*) AS rows
    FROM (SELECT * FROM f_gold UNION ALL SELECT * FROM r_gold)
    GROUP BY agency
""").fetchall()

print(f"\nSuccess! Exported to {OUTPUT_PATH}")
print(f"Fields: {len(COLUMN_TYPES)} standardised columns (+1 agency flag)")
print("\nRow counts by agency:")
for agency, count in row_counts:
    print(f"  {agency}: {count:,}")

con.close()