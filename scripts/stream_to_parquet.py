import duckdb
import os
import glob
import zipfile
import shutil

# Get the directory where the script is currently located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Go up one level to the Project Root (Mortgage-Data-Lakehouse)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Define paths relative to the Project Root
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
TEMP_EXTRACT_DIR = os.path.join(PROJECT_ROOT, "data", "temp_staging")

def process_mortgage_data():
    con = duckdb.connect()
    os.makedirs(SILVER_DIR, exist_ok=True)
    
    # Define our two different workflows
    agencies = ['freddie', 'fannie']

    for agency in agencies:
        agency_zip_path = os.path.join(RAW_DIR, agency)
        zip_files = glob.glob(f"{agency_zip_path}/*.zip")

        print(f"\n--- Processing {agency.upper()} ({len(zip_files)} zips found) ---")

        for z_file in zip_files:
            # 1. Prepare a clean temporary folder for this specific zip
            if os.path.exists(TEMP_EXTRACT_DIR):
                shutil.rmtree(TEMP_EXTRACT_DIR)
            os.makedirs(TEMP_EXTRACT_DIR)

            # 2. Extract the Outer Zip 
            print(f"Extracting outer archive {os.path.basename(z_file)}...")
            with zipfile.ZipFile(z_file, 'r') as zip_ref:
                zip_ref.extractall(TEMP_EXTRACT_DIR)

            inner_zips = glob.glob(f"{TEMP_EXTRACT_DIR}/*.zip")
            if inner_zips:
                for iz in inner_zips:
                    print(f"  --> Unpacking nested quarter: {os.path.basename(iz)}")
                    with zipfile.ZipFile(iz, 'r') as inner_ref:
                        inner_ref.extractall(TEMP_EXTRACT_DIR)

                    os.remove(iz)

            # 3. Agency-Specific Transformation Logic
            try:
                if agency == 'freddie':
                    # Identify all Origination files to determine which quarters we have
                    orig_files = glob.glob(f"{TEMP_EXTRACT_DIR}/historical_data_2*.txt")
                    
                    for o_file in orig_files:
                        # Extract the quarter string (e.g., '2020Q1') from the filename
                        # This assumes the filename is something like 'historical_data_2020Q1.txt'
                        fname = os.path.basename(o_file)
                        quarter = fname.replace("historical_data_", "").replace(".txt", "")
                        
                        # Find the matching Performance file for THIS specific quarter
                        # It usually looks like: historical_data_time_2020Q1.txt
                        p_file = os.path.join(TEMP_EXTRACT_DIR, f"historical_data_time_{quarter}.txt")
                        
                        if os.path.exists(p_file):
                            print(f"  --> Processing Quarter: {quarter}")
                            
                            # Define a unique output name for this quarter
                            output_path = os.path.join(SILVER_DIR, f"freddie_{quarter}.parquet")
                            
                            # Join ONLY this specific quarter's pair
                            con.execute(f"""
                                COPY (
                                    SELECT o.*, p.*, 'freddie' as agency 
                                    FROM read_csv_auto('{o_file}', sep='|', all_varchar=True) o
                                    LEFT JOIN read_csv_auto('{p_file}', sep='|', all_varchar=True) p
                                    ON o.column19 = p.column00
                                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'snappy');
                            """)
                            print(f"Success! Created {os.path.basename(output_path)}")                            
                        else:
                            print(f"  [!] Warning: Found origination for {quarter} but no performance file.")

                elif agency == 'fannie':
                    # Fannie is usually just one big CSV in the zip
                    fannie_csv = glob.glob(f"{TEMP_EXTRACT_DIR}/*.csv")[0]
                    output_path = os.path.join(SILVER_DIR, f"fannie_{os.path.basename(z_file).replace('.zip', '.parquet')}")
                    
                    con.execute(f"""
                        COPY (
                            SELECT *, 'fannie' as agency 
                            FROM read_csv_auto(
                                '{fannie_csv}', 
                                all_varchar=True, 
                                sep='|', 
                                header=False, 
                                sample_size=10
                            )
                        ) 
                        TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'snappy');
                    """)
                    print(f"Success! Created {os.path.basename(output_path)}")

            except Exception as e:
                print(f"Error processing {z_file}: {e}")

            # Immediate Cleanup: Delete the bulky extracted files
            shutil.rmtree(TEMP_EXTRACT_DIR)

if __name__ == "__main__":
    process_mortgage_data()