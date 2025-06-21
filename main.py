import os
import pandas as pd
import sqlite3

DATA_DIR = '.'
RESULT_DIR = 'result'
DB_FILE = os.path.join(RESULT_DIR, 'staging.db')
MASTER_FILE = os.path.join(RESULT_DIR, 'master_data.csv')
FORMAT_FILE = '09.csv'

def setup_environment():
    os.makedirs(RESULT_DIR, exist_ok=True)
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

def get_csv_files():
    return [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]

def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    return cursor.fetchone() is not None

def clean_column_name(col):
    return str(col).strip().lower().replace(' ', '_')

def main():
    setup_environment()
    conn = sqlite3.connect(DB_FILE)


    try:
        format_df = pd.read_csv(os.path.join(DATA_DIR, FORMAT_FILE), dtype=str, nrows=1)
        format_columns = [clean_column_name(col) for col in format_df.columns]
    except Exception as e:
        print(f"Error reading file '{FORMAT_FILE}'")
        return

    db_columns = ', '.join([f'"{col}" TEXT' for col in format_columns])
    conn.execute(f"CREATE TABLE products ({db_columns})")
    
    #import data from csv files
    for filename in get_csv_files():
        try:
            chunk_iterator = pd.read_csv(
                os.path.join(DATA_DIR, filename),
                dtype=str,
                chunksize=50000,
                on_bad_lines='skip'
            )

            for chunk in chunk_iterator:
                chunk.columns = [clean_column_name(col) for col in chunk.columns]
                
                if 'ean' not in chunk.columns:
                    continue

                # chossing ean having 13 characters
                chunk_filtered_ean = chunk[chunk['ean'].str.len() == 13].copy()

                if chunk_filtered_ean.empty:
                    continue
                
                chunk_final = chunk_filtered_ean.reindex(columns=format_columns)
                
                chunk_final.to_sql('products', conn, if_exists='append', index=False)


        except Exception as e:
            print(f"ERROR file {filename}")

    #merge files
    try:
        combined_df = pd.read_sql('SELECT * FROM products', conn)
    except Exception as e:
        print(f"ERROR reading: {e}")
        conn.close()
        return


    if 'product_id' not in combined_df.columns:
        combined_df['product_id'] = 'N/A'
    else:
        combined_df['product_id'] = combined_df['product_id'].fillna('N/A')
    
    master_df = combined_df.groupby(['ean', 'product_id'], as_index=False).first()
    master_df = master_df.sort_values('ean')
    master_df.to_csv(MASTER_FILE, index=False)
    print(f"master view created")
    
    # create tyres and rims files
    tyres_file = os.path.join(RESULT_DIR, 'tyres.csv')
    rims_file = os.path.join(RESULT_DIR, 'rims.csv')
    
    base_columns = ['ean', 'product_id', 'manufacturer']
    
    # filter tyres
    tyre_columns = [col for col in master_df.columns if 'tyre' in col]
    if tyre_columns:
        tyre_mask = master_df[tyre_columns].notnull().any(axis=1)
        tyres_df = master_df.loc[tyre_mask].copy()
        final_tyre_columns = [col for col in base_columns if col in tyres_df.columns] + tyre_columns
        tyres_df = tyres_df[final_tyre_columns]
        tyres_df.to_csv(tyres_file, index=False)
        print(f"typers view created")
    else:
        print("ERROR tyres")
    
    # filter rims 
    rim_columns = [col for col in master_df.columns if 'rim' in col]
    if rim_columns:
        rim_mask = master_df[rim_columns].notnull().any(axis=1)
        rims_df = master_df.loc[rim_mask].copy()
        final_rim_columns = [col for col in base_columns if col in rims_df.columns] + rim_columns
        rims_df = rims_df[final_rim_columns]
        rims_df.to_csv(rims_file, index=False)
        print(f"rims view created")
    else:
        print("ERROR rims")
    
    conn.close()

if __name__ == '__main__':
    main() 