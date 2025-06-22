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

def clean_column_name(col):
    return str(col).strip().lower().replace(' ', '_')

def main():
    setup_environment()
    conn = sqlite3.connect(DB_FILE)

    # setup the format file with 09.csv
    try:
        format_df = pd.read_csv(os.path.join(DATA_DIR, FORMAT_FILE), dtype=str, nrows=1)
        format_columns = [clean_column_name(col) for col in format_df.columns]
        print(f"Using format from {FORMAT_FILE}")
    except Exception as e:
        print(f"Error reading {FORMAT_FILE}: {e}")
        return

    # create database table
    db_columns = ', '.join([f'"{col}" TEXT' for col in format_columns])
    conn.execute(f"CREATE TABLE products ({db_columns}, source_file TEXT)")
    
    print("Importing data...")
    
    # import csv files
    for filename in get_csv_files():
        try:
            for chunk in pd.read_csv(os.path.join(DATA_DIR, filename), dtype=str, chunksize=50000, on_bad_lines='skip'):
                chunk.columns = [clean_column_name(col) for col in chunk.columns]
                
                if 'ean' not in chunk.columns:
                    continue

                valid_eans = (chunk['ean'].str.len() == 13) & (chunk['ean'].str.isdigit())
                chunk_filtered = chunk[valid_eans].copy()

                if chunk_filtered.empty:
                    continue
                

                chunk_final = chunk_filtered.reindex(columns=format_columns)
                chunk_final['source_file'] = filename
                
                chunk_final.to_sql('products', conn, if_exists='append', index=False)

            print(f"Imported {filename}")

        except Exception as e:
            print(f"Error with {filename}: {e}")

    #merging processes

    try:
        all_data = pd.read_sql('SELECT * FROM products', conn)
    except Exception as e:
        print(f"Error reading database: {e}")
        conn.close()
        return

    # check product_id
    if 'product_id' not in all_data.columns:
        all_data['product_id'] = 'N/A'
    else:
        all_data['product_id'] = all_data['product_id'].fillna('N/A')
    
    # merge EAN and product_id
    master_df = all_data.groupby(['ean', 'product_id'], as_index=False).first()
    master_df = master_df.sort_values('ean')
    

    master_df.to_csv(MASTER_FILE, index=False)
    
    # create tyres and rims files
    tyres_file = os.path.join(RESULT_DIR, 'tyres.csv')
    rims_file = os.path.join(RESULT_DIR, 'rims.csv')
    
    base_columns = ['ean', 'product_id', 'manufacturer', 'article_type', 'source_file']
    
    # filter tyres
    tyre_article = master_df['article_type'].fillna('').str.upper().str.contains('REIFEN')
    tyre_product = master_df['product_id'].str.upper().fillna('').str.startswith(('TYRE', 'REIFEN'))
    tyres_mask = tyre_article | tyre_product

    if tyres_mask.any():
        tyres_df = master_df[tyres_mask].copy()
        tyre_columns = [col for col in master_df.columns if 'tyre' in col or 'reifen' in col]
        final_tyre_columns = list(dict.fromkeys(base_columns + tyre_columns))
        tyres_df = tyres_df[final_tyre_columns]
        tyres_df.to_csv(tyres_file, index=False)
    
    # filter rims
    rim_article = master_df['article_type'].fillna('').str.upper().str.contains('FEL')
    rim_product = master_df['product_id'].str.upper().fillna('').str.startswith(('RIM', 'FELGE'))
    rims_mask = rim_article | rim_product

    if rims_mask.any():
        rims_df = master_df[rims_mask].copy()
        rim_columns = [col for col in master_df.columns if 'rim' in col or 'felge' in col]
        final_rim_columns = list(dict.fromkeys(base_columns + rim_columns))
        rims_df = rims_df[final_rim_columns]
        rims_df.to_csv(rims_file, index=False)

    # filter by providers
    tyres_df['provider'] = tyres_df['source_file'].str.replace('.csv', '', regex=False)
    rims_df['provider'] = rims_df['source_file'].str.replace('.csv', '', regex=False)
    tyre_providers = set(tyres_df['provider'].unique())
    rim_providers = set(rims_df['provider'].unique())
    valid_providers = tyre_providers & rim_providers

    for provider in sorted(valid_providers):
        tyre_subset = tyres_df[tyres_df['provider'] == provider]
        rim_subset = rims_df[rims_df['provider'] == provider]
    
        combined = pd.concat([tyre_subset, rim_subset], ignore_index=True)
        provider_file = os.path.join(RESULT_DIR, f'provider_{provider}_tyres_rims.csv')
        combined.to_csv(provider_file, index=False)

    
    conn.close()
    
    print(f"Done! Results files ready in {RESULT_DIR}")

if __name__ == '__main__':
    main() 
