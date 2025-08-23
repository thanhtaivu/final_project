import os                                                                            # os for file and directory operations
import pandas as pd                                                                  # pandas for reading, cleaning, manipulating, and writing tabular data
import sqlite3                                                                       # sqlite3 for SQLite database to store and process structured data efficiently

DATA_DIR = 'resource_files'                                                          # to specify where input CSV files are stored.
RESULT_DIR = 'result'                                                                # to specify directory where the output (cleaned data, reports) will be saved
DB_FILE = os.path.join(RESULT_DIR, 'staging.db')                                     # SQLite database file to temporarily store data from multiple CSVs
MASTER_FILE = os.path.join(RESULT_DIR, 'master_data.csv')                            # the final cleaned and deduplicated data to be saved as a file named "master_data.csv" for easily fitering 
FORMAT_FILE = '09.csv'                                                               # to specify file 09.csv as reference CSV file containing the structure used for cleaning and standardizing all files

def setup_environment():
    os.makedirs(RESULT_DIR, exist_ok=True)                                           # to create the "result" directory if it doesn’t exist.
    if os.path.exists(DB_FILE):                                                      # if the old database file already exists, remove the old database file (staging.db) to start fresh each time the script runs
        os.remove(DB_FILE)

def get_csv_files():                                                                 # to return a list of all .csv files in the current directory and exclude other types
    return [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]

def clean_column_name(col):                                                           # to standardize the column names: stripping whitespace, converting to lowercase, replacing spaces with underscores
    return str(col).strip().lower().replace(' ', '_')

def main():                                                                           # to setup a environment and connect to the SQLite database
    setup_environment()
    conn = sqlite3.connect(DB_FILE)                                                   # reason to use SQLite is for the saving the resource data and also for the progressing process.

    # setup the format file with 09.csv
    try:
        format_df = pd.read_csv(os.path.join(DATA_DIR, FORMAT_FILE), dtype=str, nrows=1)                   # to read only the header row of 09.csv to capture the column names
        format_columns = [clean_column_name(col) for col in format_df.columns]                             # the column names of 09.csv are standardized and saved to format_columns
        print(f"Using format from {FORMAT_FILE}")
    except Exception as e:
        print(f"Error reading {FORMAT_FILE}: {e}")                                                         # to return an error if it fails
        return

    # create database table
    db_columns = ', '.join([f'"{col}" TEXT' for col in format_columns])
    conn.execute(f"CREATE TABLE products ({db_columns}, source_file TEXT)")
    
    print("Importing data...")
    
    # import csv files
    for filename in get_csv_files():
        try:
            for chunk in pd.read_csv(os.path.join(DATA_DIR, filename), dtype=str, chunksize=50000, on_bad_lines='skip'):                                # to read each file in chunks of 50,000 rows to save memory
                chunk.columns = [clean_column_name(col) for col in chunk.columns]
                
                # check the 'ean' collum is valiable in the chunk
                # if it is not skip this file

                if 'ean' not in chunk.columns:                                                                                                     
                    continue                                                                                                                        

                valid_eans = (chunk['ean'].str.len() == 13) & (chunk['ean'].str.isdigit())                                                              # to filter the rows with EAN 13 numeric characters and copy to new dataframe
                chunk_filtered = chunk[valid_eans].copy()


                # check if the chunk is empty or not
                if chunk_filtered.empty:
                    continue
                

                chunk_final = chunk_filtered.reindex(columns=format_columns)
                chunk_final['source_file'] = filename                                                                                       #add source file name to the chunk
                
                chunk_final.to_sql('products', conn, if_exists='append', index=False)                                                       #to append the cleaned chunk into the products table

            print(f"Imported {filename}")

        except Exception as e:
            print(f"Error with {filename}: {e}")

    
    # read all data from the database
    try:
        all_data = pd.read_sql('SELECT * FROM products', conn)                                                  #read all data and store it in a all_data DataFrame
    except Exception as e:
        print(f"Error reading database: {e}")
        conn.close()
        return

    # check product_id is available in all_data DataFrame
    if 'product_id' not in all_data.columns:
        all_data['product_id'] = 'N/A'
    else:
        all_data['product_id'] = all_data['product_id'].fillna('N/A')
    
    # merge EAN and product_id
    master_df = all_data.groupby(['ean', 'product_id'], as_index=False).first()                                             #grouping the ean and product_id the 2 first columns and setting its as a DataFrame
    master_df['provider'] = 'provider ' + master_df['source_file'].str.replace('.csv', '', regex=False)
    master_df = master_df.sort_values('ean')
    
    #export it as the csv files
    master_df.to_csv(MASTER_FILE, index=False)
    print(f"Master files is created: ")
    
    # create tyres and rims files
    tyres_file = os.path.join(RESULT_DIR, 'tyres.csv')
    rims_file = os.path.join(RESULT_DIR, 'rims.csv')
    base_columns = ['ean', 'product_id', 'manufacturer', 'article_type','provider']                                         # define the main columns that appear in the rims and tyres files
    
    # filter tyres
    tyre_article = master_df['article_type'].fillna('').str.upper().str.contains('REIFEN')                                  # set the filter on the artical_type column
    tyre_product = master_df['product_id'].str.upper().fillna('').str.startswith(('TYRE', 'REIFEN'))                        # set filter on the product_od which is not N/A and contain the letters TYRE or REIFEN
    tyres_mask = tyre_article | tyre_product                                                                                # set both of those as a filter conditions for the importing process

    if tyres_mask.any():                                                                                                    
        tyres_df = master_df[tyres_mask].copy()                                                                             # create a new DataFrame with the filtered data and duplicate the master_data by copy it
        tyre_columns = [col for col in master_df.columns if 'tyre' in col or 'reifen' in col]                               #find all heading columns that contain the words 'tyre' or 'reifen'
        final_tyre_columns = list(dict.fromkeys(base_columns + tyre_columns))                                               # remove duplicates from the list of columns
        tyres_df = tyres_df[final_tyre_columns]                                                                             # select only the columns that are in the final_tyre_columns list
        tyres_df.to_csv(tyres_file, index=False)                                                                            # export the tyres filter as csv file
    
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
    tyre_providers = set(tyres_df['provider'].unique())
    rim_providers = set(rims_df['provider'].unique())
    valid_providers = tyre_providers & rim_providers

    for provider in sorted(valid_providers):
        tyre_providers = set(tyres_df['provider'].unique())
        rim_providers = set(rims_df['provider'].unique())
        providers_mask = tyre_providers & rim_providers

    for provider in sorted(providers_mask):
        tyre_subset = tyres_df[tyres_df['provider'] == provider]
        rim_subset = rims_df[rims_df['provider'] == provider]
    
        combined = pd.concat([tyre_subset, rim_subset], ignore_index=True)
        provider_file = os.path.join(RESULT_DIR, f'{provider}_tyres_rims.csv')
        combined.to_csv(provider_file, index=False)
        print(f"Created: {provider_file} ({len(combined)} records)")


    
    conn.close()
    
    print(f"Done! Results files in {RESULT_DIR}")

if __name__ == '__main__':
    main() 
