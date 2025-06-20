import os
import pandas as pd
import sqlite3

DATA_DIR = '.'
RESULT_DIR = 'result'
DB_FILE = os.path.join(RESULT_DIR, 'staging.db')
MASTER_FILE = os.path.join(RESULT_DIR, 'master_data.csv')

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

def main():
    setup_environment()
    conn = sqlite3.connect(DB_FILE)

    for filename in get_csv_files():
        try:
            chunk_iterator = pd.read_csv(
                os.path.join(DATA_DIR, filename),
                dtype=str,
                chunksize=50000,
                on_bad_lines='skip'
            )

            for chunk in chunk_iterator:
                chunk.columns = [str(col).strip().lower() for col in chunk.columns]
                
                if not table_exists(conn, 'products'):
                    chunk.to_sql('products', conn, index=False)
                    print(f"Created table with {filename}")
                else:
                    db_cols = set(pd.read_sql("PRAGMA table_info('products')", conn)['name'])
                    for col in chunk.columns:
                        if col not in db_cols:
                            conn.execute(f'ALTER TABLE products ADD COLUMN "{col}" TEXT')
                    
                    chunk.to_sql('products', conn, if_exists='append', index=False)
            
            print(f"Imported {filename}")

        except Exception as e:
            print(f"ERROR with {filename}: {e}")

    try:
        combined_df = pd.read_sql('SELECT * FROM products', conn)
    except Exception as e:
        print(f"ERROR reading data from database: {e}")
        conn.close()
        return

    if 'product_id' not in combined_df.columns:
        combined_df['product_id'] = 'N/A'
    else:
        combined_df['product_id'] = combined_df['product_id'].fillna('N/A')

    master_df = combined_df.groupby(['ean', 'product_id'], as_index=False).first()

    master_df.to_csv(MASTER_FILE, index=False)
    print(f"Done. {MASTER_FILE}")

    conn.close()

if __name__ == '__main__':
    main() 