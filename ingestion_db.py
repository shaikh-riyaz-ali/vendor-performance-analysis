# Use this script to save csv files into database with their filename as tablename
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.types import TEXT
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a"  
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine, mode):
    '''insert dataframe chunk into database'''

    df.to_sql(
        table_name,
        con=engine,
        if_exists=mode,      # replace first, then append
        index=False,
        chunksize=20000,     # ✅ CHUNKING INSIDE to_sql (as you wanted)
        dtype={
            "Description": TEXT,
            "Store": TEXT,
            "VendorName": TEXT
        }
    )

def load_raw_data():
    start = time.time()

    for file in os.listdir('data'):
        if file.endswith('.csv'):

            table_name = file[:-4]
            logging.info(f'Ingesting {file} in chunks of 20000')

            # Read file in chunks
            chunk_iter = pd.read_csv('data/' + file, chunksize=20000)

            first = True

            for chunk in chunk_iter:
                if first:
                    ingest_db(chunk, table_name, engine, 'replace')
                    first = False
                else:
                    ingest_db(chunk, table_name, engine, 'append')

            logging.info(f'Finished ingesting {file}')

    end = time.time()
    total_time = (end - start)/60

    logging.info('--------------Ingestion Complete------------')
    logging.info(f'Total Time Taken: {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()
