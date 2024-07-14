import pandas as pd
import numpy as np
import sys
import os
import argparse
import logging
import datetime
from sqlalchemy import create_engine, Connection
# import sqlalchemy
# import pyodbc
import dotenv

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)


DATA_PATH = 'data'


def load_support_table(filename: str, remove_blanks: bool = False) -> pd.DataFrame:
    sep = '\t'
    if remove_blanks:
        sep = '\t+'
    return pd.read_csv(filename, sep=sep, header=0, engine='python', encoding='latin1')

def load_bird_table(filename:str, connection:Connection) -> pd.DataFrame:
    d1 =datetime.datetime(2023,6,1,0,0,0,0)
    count_read = 0
    count_kept = 0
    with pd.read_csv(filename, sep='\t', header=0, engine='python', encoding='latin1', parse_dates=['LAST EDITED DATE'], chunksize=2000000) as reader:
        LOG.info("in loop")
        for block in reader:
            LOG.info(f'rows read in {block.shape[0]}')
            count_read = count_read + block.shape[0]
            block.drop(columns=[
                'ATLAS BLOCK', 'LOCALITY', 'LOCALITY ID', 'LOCALITY TYPE', 
                'OBSERVER ID', 'EFFORT AREA HA', 'NUMBER OBSERVERS',
                'ALL SPECIES REPORTED', 'GROUP IDENTIFIER', 'TRIP COMMENTS'],
                inplace=True)
            block = block[block['STATE CODE'].isin(['US-MO'])]
            LOG.info(f'rows kept in state filter {block.shape[0]}')          
            block['LAST EDITED DATE'] = pd.to_datetime(block['LAST EDITED DATE'], format='mixed')   
            t = block[block['LAST EDITED DATE'] > d1]
            LOG.info(f'rows kept in by date filter {t.shape[0]}')
            count_kept = count_kept + t.shape[0]
            try:
                lines_out = write_to_azure(t, connection, 'ebird_data')
                LOG.info(f"records to database {lines_out}")
            except Exception as e:
                LOG.error(e)
                break
            count_kept = count_kept + t.shape[0]   
    LOG.info(f"Rows keps {count_kept} out of {count_read}")

def write_to_azure(data: pd.DataFrame, database_connection: Connection, table_name: str):
    LOG.info(f'Writing {table_name} to Azure')
    data.to_sql(table_name, database_connection, if_exists='replace', index=False)

if __name__ == '__main__':
    dotenv.load_dotenv(dotenv.find_dotenv())
    args = argparse.ArgumentParser()
    args.add_argument('--data_path', type=str, default=os.getenv('DATA_PATH'))
    args.add_argument('--username', type=str, default=os.getenv('USERNAME'))
    args.add_argument('--password', type=str, default=os.getenv('PASSWORD'))
    args = args.parse_args()

    if args.password is None:
        LOG.error('Please provide a password to connect to the database')
        sys.exit(1)
        
    LOG.info(args.data_path)

    # clean up data path, removing any trailing slashes
    if args.data_path[-1] == '/':
        args.data_path = args.data_path[:-1]

    # bcr_codes = load_support_table(f"{args.data_path}/BCRCodes.txt")
    # usfws_codes = load_support_table(f"{args.data_path}/USFWSCodes.txt", remove_blanks=True)
    # iba_codes = load_support_table(f"{args.data_path}/IBACodes.txt")

    connection_string = f"mssql+pymssql://{args.username}:{args.password}@sschwabebird.database.windows.net:1433/ebird" 
    con = create_engine(connection_string)

    load_bird_table('~/bird/ebd_US_relMay-2024.txt')

 

    # write_to_azure(bcr_codes, con, 'BCRCodes')
    # write_to_azure(usfws_codes, con, 'USFWSCodes')
    # write_to_azure(iba_codes, con, 'IBACodes')
    # write_to_azure(edb, con, 'ebird_data')