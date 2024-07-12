import pandas as pd
import numpy as np
import sys
import argparse
import logging
from sqlalchemy import create_engine, Connection
# import sqlalchemy
# import pyodbc


logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)


DATA_PATH = 'data'


def load_support_table(filename: str, remove_blanks: bool = False) -> pd.DataFrame:
    sep = '\t'
    if remove_blanks:
        sep = '\t+'
    return pd.read_csv(filename, sep=sep, header=0, engine='python', encoding='latin1')

def write_to_azure(data: pd.DataFrame, database_connection: Connection, table_name: str):
    LOG.info(f'Writing {table_name} to Azure')
    data.to_sql(table_name, database_connection, if_exists='replace', index=False)

if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--data_path', type=str, default=DATA_PATH)
    args.add_argument('--username', type=str, default='')
    args.add_argument('--password', type=str, default='')
    args = args.parse_args()

    if args.password is None:
        LOG.error('Please provide a password to connect to the database')
        sys.exit(1)
        
    LOG.info(args.data_path)

    # clean up data path, removing any trailing slashes
    if args.data_path[-1] == '/':
        args.data_path = args.data_path[:-1]

    bcr_codes = load_support_table(f"{args.data_path}/BCRCodes.tsv")
    usfws_codes = load_support_table(DATA_PATH + '/USFWSCodes.tsv', remove_blanks=True)
    iba_codes = load_support_table(DATA_PATH + '/IBACodes.tsv')
    edb = load_support_table(DATA_PATH + '/bird_data1.tsv')

 
    connection_string = f"mssql+pymssql://{args.username}:{args.password}@sschwabebird.database.windows.net:1433/ebird" 
    con = create_engine(connection_string)
    write_to_azure(bcr_codes, con, 'BCRCodes')
    write_to_azure(usfws_codes, con, 'USFWSCodes')
    write_to_azure(iba_codes, con, 'IBACodes')
    write_to_azure(edb, con, 'ebird_data')