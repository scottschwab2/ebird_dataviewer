import pandas as pd
import numpy as np
import sys
import argparse
import logging
from sqlalchemy import create_engine, Connection
import sqlalchemy


logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

# CONNECTION_STRING = 'jdbc:sqlserver://sschwabebird.database.windows.net:1433;database=ebird;user=ebird@sschwabebird;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'
# CONNECTION_STRING = 'jdbc:sqlserver://sschwabebird.database.windows.net:1433;database=ebird;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'
# CONNECTION_STRING= "Driver={ODBC Driver 18 for SQL Server};Server=tcp:sschwabebird.database.windows.net,1433;Database=ebird;Uid=ebird;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
CONNECTION_STRING= "Driver={ODBC Driver 18 for SQL Server};Server=tcp:sschwabebird.database.windows.net,1433;Database=ebird;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

DATA_PATH = 'data'


def load_support_table(filename: str) -> pd.DataFrame:
    return pd.read_csv(filename, sep='\t+', header=0, engine='python', encoding='latin1')

def write_to_azure(data: pd.DataFrame, database_connection: Connection, table_name: str):
    data.to_sql(table_name, database_connection, if_exists='replace', index=False)

if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--connection_string', type=str, default=CONNECTION_STRING)
    args.add_argument('--data_path', type=str, default=DATA_PATH)
    args.add_argument('--username', type=str, default='ebird')
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
    # usfws_codes = load_support_table(DATA_PATH + 'USFWSCodes.tsv')
    # iba_codes = load_support_table(DATA_PATH + 'IBACodes.tsv')
    # edb = load_support_table(DATA_PATH + 'ebd_US-AL-101_202204_202204_relApr-2022.tsv')

#CONNECTION_STRING= "Driver={ODBC Driver 18 for SQL Server};Server=tcp:sschwabebird.database.windows.net,1433;Database=ebird;Uid=ebird;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

    connection_string = f"mssql+pymssql://{args.username}:{args.password}@sschwabebird.database.windows.net:1433/ebird" #?driver=ODBC+Driver+18+for+SQL+Server"
    con = create_engine(connection_string)
    write_to_azure(bcr_codes, con, 'BCRCodes')