import pandas as pd
import numpy as np
DATA_PATH = 'data/'
def load_support_table(filename: str) -> pd.DataFrame:
    return pd.read_csv(filename, sep='\t+', header=0, engine='python', encoding='latin1')

def write_to_azure(data: pd.DataFrame, database_connection: str, table_name: str):
    data.to_sql(table_name, database_connection, if_exists='replace', index=False)

if __name__ == '__main__':
    bcr_codes = load_support_table(DATA_PATH + 'BCRCodes.tsv')
    usfws_codes = load_support_table(DATA_PATH + 'USFWSCodes.tsv')
    iba_codes = load_support_table(DATA_PATH + 'IBACodes.tsv')
    edb = load_support_table(DATA_PATH + 'ebd_US-AL-101_202204_202204_relApr-2022.tsv')

    