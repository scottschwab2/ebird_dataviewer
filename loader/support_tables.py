import pandas as pd
import numpy as np
DATA_PATH = 'data/'
def load_support_table(filename: str) -> pd.DataFrame:
    return pd.read_csv(filename, sep='\t+', header=0, engine='python', encoding='latin1')


if __name__ == '__main__':
    bcr_codes = load_support_table(DATA_PATH + 'BCRCodes.tsv')
    usfws_codes = load_support_table(DATA_PATH + 'USFWSCodes.tsv')
    iba_codes = load_support_table(DATA_PATH + 'IBACodes.tsv')
