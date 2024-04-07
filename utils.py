import pandas as pd
import os
from tqdm import tqdm

pd.set_option('display.max_columns', None)


PATH = "/Users/danilakokin/Desktop/UPC/Semester2/SDM/dblp-to-csv"


def join_csv_and_headers(file, header_file, dtype=None):
  df_head = pd.read_csv(f'{PATH}/{header_file}', sep=';')
  if len(df_head) > 1:
    df_head = df_head.iloc[0:1]
  if dtype:
    df = pd.read_csv(f'{PATH}/{file}', sep=';', header=None, dtype=dtype)
  else:
    df = pd.read_csv(f'{PATH}/{file}', sep=';', header=None, low_memory=False)
  df.columns = df_head.columns
  return df

os.makedirs('joined', exist_ok=True)


for name in tqdm(['article', 'inproceedings', 'incollection', 'book', 'data', 'mastersthesis', 'phdthesis', 'proceedings', 'www']):
  df = join_csv_and_headers(f'dblp_{name}.csv', f'dblp_{name}_header.csv')
  df.to_csv(f'{name}.csv')
