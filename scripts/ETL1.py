# -*- coding: utf-8 -*-
"""
Proyecto Heidi Sanchez
"""

import yfinance as yf
import pandas as pd
import os
import logging


log_dir = './logs'
data_dir = './data'

if not os.path.exists(log_dir):
    os.makedirs(log_dir)
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

log_filename = os.path.join(log_dir, 'etl_process.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

def extract_data(ticker, start_date, end_date):
    try:
        logging.info(f'Extrayendo datos para {ticker} desde {start_date} hasta {end_date}')
        data = yf.download(ticker, start=start_date, end=end_date)
        data['ticker'] = ticker  # Add ticker column   
        return data
    except Exception as e:
        logging.error(f'Error extrayendo datos para {ticker}: {e}')
        return None

def transform_data(data):
    try:
        logging.info('Transformando datos')
        df = data[['Adj Close', 'ticker']].reset_index()
        df.rename(columns={'Date': 'ds', 'Adj Close': 'y'}, inplace=True)
        df = df.reindex(['ticker', 'ds', 'y'], axis=1)
        logging.info('Datos transformados exitosamente')
        return df
    except Exception as e:
        logging.error(f'Error transformando datos: {e}')
        return None

def load_data(df, ticker):
    try:
        filename = os.path.join(data_dir, f'empresas_processed.csv')
        logging.info(f'Guardando datos transformados en {filename}')
        df.to_csv(filename, index=False)
        logging.info('Datos guardados exitosamente')
    except Exception as e:
        logging.error(f'Error guardando datos: {e}')

def etl_process(ticker, start_date, end_date):
    transformed_data = None
    for tick in ticker:
        data = extract_data(tick, start_date, end_date)
        if data is not None:
            transformed_data1 = transform_data(data)
            if transformed_data is None:
                transformed_data = transformed_data1
            else:
                transformed_data = pd.concat([transformed_data, transformed_data1], axis=0)
    if transformed_data is not None:
        load_data(transformed_data, ticker)
        return transformed_data
    return None

def obtener_datos(url):
    wiki = pd.read_html(url)
    logging.info(f'Accediendo a la URL {url}')
    data = wiki[0]
    data = data[['Símbolo', 'Seguridad']].reset_index()
    logging.info(f'Se obtuvieron  {data.shape } registros de empresas del S&P 500')
    data.rename(columns={'Símbolo': 'ticket', 'Seguridad': 'empresa'}, inplace=True)
    filename = os.path.join(data_dir, f'lista_empresas_SP_500.csv')
    logging.info(f'Guardando datos en {filename}')
    data.to_csv(filename, index=False)
    return data

empresas_SP_500 =obtener_datos('https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500')

tickers = empresas_SP_500['ticket']
start_date = '2010-01-01'
end_date = '2021-12-31'
logging.info(f'------------------------------------------------------')
etl_process(tickers, start_date, end_date)
