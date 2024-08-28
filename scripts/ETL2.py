# -*- coding: utf-8 -*-
"""
Proyecto Heidi Sanchez
"""

import yfinance as yf
import pandas as pd
import os
import logging

import csv
from sqlalchemy import create_engine, types
from urllib.parse import quote_plus

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
        data['Symbol'] = ticker  # Add ticker column   
        return data
    except Exception as e:
        logging.error(f'Error extrayendo datos para {ticker}: {e}')
        return None

def transform_data(data):
    try:
        logging.info('Transformando datos')
        df = data[['Adj Close', 'Symbol']].reset_index()
        df.rename(columns={'Adj Close': 'Close'}, inplace=True)
        df = df.reindex(['Date', 'Symbol', 'Close'], axis=1)
        logging.info('Datos transformados exitosamente')
        return df
    except Exception as e:
        logging.error(f'Error transformando datos: {e}')
        return None

def load_data(df, ticker):
    try:
        filename = os.path.join(data_dir, f'Companies.csv')
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
    logging.info(f'Se obtuvieron  {data.shape } registros de empresas del S&P 500')
    data.rename(columns={'Símbolo': 'Symbol', 'Seguridad': 'Company', 'Sector GICS': 'Sector', 'Ubicación de la sede': 'Headquarters', 'FechaFundada': 'Fundada'}, inplace=True)
    data = data[['Symbol', 'Company', 'Sector', 'Headquarters', 'Fundada']]
    filename = os.path.join(data_dir, f'CompanyProfiles.csv')
    logging.info(f'Guardando datos en {filename}')
    data.to_csv(filename, index=False)
    return data

def conexionBD():
    server = 'localhost'
    username = 'app_user'
    password = 'Root1234'
    database = 'S&P_500'
    dsn = "ODBC Driver 17 for SQL Server"

    # Create engine to connect with DB
    try:
        #engine = create_engine('mssql+pyodbc://'+username+':'+password+'@'+server+'/'+database+'?driver=ODBC Driver 17 for SQL Server')
        engine = create_engine(f"mssql+pyodbc://{username}:%s@{server}/{database}?TrustServerCertificate=yes&driver={dsn}" % quote_plus(password))

        logging.info(f'Conexión a la base de datos fue exitosa')
    except:
        logging.error(f'Sin conexión a la base de datos')
    return engine


# Insertar datos
def insertar_datos(tabla):
    df = pd.read_csv('./data/' +  tabla +  '.csv')
    engine = conexionBD()
    try:
        logging.info(f'Cargando los datos en tabla ' + tabla + '.....................')
        with engine.begin() as connection:
            df.to_sql(tabla, con=connection, index=False, if_exists='append')
            logging.info(f'Los datos fueron insertados en la tabla ' + tabla)
    except Exception as e:
        logging.error(f'Los datos no fueron insertados')
        logging.error(e)
    

empresas_SP_500 =obtener_datos('https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500')

tickers = empresas_SP_500['Symbol']
start_date = '2010-01-01'
end_date = '2021-12-31'
logging.info(f'------------------------------------------------------')
etl_process(tickers, start_date, end_date)
logging.info(f'------------------------------------------------------')
insertar_datos('CompanyProfiles')
logging.info(f'------------------------------------------------------')
insertar_datos('Companies')
