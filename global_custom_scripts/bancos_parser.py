import boto3
import numpy
import io
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
import zipfile
import glob
import os
import os.path
import sys
import pytz
import time
import pandas as pd
from pandas import DataFrame
from enum import Enum
import math
from urllib.parse import urlparse
from zipfile import ZipFile
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
try:
    from core.models import ClientSetting
except:
    from models.core import ClientSettings

SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = "INPUTS" # os.environ.get('SNOWFLAKE_SCHEMA')


class Connection:

    def __init__(self):
        self.engine = self.create()

    def create(self):
        engine = create_engine(URL(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        ))
        return engine

    def query_to_df(self, query) -> pd.DataFrame:
        with self.engine.connect() as conn:
            df = pd.DataFrame()
            for chunk in pd.read_sql_query(query, conn, chunksize=1000, coerce_float=False):
                df = df.append(chunk)
        df = df.reset_index()

        return df

    def query_to_value(self, query):
        with self.engine.connect() as conn:
            value = conn.execute(query)
        return value



class S3Url(object):

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip('/') + '?' + self._parsed.query
        else:
            return self._parsed.path.lstrip('/')

    @property
    def url(self):
        return self._parsed.geturl()
        
class FileReader:

    @staticmethod
    def read(uri: str):
        origin = urlparse(uri, allow_fragments=False)
        if origin.scheme in ('s3', 's3a'):

            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')

            session = boto3.session.Session()
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            print('Inicia Descarga')
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            print('Descarga exitosa')
            if '.zip' in uri:
                with zipfile.ZipFile(BytesIO(obj)) as z:
                    name = z.namelist()[0]
                    with z.open(name, 'r') as f:
                        print('Inicia Descompresión')
                        return BytesIO(f.read()), lm
            else:
                binary_data = BytesIO(obj)
                return binary_data, lm
        else:
            print('No entró a la S3')
            with open(uri) as f:
                return uri,datetime.today()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

class ExtractorInter:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=';',header=None)
            df = df.iloc[:,0:34]
            df.columns = ["marcador_nueva_linea","numero_decuenta","codigo_para_fecha","fecha_operacion","campo_1","campo_2","campo_3","campo_4","campo_5","campo_6","monto_con_signo_operacion","campo_7","campo_8","fecha_bajada_reporte","campo_9","campo_10","texto_operacion","campo_11","campo_12","campo_13","campo_14","campo_15","campo_16","campo_17","campo_18","campo_19","campo_20","campo_21","campo_22","campo_23","campo_24","campo_25","codigo_operacion_1","codigo_operacion_2"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns = ["marcador_nueva_linea","numero_decuenta","codigo_para_fecha","fecha_operacion","campo_1","campo_2","campo_3","campo_4","campo_5","campo_6","monto_con_signo_operacion","campo_7","campo_8","fecha_bajada_reporte","campo_9","campo_10","texto_operacion","campo_11","campo_12","campo_13","campo_14","campo_15","campo_16","campo_17","campo_18","campo_19","campo_20","campo_21","campo_22","campo_23","campo_24","campo_25","codigo_operacion_1","codigo_operacion_2"]
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)


class ExtractorHub:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str)
            df.columns =["fecha","transaccion","oficina","documento","debito","credito"]
            df = df.replace({'\$': '', ',': ''}, regex=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns =["fecha","transaccion","oficina","documento","debito","credito"]
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)


class ExtractorOneLine:
    @staticmethod
    def file_stament_number_default(tabla):

        query = f"""
            select max(ifnull(replace(h_statement_number, 'S', ''), 0))::integer as statement_number from 
            inputs.{tabla}
            """
        conn = Connection()
        df = conn.query_to_df(query)
        statement_number = str(int(df.statement_number[0]) + 1)
        return statement_number

    @staticmethod
    def file_stament_number(tabla, fecha, account):
        # find the sourondings filenames:
        query = f"""
                 SELECT  
                 RIGHT(DATE_PART(year, TO_DATE('{fecha}', 'yyyy-MM-dd')), 2) || 
                 DAYOFYEAR(TO_DATE('{fecha}', 'yyyy-MM-dd')) AS statement_number
            """

        conn = Connection()
        df = conn.query_to_df(query)

        statement_number = str(int(df.statement_number[0]))

        return statement_number

    @staticmethod
    def run(filename, **kwargs):

        tablas = {'BRA': 'br_citi_swift',
                  'COL': 'co_citi_swift',
                  'UYU': 'uy_citi_swift',
                  'ARG': 'ar_citi_swift',
                  'AR_ONLINE': 'ar_citi_swift',
                  'MEB': 'mx_banco_bancomer_swift',
                  'MEX': 'mx_banco_banamex_swift',
                  'MES': 'mx_banco_santander_swift',
                  'MEP': 'mx_banco_banamex_swift'}
        nombres = {'BRA': 'MPB_102352A_',
                  'COL': 'MPO_102559A_',
                  'UYU': 'DRU_102862A_',
                  'ARG': 'MLA_102216A_',
                  'AR_ONLINE': 'MLA_102253A_',
                  'MEB': 'MLM_102604A_',
                  'MEX': 'MLM_102602A_',
                  'MES': 'MLM_102606A_',
                  'MEP': 'MEP_102020A_'}
        if 'BRA' in filename:
            pais = 'BRA'
        elif 'COL' in filename:
            pais = 'COL'
        elif 'UYU' in filename or 'URU' in filename:
            pais = 'UYU'
        elif 'AR_' in filename:
            pais = 'ARG'
        elif 'ARG_ONLINE' in filename:
            pais = 'AR_ONLINE'
        elif 'CHI_CITI' in filename:
            pais = 'CLC'
        elif 'CHI_SANT' in filename or 'URU' in filename:
            pais = 'CLS'
        elif 'PE_CITI' in filename:
            pais = 'PEC'
        elif 'PE_BBVA' in filename:
            pais = 'PEB'
        elif 'MLM_BANCOMER' in filename:
            pais = 'MEB'
        elif 'MLM_BANAMEX' in filename:
            pais = 'MEX'    
        elif 'MLM_SANTANDER' in filename:
            pais = 'MES' 
        elif 'MEP_BANAMEX' in filename:
            pais = 'MEP' 
        else :
            pais = 'otro'
        print('Leyendo archivo')
        file,lm = FileReader.read(filename)
        print('Lectura exitosa')
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        fila = file.read().decode()
        file=[]
        filas = fila.split('@')
        total = []
        for row in filas:
            total.append(row.replace('"',"").replace(',',"").replace('(',"").replace(')',"").split(';'))
        filas=[]
        df = pd.DataFrame(total)
        total=[]
        print('DataFrame creado')
        for i in range(0,len(df.columns)):
            l = i+1
            df[i] = df[i].str.replace('{l}#'.format(l=l),"")
        df.columns = ["account_number","back_val_trans_pot_dt","bank_reference","credit_debit_ind","customer_reference","extra_info","mt942_timestamp","other_reference","transaction_amount"]
        df.dropna(subset=["back_val_trans_pot_dt","bank_reference","credit_debit_ind","customer_reference","extra_info","mt942_timestamp","other_reference","transaction_amount"], how='all', inplace=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        freq_date = datetime.strptime(df['back_val_trans_pot_dt'].value_counts().idxmax(),'%m/%d/%Y')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        df['H_STATEMENT_NUMBER'] = ExtractorOneLine.file_stament_number(tabla=tablas[pais], \
                                             fecha=freq_date.strftime("%Y-%m-%d"), \
                                             account=nombres[pais])
        print("STATEMENT_NUMBER creado")
        df['filename'] = nombres[pais] + pd.Timestamp.now().strftime('%Y%m%d') + pd.to_datetime(df['back_val_trans_pot_dt'].tolist()[0],format='%m/%d/%Y').strftime('%Y%m%d') + '_RPA.csv'
        df.reset_index(drop=True)
        df['raw_uniqueness'] = df.sort_values(['report_date'], ascending=[False]) \
            .groupby(['credit_debit_ind', 'customer_reference']) \
            .cumcount() + 1
        df['original_filename'] = filename.split('/')[-1][:-4] + '_' + pd.Timestamp.now().strftime('%Y%m%d')  + pd.to_datetime(df['back_val_trans_pot_dt'].tolist()[0],format='%m/%d/%Y').strftime('%Y%m%d') + '.csv'
        df['skt_extraction_rn'] = df.index.values
        return df