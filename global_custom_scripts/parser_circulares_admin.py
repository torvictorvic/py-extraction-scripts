import os
import boto3
import pytz
import json
import time
import pandas as pd
from enum import Enum
from typing import List
from pandas import DataFrame
from io import StringIO, BytesIO
from logging import getLogger
from datetime import datetime
from urllib.parse import urlparse

from sqlalchemy.dialects import registry
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer

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

            session = boto3.session.Session()
            s3 = session.client('s3')

            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']

            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Extractor_amex:
    @staticmethod
    def run(filename, **kwargs):
        print('entro a la clase amex')
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file)
        ls_columns = [   
                    'FECHA_DE_LA_VENTA',
                    'FECHA_DE_LIQUIDACION',
                    'ENVIANDO_NUMERO_DE_ESTABLECIMIENTO',
                    'IMPORTE',
                    'ENVIANDO_ID_DE_UBICACION',
                    'NUMERO_DEL_TITULAR_DE_LA_TARJETA',
                    'TIPO_DE_AJUSTE',
                    'NUMERO_DE_REFERENCIA_DE_CARGO',
                    'TIPO',
                    'NUMERO_DE_TERMINAL']
        df.columns = ls_columns
        try:
            df['IMPORTE'] = df['IMPORTE'].str.replace('.','')
            df['IMPORTE'] = df['IMPORTE'].str.replace(',','.')
            df['IMPORTE'] = df['IMPORTE'].str.replace('(','-')
            df['IMPORTE'] = df['IMPORTE'].str.replace(')','')
            df['IMPORTE'] = pd.to_numeric(df['IMPORTE'])
        except:
            df['IMPORTE'] = pd.to_numeric(df['IMPORTE'])
        df['FECHA_DE_LA_VENTA'] = pd.to_datetime(df['FECHA_DE_LA_VENTA'], format='%d/%m/%Y').dt.date
        df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['FILE_NAME'] = out
        df.reset_index(drop=True)
        df['SKT_EXTRACTION_RN'] = df.index.values
        print('--*--'*10)
        print(f'salio {filename} . . .')
        return df


class Extractor_cabal:
    @staticmethod
    def run(filename, **kwargs):
        print('entro a la clase cabal')
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file, dtype=str)
        ls_columns = ['BCO_EMISOR', 
                    'SUC_EMISOR', 
                    'NRO_CUENTA', 
                    'COD_OPERACION', 
                    'SUB_CODIGO',
                    'NRO_COMERCIO', 
                    'NRO_CUIT', 
                    'NRO_TARJETA', 
                    'FECHA_COMPRA', 
                    'FECHA_PAGO',
                    'FECHA_FACTURACION', 
                    'FECHA_PROCESO', 
                    'NRO_AUTORIZACION', 
                    'NRO_CAJA',
                    'NRO_CUPON', 
                    'CANTIDAD_CUOTAS', 
                    'IMPORTE_COMPRA', 
                    'IMPORTE_CUOTA',
                    'IMPORTE_TOTAL']
        df.columns = ls_columns
        df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['FILE_NAME'] = out
        df.reset_index(drop=True)
        df['SKT_EXTRACTION_RN'] = df.index.values
        print('--*--'*10)
        print(f'salio {filename} . . .')
        return df

class Extractor_fd:
    @staticmethod
    def run(filename, **kwargs):
        print('entro a la clase fd')
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file, engine='pyxlsb', dtype=str)
        ls_columns = ['TARJETA',
                      'FEC_PRES',
                      'FEC_PAGO',
                        'IMPORTE_CON_DTO', 
                        'COMERCIO',
                        'CUPON',
                        'CU',
                        'TF',
                        'MC',
                        'FEC_OPER',
                        'MOV',
                        'PRODUCTO']
        df.columns = ls_columns
        df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['FILE_NAME'] = out
        df.reset_index(drop=True)
        df['SKT_EXTRACTION_RN'] = df.index.values
        print('--*--'*10)
        print(f'salio {filename} . . .')
        return df
    
class Extractor_naranja:
    @staticmethod
    def run(filename, **kwargs):
        print('entro a la clase naranja')
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df_ini = pd.read_excel(file, sheet_name = None, engine='pyxlsb', dtype=str)
        df_mid = pd.concat(df_ini, ignore_index = True)
        df     = pd.DataFrame(df_mid)  
        ls_columns = [  'COMERCIO',
                        'MONEDA',
                        'FECHA_DE_LA_TRANSACCION',
                        'FECHA_PAGO',
                        'COD_CONTABLE',
                        'PLAN',
                        'MARCA_PLAN',
                        'COMPROBANTE',
                        'COD_EMPRESA',
                        'PRODUCTO',
                        'CUPON',
                        'TITULAR',
                        'ADICIONAL',
                        'TARJETA',
                        'FECHA_CUPON',
                        'COD_AUTORIZACION',
                        'IMPORTE_BRUTO',
                        'IMP_CUOTA',
                        'TIPO_OPERACION',
                        'FECHA_DE_LIQUIDACION',
                        'NRO_DE_LIQUIDACION']
        df.columns = ls_columns
        df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['FILE_NAME'] = out
        df.reset_index(drop=True)
        df['SKT_EXTRACTION_RN'] = df.index.values
        print('--*--'*10)
        print(f'salio {filename} . . .')
        return df

class Extractor_prisma_cac:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file, sep = '\t', encoding='latin1', dtype=str)
        ls_columns = ['CUIT',
                    'NRO_ESTABLECIMIENTO',
                    'NRO_COMPROBANTE',
                    'FECHA_MOVIMIENTO',
                    'MONTO_MOVIMIENTO',
                    'NRO_TARJETA',
                    'COD_BANCO_PAGADOR',
                    'NRO_CAJA',
                    'NRO_LOTE',
                    'FECHA_PRESENTACION',
                    'FECHA_PAGO',
                    'NRO_AUTORIZACION',
                    'CANTIDAD_CUOTAS_TOTAL',
                    'NRO_CUOTA_SIGUIENTE',
                    'MARCA_PEX',
                    'MARCA_CA',                    
                    'MARCA_CUOTA_CUOTA',
                    'SALDO_RESTANTE']
        df.columns = ls_columns
        df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['FILE_NAME'] = out
        df.reset_index(drop=True)
        df['SKT_EXTRACTION_RN'] = df.index.values
        print('--*--'*10)
        print(f'salio {filename} . . .')
        return df

class Extractor_prisma_consumos:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file, sep = '\t', encoding='latin1', dtype=str)
        ls_columns = ['CUIT',
                    'NRO_ESTABLECIMIENTO',
                    'NRO_COMPROBANTE',
                    'FECHA_MOVIMIENTO',
                    'MONTO_MOVIMIENTO',
                    'NRO_TARJETA',
                    'COD_BANCO_PAGADOR',
                    'NRO_CAJA',
                    'NRO_LOTE',
                    'FECHA_PRESENTACION',
                    'FECHA_PAGO',
                    'NRO_AUTORIZACION',
                    'CANTIDAD_CUOTAS_TOTAL',
                    'NRO_CUOTA',
                    'MARCA_PEX',
                    'MARCA_CUOTA_CUOTA',
                    'MARCA_COBRO_ANTICIPADO']
        df.columns = ls_columns
        df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['FILE_NAME'] = out
        df.reset_index(drop=True)
        df['SKT_EXTRACTION_RN'] = df.index.values
        print('--*--'*10)
        print(f'salio {filename} . . .')
        return df

class Extractor_prisma_devcco:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file, sep = '\t', encoding='latin1', dtype=str)
        ls_columns = ['CUIT',
                    'NRO_ESTABLECIMIENTO',
                    'NRO_COMPROBANTE',
                    'FECHA_MOVIMIENTO',
                    'MONTO_MOVIMIENTO',
                    'NRO_TARJETA',
                    'COD_BANCO_PAGADOR',
                    'NRO_CAJA',
                    'NRO_LOTE',
                    'FECHA_PRESENTACION',
                    'FECHA_PAGO',
                    'NRO_AUTORIZACION',
                    'CANTIDAD_CUOTAS_TOTAL',
                    'NRO_CUOTA',
                    'COD_TIPO_OPERACION',
                    'DEV_CCO']
        df.columns = ls_columns
        df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['FILE_NAME'] = out
        df.reset_index(drop=True)
        df['SKT_EXTRACTION_RN'] = df.index.values
        print('--*--'*10)
        print(f'salio {filename} . . .')
        return df

