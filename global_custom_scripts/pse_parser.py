import boto3
import numpy
import pandas as pd
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

            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode()
            return obj,lm
        else:
            with open(uri) as f:
                return f.read(),datetime.today()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

class Extractor:
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
        header_df = StringIO(file)
        binary_df = StringIO(file)
        footer_df = StringIO(file)
        if 'psedet02' in filename.lower():
            header_specification = [(0, 1), (1, 9),(9, 17), (17, 23),(23, 24), (24, 39)]
            cols_header = ['registro_1','codigo_pse','fecha_cierre_ciclo','hora_inicio_ciclo','codigo_ciclo','codigo_empresa']
            header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object,)
            header.columns= cols_header
            col_specification = [(0, 1), (1, 2), (2, 22), (22, 42),(42,50),(50,60),(60,68),(68,85),(85,103),(103,121),(121,171),(171,191),(191,211),(211,231),(231,232),(232,240),(240,246),(246,247)]
            cols = ['registro_2','estado_tx','codigo_seguimiento_epp','codigo_unico_seguimiento',
                    'codigo_ef_autorizadora','codigo_servicio','codigo_banco','codigo_cuenta_credito',
                    'valor_transaccion','impuesto_ventas','codigo_auth_ef','referencia_uno','referencia_dos',
                    'referencia_tres','canal_pago','fecha_proceso','hora_registro_pse','ciclo_proceso_original']
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
            df.columns = cols
            footer_specification = [(0, 1), (1, 2),(2, 8),(8,26),(26,44)]
            cols_footer = ['tipo3','estado_tx','cantidad_tx','valor_tx','impuesto_ventas']
            footer = pd.read_fwf(footer_df, colspecs=footer_specification, skiprows=(int(len(df))), header=None, dtype=object)
            footer.columns= cols_footer
            df = df.iloc[:-1,:]
            for col in header:
                df[col] =header.loc[0,col] 
            for col in footer:
                df[col] =footer.loc[0,col]
        elif 'psedet00' in filename.lower():
            col_specification = [(0, 1), (1, 2), (2, 22), (22, 30),(30,38),(38,48),(48,65),(65,84),(84,102),(102,110),(110,114),(114,124),(124,129),(129,130),(130,138),(138,144),(144,145)]
            cols = ['registro_2','estado_tx','codigo_seguimiento_epp','codigo_unico_seguimiento',
                    'codigo_ef_autorizadora','codigo_servicio','codigo_cuenta_credito',
                    'valor_transaccion','impuesto_ventas','codigo_auth_ef','referencia_uno','referencia_dos',
                    'referencia_tres','canal_pago','fecha_proceso','hora_registro_pse','ciclo_proceso_original']
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
            df.columns = cols
            df.insert(6,"codigo_banco",numpy.nan, True)
            df[['registro_1', 'codigo_pse','fecha_cierre_ciclo', 'hora_inicio_ciclo', 'codigo_ciclo','codigo_empresa', 'tipo3', 'cantidad_tx', 'valor_tx']] = numpy.nan
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
