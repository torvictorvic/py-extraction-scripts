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

            #session = boto3.Session(profile_name="sts")
            #s3 = session.client('s3')

            session = boto3.session.Session()
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            return uri, datetime.today()

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
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        file_,lm = FileReader.read(filename)
        df = pd.read_excel(file_,dtype=str)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["nit","codigo_establecimiento","origen_de_la_compra","tipo_transaccion","franquicia","identificador_de_red","fecha_de_transaccion","fecha_de_canje","cuenta_de_consignacion","valor_compra","valor_propina","valor_iva","valor_impoconsumo","valor_total","valor_comision","valor_retefuente","valor_rete_iva","valor_rte_ica","valor_provision","valor_neto","codigo_autorizacion","tipo_tarjeta","no_terminal","tarjeta","comision_porcentual","comision_fija", "bin"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
    
class Extractor_Refund:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        file_,lm = FileReader.read(filename)
        try:
            df = pd.read_csv(file_,dtype=str,sep=",")
        except:
            df = pd.read_excel(file_,dtype=str)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fecha_compensacion","no_tarjeta","cod_autorizacion","nombre_establecimiento","vr_total","codigo_comercio","nit","cuenta_deposito","tipo_de_cuenta","fecha_comprobante","fecha_aplicacion_reembolso","concepto_en_cuenta"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df