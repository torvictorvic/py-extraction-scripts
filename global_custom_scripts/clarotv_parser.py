import boto3
import numpy
import pandas as pd
import io
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
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
import os

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

            # session = boto3.Session(profile_name="default")
            # s3 = session.client('s3')

            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            return obj,lm
        else:
            with open(uri,'rb') as f:
                return uri,datetime.today()

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
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df =None
        body_df = StringIO(file.decode())
        widths_body  = [2,8,3,5,9,8,6,15,6,8,6,8,12,20,2,8,3,12,3,12,2,12,9,8,6,57]
        df_body = pd.read_fwf(body_df,dtype=object,widths=widths_body,header=None)
        df_body.columns = ["tipo_registro","fecha_referencia","codigo_distribuidor","codigo_sucursal","operador_nsu","fecha_operador","tiempo_operador","codigo_origen","origen_nsu","fecha_origen","hora_origen","identificacion_terminal","valor","numero_telefono_completo","verificacion_digitos_telefono","codigo_localidad_cep","autorizacion_identificacion_1","codigo_autorizacion_1","autorizacion_identificacion_2","codigo_autorizacion_2","reservado","upsell_importe_original","upsell_operador_nsu","upsell_fecha_operador","upsell_tiempo_operador","relleno"]
        df_body = df_body.drop(df_body[(df_body.tipo_registro != "11") & (df_body.tipo_registro != "12") & (df_body.tipo_registro != "13") & (df_body.tipo_registro != "14")].index)
        df_body = df_body.reset_index(drop=True)
        df = df_body
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df