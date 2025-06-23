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
from rarfile import RarFile

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

            #session = boto3.session.Session()
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
        body_df_h = StringIO(file.decode())
        body_df_t = StringIO(file.decode())
        body_df_f = StringIO(file.decode())
        widths_h = [1,8,6,8,6,83]
        widths_t = [1,8,6,12,12,11,3,59]
        widths_f = [1,8,11,92]

        df_h = pd.read_fwf(body_df_h,dtype=object,widths=widths_h,header=None,nrows=1)
        df_t = pd.read_fwf(body_df_t,dtype=object,widths=widths_t,header=None)
        df_f = pd.read_fwf(body_df_f,dtype=object,widths=widths_f,header=None)


        df_h.columns = ["cod_registro","fecha_inicio","hora_inicio","fecha_corte","hora_corte","rfu_header"]
        df_t.columns = ["cod_registro","fecha_trx","hora_trx","id_tx_recarga","cod_recarga","importe_recarga","codigo_proyecto","rfu_tx"]
        df_f.columns = ["cod_registro","cantidad_registros","importe_total","rfu_footer"]

        df_h = df_h.head(1)
        df_f = df_f.tail(1)
        df_t = df_t[1:-1]
        for column in df_h.columns:
            if column != 'cod_registro':
                df_t[column] = df_h[column].values[0]
        for column in df_f.columns:
            if column != 'cod_registro':
                df_t[column] = df_f[column].values[0]
        df = df_t
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df