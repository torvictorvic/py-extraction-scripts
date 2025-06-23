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
        header_specification = [(0, 8), (8, 16),(16, 22), (22, 36)]
        cols_header = ['fecha_envio','nom_insti_rec','cantidad_registros','monto_total_archivo']
        header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object,)
        header.columns= cols_header
        col_specification =[(0,3),(3,13),(13,23),(23,35),(35,65),(65,75),(75,83),(83,86),(86,94),(94,102),(102,110),(110,112),(112,124),(124,127),(127,131),(131,143),(143,151),(151,157),(157,165)]
        cols = ["oficina","id_papel","id_ticket","id","num_documento","rut","interes","cuota","fecha_vencimiento","saldo_anterior","monto","medio_pago","num_serie_doc","banco","plaza_docto","cuenta_cte_docto","fecha_pago","hora","fecha_contable"]
        df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
        df.columns = cols
        for col in header:
            df[col] =header.loc[0,col] 
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
