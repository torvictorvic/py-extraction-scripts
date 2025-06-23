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
from bs4 import BeautifulSoup



def not_null_df(df, columns):
    for column in columns:
        df = df[df[column].notnull()]
    return df


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
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
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

        try:
            df = pd.read_excel(file,dtype=str)
            formato = ["auto","telefono","monto","fecha","descripcion","e","canal1","canal2","mensaje","cadena_id","num_secuenci","codigo"]
            df.columns = formato
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError as e:
            formato = ["auto","telefono","monto","fecha","hora","descripcion","e","canal1","canal2","mensaje","cadena_id","num_secuenci","codigo"]
            df = pd.DataFrame(columns = formato)
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

class ExtractorGmail:
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
            df = None
            soup = BeautifulSoup(file.read(), features="html.parser")
            existe_url = False
            for elem in soup.find_all('a'):
                if 'aqu' in elem.text:
                    url = elem['href']
                    existe_url = True
            if not existe_url:
                return df
            df = pd.read_table(url, sep="|", error_bad_lines=False, index_col=False,header=None,dtype=str,skiprows=4)
            if "P A Q U E T E S" in df[0].unique():
                df = pd.read_table(url,index_col=False,header=None,dtype=str,names=["abc"])
                df = df[~df["abc"].str.contains("*", regex=False)]
                df = df[~df["abc"].str.contains("-", regex=False)]
                df = df[~df["abc"].str.contains("AUTO|", regex=False)]
                df = df[~df["abc"].str.contains("P A Q U E T E S", regex=False)]
                df = df[~df["abc"].str.contains("R E C A R G A S", regex=False)]
                df = df["abc"].str.split("|", expand = True)
                df.columns = ["auto","telefono","monto","fecha","descripcion","e","canal1","canal2","mensaje","cadena_id","num_secuenci","codigo","paquete"]
                df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y %H:%M:%S').astype(str)
                df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                out = filename.split('/')[-1]
                df['file_name'] = out
                df.reset_index(drop=True, inplace=True)
                df['skt_extraction_rn'] = df.index.values
                return df
            else:
                df.columns = ["auto","telefono","monto","fecha","descripcion","e","canal1","canal2","mensaje","cadena_id","num_secuenci","codigo"]
                df = not_null_df(df,["telefono"])
                df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y %H:%M:%S').astype(str)
                df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                out = filename.split('/')[-1]
                df['file_name'] = out
                df.reset_index(drop=True, inplace=True)
                df['skt_extraction_rn'] = df.index.values
                return df
            
        except Exception as e:
            print("Error al subir la fuente: ",e)
