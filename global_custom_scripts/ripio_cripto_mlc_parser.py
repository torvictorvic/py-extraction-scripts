import boto3
import numpy as np
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
class Extractor_Transaccional:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingresó al primer try')
            df = pd.read_csv(file,encoding='utf-8',dtype=str,sep=",")
            df.columns = [col.lower().replace(" ", "_") for col in df.columns]                #change characters to lowercase and replace space with '_'
            df = df.loc[:,["end_user_id","transaction_id","operation","date_created","pair",
                          "amount","rounded_amount","asset","direction","price","charged_fee",
                          "fee_percentage","payment_id","unique_id","tag","tax"]]
            df = df[df.columns[0:]].replace(',', '.', regex=True)
            print('Contenido ', lm)
            print('leyó archivo')
            print('definió columnas proporcionadas por el archivo')
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('cargo el df')
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')
class Extractor_Balance:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingresó al primer try')
            df = pd.read_csv(file,encoding='utf-8',dtype=str,sep=",")
            df.columns = [col.lower().replace(" ", "_") for col in df.columns]    #change characters to lowercase and replace space with '_'
            df = df.loc[:,["timestamp","asset","balance"]]
            df = df[df.columns[0:]].replace(',', '.', regex=True)
            print('Contenido ', lm)
            print('leyó archivo')
            print('definió columnas proporcionadas por el archivo')
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('cargo el df')
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')

