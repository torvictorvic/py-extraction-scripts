import io
import os
import sys
import glob
import time
import math
import pytz
import numpy
import boto3
import os.path
import zipfile
import pandas as pd

from io import StringIO, BytesIO
from urllib.parse import urlparse
from datetime import date, timedelta, datetime

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

class Extractor_orama:
    @staticmethod
    def run(filename, **kwargs):
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        
        
        print(f'Uploading {filename} . . .')
        try:
            file,lm = FileReader.read(filename)
            upload_date = lm.astimezone(new_timezone)
            df = pd.read_csv(file,encoding='utf-8',dtype=str,sep=";", usecols=range(18))
            df.columns = ["ID_CLIENTE",	"ID_FUNDO",	"ID_MOVIMENTACAO",	"TIPO_OPERACAO", 
                        "DATA_COTIZACAO", "DATA_LIQUIDACAO", "VALOR_BRUTO", "VALOR_IR",	
                        "VALOR_IOF", "VALOR_LIQUIDO", "QTD_COTAS", "DATA_SOLICITACAO", "DATA_AGENDAMENTO",
                        "EXTERNAL_ID",	"STATUS", "CONVERTIDO", "VALOR_BRUTO_ORIGINAL", "DATA_CONCLUSAO"]
            df = df[df.columns[0:]].replace(',', '.', regex=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)