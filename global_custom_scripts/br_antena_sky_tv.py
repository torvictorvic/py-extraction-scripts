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
        widths = [2,8,3,5,9,8,6,15,6,8,6,8,12,20,2,8,3,12,3,12,2,12,9,8,6]
        try:
            df = pd.read_fwf(file,dtype=str,header=None,widths=widths)
            formato = ["tipo_de_registro","data_de_referencia","codigo_da_concessionaria","codigo_da_filial","nsu_operadora","data_na_operadora","hora_na_operadora","codigo_da_origem","nsu_origem","data_na_origem","hora_na_origem","identificacao_do_terminal","valor","numero_do_telefone_completo","check_digit_do_telefone","codigo_de_localidade_cep","identificacao_autorizador_1","codigo_de_autorizacao_1","identificacao_autorizador_2","codigo_de_autorizacao_2","reservado","upsell_valor_original","upsell_nsu_operadora","upsell_data_operadora","upsell_hora_operadora"]
            df.columns = formato

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError as e:
            formato = ["tipo_de_registro","data_de_referencia","codigo_da_concessionaria","codigo_da_filial","nsu_operadora","data_na_operadora","hora_na_operadora","codigo_da_origem","nsu_origem","data_na_origem","hora_na_origem","identificacao_do_terminal","valor","numero_do_telefone_completo","check_digit_do_telefone","codigo_de_localidade_cep","identificacao_autorizador_1","codigo_de_autorizacao_1","identificacao_autorizador_2","codigo_de_autorizacao_2","reservado","upsell_valor_original","upsell_nsu_operadora","upsell_data_operadora","upsell_hora_operadora"]
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
