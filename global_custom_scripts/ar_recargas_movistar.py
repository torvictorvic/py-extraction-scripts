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
        try:
            df = pd.read_csv(file,dtype=str,sep=';')
            formato = ['fecha_hora','num_abonado','cod_tipmov','monto','cod_tipactor_cod_actor','razon_social','plataforma','p','cod_transac_soap','datos_extra_payment_id','error_code','error_desc','punto_venta']
            df.columns = formato
            df = df.drop(['punto_venta'], axis=1)
            df["plataforma"] = df["plataforma"] + " " + df["p"]
            df['fecha_hora'] = df['fecha_hora'].str.replace('/','-')
            df = df.drop(["p"], axis=1)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except pd.io.common.EmptyDataError as e:
            formato = ['fecha_hora','hora_movimiento','num_abonado','cod_tipmov','monto','cod_tipactor_cod_actor','razon_social','plataforma','p','cod_transac_soap','datos_extra_payment_id','error_code','error_desc','punto_venta']
            df = pd.DataFrame(columns = formato)
            df = df.drop(['punto_venta'], axis=1)
            df = df.drop(["hora_movimiento"], axis=1)
            df = df.drop(["p"], axis=1)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
   
            formato = ['fecha_hora','num_abonado','cod_tipmov','monto','cod_tipactor_cod_actor','razon_social','plataforma','cod_transac_soap','datos_extra_payment_id','error_code','error_desc']
            df = pd.DataFrame(columns = formato)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)
