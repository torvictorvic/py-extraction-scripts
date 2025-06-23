import io
import os
import sys
import pytz
import time
import math
import glob
import numpy
import boto3
import zipfile
import os.path
import pandas as pd

from enum import Enum
from zipfile import ZipFile
from pandas import DataFrame
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
            return obj,lm
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
    def run(filename):
        file,lm = FileReader.read(filename)   
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
       
        buffer = io.BytesIO(file)
        buffer_2 = io.BytesIO(file)
        
        try:
            columns = ["fecha_proceso","rut_comercio","codigo_comercio","tipo_contrato","descripcion_tipo_contrato","tipo_flujo","descripcion_tipo_flujo","fecha_venta","fecha_abono","tarjeta","nro_cuota","monto_cuota","lnin_sec","fecha_proceso_txs","monto_venta","codigo_autorizacion","orden_pedido","id_servicio","pareada"]
            df = pd.read_csv(buffer, dtype=str, skiprows=2, names=columns, sep=";", encoding="utf-8")
        except:
            columns = ["fecha_proceso","rut_comercio","codigo_comercio","tipo_contrato","descripcion_tipo_contrato","tipo_flujo","descripcion_tipo_flujo","fecha_venta","fecha_abono","tarjeta","nro_cuota","monto_cuota","lnin_sec","fecha_proceso_txs","monto_venta","codigo_autorizacion","orden_pedido","id_servicio"]
            df = pd.read_csv(buffer_2, dtype=str, skiprows=2, names=columns, sep=";", encoding="utf-8")
        
        df['descripcion_tipo_contrato'] = df['descripcion_tipo_contrato'].str.replace('�','e')
        df['monto_venta'] = df['monto_venta'].str.replace('.','')
        df['monto_venta'] = df['monto_venta'].str.replace(',','.')
        out = (filename.split('/')[-1])
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df