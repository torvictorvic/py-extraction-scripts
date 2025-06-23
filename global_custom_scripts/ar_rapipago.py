import os
import pytz
import boto3 
import pandas as pd

from enum import Enum
from pandas import DataFrame
from datetime import datetime
from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper

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
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
  
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            #_bytes = BytesIO(obj)
            return obj,lm
        else:
            with open(uri,'rb') as f:
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
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        columns = ["fecha_cobro","fecha_caja","hora_caja","id_operacion","id_sucursal","id_transaccion","barra","codigo_empresa_puesto","descripcion_empresa_puesto","importe","tipo_cobranza","provincia","id_mp","modelo_recaudador"]
        
        try:
            df = pd.read_csv(BytesIO(file), sep=";",dtype=str,names=columns)
        except:
            df = pd.read_csv(BytesIO(file), sep=";",dtype=str,names=columns, encoding='latin-1')

        if df.empty:
            columns = ["fecha_cobro","fecha_caja","hora_caja","id_operacion","id_sucursal","id_transaccion","barra","codigo_empresa_puesto","descripcion_empresa_puesto","importe","tipo_cobranza","provincia","id_mp","modelo_recaudador"]
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
        df2 = df.iloc[[0, -1]].reset_index(drop=True)
        df['id_header'] = df2.fecha_cobro[0][0:8]
        df['nombre_empresa'] = df2.fecha_cobro[0][8:28]
        df['fh_proceso'] = df2.fecha_cobro[0][28:36]
        df['id_archivo'] = df2.fecha_cobro[0][36:56]
        df['id_trailer'] = df2.fecha_cobro[1][0:8]
        df['cant_reg'] = df2.fecha_cobro[1][8:16]
        df['importe_total'] = df2.fecha_cobro[1][16:34]
        df.drop(df.head(1).index,inplace=True)
        df.drop(df.tail(1).index,inplace=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df