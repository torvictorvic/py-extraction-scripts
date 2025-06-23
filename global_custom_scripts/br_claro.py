import pytz
import boto3
import numpy
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

            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')

            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode()
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
        registro = StringIO(file)
        widths = [1,20,8,8,44,12,5,6,8,5,23,1,9]
        df = pd.read_fwf(registro,dtype=str,skiprows=1,skipfooter=1,widths=widths,header=None)
        if df.empty:
            columns = ["cod_registro","identificacion_agencia_acreditada","fecha_pago","fecha_credito","cod_barras","valor_recibido","valor_tarifa","NSR","identificacion_agencia_cobranza","metodo_recoleccion","numero_autenticacion","forma_pago","reservado"]
            df = pd.DataFrame(columns=columns)
            df = df.append(pd.Series(),ignore_index=True)
        else:
            columns = ["cod_registro","identificacion_agencia_acreditada","fecha_pago","fecha_credito","cod_barras","valor_recibido","valor_tarifa","NSR","identificacion_agencia_cobranza","metodo_recoleccion","numero_autenticacion","forma_pago","reservado"]
            df.columns = columns
            df = df.reset_index(drop=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
