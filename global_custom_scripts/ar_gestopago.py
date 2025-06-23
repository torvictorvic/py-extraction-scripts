import boto3
import numpy
import pandas as pd
import pytz
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
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
        df = pd.read_csv(registro,dtype=str,skiprows=1,skipfooter=1,sep=",")
        df['num'] = df.index
        if df.empty:
            columns = ["num","servicio","producto","sucursal","dispositivo","fecha","hora","telefono","referencia","concepto","monto","id_transaccion","num_autorizacion","upc"]
            df = pd.DataFrame(columns=columns)
            df = df.append(pd.Series(),ignore_index=True)
        else:
            columns = ["servicio","producto","sucursal","dispositivo","fecha","hora","telefono","referencia","concepto","monto","id_transaccion","num_autorizacion","upc","drop","num"]
            df.columns = columns
        df.drop("drop", axis=1,inplace=True)
        df = df.reset_index(drop=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df





