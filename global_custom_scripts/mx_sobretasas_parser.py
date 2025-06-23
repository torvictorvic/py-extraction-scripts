import pytz
import boto3
import pandas as pd

from io import BytesIO
from urllib.parse import urlparse
from datetime import datetime

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

            #session = boto3.Session(profile_name="sts")
            #s3 = session.client('s3')
            session = boto3.session.Session()
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

class ExtractorExcel:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        
        col = ["fecha_operacion","numero_afiliacion","bin","digitos","numero_autorizacion","plazo_msi","porcentaje_sobretasa","importe_sobretasa","importe_msi","num_transacciones"]
        df = pd.read_excel(file,dtype=object, names=col)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        
        col = ["cuenta","no_afiliacion","nombre_negocio","f_proc","fch","no","no_ii","num_tarjeta","compra","pzo","s_tasa","is_tasa"]
        df = pd.read_excel(file,dtype=object,skiprows=3)
        pd.set_option('display.float_format', lambda x: '%.3f' % x)
        df = df.loc[:,~df.columns.str.startswith('Unnamed')]
        
        df1 = pd.read_excel(file,dtype=object,skiprows=8, names=col)
        df1['fecha_proceso_h'] = df["Fecha Proceso"][0]
        df1['no_transacciones_h'] = df["NoTransacciones"][0]
        df1['facturacion_h'] = df["FACTURACIÓN"][0]
        df1['imp_sobretasa_h'] = df["IVA"][0]
        df1['iva_total_h'] = df["TOTAL"][0]
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df1['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df1['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df1['file_name'] = out
        df1.reset_index(drop=True, inplace=True)
        df1['skt_extraction_rn'] = df1.index.values
        return df1 

