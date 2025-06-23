import boto3
import numpy as np
import pandas as pd
from io import StringIO, BytesIO
from datetime import  datetime
import csv
import pytz

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
        print(f'Uploading {filename} . . .')
        dialect = csv.Sniffer().sniff(file.getvalue().decode("utf-8"))
        print(dialect.delimiter)
        df = pd.read_csv(file,dtype=str,sep=dialect.delimiter)
        columnas = ['Codigo', 'Producto', 'Tipo_Mov', 'Fecha_Proceso', 'Fecha_Lote',
            'Lote_Manual', 'Lote_Pos', 'Terminal', 'Voucher', 'Autorizacion',
            'Cuotas', 'Tarjeta', 'Origen', 'Transaccion', 'Fecha_Consumo',
            'Importe', 'Status', 'Comision', 'Comision_Afecta', 'IGV',
            'Neto_Parcial', 'Neto_Total', 'Fecha_Abono', 'Fecha_Abono_8Dig',
            'Observaciones','Comision_Merchant']
        df= df[filter(lambda x: 'Unnamed' not in str(x),df.columns)]
        for col in columnas:
            if col not in df.columns:
                df[col] = np.nan
        df = df[columnas]
        df.columns = list(map(lambda x: x.lower(),df.columns))
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

class ExtractorF5:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print(type(file))
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingresó al try')
            df = pd.read_fwf(file, widths = [9,12,12,4,19,3,14,8,8,12,12,12,12,12,1,1,20,6,15,11,2,10,35,16], header=None, encoding='latin-1')
            df.columns = [
                'COMERCIO',
                'REFERENCIA',
                'LOTE',
                'TIPO_OPERACION',
                'TARJETA',
                'TIPO_TARJETA',
                'FECHA_TRANSACCION_HORA',
                'FECHA_PROCESO',
                'FECHA_ABONO',
                'IMPORTE_TRANSACCION',
                'COMISION_TOTAL',
                'COMISION_IZIPAY',
                'COMISION_IGV',
                'IMPORTE_NETO',
                'TIPO_CAPTURA',
                'ESTADO_ABONO',
                'CTA_BANCARIA',
                'AUTORIZACION',
                'ID_TRANSACCION',
                'TERMINAL',
                'NRO_CUOTAS',
                'CASHBACK',
                'OBSERVACIONES',
                'ID_OTROS'
            ]
            df = df.replace({'\$': '', ',': ''}, regex=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('cargó el df')
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)


