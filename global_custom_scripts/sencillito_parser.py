import pytz
import boto3
import pandas as pd

from enum import Enum
from pandas import DataFrame
from datetime import datetime
from io import StringIO, BytesIO
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
            #session = boto3.session.Session()
            session = boto3.session.Session(region_name='us-east-1')
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
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            positions = [0,8,23,31,51,61,63,73,109,123,137,152,154,423]
            col_specification =[]
            for i in range(0,len(positions) -1):
                cordenate = (positions[i],positions[i+1] )
                col_specification.append(cordenate)
            print('sale del for')
            binary_df = StringIO(file.read().decode().strip().strip('\x00'))
            print('sale del binary')
            cols = ["fecha_de_autorizacion","codigo_de_autorizacion","numero_de_convenio","numero_de_cliente","monto","estado_de_la_transaccion","transaccion_id_igt","transaccion_id_del_canal","fecha_de_pago","fecha_de_anulacion","codigo_de_anulacion","codigo_de_conciliacion","mensaje_de_conciliacion"]
            df = pd.read_fwf(binary_df, colspecs=col_specification, header=None, dtype=object)
            df.columns = cols
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns = ["fecha_de_autorizacion","codigo_de_autorizacion","numero_de_convenio","numero_de_cliente","monto","estado_de_la_transaccion","transaccion_id_igt","transaccion_id_del_canal","fecha_de_pago","fecha_de_anulacion","codigo_de_anulacion","codigo_de_conciliacion","mensaje_de_conciliacion"]
            df = pd.DataFrame(columns = columns)
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
