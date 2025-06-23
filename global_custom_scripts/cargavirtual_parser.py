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


            session = boto3.session.Session()
            s3 = session.client('s3')

            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode()
            return obj,lm
        else:
            with open(uri) as f:
                return f.read(),datetime.now()

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
            header_df = StringIO(file)
            binary_df = StringIO(file)
            footer_df = StringIO(file)
            header_specification = [(0, 8), (8, 16)]
            cols_header = ['fecha_header','name_header']
            header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object)
            header.columns= cols_header
            col_specification = [(0, 5), (5, 13), (13, 21), (21, 32),(32,42),(42,54),(54,64),(64,72),(72,83),(83,92),(92,122),(122,122)]
            cols = ['punto_venta','fecha_cobro','fecha_peticion','num_pago_mp',
                    'id_referencia_tx_mp','id_metodo_pago_mp','id_user_mp','fecha_compra_mp',
                    'importe','comision','num_tx_seac','flag_carga']
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
            df.columns = cols
            footer_specification = [(0, 17), (17, 27),(27, 37)]
            cols_footer = ['total_importe_footer','total_comision_footer','total_registros_footer']
            footer = pd.read_fwf(footer_df, colspecs=footer_specification, skiprows=(int(len(df))), header=None, dtype=object)
            footer.columns= cols_footer    
            df['fecha_header'] =header.loc[0,'fecha_header'] 
            df['titulo_header'] =header.loc[0,'name_header'] 
            df['total_importe_footer'] =footer.loc[0,'total_importe_footer'] 
            df['total_comision_footer'] =footer.loc[0,'total_comision_footer'] 
            df['total_registros_footer'] =footer.loc[0,'total_registros_footer'] 
            df.comision = df.comision.astype('float')
            df.importe = df.importe.astype('float')
            df.total_importe_footer = df.total_importe_footer.astype('float')
            df.total_comision_footer = df.total_comision_footer.astype('float')
            df.total_registros_footer = df.total_registros_footer.astype('int')
            df = df.iloc[:-1,:]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:

            columns= ["punto_venta","fecha_cobro","fecha_peticion","num_pago_mp","id_referencia_tx_mp","id_metodo_pago_mp","id_user_mp","fecha_compra_mp","importe","comision","num_tx_seac","flag_carga","fecha_header","titulo_header","total_importe_footer","total_comision_footer","total_registros_footer"]
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
