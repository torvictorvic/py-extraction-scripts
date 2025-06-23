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

            session = boto3.session.Session()
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
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
        colspecs = [
            (0,1),
            (1,17),
            (17,25),
            (25,38),
            (38,40),
            (40,50),
            (50,58),
            (58,66),
            (66,74),
            (74,82),
            (82,84),
            (84,88),
            (88,90),
            (90,103),
            (103,116),
            (116,129),
            (129,142),
            (142,152),
            (152,None)
            ]

        widths_st = [2,8,12,12,2,12,12,12]
        widths_t = [1,8,12,12,2,12,12,12]

        try:
            
            body_df = StringIO(file)
            footer1_df = StringIO(file)
            footer2_df = StringIO(file)

            # Main Dataframe
            df = pd.read_fwf(body_df,dtype=object,colspecs=colspecs,header=None)
            formato = ["tipo_de_linea","nro_tarjeta","fecha","importe","cant_cuotas","sucursal","terminal","fecha_presentacion","fecha_liquidacion","fecha_vencimiento","tipo_de_transaccion","nro_ticket","moneda","dgi","comision","iva","neto","nro_autorizacion","externalid"]
            df.columns = formato

            # Extract and append footer 1

            df_st = pd.read_fwf(footer1_df,dtype=object,widths=widths_st,header=None)
            #df_st = df_st.iloc[-2,:]
            df_st = df_st[df_st[0]=='ST']
            #df_st = df_st.to_frame()
            #df_st = df_st.transpose()
            formato_st = ["st_identificador","st_fecha","st_importe_total_bruto","st_importe_total_liquido","st_moneda","st_dgi","st_comision","st_iva"]
            df_st.columns = formato_st
            df_st = df_st.reset_index()
            for column in df_st.columns:
                df[column] = df_st.loc[0,column]
            # Extract and append footer 2
            df_t = pd.read_fwf(footer2_df,dtype=object,widths=widths_t,header=None)
            df_t = df_t.iloc[-1,:]
            df_t = df_t.to_frame()
            df_t = df_t.transpose()
            formato_t = ["t_identificador","t_fecha","t_importe_total_bruto","t_importe_total_liquido","t_moneda","t_dgi","t_comision","t_iva"]
            df_t.columns = formato_t
            df_t = df_t.reset_index()
            for column in df_t.columns:
                df[column] = df_t.loc[0,column]
            # adding outputs
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            #df = df.iloc[:-2,:]
            df = df[df['tipo_de_linea'] != 'S']
            df = df.reset_index(drop=True)
            df = df.iloc[:-1,:]
            print('retono df')
            return df
        except pd.io.common.EmptyDataError as e:
            formato = ['tipo_de_linea','nro_tarjeta','fecha','importe','cant_cuotas','sucursal','terminal','fecha_presentacion','fecha_liquidacion','fecha_vencimiento','tipo_de_transaccion','nro_ticket','moneda','dgi','comision','iva','neto','nro_autorizacion','externalid','index','st_identificador','st_fecha','st_importe_total_bruto','st_importe_total_liquido','st_moneda','st_dgi','st_comision','st_iva','t_identificador','t_fecha','t_importe_total_bruto','t_importe_total_liquido','t_moneda','t_dgi','t_comision','t_iva','upload_date','report_date','file_name','skt_extraction_rn']
            df = pd.DataFrame(columns = formato)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('retono df')
            return df
            
        except Exception as e:
            print("Error al subir la fuente: ",e)
