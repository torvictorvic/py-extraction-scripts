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
    def run(filename, **kwargs):
        tipo_tabla = kwargs['tipo_tabla']
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        widths_b1 = [2,2,7,6,4,4,4,1,1,6,14,1,4,4,10,1,1,2,5,1]
        widths_b2 = [2,2,7,6,4,4,4,4,10,9,4,10,1,4,1,1,1,5,1,40,36,100]

        try:
            # b1 records
            b1 = StringIO(file)
            df_b1 = pd.read_fwf(b1,dtype=object,widths=widths_b1,header=None)
            formato_b1 = ["cr","codigo_da_unidade_operacional","terminal","nsu","data","hora","data_lan","mo","status","cp","valor","tipo_conta","if","agencia","nro_conta","via_do_cartao","indicador_t","n_parc","nsu_reg","ie"]
            df_b1.columns = formato_b1
            df_b1 = df_b1.copy().loc[(df_b1["cr"] == 'B1')]
            df_b1.reset_index(drop=True,inplace=True)

            df_b1['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_b1['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df_b1['file_name'] = out
            df_b1.reset_index(drop=True)
            df_b1['skt_extraction_rn'] = df_b1.index.values

            # b2 records
            b2 = StringIO(file)
            df_b2 = pd.read_fwf(b2,dtype=object,widths=widths_b2,header=None)
            formato_b2 = ["cr","cod_da_unidade_operacional","terminal","nsu","data","hora","if","agencia","nro_conta","nsu_host","filler","cod_do_fundo","senha_de_prot","co","chip","idioma","filler_1","nsu_reg","ie","cards_id","bari_id","authorization_id"]
            df_b2.columns = formato_b2
            df_b2 = df_b2.copy().loc[(df_b2["cr"] == 'B2')]
            df_b2.drop(['filler',"filler_1"],axis=1,inplace=True)
            df_b2.reset_index(drop=True,inplace=True)

            df_b2['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_b2['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df_b2['file_name'] = out
            df_b2.reset_index(drop=True)
            df_b2['skt_extraction_rn'] = df_b2.index.values

            if tipo_tabla == 'b1':
                return df_b1

            if tipo_tabla == 'b2':
                return df_b2

        except pd.io.common.EmptyDataError as e:
            formato_b1 = ["cr","codigo_da_unidade_operacional","terminal","nsu","data","hora","data_lan","mo","status","cp","valor","tipo_conta","if","agencia","nro_conta","via_do_cartao","indicador_t","n_parc","nsu_reg","ie"]            
            df_b1 = pd.DataFrame(columns = formato_b1)
            df_b1 = df_b1.append(pd.Series(), ignore_index=True)
            df_b1['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_b1['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df_b1['file_name'] = out
            df_b1.reset_index(drop=True)
            df_b1['skt_extraction_rn'] = df_b1.index.values

            formato_b2 = ["cr","cod_da_unidade_operacional","terminal","nsu","data","hora","if","agencia","nro_conta","nsu_host","filler","cod_do_fundo","senha_de_prot","co","chip","idioma","filler_1","nsu_reg","ie","cards_id","bari_id","authorization_id"]
            df_b2 = pd.DataFrame(columns = formato_b2)
            df_b2 = df_b2.append(pd.Series(), ignore_index=True)
            df_b2['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_b2['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df_b2['file_name'] = out
            df_b2.reset_index(drop=True)
            df_b2['skt_extraction_rn'] = df_b2.index.values

            if tipo_tabla == 'b1':
                return df_b1

            if tipo_tabla == 'b2':
                return df_b2

            
        except Exception as e:
            print("Error al subir la fuente: ",e)