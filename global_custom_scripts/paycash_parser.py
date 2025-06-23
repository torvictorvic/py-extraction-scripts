import io
import os
import sys
import glob
import time
import math
import pytz
import numpy
import boto3
import os.path
import zipfile
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

class ExtractorCSV:
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
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str)
            df.columns = ["folio","secuencia","fecha","hora","cadena","sucursal","referencia","monto","comision","autorizacion","ref_emisor"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns = ["folio","secuencia","fecha","hora","cadena","sucursal","referencia","monto","comision","autorizacion","ref_emisor"]
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
        if "txt" in filename:
            buffer_h = BytesIO(file)
            buffer_trx = BytesIO(file)
            # header
            df_h = pd.read_csv(buffer_h,dtype=str,index_col=False,header=None,nrows=1)
            df_h.columns = ["header","fecha_inicial_h","fecha_final_h","recaudacion_h","comisiones_h","iva_comision_h","comision_trx_h","iva_comision_trx_h","factor_total_h","deposito_neto_h"]
            # trx
            df = pd.read_csv(buffer_trx,dtype=str,index_col=False,header=None,skiprows=1)
            df.columns = ["detalle","folio","referencia","fecha","hora","autorizacion","secuencia","importe","operaciones","comision_operacion","iva","comision","comision_transferencia","iva_comision_transferencia","factor","importe_bruto","iva_factor","comision_factor","neto","fecha_pago"]
            df['fecha_inicial_h'] = df_h["fecha_inicial_h"][0]
            df['fecha_final_h'] = df_h["fecha_final_h"][0]
            df['recaudacion_h'] = df_h["recaudacion_h"][0]
            df['comisiones_h'] = df_h["comisiones_h"][0]
            df['iva_comision_h'] = df_h["iva_comision_h"][0]
            df['iva_comision_trx_h'] = df_h["iva_comision_trx_h"][0]
            df['factor_total_h'] = df_h["factor_total_h"][0]
            df['deposito_neto_h'] = df_h["deposito_neto_h"][0]
            del df_h   
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        else:
            df =None
            return df
