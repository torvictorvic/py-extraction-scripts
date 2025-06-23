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
import csv
from pandas.errors import EmptyDataError

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
            with open(uri,"rb") as f:
                return BytesIO(f.read()),datetime.today()

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
        print(f'Uploading {filename} . . .')

        try:
            cols=["numeral","servicio","producto","sucursal","dispositivo","fecha","hora","telefono","referencia","concepto","monto","id_transaccion","no_autorizacion","upc"]
            df = pd.read_csv(file,sep=",", dtype=str, skiprows=1, names=cols, header=0, index_col=False)
            #df = df.iloc[2:,:].reset_index(drop=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            df = df[:-1]
            return df

        except:
            
            try:
                dialect = csv.Sniffer().sniff(file.getvalue().decode())
                separador = dialect.delimiter
                if separador ==';':
                    separador =';'
                else:
                    separador = ','
                formato=["numeral","servicio","producto","sucursal","dispositivo","fecha","hora","telefono","referencia","concepto","monto","id_transaccion","no_autorizacion"]
                df = pd.read_csv(file,sep=separador, dtype=str, skiprows=1, names=formato, header=0, index_col=False)
                #df = df.iloc[2:,:].reset_index(drop=True)
                df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                out = filename.split('/')[-1]
                df['file_name'] = out
                df.reset_index(drop=True)
                df['skt_extraction_rn'] = df.index.values
                df = df[:-1]
                return df

            except pd.io.common.EmptyDataError as e:
                formato=["numeral","servicio","producto","sucursal","dispositivo","fecha","hora","telefono","referencia","concepto","monto","id_transaccion","no_autorizacion"]
                df = pd.DataFrame(columns = formato)
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

class ExtractorGestopago:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print(f'Uploading {filename} . . .')
        try:
            dialect = csv.Sniffer().sniff(file.getvalue().decode())
            separador = dialect.delimiter
            if separador ==';':
                separador =';'
            else:
                separador = ','
            formato=["numeral","servicio","producto","sucursal","dispositivo","fecha","hora","telefono","referencia","concepto","monto","id_transaccion","no_autorizacion","upc"]
            df = pd.read_csv(file,dtype=str,names=formato,sep=separador)
            df = df.iloc[2:,:]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            df = df[:-1]
            return df
        except EmptyDataError as e:
            formato=["numeral","servicio","producto","sucursal","dispositivo","fecha","hora","telefono","referencia","concepto","monto","id_transaccion","no_autorizacion","upc"]
            df = pd.DataFrame(columns = formato)
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

