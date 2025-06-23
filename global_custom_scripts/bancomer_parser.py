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
import msoffcrypto

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

            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            session = boto3.session.Session()
            s3 = session.client('s3')

            # aws_access_key_id = AWS_ACCESS_KEY,
            # aws_secret_access_key = AWS_SECTRET_KEY  
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode("utf-8").encode('ascii', 'xmlcharrefreplace')
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri, datetime.today()


class FileReaderDebits:

    @staticmethod
    def read(uri: str):
        origin = urlparse(uri, allow_fragments=False)
        if origin.scheme in ('s3', 's3a'):

            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            session = boto3.session.Session()
            s3 = session.client('s3')

            # aws_access_key_id = AWS_ACCESS_KEY,
            # aws_secret_access_key = AWS_SECTRET_KEY  
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri, datetime.today()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

class ExtractorChargebacks:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReaderDebits.read(filename)  
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file,dtype=str)
        if df.empty:
            columns = ["id","afiliacion","importe","cuenta","plataforma","num_autorizacion","usuario_acepto_cargo","fecha_aplicacion","numero_referencia","fecha_transaccion","fecha_remesa","folio_Eglobal","origen"]        # df.columns = formato
            df = pd.DataFrame(columns=columns)
            df = df.append(pd.Series(),ignore_index=True)
        else:
            columns = ["id","afiliacion","importe","cuenta","plataforma","num_autorizacion","usuario_acepto_cargo","fecha_aplicacion","numero_referencia","fecha_transaccion","fecha_remesa","folio_Eglobal","origen"]        # df.columns = formato
            df.columns = columns
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
        df['importe'] = df['importe'].apply(lambda x : x.replace(',','') if ',' in x else x)
        return df


class ExtractorDebits:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReaderDebits.read(filename)  
        print(f'Uploading {filename} . . .')
        # try:
        try:
            df = pd.read_excel(file, dtype=str)
        except:
            file = msoffcrypto.OfficeFile(file)
            file.load_key(password="Gestion*2018")
            file.decrypt(open("/tmp/decrypted.xlsx", "wb"))
            df = pd.read_excel("/tmp/decrypted.xlsx",dtype=str)

        df.columns  = ["no_afiliac","fe_txn","num_autori","num_ref","no_plastic","plataforma","imp_txn","cod_err","fecha_lote"]
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
        byte_str = file.read()
        text_obj = byte_str.decode('UTF-8')
        text_obj = text_obj.replace('&#243;','ó')
        file = StringIO(text_obj)
        print(f'Uploading {filename} . . .')
        widths = [43,13,6,2,10,30,7,27,30,12,22,10,2,15,45,3,22,22]
        try:
            df = pd.read_fwf(file,header=None,widths=widths,dtype=str,encoding='utf-8')
            df.columns = ["fecha_pago","nombre_comercio","concepto","tipo_tarjeta_corto","tipo_pago_id_mp1","numero_tarjeta","autorizacion","id_pago","secuencia_transmision","referencia","monto_pagado","fecha_de_dispersion","promocion_tarjeta","afiliacion","status","tipo_tarjeta","comision_comercio","iva_comision"]
        except pd.io.common.EmptyDataError:  
            columns = ["fecha_pago","nombre_comercio","concepto","tipo_tarjeta_corto","tipo_pago_id_mp1","numero_tarjeta","autorizacion","id_pago","secuencia_transmision","referencia","monto_pagado","fecha_de_dispersion","promocion_tarjeta","afiliacion","status","tipo_tarjeta","comision_comercio","iva_comision"]
            df = pd.DataFrame(columns=columns) 
            df = df.append(pd.Series(),ignore_index=True)
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

