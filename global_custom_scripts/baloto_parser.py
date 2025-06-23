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
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file, header=None)
        df['fecha_pagos'] = df.iloc[0][0]
        df['fecha_pagos'] = df['fecha_pagos'].str.slice(12, 20, 1)
        # #borra primeras dos columnas 
        df['referencia_pago'] = df[0].str.slice(3, 50, 1)
        df['valor'] = df[0].str.slice(50, 62, 1)
        df['total_registros_lote'] = list(df.iloc[-2])[0][2:11]
        df['valor_servicio_principal'] = list(df.iloc[-2])[0][11:27]
        df['lote'] = list(df.iloc[-2])[0][29:33]
        df = df.iloc[2:-2]
        df = df.drop(0, 1)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        df['original_filename'] = out
        return df

class ExtractorPagos:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file, dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["textbox4","textbox1","textbox54","textbox60","textbox63","codigo_proyecto","proyecto","forma_pago","fecha_inicial","fecha_final","total_cupones","total_movilizado","total_comision","total_retefuente","total_ica","total_iva","total_reteiva","total_timbre","total_cupones_notas_debito","proliqtotalnd","proliqvalorcomisionnd","proliqtotalrtefuentend","proliqtotalimpicand","proliqtotalivand","proliqtotalrteivand","proliqtotalimptimbrend","proliqtotalcuponescre","proliqtotalnc","proliqvalorcomisionnc","proliqtotalrtefuentenc","proliqtotalimpicanc","proliqtotalivanc","proliqtotalrteivanc","proliqtotalimptimbrenc","proliqfechagrabacionbd","proliqtotalapagar"]
        df = df.replace({'\$': '', ',': ''}, regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        df['original_filename'] = out
        return df

class ExtractorComisiones:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file, dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = [
                        'textbox670',	 
                        'textbox672',	 
                        'textbox695',	 
                        'textbox697',	 
                        'textbox700',	 
                        'group1',	 
                        'detliqproyecto',	 
                        'prodescripcion',	 
                        'detliqfecha',	 
                        'detliqtiporecaudo',	 
                        'detliqtipooperacion',	 
                        'detliqcodps2',	 
                        'detliqdocumento',	 
                        'detliqtipo',	 
                        'detliqgrupo',	 
                        'detliqvalor',	 
                        'detliqrtefte',	 
                        'detliqcomisioninformativa',	 
                        'detliqrteiva',	 
                        'detliqiva',	 
                        'detliqica',	 
                        'detliqimptim',	 
                        'detliqtotal',	 
                        'detliqfechacredb',	 
                        'detliqtarifaxoperaciones',	 
                        'detliqdescripciontarifaxoper',	 
                        'detliqtarifaxmovilizado',	 
                        'detliqdescripciontarifaxmovil',	 
                        'detliqcantcupones',	 
                        'detliqobservaciones',	 
                        'proliqdocumentocont'
                    ]
        df = df.replace({'\$': '', ',': ''}, regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

