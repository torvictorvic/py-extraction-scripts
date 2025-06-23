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
import csv
import re

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
        dialect = csv.Sniffer().sniff(file.getvalue().decode("latin1"))
        df = pd.read_csv(file,dtype=str,sep=dialect.delimiter,encoding='latin1')
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df = df[filter(lambda x: "Unnamed" not in str(x),df.columns)]
        df.columns = ["numero_radicado","fecha_radicado","numero_cedula","nombre_y_apellido","valor_transaccion","valor_iva","fecha_transaccion","codigo_unico_sucursal","nombre_sucursal","cuotas","codigo_autorizacion","auto_cargue","subtotal_compra","valor_neto","rete_fuente","comision","rete_iva","rete_ica","fondos_mercadeo","propina","impo_consumo","esp","tipo_transaccion","categoria_tarjeta"]
        #df.columns = ["transaccion","numero_cedula","nombre_apellido","valor_transaccion","valor_iva","fecha_transaccion","fecha_recaudacion","codigo_unico_sucursal","nombre_sucursal","cuotas","codigo_autorizacion","autocargue","subtotal_compra","valor_neto","rete_fuente","comision","rete_iva","rete_ica","fondos_mercadeo","propina","esp"]
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class ExtractorDebitos:
    @staticmethod
    def get_format(cadena):
        #En esta función se valida el formato de fecha_compra
        formatos = dict(formato1="([0-2]{1}[0-9]{1}|30|31)/(01|02|03|04|05|06|07|08|09|10|11|12)/[0-9]{4}",
                        formato2="([0-2]{1}[0-9]{1}|30|31)-(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)-[0-9]{2}",
                        formato3 ="[0-9]{4}-(01|02|03|04|05|06|07|08|09|10|11|12)-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}",
                        formato4 = "[0-9]{4}-(01|02|03|04|05|06|07|08|09|10|11|12)-[0-9]{2}",
                        formato5 = "([0-2]{1}[0-9]{1}|30|31)-(01|02|03|04|05|06|07|08|09|10|11|12)-[0-9]{2}")
        
        formatos_re = re.compile("|".join("(?P<%s>%s)" % item for item in formatos.items()), flags=re.IGNORECASE)
        group_to_format = dict(formato1 ="%d/%m/%Y %H:%M:%S", formato2="%d-%b-%Y", 
                       formato3 = '%Y-%m-%d %H:%M:%S', formato4 = '%Y-%m-%d', formato5 = '%d-%m-%y')

        
        match = formatos_re.fullmatch(cadena)

        if(match is None):
            cumple = False
        else:
            cumple = True

        return cumple

    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file,dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fecha_compra","no_autorizacion","tipo_tarjeta","vr_inicial_compra","vr_iva","valor_neto","codigo_sucursal","fecha_solicitud","enviado_por","valor_reverso_tx","comision","fondo","retefuente","reteica","reteiva","fecha_movimiento_bancario","observaciones"]

        for i in range(len(df['fecha_compra'])):
            cumple = ExtractorDebitos.get_format(df['fecha_compra'][i])
            if cumple == False:
                df['fecha_compra'][i] = ''

        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
