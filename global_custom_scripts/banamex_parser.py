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
                return uri, datetime.today()

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
        try:
            df = pd.read_csv(file,dtype=str)
            df.columns = ["id_comerciante","id_pedido","referencia_pedido","id_transaccion","id_adquirente","fecha_transaccion","metodo_pago","tipo_transaccion","monto","moneda","codigo_respuesta_motor_pagos_transaccion","identificador_cuenta","titular_cuenta","codigo_autorizacion","codigo_respuesta_adquirente","numero_lote_adquirente","fuente_transaccion","codigo_respuesta_motor_pagos_para_avs","codigo_respuesta_motor_pagos_para_csc","estado_autenticacion","3_d_secure_acs_eci","rrn","id_cliente"]
            df['monto']= df['monto'].apply(lambda x: x.replace(',',''))
            fecha = sorted(list(set(list(map(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d') ,df['fecha_transaccion'])))))[1]
            df = df[pd.to_datetime(df['fecha_transaccion']).dt.strftime('%Y-%m-%d') == fecha]
        except pd.io.common.EmptyDataError:      
            columns = ["id_comerciante","id_pedido","referencia_pedido","id_transaccion","id_adquirente","fecha_transaccion","metodo_pago","tipo_transaccion","monto","moneda","codigo_respuesta_motor_pagos_transaccion","identificador_cuenta","titular_cuenta","codigo_autorizacion","codigo_respuesta_adquirente","numero_lote_adquirente","fuente_transaccion","codigo_respuesta_motor_pagos_para_avs","codigo_respuesta_motor_pagos_para_csc","estado_autenticacion","3_d_secure_acs_eci","rrn","id_cliente"]
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class ExtractorChargebacks:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        df = pd.read_excel(file,dtype=str)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        if df.empty:
            columns =['compania', 'afiliacion', 'importe', 'costo_operativo', 'iva','fecha_aplicacion', 'naturaleza', 'concepto', 'txs', 'remesa','terminacion_tc', 'autorizacion']
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(),ignore_index=True)
        else:
            # returns datetime in the new timezone
            df.columns =['compania', 'afiliacion', 'importe', 'costo_operativo', 'iva','fecha_aplicacion', 'naturaleza', 'concepto', 'txs', 'remesa','terminacion_tc', 'autorizacion']
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


