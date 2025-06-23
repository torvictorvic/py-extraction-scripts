import boto3
import numpy as np
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
            session = boto3.session.Session()
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

class santander:
    @staticmethod
    def run(filename, **kwargs):
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        
        
        print(f'Uploading {filename} . . .')
        try:
            print('Ingresó al primer try')
            try:
                file,lm = FileReader.read(filename)
                upload_date = lm.astimezone(new_timezone)
                df = pd.read_csv(file,encoding='latin1',dtype=str,sep=',')
                df.columns = ["Fecha_de_transaccion","Fecha_de_presentacion","Fecha_de_liquidacion","Fecha_de_pago_abono","Referencia","RefundId","Referencia_CBK",
                              "Numero_de_liquidacion","Codigo_de_comercio","Marca_de_tarjeta","Tipo_de_tarjeta","Origen_de_tarjeta","Numero_de_tarjeta",
                              "Codigo_de_autorizacion","Cupon_de_la_transaccion","Tipo_de_operacion","Cuotas","Cuota_liquidada","Importe_bruto","Importe_neto",
                              "Tasa_de_procesamiento","Costo_de_procesamiento","IVA_costo_procesamiento","Sobre_tasa","Costo_de_financiacion","IVA_costo_financiacion",
                              "Tasa_de_intercambio","Importe_de_intercambio","IVA_de_Intercambio","Monto_bruto_a_devolver","IVA_del_Ajuste","Monto_total_a_devolver",
                              "Fecha_de_pago"]
                df = df[df.columns[0:]].replace(',', '.', regex=True)
                print(',')
            except:
                file,lm = FileReader.read(filename)
                upload_date = lm.astimezone(new_timezone)
                df = pd.read_csv(file,encoding='latin1',dtype=str,sep=';')
                df.columns = ["Fecha_de_transaccion","Fecha_de_presentacion","Fecha_de_liquidacion","Fecha_de_pago_abono","Referencia","RefundId","Referencia_CBK",
                              "Numero_de_liquidacion","Codigo_de_comercio","Marca_de_tarjeta","Tipo_de_tarjeta","Origen_de_tarjeta","Numero_de_tarjeta",
                              "Codigo_de_autorizacion","Cupon_de_la_transaccion","Tipo_de_operacion","Cuotas","Cuota_liquidada","Importe_bruto","Importe_neto",
                              "Tasa_de_procesamiento","Costo_de_procesamiento","IVA_costo_procesamiento","Sobre_tasa","Costo_de_financiacion","IVA_costo_financiacion",
                              "Tasa_de_intercambio","Importe_de_intercambio","IVA_de_Intercambio","Monto_bruto_a_devolver","IVA_del_Ajuste","Monto_total_a_devolver",
                              "Fecha_de_pago"]
                df = df[df.columns[0:]].replace(',', '.', regex=True)
                print(';')
            print('Contenido ', lm)
            print('leyó archivo')
            print('definió columnas proporcionadas por el archivo')
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('cargo el df')
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)