from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper
import boto3
import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
import os
import os.path
import pytz

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
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class OcaFinanciera:
    
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')

            # Financiera columns
            col_list = ["fecha_facturacion","neto_a_cobrar","fecha_vencimiento","producto",
                        "subproducto","importe_retencion","fecha_pago","rut","importe_sobre",
                        "cod_producto","retencion_del_minorista","id_liquidacion","arancel",
                        "porcentaje_arancel","importe_promocion","importe_leyes","cod_moneda",
                        "iva_arancel","recargo_financiacion","nro_comercio","fecha_liquidacion",
                        "id_sobre","iva_recargo_financiacion","plan_venta","neto_calculado"]

            df = pd.read_csv(file, names=col_list, header=0)
            
            print('Se leyo el archivo')

            # Adicion de trazabilidad
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
            df['skt_extraction_rn'] = df.index.values
            print('Cargo el df')
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')

class OcaTransaccional:
    
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')

            df = pd.read_csv(file, low_memory=False)
            
            print('Se leyo el archivo')

            # Adicion de trazabilidad
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
            df['skt_extraction_rn'] = df.index.values
            print('Cargo el df')
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')