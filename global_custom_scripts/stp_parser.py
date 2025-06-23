from typing import List
from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper
import pdb
import boto3 as boto3
import pandas as pd
from datetime import datetime
import glob
import json
import pandas as pd
import os
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
            session = boto3.session.Session()
            s3 = session.client('s3')
            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            # session = boto3.session.Session()
            # s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode("utf-8").encode('ascii', 'xmlcharrefreplace')
            bytes_io = BytesIO(obj)
            return bytes_io,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs ):
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        file,lm = FileReader.read(filename) 
        upload_date = lm.astimezone(new_timezone)
      
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,dtype=object,sep='|',encoding='utf-8')
            df.columns = ["bc_id","institucion","contraparte","rastreo","fecha_operacion","fecha_captura","fecha_liquidacion","tiempo_liquidacion","concepto_del_pago","beneficiario","causa_devolucion","clave_tipo_cta_benef","clave_tipo_cta_ord","clave_tipo_pago","cta_beneficiario","cta_ordenante","empresa","estado_de_la_orden","hora_captura","hora_envio_banxico","medio_entrega","monto","ordenante","rfc_curp_beneficiario","rfc_curp_ordenante","tipo_cta_beneficiario","tipo_cta_ordenante","tipo_pago","usuario"]
            df['upload_date'] = upload_date
            df['report_date'] = arg_datetime
            df['filename'] = filename.split('/')[-1]
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            df['cuenta'] = filename.split('/')[-2]
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorSaldo:
    @staticmethod
    def run(filename, **kwargs ):
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        file,lm = FileReader.read(filename) 
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,dtype=object,encoding='utf-8')
        df.columns = ["saldo"]
        df['upload_date'] = upload_date
        df['report_date'] = arg_datetime
        df['filename'] = filename.split('/')[-1]
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        df['cuenta'] = filename.split('/')[-2]
        return df

class ExtractorSelector:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando Selector de STP')
        tipo_tabla = kwargs['tipo_tabla']
        df = None
        if '-S.csv' in filename:
            if tipo_tabla == 'SALDO':
                df = ExtractorSaldo.run(filename)
        elif ('-R.csv' in filename) | ('-E.csv' in filename):
            if tipo_tabla == 'LIQUIDACIONES':
                df = Extractor.run(filename)
        return df
