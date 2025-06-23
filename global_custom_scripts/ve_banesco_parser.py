#banesco_parser.py
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
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Extractor_Liq:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone) 
        print(type(file))
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingresó al try')
            #Header
            df = pd.read_csv(file,encoding='latin1', sep='\t', header=None)
            header=df[0][0]
            afiliacion=df[0][3]
            acquirer_h=header[8:30].strip()
            fecha_file_h=header[-10:]
            afiliacion_h=afiliacion[-10:]
            #Data
            file,lm = FileReader.read(filename)
            df = pd.read_csv(file,encoding='latin1', delim_whitespace = True, skiprows=9, header=None)
            df=df.iloc[:, 0:8]
            df.columns=['tarjeta','f_trans','auto','monto_bruto','porcentaje_com','comision','i_s_l_r','monto_neto',]
            df=df[df.tarjeta.str.contains('\*\*\*\*\*|FECHA')].reset_index( drop=True)
            df['liq_fecha_liquidacion_h']='NaN'
            df['auto'].fillna(0, inplace=True)
            for i in range(len(df)):
              if df.tarjeta[i] =='FECHA':
                if df.auto[i]== 0 :
                  Fecha_h=df.f_trans[i] 
                else: 
                  Fecha_h=df.auto[i]
              else:
                df['liq_fecha_liquidacion_h'][i]=Fecha_h
            df["liq_fecha_liquidacion_h"] = df["liq_fecha_liquidacion_h"].replace({':':''}, regex=True)
            df=df.dropna().reset_index( drop=True)
            df['acquirer_h']=acquirer_h
            df['fecha_pago_h']=fecha_file_h
            df['afiliacion_h']=afiliacion_h
            #Columnas Etiquetas
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('Se retorna df')
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)

class Extractor_Liq_deb:
    @staticmethod
    def run(filename, **kwargs):
        file, lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print(type(file))
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingresó al try')
            # Header
            df = pd.read_csv(file,
                             encoding='latin1',
                             sep='\t',
                             header=None)
            header = df[0][0]
            afiliacion = df[0][4]
            acquirer_h = header[8:30].strip()
            fecha_file_h = header[-10:]
            afiliacion_h = afiliacion[-10:]
            
            # Data

            file, lm = FileReader.read(filename)
            df = pd.read_csv(file,
                             encoding='latin1',
                             delim_whitespace=True,
                             skiprows=29,
                             header=None)
            df = df.iloc[:, 0:7]
            df.columns = ['tarjeta',
                          'auto',
                          'monto_bruto',
                          'porcentaje_com',
                          'comision',
                          'monto_neto',
                          'STS', ]

            df = df[df.tarjeta.str.contains('\*\*\*\*\*|FECHA')].reset_index(drop=True)
            df['liq_fecha_liquidacion_h'] = 'NaN'
            df['auto'].fillna(0, inplace=True)

            for i in range(len(df)):
                if df.tarjeta[i] == 'FECHA':
                    if df.auto[i].strip() == ':':
                        Fecha_h = df.monto_bruto[i].strip()
                    else:
                        Fecha_h = df.auto[i][1:]
                else:
                    df['liq_fecha_liquidacion_h'][i] = Fecha_h

            df.drop(df[df.tarjeta.str.contains('FECHA')].index, inplace=True)
            df.reset_index(drop=True, inplace=True)
            df["liq_fecha_liquidacion_h"] = df["liq_fecha_liquidacion_h"].replace({':':''}, regex=True)
            df["f_trans"] = df["liq_fecha_liquidacion_h"]
            df['acquirer_h'] = acquirer_h
            df['fecha_pago_h'] = fecha_file_h
            df['afiliacion_h'] = afiliacion_h
            # Columnas Etiquetas
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('Se retorna df')
            print("...")
            return df

        except Exception as e:
            print("Error al subir la fuente: ", e)