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


class Extractor:
    @staticmethod
    def extractor_encabezado(filename):
      file,lm = FileReader.read(filename)
      df_1 = pd.read_csv(file,encoding='utf-8',dtype=str,sep=",", nrows=3,header=None)
      return df_1
    
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
            print('Ingreso al try')

            #Data Frame de transacciones
            df = pd.read_csv(file,encoding='utf-8',dtype=str,sep=",", header=3)
            last_row = df.shape[0]
                
            print('Leyo el df de transacciones')

            #DataFrame encabezado
            df_1 = Extractor.extractor_encabezado(filename)

            print('Leyo el df de encabezado')

            #Se eliminan los valores diferentes a SB
            mask = df['CH'] == 'SB'
            df = df[mask]

            print('Leyo el df de valores diferentes a SB')

            # Agregar tercer row como columna al DataFrame
            df['fecha_de_recep_archivo'] = df_1[2][2]

            # Cambio/Limpieza de nombre de columnas
            lista_columnas = list(df.columns)
            columnas = list()
            for i in lista_columnas:
                columna = i.lower()
                for letra_error, correccion in {"á":"a", "é":"e", "í":"i", "ó":"o", "ú":"u", " ":"_", ".":"", "(":"", ")":""}.items():
                    columna = columna.replace(letra_error, correccion)
                columnas.append(columna)
            df.set_axis(columnas, axis=1, inplace=True)
                        
            print('Se limpiaron los nombres de los columnas')

            # Adición de trazabilidad
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('Cargó el df')
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')