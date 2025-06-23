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
            

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file, lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        print(f'Uploading {filename} . . .')

        try:
            print('Ingresó al primer try')
            # Definir las columnas del DataFrame
            columns = ['numero_consecutivo_registro', 'numero_emisora', 'nombre_emisora', 'tipo_domiciliacion',
                       'referencia_servicio_emisor', 'cuenta_cargo_domiciliatario', 'fecha_operacion',
                       'fecha_contracargo', 'motivo', 'importe']
            df = pd.DataFrame(columns=columns)
            # Leer el archivo línea por línea desde el objeto BytesIO
            for i, linea in enumerate(file.getvalue().splitlines()):
                # Decodificar la línea de binario a texto
                linea = linea.decode('utf-8') 
                # Extraer datos de cada línea
                numero_consecutivo_registro = linea[:8]
                numero_emisora = linea[8:12]
                nombre_emisora = linea[12:51]
                tipo_domiciliacion = linea[51:65]
                referencia_servicio_emisor = linea[65:105]
                cuenta_cargo_domiciliatario = linea[105:126]
                fecha_operacion = linea[126:136]
                fecha_contracargo = linea[136:146]
                motivo = linea[146:188]
                importe = linea[188:203].lstrip(b'0'.decode('utf-8'))

                # Agregar datos al DataFrame
                df.loc[i] = [
                    numero_consecutivo_registro,
                    numero_emisora,
                    nombre_emisora,
                    tipo_domiciliacion,
                    referencia_servicio_emisor,
                    cuenta_cargo_domiciliatario,
                    fecha_operacion,
                    fecha_contracargo,
                    motivo,
                    importe
                ]
            print('leyó archivo')
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('cargo el df')
            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)
        print('Se retorna df')