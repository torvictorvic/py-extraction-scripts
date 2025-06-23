import zipfile_deflate64
from urllib.parse import urlparse
import io
from io import BytesIO,StringIO,TextIOWrapper
import boto3
import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
import os
import os.path
import pytz
import zipfile

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
            print(uri)

            if ".zip" in uri:
                Bucket=s3_url.bucket
                obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()
                zip_file = zipfile.ZipFile(io.BytesIO(obj))
                for fn in zip_file.namelist():
                    print('ingreso al for...')
                    # Now copy the files to the 'unzipped' S3 folder 
                    print(f"Copying file {fn}") 
                    s3.put_object( 
                    Body=zip_file.open(fn).read(),
                    Bucket=Bucket, 
                    Key='ARG_CVU/LIQUIDACIONES_CVU/'+ fn
                    )                     
                df = None
                return df

            elif ".csv" in uri:
                obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
                lm = obj['LastModified']
                obj = obj['Body'].read()
                binary_data = BytesIO(obj)
                return binary_data,lm
            
        else:
            with open(uri) as f:
                return uri,datetime.today()

class ExtractorCVU:
    
    @staticmethod
    def run(filename, **kwargs):
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        # print(type(file))
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')
            if '.csv' in filename:
                file,lm = FileReader.read(filename)
                upload_date = lm.astimezone(new_timezone)
                print("Is a csv file")
                # Lectura de dataframe
                df_ = pd.read_csv(file, encoding='latin-1', sep=';', header=0, engine='python', index_col=False)
                file = []
                print('Se leyo el archivo')

                # Delete wrong column
                if df_.columns[0] != "BANCO":
                    df_.drop(df_.columns[[0]], axis=1, inplace=True)

                # Update column name
                df = df_.rename({'FINANCIAL ENTITY': 'FINANCIAL_ENTITY'}, axis='columns')

                # Adicion de trazabilidad
                df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                out = filename.split('/')[-1]
                df['filename'] = filename
                df.reset_index(drop=True, inplace=True)
                df.index = df.index + 1
                df['skt_extraction_rn'] = df.index.values
                print('Cargó el df')
                return df
            
            elif '.zip' in filename:
                print('leyendo .zip con solucion deflate64')
                obj = FileReader.read(filename)
                df = None
                return df

            else:
                print("cvs or zip files didn't found")
            
        except Exception as e:
            print("Error al subir la fuente: ",e)
            
        print('Se retorna df')

class ExtractorBalanceCVU:
    
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        # print(type(file))
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')

            # Lectura de dataframe
            df = pd.read_csv(file, sep=";")
            print('Se leyo  el archivo')

            # Adicion de trazabilidad
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['filename'] = filename
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
            df['skt_extraction_rn'] = df.index.values
            print('Cargó el df')
            
            return df 
            
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')