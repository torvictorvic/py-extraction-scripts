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
import numpy as np

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
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        if ('.csv' in filename.lower()):
            print('CSV')
            if (('SOPORTE_MERCADOPAGO' in filename.upper()) or
                ('AJUSTE_DEBITO' in filename.upper()) or
                ('REEMBOLSOS_MERCADOPAGO' in filename.upper())):
                print("Nuevos")
                df = pd.read_csv(file,dtype=str)
                # df = df.dropna(axis=1)
                df.columns =["fec_consi","fec_tr","fec_canje","tipo","origen","cuenta","tr","codigo","nom_estab","autori","tarjeta","t","marca","franq","compra","iva","val_inc","propina","total","descuento","comision","ivacom","reterenta","reteiva","reteica","neto","ofi","cpbte","sec","red","term","observacion"]
                df["fec_consi"] =  df["fec_consi"].str.replace("-", "/", regex=True)
                df["fec_tr"] =  df["fec_tr"].str.replace("-", "/", regex=True)
                df["fec_canje"] =  df["fec_canje"].str.replace("-", "/", regex=True)
                df["compra"] =  df["compra"].str.replace("[^\d.]", "", regex=True)
                df["iva"] =  df["iva"].str.replace("[^\d.]", "", regex=True)
                df["val_inc"] =  df["val_inc"].str.replace("[^\d.]", "", regex=True)
                df["propina"] =  df["propina"].str.replace("[^\d.]", "", regex=True)
                df["total"] =  df["total"].str.replace("[^\d.]", "", regex=True)
                df["descuento"] =  df["descuento"].str.replace("[^\d.]", "", regex=True)
                df["comision"] =  df["comision"].str.replace("[^\d.]", "", regex=True)
                df["ivacom"] =  df["ivacom"].str.replace("[^\d.]", "", regex=True)
                df["reterenta"] =  df["reterenta"].str.replace("[^\d.]", "", regex=True)
                df["reteiva"] =  df["reteiva"].str.replace("[^\d.]", "", regex=True)
                df["reteica"] =  df["reteica"].str.replace("[^\d.]", "", regex=True)
                df["neto"] =  df["neto"].str.replace("[^\d.]", "", regex=True)
            else:
                print('Viejos')
                df = pd.read_csv(file,dtype=str)
                df = df.dropna(axis=1)
                df.columns =["fec_consi","fec_tr","fec_canje","tipo","origen","cuenta","tr","codigo","nom_estab","autori","tarjeta","t","marca","franq","compra","iva","val_inc","propina","total","descuento","comision","ivacom","reterenta","reteiva","reteica","neto","ofi","cpbte","sec","red","term"]
                df['observacion'] = np.nan
        else:
            print('ExCEL')
            df = pd.read_excel(file,dtype=str)
            df.columns = ["fec_consi","fec_tr","fec_canje","tipo","origen","cuenta","tr","codigo","nom_estab","autori","tarjeta","t","marca","franq","compra","iva","val_inc","propina","total","descuento","comision","ivacom","reterenta","reteiva","reteica","neto","ofi","cpbte","sec","red","term","observacion"]
            df['observacion'] = df['observacion'].str.replace('\n', ' ')
        
        fechas = ["fec_consi","fec_tr","fec_canje"]
        df['fec_tr'] = list(map(lambda x : x.replace('ago','aug').replace('ene','jan').replace('abr','apr').replace('dic','dec'),df['fec_tr']))
        
        for fecha in fechas:
            df[fecha]=pd.to_datetime(df[fecha],dayfirst = True).astype(str)   
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1]).replace('.TXT','.csv')
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df