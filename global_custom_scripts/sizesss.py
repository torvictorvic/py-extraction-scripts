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

def list_files_bkp(client,bucket,date):
    paginator = client.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket)
    df = pd.DataFrame(columns = ['FUENTE','ARCHIVO','FECHA_MODIFICACION','SIZE'])
    file_size = []
    for page in page_iterator:
        cont = page['Contents']
        for  obj in cont:
            fs = []
            if obj['LastModified'].date() >= date[0].date() and   obj['LastModified'].date() <= date[1].date()  :
                fs.append(str(obj['Key']).split('/')[-2]) # Fuente
                fs.append(str(obj['Key']).split('/')[-1]) # Archivo
                fs.append(obj['LastModified'].strftime('%Y-%m-%d')) # Fecha de modificación
                fs.append(obj['Size'])
                df = pd.concat([df,pd.DataFrame([fs], 
                        columns=['FUENTE','ARCHIVO','FECHA_MODIFICACION','SIZE'])])
    return df

    
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
            obj = obj['Body'].read().decode("utf-8").encode('ascii', 'xmlcharrefreplace')
            bytes_io = BytesIO(obj)
            return bytes_io,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()
class Extractor:
    @staticmethod
    def run(filename, **kwargs ):
        file,lm = FileReader.read(filename) 
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        print("checkpoint  try")
        df = pd.read_csv(file,dtype=object,sep=',',encoding='utf-8')
        client = boto3.client('s3')
        time_ini =  datetime.strptime(str(df['time_ini'][0]), '%Y-%m-%d %H:%M:%S' ) 
        time_ini = datetime.strptime(str(df['time_fin'][0]), '%Y-%m-%d %H:%M:%S' )  
        bucket = str(df['bucket'][0])
        date = [time_ini,time_ini]
        df  = list_files_bkp(client,bucket,date)
        print('INSERT DATAAA')
        return df