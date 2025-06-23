import os
import pytz
import boto3
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper

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
            return obj,lm
        else:
            with open(uri,'rb') as f:
                return f.read(),datetime.today()

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
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        bufferA = BytesIO(file)
        bufferB = BytesIO(file)
        bufferC = BytesIO(file)
        try:
            df = pd.read_csv(bufferA, encoding="ISO-8859-1", sep=";",dtype=str )   
            if df.empty:
                columns = ["idpessoa","razaosocial","nomefantasia","cnpj","idoperador","login","nomeoperador","numerocaixa","dataaberturacaixa","datafechamentocaixa","numerooperacao","dataoperacao","descricao","valoroperacao","desctipocredito","numerocarteira","codigoexterno","nomeusuario"]
                df = pd.DataFrame(columns = columns)
                df = df.append(pd.Series(), ignore_index=True)
            columns = ["idpessoa","razaosocial","nomefantasia","cnpj","idoperador","login","nomeoperador","numerocaixa","dataaberturacaixa","datafechamentocaixa","numerooperacao","dataoperacao","descricao","valoroperacao","desctipocredito","numerocarteira","codigoexterno","nomeusuario"]
            df.columns = columns
            df = df[df['razaosocial'].notna()]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except:
            
            first_row = file.splitlines()[1].split(b"NOMEUSUARIO")[-1].decode("ISO-8859-1").split(",")
            columns = ["idpessoa","razaosocial","nomefantasia","cnpj","idoperador","login","nomeoperador","numerocaixa","dataaberturacaixa","datafechamentocaixa","numerooperacao","dataoperacao","descricao","valoroperacao","desctipocredito","numerocarteira","codigoexterno","nomeusuario"]
            df_first = pd.DataFrame([first_row],columns=columns)
            df = pd.read_csv(bufferB, encoding="ISO-8859-1", sep=",",dtype=str,skiprows=2, names=columns)
            df = df[df['razaosocial'].notna()]
            df = pd.concat([df_first, df], ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
