import boto3 
import base64
import pandas as pd

from io import BytesIO
from datetime import datetime
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

class FileReaderTest:
    @staticmethod
    def read(uri: str):
        origin = urlparse(uri, allow_fragments=False)
        if origin.scheme in ('s3', 's3a'):
            session = boto3.session.Session()
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = BytesIO(obj['Body'].read())
            return obj, lm
        else:
            with open(uri,"rb") as f:
                return BytesIO(f.read()),datetime.today()


class ExtractorNormal:
    @staticmethod
    def run(filename, **kwargs):
       
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #a= decode_base(file.encode())

        df = pd.read_csv(file,dtype=str,header=None)
        df.columns = ['ID_INPUT']
        for idx,valor in df.iterrows():
            mensaje = decode_base(valor['ID_INPUT'])
            df.loc[idx, 'ID_INPUT_CLEAN'] = mensaje
        for idx,valor in df.iterrows():
            id_input_clean = valor['ID_INPUT_CLEAN'].split('_')[-2]
            df.loc[idx, 'SKT_ID_A'] = id_input_clean
            id_input_clean = valor['ID_INPUT_CLEAN'].split('_')[-1]
            df.loc[idx, 'SKT_ID_B'] = id_input_clean
            report_name = ('_').join(valor['ID_INPUT_CLEAN'].split('_')[0:-2])
            df.loc[idx, 'REPORT_NAME'] = report_name
        return df


class ExtractorBolsa:
    @staticmethod
    def run(filename, **kwargs):
       
        file = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #a= decode_base(file.encode())

        df = pd.read_csv(file,dtype=str,header=None)
        df.columns = ['ID_INPUT']
        for idx,valor in df.iterrows():
            mensaje = decode_base(valor['ID_INPUT'])
            df.loc[idx, 'ID_INPUT_CLEAN'] = mensaje
        for idx,valor in df.iterrows():
            id_input_clean = valor['ID_INPUT_CLEAN'].split('_')[-1]
            df.loc[idx, 'BATCH_ID'] = id_input_clean
            report_name = ('_').join(valor['ID_INPUT_CLEAN'].split('_')[0:-1])
            df.loc[idx, 'REPORT_NAME'] = report_name
        print(df['REPORT_NAME'])
        return df

def decode_base(base64_message):
    base64_bytes = base64_message.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode('ascii')
    return message

class ExtractorTest:
    @staticmethod
    def run(filename, **kwargs):
        file, lm = FileReaderTest.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #a= decode_base(file.encode())
        if ".xlsx" in filename:
            df = pd.read_excel(file,dtype=str,header=None)
        else:
            df = pd.read_csv(file,dtype=str,header=None)
        df.columns = ['id_input']
        df['file_name'] = filename.split("/")[-1]
        return df

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        if ".xlsx" in filename:
            df = pd.read_excel(file,dtype=str)
        else:
            df = pd.read_csv(file,dtype=str)
        columns = ["file_name", "last_modified", "aws_key"]
        df.columns = columns
        return df